use super::*;
use crate::wal::codec::RecordKind;
use crate::wal::shard::{CommitterHandle, RecordLocation, Shard};
use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{LogState, RaftLogReader, Vote};
use std::ops::RangeBounds;
use std::sync::Mutex;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotRef {
    pub meta: SnapshotMeta<u64, BasicNode>,
    pub file: String,
    pub sha256: String,
}

#[derive(Debug, Serialize, Deserialize)]
enum Event {
    Entry(Entry),
    Vote(Vote<u64>),
    Commit(Option<LogId<u64>>),
    Truncate(LogId<u64>),
    Purge(LogId<u64>),
    Snapshot(SnapshotRef),
}

#[derive(Default)]
pub struct Index {
    entries: BTreeMap<u64, (LogId<u64>, RecordLocation)>,
    pub vote: Option<Vote<u64>>,
    pub committed: Option<LogId<u64>>,
    pub purged: Option<LogId<u64>>,
    pub snapshot: Option<SnapshotRef>,
}

impl Index {
    fn apply(&mut self, event: &Event, location: RecordLocation) -> io::Result<()> {
        match event {
            Event::Entry(entry) => {
                let next = self
                    .entries
                    .last_key_value()
                    .map(|(i, _)| i + 1)
                    .or_else(|| self.purged.map(|id| id.index + 1))
                    .unwrap_or(0);
                if entry.log_id.index != next {
                    return Err(io::Error::other("nonconsecutive Raft log append"));
                }
                self.entries
                    .insert(entry.log_id.index, (entry.log_id, location));
            }
            Event::Vote(vote) => self.vote = Some(*vote),
            Event::Commit(id) => {
                if *id < self.committed {
                    return Err(io::Error::other("commit regression"));
                }
                self.committed = *id;
            }
            Event::Truncate(id) => {
                if self.committed.is_some_and(|c| id.index <= c.index) {
                    return Err(io::Error::other("truncate committed entry"));
                }
                self.entries.split_off(&id.index);
            }
            Event::Purge(id) => {
                if self
                    .snapshot
                    .as_ref()
                    .and_then(|s| s.meta.last_log_id)
                    .is_none_or(|s| s.index < id.index)
                {
                    return Err(io::Error::other("purge without durable snapshot"));
                }
                self.entries = self.entries.split_off(&(id.index + 1));
                self.purged = Some(*id);
            }
            Event::Snapshot(snapshot) => self.snapshot = Some(snapshot.clone()),
        }
        Ok(())
    }
}

pub struct Journal {
    pub shard: Arc<Shard>,
    pub index: Mutex<Index>,
    snapshot_ready: tokio::sync::Notify,
    // Dropped last: stop + join the native fsync thread.
    _committer: CommitterHandle,
}

impl Journal {
    pub fn open(dir: PathBuf, segment_bytes: u64) -> io::Result<Arc<Self>> {
        let shard = Shard::open_with_segment_size(dir, segment_bytes)?;
        let mut index = Index::default();
        shard.resume_journal(|location, data| {
            let event: Event = bincode::deserialize(data).map_err(io::Error::other)?;
            index.apply(&event, location)
        })?;
        let committer = shard.spawn_committer();
        Ok(Arc::new(Self {
            shard,
            index: Mutex::new(index),
            snapshot_ready: tokio::sync::Notify::new(),
            _committer: committer,
        }))
    }

    fn stage(&self, event: Event) -> io::Result<u64> {
        let data = bincode::serialize(&event).map_err(io::Error::other)?;
        let mut index = self.index.lock().unwrap();
        let location = self
            .shard
            .reserve_and_stage_indexed(RecordKind::Raft, 0, 0, &data)?;
        // A failed index transition after a durable stage is fatal. Callers must
        // not continue on storage errors; startup also rejects the same history.
        index.apply(&event, location)?;
        Ok(location.lsn)
    }

    async fn persist(&self, event: Event) -> io::Result<()> {
        let lsn = self.stage(event)?;
        self.shard.wait_durable(lsn).await;
        Ok(())
    }

    pub async fn save_snapshot(&self, snapshot: SnapshotRef) -> io::Result<()> {
        self.persist(Event::Snapshot(snapshot)).await?;
        self.snapshot_ready.notify_waiters();
        Ok(())
    }

    pub fn read(&self, location: RecordLocation) -> io::Result<Entry> {
        match bincode::deserialize(&self.shard.read_record(location)?).map_err(io::Error::other)? {
            Event::Entry(entry) => Ok(entry),
            _ => Err(io::Error::other("index points at non-entry record")),
        }
    }
}

impl RaftLogReader<Types> for Arc<Journal> {
    async fn try_get_log_entries<R: RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
        &mut self,
        range: R,
    ) -> Result<Vec<Entry>, StorageError<u64>> {
        let locations: Vec<_> = self
            .index
            .lock()
            .unwrap()
            .entries
            .range(range)
            .map(|(_, (_, p))| *p)
            .collect();
        locations
            .into_iter()
            .map(|p| self.read(p).map_err(storage_error))
            .collect()
    }
}

impl RaftLogStorage<Types> for Arc<Journal> {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<Types>, StorageError<u64>> {
        let index = self.index.lock().unwrap();
        Ok(LogState {
            last_purged_log_id: index.purged,
            last_log_id: index
                .entries
                .last_key_value()
                .map(|(_, (id, _))| *id)
                .or(index.purged),
        })
    }
    async fn get_log_reader(&mut self) -> Self {
        self.clone()
    }
    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.index.lock().unwrap().vote)
    }
    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.persist(Event::Vote(*vote))
            .await
            .map_err(storage_error)
    }
    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(self.index.lock().unwrap().committed)
    }
    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        self.persist(Event::Commit(committed))
            .await
            .map_err(storage_error)
    }
    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<Types>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry> + Send,
        I::IntoIter: Send,
    {
        let mut last = None;
        for entry in entries {
            last = Some(self.stage(Event::Entry(entry)).map_err(storage_error)?);
        }
        let shard = self.shard.clone();
        tokio::spawn(async move {
            if let Some(lsn) = last {
                shard.wait_durable(lsn).await;
            }
            callback.log_io_completed(Ok(()));
        });
        Ok(())
    }
    async fn truncate(&mut self, id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.persist(Event::Truncate(id))
            .await
            .map_err(storage_error)
    }
    async fn purge(&mut self, id: LogId<u64>) -> Result<(), StorageError<u64>> {
        // OpenRaft 0.9.25 dispatches installation to its separate SM worker,
        // then invokes purge without waiting for that worker. This barrier is
        // essential on a fresh learner: never persist a purge before the
        // received snapshot's durable reference. Register BEFORE checking.
        loop {
            let ready = self.snapshot_ready.notified();
            tokio::pin!(ready);
            ready.as_mut().enable();
            let covered = self
                .index
                .lock()
                .unwrap()
                .snapshot
                .as_ref()
                .and_then(|s| s.meta.last_log_id)
                .is_some_and(|s| s.index >= id.index);
            if covered {
                break;
            }
            ready.await;
        }
        // Logical reclamation only. Native single-node checkpoint cannot know
        // about consensus votes/commit/truncation and MUST NOT unlink this WAL.
        self.persist(Event::Purge(id)).await.map_err(storage_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::{storage::RaftStateMachine, RaftSnapshotBuilder};
    use proptest::prelude::*;

    async fn issue(
        machine: &mut Arc<machine::Machine>,
        method: &str,
        path: &str,
        time: u64,
        headers: Vec<(&str, String)>,
        body: Vec<u8>,
    ) -> Reply {
        let index = machine
            .view
            .read()
            .await
            .applied
            .map(|id| id.index + 1)
            .unwrap_or(0);
        let entry = Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(Command {
                method: method.into(),
                path: path.into(),
                headers: headers.into_iter().map(|(k, v)| (k.into(), v)).collect(),
                body,
                time,
            }),
        };
        machine
            .journal
            .persist(Event::Entry(entry.clone()))
            .await
            .unwrap();
        machine
            .journal
            .persist(Event::Commit(Some(entry.log_id)))
            .await
            .unwrap();
        machine.apply([entry]).await.unwrap().pop().unwrap()
    }

    async fn bytes(machine: &Arc<machine::Machine>, path: &str) -> (u16, Vec<u8>) {
        use std::os::unix::fs::FileExt;
        let view = machine.view.read().await;
        let response = crate::handlers::handle(
            view.store.clone(),
            Req {
                method: Method::Get,
                path: path.into(),
                query: Some("offset=-1".into()),
                headers: vec![],
                body: vec![].into(),
            },
        )
        .await;
        let data = match response.body {
            Body::Full(b) => b.to_vec(),
            Body::FileRange {
                segments,
                prefix,
                suffix,
                ..
            } => {
                let mut data = prefix.to_vec();
                for segment in segments {
                    let begin = data.len();
                    data.resize(begin + segment.len as usize, 0);
                    segment
                        .file
                        .read_exact_at(&mut data[begin..], segment.file_start)
                        .unwrap();
                }
                data.extend_from_slice(suffix);
                data
            }
            Body::Empty => vec![],
            _ => panic!("unexpected native read body"),
        };
        (response.status, data)
    }

    async fn control(
        machine: &mut Arc<machine::Machine>,
        action: subscriptions::Action,
        time: u64,
    ) -> Reply {
        issue(
            machine,
            "SUB",
            "/r/__ds/subscriptions/test",
            time,
            vec![],
            serde_json::to_vec(&action).unwrap(),
        )
        .await
    }

    async fn fork_control(
        machine: &mut Arc<machine::Machine>,
        path: &str,
        action: forks::Action,
    ) -> Reply {
        issue(
            machine,
            "FORK",
            path,
            1005,
            vec![],
            bincode::serialize(&action).unwrap(),
        )
        .await
    }

    async fn reopen(mut machine: Arc<machine::Machine>, snapshot: bool) -> Arc<machine::Machine> {
        if snapshot {
            machine.build_snapshot().await.unwrap();
        }
        let dir = machine.dir.clone();
        drop(machine);
        let journal = Journal::open(dir.parent().unwrap().join("wal"), 256 * 1024).unwrap();
        machine::Machine::open(dir, journal).await.unwrap()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(48))]
        #[test]
        fn remote_fork_native_import_retries_and_descendant_retention_survive_restart(
            len in 1usize..150000, cut_seed in 0usize..150001,
            checkpoint in 0u8..5, salt in any::<u8>(),
        ) {
            use forks::{Action, Decision};
            let _mode = crate::handlers::test_support::DurabilityGuard::memory();
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let dir = tempfile::tempdir().unwrap();
                let mut source = machine::Machine::open(dir.path().join("s/state"),
                    Journal::open(dir.path().join("s/wal"),256*1024).unwrap()).await.unwrap();
                let mut target = machine::Machine::open(dir.path().join("d/state"),
                    Journal::open(dir.path().join("d/wal"),256*1024).unwrap()).await.unwrap();
                let data: Vec<u8> = (0..len).map(|i| (i as u8).wrapping_mul(31).wrapping_add(salt)).collect();
                let cut = cut_seed % (len+1);
                assert_eq!(issue(&mut source,"PUT","/source",1000,vec![],data.clone()).await.status,201);
                let headers = vec![("stream-forked-from".into(),"/source".into()),
                    ("stream-fork-offset".into(),crate::store::format_offset(cut as u64)),
                    ("stream-ttl".into(),"2".into())];
                let prepared = crate::handlers::prepare_create(&source.view.read().await.store,
                    &Req { method:Method::Put,path:"/child".into(),query:None,headers:headers.clone(),body:Default::default() })
                    .await.ok().unwrap();
                let config = prepared.config;
                drop(prepared.parent);
                let reserve = Action::Reserve {group:1,source_group:0,source_id:0,config:config.clone(),
                    headers:headers.clone(),wire:vec![249,11,73]};
                let first = fork_control(&mut target,"/child",reserve.clone()).await;
                assert_eq!(first.status,202);
                let tx: String = serde_json::from_slice(&first.body).unwrap();
                assert_eq!(fork_control(&mut target,"/child",reserve).await.body,first.body);
                assert_eq!(issue(&mut target,"PUT","/child",1000,vec![],vec![9]).await.status,409);
                if checkpoint == 0 { target = reopen(target,true).await; }
                assert!(target.view.read().await.forks.pending("/child"));
                let grant = Action::Grant {tx:tx.clone(),source_id:0,headers,expected:config};
                let granted = fork_control(&mut source,"/source",grant.clone()).await;
                let decision: Decision = serde_json::from_slice(&granted.body).unwrap();
                assert!(matches!(decision, Decision::Granted(_)));
                if checkpoint == 1 { source = reopen(source,true).await; }
                assert_eq!(fork_control(&mut source,"/source",grant.clone()).await.body,granted.body);
                assert_eq!(source.view.read().await.store.streams.get("/source").unwrap().shared.read().unwrap().ref_count,1);
                // A later append must not extend the granted prefix; DELETE
                // must retain that prefix even before the destination imports.
                assert_eq!(issue(&mut source,"POST","/source",1005,
                    vec![("content-type","application/octet-stream".into())],vec![61,92]).await.status,204);
                assert_eq!(issue(&mut source,"DELETE","/source",1005,vec![],vec![]).await.status,204);
                assert_eq!(bytes(&source,"/source").await.0,410);
                assert_eq!(fork_control(&mut target,"/child",Action::Accept {tx:tx.clone(),decision}).await.status,204);
                if cut > 1 {
                    assert_eq!(fork_control(&mut target,"/child",Action::Chunk {tx:tx.clone(),start:1,bytes:vec![data[1]]}).await.status,409);
                }
                if cut > 0 {
                    assert_eq!(fork_control(&mut target,"/child",Action::Publish {tx:tx.clone()}).await.status,409);
                }
                if checkpoint == 2 { target = reopen(target,true).await; }
                for (i, chunk) in data[..cut].chunks(forks::CHUNK).enumerate() {
                    let start = i * forks::CHUNK;
                    let src = source.view.read().await.store.streams.get("/source").unwrap().clone();
                    let wire = crate::handlers::read_range_bytes(&src,start as u64,(start+chunk.len()) as u64).await.unwrap();
                    assert_eq!(wire.as_ref(),chunk);
                    let action = Action::Chunk {tx:tx.clone(),start:start as u64,bytes:wire.to_vec()};
                    assert_eq!(fork_control(&mut target,"/child",action.clone()).await.status,204);
                    if checkpoint == 3 { target = reopen(target,true).await; }
                    assert_eq!(fork_control(&mut target,"/child",action).await.status,204);
                }
                assert_eq!(fork_control(&mut target,"/child",Action::Publish {tx:tx.clone()}).await.status,201);
                if checkpoint == 4 { target = reopen(target,true).await; }
                source = reopen(source,false).await;
                target = reopen(target,false).await;
                let mut expected = data[..cut].to_vec(); expected.extend_from_slice(&[249,11,73]);
                assert_eq!(bytes(&target,"/child").await,(200,expected.clone()));
                assert_eq!(clock::millis(target.view.read().await.store.streams.get("/child").unwrap().shared.read().unwrap().last_access),1005);
                assert_eq!(issue(&mut target,"PUT","/desc",1006,
                    vec![("stream-forked-from","/child".into())],vec![177]).await.status,201);
                assert_eq!(issue(&mut target,"DELETE","/child",1006,vec![],vec![]).await.status,204);
                assert_eq!(fork_control(&mut target,"/child",Action::Retire {tx:tx.clone()}).await.status,409);
                target = reopen(target,true).await;
                expected.push(177);
                assert_eq!(bytes(&target,"/desc").await,(200,expected));
                assert_eq!(bytes(&source,"/source").await.0,410);
                assert_eq!(issue(&mut target,"DELETE","/desc",1007,vec![],vec![]).await.status,204);
                assert_eq!(fork_control(&mut target,"/child",Action::Retire {tx:tx.clone()}).await.status,204);
                for _ in 0..2 {
                    assert_eq!(fork_control(&mut source,"/source",Action::Release {tx:tx.clone()}).await.status,204);
                    source = reopen(source,true).await;
                    assert_eq!(bytes(&source,"/source").await.0,404);
                }
                // A delayed duplicate grant cannot resurrect a terminal pin.
                let late = fork_control(&mut source,"/source",grant).await;
                assert!(matches!(serde_json::from_slice::<Decision>(&late.body).unwrap(),Decision::Released));
                assert_eq!(fork_control(&mut target,"/child",Action::Released {tx}).await.status,204);
                assert_eq!(issue(&mut source,"PUT","/source",1008,vec![],vec![85]).await.status,201);
                assert_eq!(bytes(&source,"/source").await,(200,vec![85]));
            });
        }

        #[test]
        fn subscription_intent_snapshot_and_native_replay_preserve_exact_ack_cut(
            chunks in prop::collection::vec(1usize..50, 2..12), checkpoint in 0usize..12,
        ) {
            use subscriptions::{Action, Observation};
            use serde_json::json;
            let _mode = crate::handlers::test_support::DurabilityGuard::memory();
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let dir = tempfile::tempdir().unwrap();
                let journal = Journal::open(dir.path().join("wal"), 4096).unwrap();
                let mut machine = machine::Machine::open(dir.path().join("state"), journal).await.unwrap();
                let path = "/r/__ds/subscriptions/test";
                let observe = |tail,index| Observation {path:"/r/data/a".into(),group:0,index,incarnation:Some(0),tail};
                assert_eq!(issue(&mut machine,"PUT","/r/data/a",1000,vec![],vec![71;7]).await.status,201);
                assert_eq!(control(&mut machine,Action::Keys(subscriptions::Keys::generate().unwrap()),1001).await.status,204);
                let config = serde_json::from_value(json!({"type":"webhook","pattern":"data/*","webhook":{"url":"http://localhost/hook"}})).unwrap();
                assert_eq!(control(&mut machine,Action::Create {config,root:"/r/".into(),base_url:"http://localhost/r/".into(),observations:vec![observe(7,1)]},1002).await.status,201);
                let keys = machine.view.read().await.subscriptions.keys.as_ref().unwrap().jwk();
                let mut total = 7;
                let mut expected_bytes = vec![71;7];
                let mut attempt = 0;
                let mut envelope = serde_json::Value::Null;
                for (i,n) in chunks.iter().enumerate() {
                    assert_eq!(issue(&mut machine,"POST","/r/data/a",1010+i as u64,
                        vec![("content-type","application/octet-stream".into())],vec![i as u8;*n]).await.status,204);
                    expected_bytes.extend_from_slice(&vec![i as u8;*n]);
                    total += *n as u64;
                    let at = machine.view.read().await.applied.unwrap().index;
                    assert_eq!(control(&mut machine,Action::Observe {targets:BTreeMap::from([(path.into(),2)]),observations:vec![observe(total,at)]},1020+i as u64).await.status,204);
                    if i == 0 {
                        let reserved = control(&mut machine,Action::Reserve {incarnation:2,generation:1},1030).await;
                        assert_eq!(reserved.status,200);
                        envelope = serde_json::from_slice(&reserved.body).unwrap();
                        attempt = machine.view.read().await.applied.unwrap().index;
                    }
                    if i == checkpoint % chunks.len() {
                        machine.build_snapshot().await.unwrap();
                        drop(machine);
                        let journal = Journal::open(dir.path().join("wal"),4096).unwrap();
                        machine = machine::Machine::open(dir.path().join("state"),journal).await.unwrap();
                        assert_eq!(machine.view.read().await.subscriptions.keys.as_ref().unwrap().jwk(),keys);
                    }
                }
                // Completion is a committed suffix after the snapshot. It must
                // consume the issued cut, not all bytes appended while in flight.
                assert_eq!(control(&mut machine,Action::Delivered {incarnation:2,generation:1,attempt,ok:true,done:true,jitter:0},1100).await.status,204);
                drop(machine);
                let journal = Journal::open(dir.path().join("wal"),4096).unwrap();
                let mut machine = machine::Machine::open(dir.path().join("state"),journal).await.unwrap();
                assert_eq!(bytes(&machine,"/r/data/a").await,(200,expected_bytes));
                {
                    let view = machine.view.read().await;
                    let sub = &view.subscriptions.subscriptions[path];
                    assert_eq!(sub.links["data/a"].acked,7+chunks[0] as u64);
                    assert_eq!(sub.links["data/a"].observation.tail,total);
                    assert_eq!(sub.generation,2);
                    assert!(sub.pending());
                }
                let request = Action::Request {incarnation:2,operation:"callback".into(),
                    token:envelope["callback_token"].as_str().unwrap().into(),observations:vec![],
                    body:json!({"generation":1,"wake_id":envelope["wake_id"],"done":true,
                        "acks":[{"stream":"data/a","offset":crate::store::format_offset(total)}]})};
                assert_eq!(control(&mut machine,request,1101).await.status,409);
                let view = machine.view.read().await;
                assert_eq!(view.subscriptions.subscriptions[path].links["data/a"].acked,7+chunks[0] as u64);
            });
        }

        #[test]
        fn native_fork_dedup_and_millisecond_expiry_survive_snapshot_and_replay(
            a in prop::collection::vec(any::<u8>(), 1..35),
            b in prop::collection::vec(any::<u8>(), 1..27),
            split in 0usize..100, time in 1001u64..1999,
        ) {
            let _mode = crate::handlers::test_support::DurabilityGuard::memory();
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let dir = tempfile::tempdir().unwrap();
                let mut expected = a.clone(); expected.extend_from_slice(&b);
                let cut = split % (expected.len() + 1);
                let journal = Journal::open(dir.path().join("wal"), 4096).unwrap();
                let mut machine = machine::Machine::open(dir.path().join("state"), journal.clone()).await.unwrap();
                assert_eq!(issue(&mut machine, "PUT", "/source", time, vec![("stream-ttl", "1".into())], a).await.status, 201);
                let producer = || vec![("content-type", "application/octet-stream".into()),
                    ("producer-id", "p".into()), ("producer-epoch", "3".into()), ("producer-seq", "0".into())];
                assert_eq!(issue(&mut machine, "POST", "/source", time+10, producer(), b.clone()).await.status, 200);
                assert_eq!(issue(&mut machine, "POST", "/source", time+20, producer(), b).await.status, 204);
                assert_eq!(issue(&mut machine, "PUT", "/fork", time+30,
                    vec![("stream-forked-from", "/source".into()), ("stream-fork-offset", format!("0000000000000000_{cut:016}")),
                         ("stream-ttl", "2".into())], vec![241, 17, 94]).await.status, 201);
                let mut fork = expected[..cut].to_vec(); fork.extend_from_slice(&[241, 17, 94]);
                machine.build_snapshot().await.unwrap();
                drop(machine); drop(journal);
                let journal = Journal::open(dir.path().join("wal"), 4096).unwrap();
                let mut machine = machine::Machine::open(dir.path().join("state"), journal).await.unwrap();
                assert_eq!(bytes(&machine, "/source").await, (200, expected));
                assert_eq!(bytes(&machine, "/fork").await, (200, fork.clone()));
                issue(&mut machine, "TICK", "/source", time+1010, vec![], vec![]).await;
                assert_eq!(bytes(&machine, "/source").await.0, 200); // exact TTL edge is live
                issue(&mut machine, "TICK", "/source", time+1011, vec![], vec![]).await;
                assert_eq!(bytes(&machine, "/source").await.0, 410); // parent retained by fork
                drop(machine);
                let journal = Journal::open(dir.path().join("wal"), 4096).unwrap();
                let machine = machine::Machine::open(dir.path().join("state"), journal).await.unwrap();
                assert_eq!(bytes(&machine, "/source").await.0, 410);
                assert_eq!(bytes(&machine, "/fork").await, (200, fork));
            });
        }

        #[test]
        fn native_wal_suffix_replacement_survives_two_restarts(
            sizes in prop::collection::vec(1usize..500, 2..24), split in 0usize..24,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let dir = tempfile::tempdir().unwrap();
                let keep = split % sizes.len();
                let make = |i, n, term| Entry {
                    log_id: LogId::new(openraft::CommittedLeaderId::new(term, 1), i),
                    payload: EntryPayload::Normal(Command { method: "POST".into(), path: "/p".into(),
                        headers: vec![], body: vec![i as u8 + term as u8; n], time: 0 }),
                };
                let journal = Journal::open(dir.path().into(), 1024).unwrap();
                for (i, n) in sizes.iter().enumerate() {
                    journal.persist(Event::Entry(make(i as u64, *n, 1))).await.unwrap();
                }
                journal.persist(Event::Truncate(LogId::new(openraft::CommittedLeaderId::new(1, 1), keep as u64))).await.unwrap();
                journal.persist(Event::Entry(make(keep as u64, 31, 2))).await.unwrap();
                drop(journal);
                for _ in 0..2 {
                    let mut journal = Journal::open(dir.path().into(), 1024).unwrap();
                    let entries = journal.try_get_log_entries(0..=keep as u64).await.unwrap();
                    assert_eq!(entries.len(), keep + 1);
                    for (i, entry) in entries.iter().enumerate() {
                        let expected = if i == keep { vec![i as u8 + 2; 31] } else { vec![i as u8 + 1; sizes[i]] };
                        let EntryPayload::Normal(command) = &entry.payload else { panic!("missing command") };
                        assert_eq!(command.body, expected);
                    }
                }
            });
        }
    }
}
