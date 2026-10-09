use super::*;
use crate::wal::codec::RecordKind;
use crate::wal::shard::{CommitterHandle, RecordLocation, Shard};
use futures_util::{stream, Stream, StreamExt};
use openraft::storage::{IOFlushed, RaftLogStorage};
use openraft::{LogState, RaftLogReader};
use sha2::{Digest, Sha256};
use std::io::Write;
use std::ops::{Bound, RangeBounds};
use std::sync::Mutex;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotRef {
    pub meta: SnapshotMeta,
    pub file: String,
    pub sha256: String,
}

#[derive(Debug, Serialize, Deserialize)]
enum Event {
    Entry(Entry),
    Vote(Vote),
    Commit(Option<LogId>),
    Truncate(LogId),
    Purge(LogId),
    Snapshot(SnapshotRef),
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Index {
    entries: BTreeMap<u64, (LogId, RecordLocation)>,
    pub vote: Option<Vote>,
    pub committed: Option<LogId>,
    pub purged: Option<LogId>,
    pub snapshot: Option<SnapshotRef>,
    last_record: Option<RecordLocation>,
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
        self.last_record = Some(location);
        Ok(())
    }
}

pub struct Journal {
    pub shard: Arc<Shard>,
    pub index: Mutex<Index>,
    pub changed: tokio::sync::Notify,
    dir: PathBuf,
    maintenance: tokio::sync::Mutex<()>,
    compact_pending: tokio::sync::Notify,
    snapshot_ready: tokio::sync::Notify,
    // Dropped last: stop + join the native fsync thread.
    _committer: CommitterHandle,
}

impl Journal {
    pub fn open(dir: PathBuf, segment_bytes: u64) -> io::Result<Arc<Self>> {
        let mut index = match std::fs::read(dir.join("journal-checkpoint")) {
            Ok(data) => {
                if data.len() < 40 || &data[..8] != b"ERJC0001"
                    || Sha256::digest(&data[40..])[..] != data[8..40]
                {
                    return Err(io::Error::other("journal checkpoint checksum/version"));
                }
                bincode::deserialize::<Index>(&data[40..]).map_err(io::Error::other)?
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Index::default(),
            Err(e) => return Err(e),
        };
        let shard = Shard::open_with_segment_size(dir.clone(), segment_bytes)?;
        for (id, location) in index.entries.values() {
            match bincode::deserialize(&shard.read_record(*location)?).map_err(io::Error::other)? {
                Event::Entry(entry) if entry.log_id == *id => (),
                _ => return Err(io::Error::other("checkpoint retained entry mismatch")),
            }
        }
        shard.resume_journal(index.last_record, |location, data| {
            let event: Event = bincode::deserialize(data).map_err(io::Error::other)?;
            index.apply(&event, location)
        })?;
        let committer = shard.spawn_committer();
        Ok(Arc::new(Self {
            shard,
            index: Mutex::new(index),
            changed: tokio::sync::Notify::new(),
            dir,
            maintenance: tokio::sync::Mutex::new(()),
            compact_pending: tokio::sync::Notify::new(),
            snapshot_ready: tokio::sync::Notify::new(),
            _committer: committer,
        }))
    }

    /// One worker per group, owned by the server lifecycle, not one task per
    /// purge. A stored Notify permit coalesces work arriving during filesystem
    /// I/O; consume it before capture, never after completing the old work.
    pub async fn maintain(&self) -> io::Result<()> {
        self.compact_pending.notify_one(); // Recover a notification lost at crash.
        loop {
            self.compact_pending.notified().await;
            self.compact().await.map_err(storage_error)?;
        }
    }

    async fn compact(&self) -> io::Result<()> {
        // Serialize captures as well as publication; otherwise an older capture
        // could overwrite a newer checkpoint after its segments were reclaimed.
        let _timer = timing::JOURNAL_COMPACT.start();
        let _maintenance = self.maintenance.lock().await;
        let captured = self.index.lock().unwrap().clone();
        let Some(cut) = captured.last_record else { return Ok(()) };
        self.shard.wait_durable(cut.lsn).await;
        // Keep maintenance serialization and all durability barriers while
        // handing off the worker that drives Raft RPCs, timers and publication.
        tokio::task::block_in_place(|| {
            let data = bincode::serialize(&captured).map_err(io::Error::other)?;
            let temporary = self.dir.join("journal-checkpoint-next");
            let mut file = std::fs::File::create(&temporary)?;
            file.write_all(b"ERJC0001")?;
            file.write_all(&Sha256::digest(&data))?;
            file.write_all(&data)?;
            file.sync_all()?;
            std::fs::rename(&temporary, self.dir.join("journal-checkpoint"))?;
            crate::store::fsync_parent_dir(&temporary)?;

            let floor = captured.entries.values()
                .map(|(_, p)| p.segment).min().unwrap_or(cut.segment).min(cut.segment);
            // Capture joined earlier indexed readers. Later readers need only
            // retained entries or frames at/after the cut, so filesystem I/O
            // need not hold the index mutex against new reads and appends.
            for entry in std::fs::read_dir(&self.dir)? {
                let path = entry?.path();
                if path.extension().and_then(|s| s.to_str()) == Some("wal") {
                    let start = path.file_stem().unwrap().to_str().unwrap()
                        .parse::<u64>().map_err(io::Error::other)?;
                    if start < floor { std::fs::remove_file(path)?; }
                }
            }
            crate::store::fsync_parent_dir(&temporary)?;
            Ok(())
        })
    }

    fn stage(&self, event: Event) -> io::Result<u64> {
        let mut probe = match &event {
            Event::Entry(_) => timing::ENTRY_STAGE.start(),
            Event::Commit(_) => timing::MARKER_STAGE.start(),
            _ => timing::OTHER_STAGE.start(),
        };
        let data = bincode::serialize(&event).map_err(io::Error::other)?;
        if let Some(probe) = &mut probe { probe.bytes = data.len() as u64; }
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
        let marker = matches!(event, Event::Commit(_));
        let lsn = self.stage(event)?;
        let _probe = if marker { timing::MARKER_WAIT.start() } else { timing::OTHER_WAIT.start() };
        self.shard.wait_durable(lsn).await;
        self.changed.notify_waiters();
        Ok(())
    }

    pub fn id_at(&self, index: u64) -> Option<LogId> {
        self.index.lock().unwrap().entries.get(&index).map(|(id, _)| *id)
    }

    pub fn uncommitted(&self) -> Vec<LogId> {
        let index = self.index.lock().unwrap();
        let from = index.committed.map_or(0, |id| id.index + 1);
        index.entries.range(from..).map(|(_, (id, _))| *id).collect()
    }

    /// Called only by the serialized apply worker before native publication.
    /// Private startup replay can cover this batch with an already durable
    /// marker; never regress it when replaying in smaller chunks.
    pub async fn cover_apply(&self, id: LogId) -> io::Result<()> {
        let covered = self.index.lock().unwrap().committed.is_some_and(|old| old >= id);
        if !covered {
            self.persist(Event::Commit(Some(id))).await?;
        }
        Ok(())
    }

    pub async fn save_snapshot(&self, snapshot: SnapshotRef) -> io::Result<()> {
        self.persist(Event::Snapshot(snapshot)).await?;
        self.snapshot_ready.notify_waiters();
        Ok(())
    }

    pub fn read(&self, location: RecordLocation) -> io::Result<Entry> {
        let mut probe = timing::READ.start();
        let data = self.shard.read_record(location)?;
        if let Some(probe) = &mut probe { probe.bytes = data.len() as u64; }
        match bincode::deserialize(&data).map_err(io::Error::other)? {
            Event::Entry(entry) => Ok(entry),
            _ => Err(io::Error::other("index points at non-entry record")),
        }
    }
}

impl RaftLogReader<Types> for Arc<Journal> {
    async fn try_get_log_entries<R: RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
        &mut self,
        range: R,
    ) -> io::Result<Vec<Entry>> {
        let index = self.index.lock().unwrap();
        index.entries
            .range(range)
            .map(|(_, (_, p))| self.read(*p).map_err(storage_error))
            .collect()
    }

    async fn read_vote(&mut self) -> io::Result<Option<Vote>> {
        Ok(self.index.lock().unwrap().vote)
    }

    async fn limited_get_log_entries(&mut self, start: u64, end: u64) -> io::Result<Vec<Entry>> {
        self.try_get_log_entries(start..end.min(start.saturating_add(64))).await
    }

    async fn entries_stream<R>(&mut self, range: R) -> impl Stream<Item = io::Result<Entry>> + Send
    where R: RangeBounds<u64> + Clone + std::fmt::Debug + Send {
        // The SM worker uses this for committed replay. It cannot be purged
        // ahead of apply: a covering durable snapshot is required first. Capture
        // the upper bound once; concurrent appends must not extend this stream.
        let bounds = {
            let index = self.index.lock().unwrap();
            let mut entries = index.entries.range(range);
            entries.next().map(|(first, _)| (*first,
                entries.next_back().map_or(*first, |(last, _)| *last) + 1))
        };
        stream::unfold((self.clone(), bounds), |(mut reader, bounds)| async move {
            let (start, end) = bounds?;
            let next = start.saturating_add(64).min(end);
            let entries = match reader.try_get_log_entries(start..next).await {
                Ok(entries) if entries.len() as u64 == next-start => entries.into_iter().map(Ok).collect(),
                Ok(_) => vec![Err(storage_error(io::Error::other("committed stream log gap")))],
                Err(error) => vec![Err(error)],
            };
            let more = (next < end).then_some((next, end));
            Some((stream::iter(entries), (reader, more)))
        }).flatten()
    }
}

impl RaftLogStorage<Types> for Arc<Journal> {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> io::Result<LogState<Types>> {
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
    async fn save_vote(&mut self, vote: &Vote) -> io::Result<()> {
        self.persist(Event::Vote(*vote))
            .await
            .map_err(storage_error)
    }
    // Both optional committed methods retain their default no-op/None contract.
    // The SM's pre-publication barrier and private startup replay own our marker.
    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<Types>,
    ) -> io::Result<()>
    where
        I: IntoIterator<Item = Entry> + Send,
        I::IntoIter: Send,
    {
        let mut last = None;
        let mut accepted = Vec::new();
        for entry in entries {
            if let EntryPayload::Normal(batch) = &entry.payload {
                if let Some(durable) = &batch.durable {
                    accepted.push((entry.log_id, durable.clone()));
                }
            }
            last = Some(self.stage(Event::Entry(entry)).map_err(storage_error)?);
        }
        let shard = self.shard.clone();
        let journal = self.clone();
        let probe = timing::ENTRY_WAIT.start();
        tokio::spawn(async move {
            if let Some(lsn) = last {
                shard.wait_durable(lsn).await;
            }
            drop(probe);
            for (id, durable) in accepted {
                if let Some(send) = durable.lock().unwrap().take() { let _ = send.send(id); }
            }
            callback.io_completed(Ok(()));
            journal.changed.notify_waiters();
        });
        Ok(())
    }
    async fn truncate_after(&mut self, last: Option<LogId>) -> io::Result<()> {
        // The on-disk event is inclusive; the 0.10 API keeps `last` itself.
        // None removes the complete retained suffix, not the purged prefix.
        let first = {
            let index = self.index.lock().unwrap();
            index.entries.range((last.map_or(Bound::Unbounded, |id| Bound::Excluded(id.index)), Bound::Unbounded))
                .next().map(|(_, (id, _))| *id)
        };
        if let Some(id) = first {
            self.persist(Event::Truncate(id)).await.map_err(storage_error)?;
        }
        Ok(())
    }
    async fn purge(&mut self, id: LogId) -> io::Result<()> {
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
        // The native single-node checkpoint cannot know about consensus
        // metadata. Our complete durable index, not Purge alone, permits unlink.
        self.persist(Event::Purge(id)).await.map_err(storage_error)?;
        // Raft core awaits purge. Logical deletion is already durable; physical
        // GC must not stop this group's heartbeats and request processing.
        self.compact_pending.notify_one();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::{storage::RaftStateMachine, RaftSnapshotBuilder};
    use openraft::vote::RaftLeaderId;
    use proptest::prelude::*;

    async fn issue(
        machine: &mut Arc<machine::Machine>,
        method: &str,
        path: &str,
        time: u64,
        headers: Vec<(&str, String)>,
        body: Vec<u8>,
    ) -> Reply {
        issue_batch(machine, vec![Command {
            method: method.into(), path: path.into(),
            headers: headers.into_iter().map(|(k, v)| (k.into(), v)).collect(),
            body, time,
        }]).await.pop().unwrap()
    }

    async fn issue_batch(machine: &mut Arc<machine::Machine>, commands: Vec<Command>) -> Vec<Reply> {
        let index = machine
            .view
            .read()
            .await
            .applied
            .map(|id| id.index + 1)
            .unwrap_or(0);
        let entry = Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(commands.into()),
        };
        machine
            .journal
            .persist(Event::Entry(entry.clone()))
            .await
            .unwrap();
        machine.apply_committed([entry]).await.unwrap().pop().unwrap()
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

    #[tokio::test(flavor = "multi_thread")]
    async fn fork_backlog_bounds_reclaim_without_forgetting_pending_work() {
        use forks::{Action, Decision, MAX_RESULTS, MAX_TRANSACTIONS};
        let _mode = crate::handlers::test_support::DurabilityGuard::memory();
        let dir = tempfile::tempdir().unwrap();
        let mut source = machine::Machine::open(dir.path().join("s/state"),
            Journal::open(dir.path().join("s/wal"),256*1024).unwrap()).await.unwrap();
        let mut target = machine::Machine::open(dir.path().join("d/state"),
            Journal::open(dir.path().join("d/wal"),256*1024).unwrap()).await.unwrap();
        assert_eq!(issue(&mut source,"PUT","/source",1000,vec![],vec![]).await.status,201);
        let headers = vec![("stream-forked-from".into(),"/source".into())];
        let config = crate::handlers::prepare_create(&source.view.read().await.store,
            &Req {method:Method::Put,path:"/child".into(),query:None,headers:headers.clone(),body:Default::default()})
            .await.ok().unwrap().config;
        let reserve = Action::Reserve {group:1,source_group:0,source_id:0,
            config:config.clone(),headers:headers.clone(),wire:vec![]};
        let grant = |tx:String|Action::Grant {tx,source_id:0,headers:headers.clone(),expected:config.clone()};
        let first = fork_control(&mut target,"/pending",reserve.clone()).await;
        let pending: String = serde_json::from_slice(&first.body).unwrap();
        // Model a source disappearing after validation but before its Grant.
        assert_eq!(issue(&mut source,"DELETE","/source",1001,vec![],vec![]).await.status,204);
        let mut ids = Vec::new();
        for i in 0..MAX_TRANSACTIONS-1 {
            let path = format!("/aborted/{i}");
            let reserved = fork_control(&mut target,&path,reserve.clone()).await;
            assert_eq!(reserved.status,202);
            let tx: String = serde_json::from_slice(&reserved.body).unwrap();
            let decision = fork_control(&mut source,"/source",grant(tx.clone())).await;
            assert_eq!(decision.status,200);
            let decision: Decision = serde_json::from_slice(&decision.body).unwrap();
            assert!(matches!(decision,Decision::Aborted(_)));
            assert_eq!(fork_control(&mut target,&path,Action::Accept {tx:tx.clone(),decision}).await.status,204);
            ids.push(tx);
        }
        // Fill source independently; the pending destination is an active hole.
        assert_eq!(fork_control(&mut source,"/source",grant(pending.clone())).await.status,200);
        assert_eq!(fork_control(&mut target,"/over-limit",reserve.clone()).await.status,429);
        assert_eq!(fork_control(&mut source,"/source",grant("1:1000000".into())).await.status,429);
        assert_eq!(fork_control(&mut source,"/source",grant(ids[0].clone())).await.status,200,
            "duplicate decisions remain resolvable at the bound");
        source = reopen(source,true).await;
        target = reopen(target,true).await;
        let (source_group,start,end) = {
            let view = target.view.read().await;
            view.forks.certificate(&ids[0],view.applied.unwrap().index).unwrap()
        };
        assert!(start > pending.split_once(':').unwrap().1.parse::<u64>().unwrap());
        assert_eq!(fork_control(&mut source,"",Action::Compact {destination_group:1,start,end}).await.status,204);
        source = reopen(source,true).await; // A lost ACK cannot drop the destination backlog.
        assert_eq!(target.view.read().await.forks.destinations.len(),MAX_TRANSACTIONS);
        assert_eq!(fork_control(&mut source,"",Action::Compact {destination_group:1,start,end}).await.status,204);
        assert_eq!(fork_control(&mut target,"",Action::Collect {source_group,start,end}).await.status,204);
        target = reopen(target,true).await;
        {
            let view = target.view.read().await;
            assert!(view.forks.pending("/pending"));
            assert_eq!(view.forks.destinations.len(),1);
            assert_eq!(view.forks.results.len(),MAX_RESULTS);
            for (i,tx) in ids.iter().enumerate() {
                assert_eq!(view.forks.result(tx).map(|r|r.status),
                    (i >= ids.len()-MAX_RESULTS).then_some(404));
            }
        }
        assert_eq!(fork_control(&mut target,"/new",reserve).await.status,202);
        assert_eq!(fork_control(&mut source,"/source",grant(ids[0].clone())).await.status,410);
        assert_eq!(fork_control(&mut source,"/source",grant("1:1000000".into())).await.status,200);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(48))]
        #[test]
        fn streamed_apply_error_preserves_only_complete_durable_cohorts(
            sizes in prop::collection::vec(1usize..97, 130..195), failure in 0usize..195,
        ) {
            let _mode = crate::handlers::test_support::DurabilityGuard::memory();
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let dir = tempfile::tempdir().unwrap();
                let mut machine = machine::Machine::open(dir.path().join("state"),
                    Journal::open(dir.path().join("wal"),256*1024).unwrap()).await.unwrap();
                assert_eq!(issue(&mut machine,"PUT","/streamed",1000,vec![],vec![241]).await.status,201);
                let mut entries = Vec::new();
                let mut expected = vec![241];
                let mut prefix_lengths = vec![1];
                for (i, size) in sizes.iter().enumerate() {
                    let data = vec![(i%239) as u8; *size];
                    expected.extend_from_slice(&data);
                    prefix_lengths.push(expected.len());
                    let entry = Entry {log_id:LogId::new(CommittedLeaderId::new(1,1),i as u64+1),
                        payload:EntryPayload::Normal(vec![Command {method:"POST".into(),path:"/streamed".into(),
                            headers:vec![("content-type".into(),"application/octet-stream".into())],body:data,time:1001}].into())};
                    machine.journal.persist(Event::Entry(entry.clone())).await.unwrap();
                    entries.push(entry);
                }
                let failure = failure % sizes.len();
                let stream = stream::iter(entries.clone().into_iter().enumerate().map(|(i,e)| {
                    if i==failure {Err(io::Error::other("injected apply-stream read failure"))} else {Ok((e,None))}
                }));
                assert!(machine.apply(stream).await.is_err());
                let published = failure/64*64;
                assert_eq!(machine.view.read().await.applied.unwrap().index,published as u64);
                assert_eq!(bytes(&machine,"/streamed").await,(200,expected[..prefix_lengths[published]].to_vec()));
                machine = reopen(machine,false).await;
                assert_eq!(bytes(&machine,"/streamed").await,(200,expected[..prefix_lengths[published]].to_vec()));
                let mut reader = machine.journal.clone();
                let limited = reader.limited_get_log_entries(1,sizes.len() as u64+1).await.unwrap();
                assert_eq!(limited.len(),64);
                assert_eq!(limited.first().unwrap().log_id.index,1);
                assert_eq!(limited.last().unwrap().log_id.index,64);
                let stream = reader.entries_stream(published as u64+1..).await;
                // A later append must not extend the captured replay interval.
                machine.journal.persist(Event::Entry(Entry {
                    log_id:LogId::new(CommittedLeaderId::new(1,1),sizes.len() as u64+1),payload:EntryPayload::Blank,
                })).await.unwrap();
                machine.apply(Box::pin(stream.map(|e|e.map(|entry|(entry,None))))).await.unwrap();
                assert_eq!(machine.view.read().await.applied.unwrap().index,sizes.len() as u64);
                assert_eq!(bytes(&machine,"/streamed").await,(200,expected.clone()));
                drop(reader);
                machine = reopen(machine,true).await;
                assert_eq!(bytes(&machine,"/streamed").await,(200,expected));
                assert_eq!(machine.journal.uncommitted().len(),1);
            });
        }

        #[test]
        fn async_mixed_results_snapshot_replay_and_shorter_replacement(
            sizes in prop::collection::vec(1usize..300,2..10), snapshot in any::<bool>(),
            orphan_count in 2u64..9,
        ) {
            let _mode = crate::handlers::test_support::DurabilityGuard::memory();
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let dir = tempfile::tempdir().unwrap();
                let mut machine = machine::Machine::open(dir.path().join("state"),
                    Journal::open(dir.path().join("wal"),256*1024).unwrap()).await.unwrap();
                assert_eq!(issue(&mut machine,"PUT","/async",1000,vec![],vec![3,251]).await.status,201);
                let id = |term,index|LogId::new(CommittedLeaderId::new(term,1),index);
                let mut commands = Vec::new();
                let mut expected = vec![3,251];
                for (i,size) in sizes.iter().enumerate() {
                    expected.extend(vec![i as u8+37;*size]);
                    for kind in 0..3 {
                        commands.push(Command {method:"POST".into(),path:"/async".into(),time:1001,
                            headers:vec![("content-type".into(),"application/octet-stream".into()),
                                ("producer-id".into(),"receipts".into()),("producer-epoch".into(),"0".into()),
                                ("producer-seq".into(),(if kind==2 {i+2} else {i}).to_string()),
                                ("stream-durability".into(),if (i*3+kind)%2==0 {"local-fsync"} else {"quorum-fsync"}.into())],
                            body:vec![if kind==0 {i as u8+37} else {0xEE};*size]});
                    }
                }
                let commands_count=commands.len();
                let replies=issue_batch(&mut machine,commands).await;
                assert_eq!(bytes(&machine,"/async").await,(200,expected.clone()));
                machine=reopen(machine,snapshot).await;
                for (ordinal,expected_reply) in replies.iter().enumerate() {
                    assert_eq!(expected_reply.status,[200,204,409][ordinal%3]);
                    let view=machine.view.read().await;
                    let position=receipts::Position {log_id:id(1,1),ordinal};
                    let (state,reply)=view.receipts.lookup(position,view.applied,machine.journal.id_at(1));
                    assert_eq!(state,if ordinal%2!=0 {"unknown"} else if ordinal%3==2 {"rejected"} else {"committed"});
                    if ordinal%2==0 {
                        assert_eq!(bincode::serialize(reply.unwrap()).unwrap(),bincode::serialize(expected_reply).unwrap());
                    }
                }
                assert!(commands_count>3);
                for index in 2..2+orphan_count {
                    machine.journal.persist(Event::Entry(Entry {log_id:id(1,index),payload:EntryPayload::Normal(vec![Command {
                        method:"POST".into(),path:"/async".into(),time:1002,
                        headers:vec![("stream-durability".into(),"local-fsync".into())],body:vec![0xDD;91],
                    }].into())})).await.unwrap();
                }
                machine=reopen(machine,snapshot).await;
                let suffix=machine.journal.uncommitted();
                assert_eq!(suffix.len(),orphan_count as usize);
                assert!(machine.unresolved(&suffix).await);
                assert_eq!(bytes(&machine,"/async").await,(200,expected.clone()));
                let position=receipts::Position {log_id:id(1,2),ordinal:0};
                {
                    let view=machine.view.read().await;
                    assert_eq!(view.receipts.lookup(position,view.applied,machine.journal.id_at(2)).0,"pending");
                }
                machine.journal.persist(Event::Truncate(id(1,2))).await.unwrap();
                let replacement=Entry {log_id:id(2,2),payload:EntryPayload::Blank};
                machine.journal.persist(Event::Entry(replacement.clone())).await.unwrap();
                assert!(!machine.unresolved(&suffix).await,"must not wait for the old numeric high-water index");
                {
                    let view=machine.view.read().await;
                    assert_eq!(view.receipts.lookup(position,view.applied,machine.journal.id_at(2)).0,"unknown",
                        "an uncommitted replacement cannot prove invalidation");
                }
                machine.apply_committed([replacement]).await.unwrap();
                machine=reopen(machine,snapshot).await;
                let view=machine.view.read().await;
                assert_eq!(view.receipts.lookup(position,view.applied,machine.journal.id_at(2)).0,"invalidated");
                assert_eq!(view.receipts.lookup(receipts::Position {log_id:id(1,1),ordinal:0},view.applied,None).0,"committed");
            });
        }

        #[test]
        fn append_batch_replies_dedup_and_partial_materialization_recover(
            sizes in prop::collection::vec(1usize..511,3..18), partial in 0usize..18,
            snapshot in any::<bool>(),
        ) {
            let _mode = crate::handlers::test_support::DurabilityGuard::memory();
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let dir = tempfile::tempdir().unwrap();
                let mut machine = machine::Machine::open(dir.path().join("state"),
                    Journal::open(dir.path().join("wal"),256*1024).unwrap()).await.unwrap();
                let mut expected = vec![1,255];
                assert_eq!(issue(&mut machine,"PUT","/batched",1000,vec![],expected.clone()).await.status,201);
                let command = |seq:usize, body:Vec<u8>| Command {
                    method:"POST".into(),path:"/batched".into(),time:1001,
                    headers:vec![("content-type".into(),"application/octet-stream".into()),
                        ("producer-id".into(),"batch-producer".into()),("producer-epoch".into(),"2".into()),
                        ("producer-seq".into(),seq.to_string())],body,
                };
                let mut commands = Vec::new();
                let mut tails = Vec::new();
                for (i,size) in sizes.iter().enumerate() {
                    let body=vec![(i+17) as u8;*size];
                    expected.extend_from_slice(&body);
                    tails.push(crate::store::format_offset(expected.len() as u64));
                    commands.push(command(i,body));
                    commands.push(command(i,vec![0xDD;7])); // Duplicate must not append this body.
                    commands.push(command(i+2,vec![0xEE;13])); // Gap must not reserve/consume sequence.
                }
                let id=|index|LogId::new(CommittedLeaderId::new(1,1),index);
                let entry=Entry {log_id:id(1),payload:EntryPayload::Normal(commands.into())};
                machine.journal.persist(Event::Entry(entry.clone())).await.unwrap();
                assert_eq!(bytes(&machine,"/batched").await,(200,vec![1,255]));
                machine.journal.shard.fail_next_write();
                assert!(machine.apply_committed([entry.clone()]).await.is_err());
                assert_eq!(bytes(&machine,"/batched").await,(200,vec![1,255]));
                let replies=machine.apply_committed([entry]).await.unwrap().pop().unwrap();
                assert_eq!(replies.len(),3*sizes.len());
                for (i,chunk) in replies.chunks_exact(3).enumerate() {
                    assert_eq!(chunk.iter().map(|r|r.status).collect::<Vec<_>>(),vec![200,204,409]);
                    for reply in &chunk[..2] {
                        assert!(reply.headers.contains(&("stream-next-offset".into(),tails[i].clone())));
                    }
                    assert!(chunk[2].headers.contains(&("producer-expected-seq".into(),(i+1).to_string())));
                }
                assert_eq!(bytes(&machine,"/batched").await,(200,expected.clone()));
                machine=reopen(machine,snapshot).await;
                let mut next=Vec::new();
                for (i,size) in sizes.iter().rev().enumerate() {
                    let body=vec![(i+117) as u8;size+3];
                    expected.extend_from_slice(&body);
                    next.push(command(sizes.len()+i,body));
                }
                machine.journal.persist(Event::Entry(Entry {log_id:id(2),payload:EntryPayload::Normal(next.clone().into())})).await.unwrap();
                machine.journal.cover_apply(id(2)).await.unwrap();
                // Crash after publishing only a strict prefix of a COMMITTED
                // batch. Hot files are not a recovery authority; replay must
                // rebuild the complete batch from WAL/snapshot without duplicates.
                let cut=partial%next.len();
                for c in &next[..cut] {
                    let store=machine.view.read().await.store.clone();
                    let r=crate::handlers::handle(store,Req {method:Method::Post,path:c.path.clone(),query:None,
                        headers:c.headers.clone(),body:c.body.clone().into()}).await;
                    assert_eq!(r.status,200);
                }
                assert_eq!(machine.view.read().await.applied,Some(id(1)));
                machine=reopen(machine,false).await;
                assert_eq!(machine.view.read().await.applied,Some(id(2)));
                assert_eq!(bytes(&machine,"/batched").await,(200,expected.clone()));
                let retry=issue_batch(&mut machine,vec![next.last().unwrap().clone()]).await;
                assert_eq!(retry[0].status,204);
                let tail=command(sizes.len()*2,vec![7,91,219]);
                assert_eq!(issue_batch(&mut machine,vec![tail]).await[0].status,200);
                expected.extend_from_slice(&[7,91,219]);
                machine.journal.persist(Event::Entry(Entry {log_id:id(5),
                    payload:EntryPayload::Normal(vec![command(sizes.len()*2+1,vec![0xAA]),
                        command(sizes.len()*2+2,vec![0xBB])].into())})).await.unwrap();
                machine=reopen(machine,true).await;
                assert_eq!(machine.view.read().await.applied,Some(id(4)));
                assert_eq!(bytes(&machine,"/batched").await,(200,expected));
            });
        }

        #[test]
        fn checkpoint_retains_suffix_votes_and_open_snapshot_descriptors(
            count in 40u64..80, payload in 211usize..618, before_rename in any::<bool>(),
        ) {
            let _mode = crate::handlers::test_support::DurabilityGuard::memory();
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                use tokio::io::AsyncReadExt;
                let dir = tempfile::tempdir().unwrap();
                let wal = dir.path().join("wal");
                let state = dir.path().join("state");
                let mut machine = machine::Machine::open(state.clone(),
                    Journal::open(wal.clone(),4096).unwrap()).await.unwrap();
                assert_eq!(issue(&mut machine,"PUT","/compact",1000,vec![],vec![]).await.status,201);
                let mut expected = Vec::new();
                for i in 1..=count {
                    let body = vec![i as u8;payload];
                    expected.extend_from_slice(&body);
                    assert_eq!(issue(&mut machine,"POST","/compact",1001,
                        vec![("content-type","application/octet-stream".into())],body).await.status,204);
                }
                let mut old = machine.build_snapshot().await.unwrap();
                let snapshot_file = state.join(&machine.journal.index.lock().unwrap().snapshot.as_ref().unwrap().file);
                let snapshot_bytes = std::fs::read(&snapshot_file).unwrap();
                let retained_from = count-3;
                let id = |index| LogId::new(CommittedLeaderId::new(1,1),index);
                let vote = Vote::new(7,2);
                machine.journal.persist(Event::Vote(vote)).await.unwrap();
                // Persist but DO NOT commit this suffix. Recovery must not make
                // it visible, and compaction must not discard its payload.
                for index in count+1..=count+3 {
                    machine.journal.persist(Event::Entry(Entry {log_id:id(index),payload:EntryPayload::Normal(vec![Command {
                        method:"POST".into(),path:"/compact".into(),headers:vec![],body:vec![0xEE;payload],time:1002,
                    }].into())})).await.unwrap();
                }
                let old_files: Vec<_> = std::fs::read_dir(&wal).unwrap().map(|p|p.unwrap().path())
                    .filter(|p|p.extension().is_some_and(|e|e=="wal")).collect();
                let purged = id(retained_from-1);
                let mut journal = machine.journal.clone();
                let worker = journal.clone();
                let maintenance = tokio::spawn(async move {worker.maintain().await});
                let mut reader = journal.clone();
                let reading = tokio::spawn(async move {
                    for _ in 0..32 {
                        let entries = reader.try_get_log_entries(0..=count+3).await.unwrap();
                        let first = entries.first().unwrap().log_id.index;
                        assert!(first == 0 || first == retained_from);
                        assert_eq!(entries.len() as u64,count+4-first);
                        for (index, entry) in (first..=count+3).zip(entries) {
                            assert_eq!(entry.log_id,id(index));
                            let EntryPayload::Normal(batch) = entry.payload else {panic!("lost command")};
                            let body = if index == 0 {vec![]} else {
                                vec![if index <= count {index as u8} else {0xEE};payload]
                            };
                            assert_eq!(batch.commands[0].body,body);
                        }
                        tokio::task::yield_now().await;
                    }
                });
                journal.purge(purged).await.unwrap();
                reading.await.unwrap();
                tokio::time::timeout(Duration::from_secs(5),async {
                    while old_files.iter().all(|p|p.exists()) {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }).await.expect("must physically reclaim segments");
                // Join before deliberate disk corruption/reopen. Cancellation
                // cannot interrupt the synchronous publication/unlink region.
                maintenance.abort();
                assert!(maintenance.await.unwrap_err().is_cancelled());
                journal.persist(Event::Entry(Entry {log_id:id(count+4),payload:EntryPayload::Normal(vec![Command {
                    method:"POST".into(),path:"/compact".into(),headers:vec![],body:vec![0xCD;payload],time:1002,
                }].into())})).await.unwrap();
                let retained = journal.index.lock().unwrap().entries[&retained_from].1;
                let checkpoint = wal.join("journal-checkpoint");
                let good = std::fs::read(&checkpoint).unwrap();
                let mut damaged = good.clone();
                damaged[40] ^= 1;
                drop(journal);
                drop(machine);
                std::fs::write(&checkpoint,&damaged).unwrap();
                assert!(Journal::open(wal.clone(),4096).err().unwrap().to_string().contains("checkpoint checksum"));
                std::fs::write(&checkpoint,&good).unwrap();
                let retained_file = wal.join(format!("{}.wal",retained.segment));
                let original = std::fs::read(&retained_file).unwrap();
                let mut corrupted = original.clone();
                corrupted[retained.offset as usize + retained.len - 1] ^= 1;
                std::fs::write(&retained_file,&corrupted).unwrap();
                assert!(Journal::open(wal.clone(),4096).err().unwrap().to_string().contains("CRC/identity"));
                std::fs::write(retained_file,original).unwrap();
                if before_rename {
                    // An incomplete next checkpoint is not a published one.
                    std::fs::write(wal.join("journal-checkpoint-next"),&good[..good.len()/2]).unwrap();
                }
                let mut journal = Journal::open(wal.clone(),4096).unwrap();
                assert_eq!(journal.read_vote().await.unwrap(),Some(vote));
                assert_eq!(journal.get_log_state().await.unwrap().last_purged_log_id,Some(purged));
                let entries = journal.try_get_log_entries(retained_from..=count+4).await.unwrap();
                assert_eq!(entries.len(),8);
                for (index,entry) in (retained_from..=count+4).zip(entries) {
                    assert_eq!(entry.log_id,id(index));
                    let EntryPayload::Normal(batch) = entry.payload else {panic!("lost command")};
                    let commands = batch.commands;
                    assert_eq!(commands.len(),1);
                    assert_eq!(commands[0].body,vec![if index<=count {index as u8} else if index==count+4 {0xCD} else {0xEE};payload]);
                }
                machine = machine::Machine::open(state.clone(),journal.clone()).await.unwrap();
                assert_eq!(bytes(&machine,"/compact").await,(200,expected.clone()));
                journal.truncate_after(Some(id(count))).await.unwrap();
                assert_eq!(issue(&mut machine,"POST","/compact",1003,
                    vec![("content-type","application/octet-stream".into())],b"replacement".to_vec()).await.status,204);
                expected.extend_from_slice(b"replacement");
                let next = machine.build_snapshot().await.unwrap();
                assert!(!snapshot_file.exists(),"obsolete snapshot should be unlinked");
                let mut still_readable = Vec::new();
                old.snapshot.read_to_end(&mut still_readable).await.unwrap();
                assert_eq!(still_readable,snapshot_bytes,"in-flight snapshot descriptor survives cleanup");
                journal.purge(next.meta.last_log_id.unwrap()).await.unwrap();
                assert!(journal.try_get_log_entries(0..).await.unwrap().is_empty());
                drop(journal);
                drop(machine);
                // A second restart covers an empty retained index and a cut
                // near the active segment's boundary, not only a retained tail.
                let journal = Journal::open(wal,4096).unwrap();
                let machine = machine::Machine::open(state,journal).await.unwrap();
                assert_eq!(bytes(&machine,"/compact").await,(200,expected));
            });
        }

        #[test]
        fn apply_barrier_precedes_visibility_and_chunked_replay_never_regresses(
            count in 65usize..145, split_seed in 0usize..145, snapshot in any::<bool>(),
        ) {
            let _mode = crate::handlers::test_support::DurabilityGuard::memory();
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let dir = tempfile::tempdir().unwrap();
                let mut machine = machine::Machine::open(dir.path().join("state"),
                    Journal::open(dir.path().join("wal"),256*1024).unwrap()).await.unwrap();
                assert_eq!(issue(&mut machine,"PUT","/batch",1000,vec![],b"base".to_vec()).await.status,201);
                let original = machine.view.read().await.applied;
                let mut expected = b"base".to_vec();
                let mut entries = Vec::new();
                for index in 1..=count {
                    let payload = if index % 7 == 0 {
                        EntryPayload::Blank
                    } else {
                        let data = vec![index as u8, 0xDA];
                        expected.extend_from_slice(&data);
                        EntryPayload::Normal(vec![Command {method:"POST".into(),path:"/batch".into(),
                            headers:vec![("content-type".into(),"application/octet-stream".into())],body:data,time:1001}].into())
                    };
                    let entry = Entry {log_id:LogId::new(CommittedLeaderId::new(1,1),index as u64),payload};
                    machine.journal.persist(Event::Entry(entry.clone())).await.unwrap();
                    entries.push(entry);
                }
                let last = entries.last().unwrap().log_id;
                let mut log_store = machine.journal.clone();
                log_store.save_committed(Some(last)).await.unwrap();
                assert_eq!(log_store.read_committed().await.unwrap(),None);
                assert_eq!(log_store.index.lock().unwrap().committed,original);
                drop(log_store);
                // The actual native WAL stage fails. If the barrier moves after
                // handlers, bytes/clock/wakes would already have been published.
                machine.journal.shard.fail_next_write();
                assert!(machine.apply(stream::iter(entries.clone().into_iter().map(|e| Ok((e,None))))).await.is_err());
                assert_eq!(bytes(&machine,"/batch").await,(200,b"base".to_vec()));
                assert_eq!(machine.view.read().await.applied,original);
                let split = 1 + split_seed % count;
                for batch in [&entries[..split], &entries[split..]] {
                    assert_eq!(machine.apply_committed(batch.to_vec()).await.unwrap().len(),batch.len());
                }
                assert_eq!(machine.journal.index.lock().unwrap().committed,Some(last));
                assert_eq!(bytes(&machine,"/batch").await,(200,expected.clone()));
                if snapshot { machine.build_snapshot().await.unwrap(); }
                let end = machine.journal.shard.tail_lsn();
                machine = reopen(machine,false).await;
                assert_eq!(machine.journal.shard.tail_lsn(),end,"replay must not append regressing markers");
                assert_eq!(machine.view.read().await.applied,Some(last));
                assert_eq!(bytes(&machine,"/batch").await,(200,expected));
            });
        }

        #[test]
        fn remote_fork_native_import_retries_and_descendant_retention_survive_restart(
            len in 1usize..150000, cut_seed in 0usize..150001,
            checkpoint in 0u8..5, salt in any::<u8>(),
            override_ttl in any::<bool>(),
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
                assert_eq!(issue(&mut source,"PUT","/source",1000,
                    vec![("stream-ttl","1".into())],data.clone()).await.status,201);
                let mut headers = vec![("stream-forked-from".into(),"/source".into()),
                    ("stream-fork-offset".into(),crate::store::format_offset(cut as u64))];
                if override_ttl { headers.push(("stream-ttl".into(),"2".into())); }
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
                let grant = Action::Grant {tx:tx.clone(),source_id:0,headers:headers.clone(),expected:config};
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
                let reconfirm = Action::Reconfirm { headers: headers.clone() };
                assert_eq!(fork_control(&mut target,"/child",reconfirm.clone()).await.status,200);
                let mut equivalent = headers.clone();
                equivalent.retain(|(k,_)| k != "stream-ttl");
                equivalent.extend([("stream-ttl".into(),if override_ttl { "2" } else { "1" }.into()),
                    ("content-type".into(),"APPLICATION/OCTET-STREAM; charset=binary".into()),
                    ("stream-fork-sub-offset".into(),"0".into())]);
                assert_eq!(fork_control(&mut target,"/child",Action::Reconfirm {headers:equivalent}).await.status,200);
                let mut different = headers.clone();
                different.retain(|(k,_)| k != "stream-ttl");
                if !override_ttl { different.push(("stream-ttl".into(),"2".into())); }
                assert_eq!(fork_control(&mut target,"/child",Action::Reconfirm {headers:different}).await.status,409);
                assert_eq!(bytes(&target,"/child").await,(200,expected.clone()));
                assert_eq!(issue(&mut target,"PUT","/desc",1006,
                    vec![("stream-forked-from","/child".into())],vec![177]).await.status,201);
                assert_eq!(issue(&mut target,"DELETE","/child",1006,vec![],vec![]).await.status,204);
                assert_eq!(fork_control(&mut target,"/child",reconfirm.clone()).await.status,409);
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
                let late = fork_control(&mut source,"/source",grant.clone()).await;
                assert!(matches!(serde_json::from_slice::<Decision>(&late.body).unwrap(),Decision::Released));
                assert!(target.view.read().await.forks.certificate(&tx,u64::MAX).is_none(),
                    "source release without destination confirmation is not a certificate");
                assert_eq!(fork_control(&mut target,"/child",Action::Released {tx:tx.clone()}).await.status,204);
                let (source_group,start,end) = {
                    let view = target.view.read().await;
                    view.forks.certificate(&tx,view.applied.unwrap().index).unwrap()
                };
                assert_eq!(fork_control(&mut source,"",Action::Compact {destination_group:1,start,end}).await.status,204);
                source = reopen(source,true).await; // Lost certificate acknowledgement.
                assert_eq!(fork_control(&mut source,"",Action::Compact {destination_group:1,start,end}).await.status,204);
                assert_eq!(fork_control(&mut target,"",Action::Collect {source_group,start,end}).await.status,204);
                target = reopen(target,true).await;
                {
                    let view = target.view.read().await;
                    assert!(view.forks.destinations.is_empty());
                    assert!(view.forks.mirrors.is_empty());
                    assert_eq!(view.forks.result(&tx).unwrap().status,201);
                }
                assert!(source.view.read().await.forks.decisions.is_empty());
                // Forgotten records must remain fenced after snapshot-only recovery.
                assert_eq!(fork_control(&mut source,"/source",grant.clone()).await.status,410);
                assert_eq!(fork_control(&mut source,"/source",Action::Release {tx:tx.clone()}).await.status,204);
                assert_eq!(fork_control(&mut target,"/child",Action::Publish {tx}).await.status,409);
                assert_eq!(fork_control(&mut target,"/child",reconfirm.clone()).await.status,404);
                assert_eq!(issue(&mut target,"PUT","/child",1008,vec![],vec![23]).await.status,201);
                assert_eq!(fork_control(&mut target,"/child",reconfirm).await.status,409);
                assert_eq!(bytes(&target,"/child").await,(200,vec![23]));
                assert_eq!(issue(&mut source,"PUT","/source",1008,vec![],vec![85]).await.status,201);
                assert_eq!(fork_control(&mut source,"/source",grant).await.status,410);
                assert_eq!(bytes(&source,"/source").await,(200,vec![85]));
            });
        }

        #[test]
        fn retired_fork_intervals_are_monotonic_group_scoped_and_recoverable(
            certificates in prop::collection::vec((1usize..3,0u64..64,0u64..64),2..30),
            snapshot in any::<bool>(),
        ) {
            use forks::Action;
            let _mode = crate::handlers::test_support::DurabilityGuard::memory();
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let dir = tempfile::tempdir().unwrap();
                let mut machine = machine::Machine::open(dir.path().join("state"),
                    Journal::open(dir.path().join("wal"),256*1024).unwrap()).await.unwrap();
                assert_eq!(issue(&mut machine,"PUT","/source",1000,vec![],vec![]).await.status,201);
                let headers = vec![("stream-forked-from".into(),"/source".into())];
                let config = crate::handlers::prepare_create(&machine.view.read().await.store,
                    &Req {method:Method::Put,path:"/unused".into(),query:None,headers:headers.clone(),body:Default::default()})
                    .await.ok().unwrap().config;
                // Include both ends of u64; adjacent ranges must not wrap at MAX.
                let position = |i:u64| if i<32 {i} else {u64::MAX-(63-i)};
                let ranges: Vec<_> = certificates.iter().map(|&(g,a,b)|
                    (g,position(a).min(position(b)),position(a).max(position(b)))).collect();
                for &(destination_group,start,end) in ranges.iter().chain(ranges.iter().rev()) {
                    assert_eq!(fork_control(&mut machine,"",Action::Compact {destination_group,start,end}).await.status,204);
                }
                machine = reopen(machine,snapshot).await;
                for group in 0..3 {
                    for i in 0..64 {
                        let index = position(i);
                        let retired = ranges.iter().any(|&(g,lo,hi)|g==group && lo<=index && index<=hi);
                        let grant = Action::Grant {tx:format!("{group}:{index}"),source_id:0,
                            headers:headers.clone(),expected:config.clone()};
                        assert_eq!(fork_control(&mut machine,"/source",grant).await.status,
                            if retired {410} else {200},"group={group}, index={index}");
                    }
                }
                let expected = (0..3).flat_map(|g|(0..64).map(move |i|(g,position(i))))
                    .filter(|&(g,i)|!ranges.iter().any(|&(owner,lo,hi)|g==owner && lo<=i && i<=hi)).count();
                assert_eq!(machine.view.read().await.store.streams.get("/source").unwrap()
                    .shared.read().unwrap().ref_count,expected as u32);
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
        fn pinned_snapshot_cut_excludes_later_appends_recreation_and_receipts(
            a in prop::collection::vec(any::<u8>(), 1..150),
            b in prop::collection::vec(any::<u8>(), 1..200),
            recreate in any::<bool>(), time in 1001u64..1999,
        ) {
            use openraft_legacy::network_v1::SnapshotReceiverFactory;
            let _mode = crate::handlers::test_support::DurabilityGuard::memory();
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let dir = tempfile::tempdir().unwrap();
                let mut source = machine::Machine::open(dir.path().join("s/state"),
                    Journal::open(dir.path().join("s/wal"),4096).unwrap()).await.unwrap();
                assert_eq!(issue(&mut source,"PUT","/pinned",time,
                    vec![("stream-ttl","1".into())],vec![197,1]).await.status,201);
                assert_eq!(issue(&mut source,"PUT","/replaced",time,vec![],vec![13,248]).await.status,201);
                let headers = |seq| vec![("content-type","application/octet-stream".into()),
                    ("producer-id","cut".into()),("producer-epoch","5".into()),("producer-seq",format!("{seq}")),
                    ("stream-durability","local-fsync".into())];
                assert_eq!(issue(&mut source,"POST","/pinned",time+10,headers(0),a.clone()).await.status,200);
                let cut = {
                    let view = source.view.write().await;
                    machine::SnapshotCut::capture(&view).unwrap()
                };
                let meta = cut.meta.clone();
                let id = |index|LogId::new(CommittedLeaderId::new(1,1),index);
                assert_eq!(meta.last_log_id,Some(id(2)));
                // Copy is deliberately deferred until after committed mutations.
                // Reopening names, copying to current EOF, or late control-state
                // capture would all produce a different snapshot here.
                assert_eq!(issue(&mut source,"POST","/pinned",time+20,headers(1),b.clone()).await.status,200);
                assert_eq!(issue(&mut source,"DELETE","/replaced",time+20,vec![],vec![]).await.status,204);
                assert_eq!(issue(&mut source,"PUT","/replaced",time+20,vec![],vec![44;51]).await.status,201);
                if recreate {
                    assert_eq!(issue(&mut source,"DELETE","/pinned",time+30,vec![],vec![]).await.status,204);
                    assert_eq!(issue(&mut source,"PUT","/pinned",time+30,vec![],vec![61;399]).await.status,201);
                }
                let archive = source.dir.join("snapshot-pinned-test");
                cut.write(&archive).unwrap();
                let mut target = machine::Machine::open(dir.path().join("d/state"),
                    Journal::open(dir.path().join("d/wal"),4096).unwrap()).await.unwrap();
                let mut receiving = target.begin_receiving_snapshot().await.unwrap();
                tokio::io::copy(&mut tokio::fs::File::open(archive).await.unwrap(),&mut receiving).await.unwrap();
                target.install_snapshot(&meta,receiving).await.unwrap();
                target.journal.clone().purge(id(2)).await.unwrap();
                target = reopen(target,false).await;
                let expected = [vec![197,1],a.clone()].concat();
                assert_eq!(bytes(&target,"/pinned").await,(200,expected.clone()));
                assert_eq!(bytes(&target,"/replaced").await,(200,vec![13,248]));
                {
                    let view = target.view.read().await;
                    assert_eq!(clock::millis(view.store.clock.now()),time+10);
                    let (state,reply) = view.receipts.lookup(receipts::Position {log_id:id(2),ordinal:0},view.applied,None);
                    assert_eq!(state,"committed");
                    assert_eq!(reply.unwrap().status,200);
                    assert_eq!(view.receipts.lookup(receipts::Position {log_id:id(3),ordinal:0},view.applied,None).0,"unknown");
                }
                assert_eq!(issue(&mut target,"POST","/pinned",time+25,headers(0),vec![99;91]).await.status,204);
                assert_eq!(bytes(&target,"/pinned").await,(200,expected));
                issue(&mut target,"TICK","/pinned",time+1010,vec![],vec![]).await;
                assert_eq!(bytes(&target,"/pinned").await.0,200);
                issue(&mut target,"TICK","/pinned",time+1011,vec![],vec![]).await;
                assert_eq!(bytes(&target,"/pinned").await.0,404);
                source = reopen(source,false).await;
                let current = if recreate {vec![61;399]} else {[vec![197,1],a,b].concat()};
                assert_eq!(bytes(&source,"/pinned").await,(200,current));
                assert_eq!(bytes(&source,"/replaced").await,(200,vec![44;51]));
            });
        }

        #[test]
        fn snapshot_captures_fresh_metadata_without_rewriting_disposable_sidecars(
            chunks in prop::collection::vec(prop::collection::vec(any::<u8>(), 1..40), 2..7),
            time in 1001u64..1999,
        ) {
            use openraft_legacy::network_v1::SnapshotReceiverFactory;
            let _mode = crate::handlers::test_support::DurabilityGuard::memory();
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let dir = tempfile::tempdir().unwrap();
                let mut source = machine::Machine::open(dir.path().join("s/state"),
                    Journal::open(dir.path().join("s/wal"),4096).unwrap()).await.unwrap();
                assert_eq!(issue(&mut source,"PUT","/fresh",time,
                    vec![("stream-ttl","1".into())],vec![91,3]).await.status,201);
                let sidecar = {
                    let view = source.view.read().await;
                    let stream = view.store.streams.get("/fresh").unwrap();
                    crate::store::meta_path(&stream.file_path)
                };
                let stale = std::fs::read(&sidecar).unwrap();
                let headers = |seq:usize| vec![("content-type","application/octet-stream".into()),
                    ("producer-id","snapshot-p".into()),("producer-epoch","7".into()),("producer-seq",seq.to_string())];
                let mut expected = vec![91,3];
                for (seq,chunk) in chunks.iter().enumerate() {
                    expected.extend_from_slice(chunk);
                    assert_eq!(issue(&mut source,"POST","/fresh",time+10*(seq as u64+1),
                        headers(seq),chunk.clone()).await.status,200);
                }
                assert_eq!(std::fs::read(&sidecar).unwrap(),stale,"fixture sidecar must lag current producer/access state");
                let mut snapshot = source.build_snapshot().await.unwrap();
                assert_eq!(std::fs::read(&sidecar).unwrap(),stale,"archive creation must not rewrite disposable sidecars");
                // A new replica has NONE of the source WAL. Replaying that WAL
                // would hide a snapshot accidentally copying stale metadata.
                let mut target = machine::Machine::open(dir.path().join("d/state"),
                    Journal::open(dir.path().join("d/wal"),4096).unwrap()).await.unwrap();
                let mut receiving = target.begin_receiving_snapshot().await.unwrap();
                tokio::io::copy(&mut snapshot.snapshot,&mut receiving).await.unwrap();
                target.install_snapshot(&snapshot.meta,receiving).await.unwrap();
                // Emulate the consensus engine's install-then-purge sequence:
                // the snapshot alone does not advance the log store's floor.
                target.journal.clone().purge(snapshot.meta.last_log_id.unwrap()).await.unwrap();
                target = reopen(target,false).await;
                assert_eq!(bytes(&target,"/fresh").await,(200,expected.clone()));
                let touched = time+10*chunks.len() as u64;
                assert_eq!(issue(&mut target,"POST","/fresh",touched+5,
                    headers(chunks.len()-1),chunks.last().unwrap().clone()).await.status,204);
                assert_eq!(bytes(&target,"/fresh").await,(200,expected));
                issue(&mut target,"TICK","/fresh",touched+1000,vec![],vec![]).await;
                assert_eq!(bytes(&target,"/fresh").await.0,200);
                issue(&mut target,"TICK","/fresh",touched+1001,vec![],vec![]).await;
                assert_eq!(bytes(&target,"/fresh").await.0,404);
            });
        }

        #[test]
        fn native_fork_dedup_and_millisecond_expiry_survive_snapshot_and_replay(
            a in prop::collection::vec(any::<u8>(), 1..35),
            b in prop::collection::vec(any::<u8>(), 1..27),
            split in 0usize..100, time in 1001u64..1999,
            override_ttl in any::<bool>(), delete_source in any::<bool>(),
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
                let mut headers = vec![("stream-forked-from", "/source".into()),
                    ("stream-fork-offset", format!("0000000000000000_{cut:016}"))];
                if override_ttl { headers.push(("stream-ttl", "2".into())); }
                assert_eq!(issue(&mut machine, "PUT", "/fork", time+30,
                    headers.clone(), vec![241, 17, 94]).await.status, 201);
                let mut fork = expected[..cut].to_vec(); fork.extend_from_slice(&[241, 17, 94]);
                machine.build_snapshot().await.unwrap();
                drop(machine); drop(journal);
                let journal = Journal::open(dir.path().join("wal"), 4096).unwrap();
                let mut machine = machine::Machine::open(dir.path().join("state"), journal).await.unwrap();
                assert_eq!(bytes(&machine, "/source").await, (200, expected));
                assert_eq!(bytes(&machine, "/fork").await, (200, fork.clone()));
                if delete_source {
                    assert_eq!(issue(&mut machine, "DELETE", "/source", time+40, vec![], vec![]).await.status,204);
                } else {
                    issue(&mut machine, "TICK", "/source", time+1010, vec![], vec![]).await;
                    assert_eq!(bytes(&machine, "/source").await.0, 200); // exact TTL edge is live
                    issue(&mut machine, "TICK", "/source", time+1011, vec![], vec![]).await;
                }
                machine = reopen(machine, true).await;
                assert_eq!(bytes(&machine, "/source").await.0, 410);
                // Existing-child equality does not require a live parent. The
                // original parent's defaults, not the child's overrides, apply.
                assert_eq!(issue(&mut machine, "PUT", "/fork", time+1011,
                    headers.clone(), vec![13, 59]).await.status,200);
                let mut equivalent = headers.clone();
                equivalent.retain(|(k,_)| *k != "stream-ttl");
                equivalent.extend([("stream-ttl", if override_ttl { "2" } else { "1" }.into()),
                    ("content-type", "APPLICATION/OCTET-STREAM; charset=binary".into()),
                    ("stream-fork-sub-offset", "0".into())]);
                assert_eq!(issue(&mut machine, "PUT", "/fork", time+1011,
                    equivalent.clone(), vec![37]).await.status,200);
                let mut different = headers.clone();
                different.retain(|(k,_)| *k != "stream-ttl");
                if !override_ttl { different.push(("stream-ttl", "2".into())); }
                assert_eq!(issue(&mut machine, "PUT", "/fork", time+1011,
                    different, vec![]).await.status,409);
                assert_eq!(bytes(&machine, "/fork").await, (200, fork.clone()));
                assert_eq!(issue(&mut machine, "POST", "/fork", time+1011,
                    vec![("stream-closed", "true".into())],vec![]).await.status,204);
                assert_eq!(issue(&mut machine, "PUT", "/fork", time+1011,
                    equivalent.clone(),vec![]).await.status,409);
                equivalent.push(("stream-closed", "true".into()));
                assert_eq!(issue(&mut machine, "PUT", "/fork", time+1011,
                    equivalent,vec![]).await.status,200);
                machine = reopen(machine, true).await;
                assert_eq!(bytes(&machine, "/fork").await, (200, fork));
                assert_eq!(issue(&mut machine, "DELETE", "/fork", time+1011,vec![],vec![]).await.status,204);
                assert_eq!(issue(&mut machine, "PUT", "/fork", time+1011,
                    headers,vec![]).await.status,404);
            });
        }

        #[test]
        fn json_fork_suboffset_native_wal_and_snapshot(
            values in prop::collection::vec((any::<i64>(), ".*"), 2..7),
            padding in prop_oneof![0usize..80, 65520usize..65552, 131056usize..131088],
            cut_seed in any::<usize>(), snapshot in any::<bool>(),
        ) {
            use serde_json::json;
            let _mode = crate::handlers::test_support::DurabilityGuard::memory();
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let dir = tempfile::tempdir().unwrap();
                let mut machine = machine::Machine::open(dir.path().join("state"),
                    Journal::open(dir.path().join("wal"), 256*1024).unwrap()).await.unwrap();
                let ct = || vec![("content-type", "application/json".into())];
                let anchor_value = json!({"ancestor": [false, "quoted,comma"]});
                let created = issue(&mut machine,"PUT","/ancestor",1000,ct(),
                    serde_json::to_vec(&anchor_value).unwrap()).await;
                assert_eq!(created.status,201);
                let anchor = created.headers.iter().find(|(k,_)| k == "stream-next-offset").unwrap().1.clone();
                assert_eq!(issue(&mut machine,"PUT","/source",1000,
                    vec![("stream-forked-from","/ancestor".into())],vec![]).await.status,201);
                let mut messages = vec![json!(format!("{}\\\",[],{{}},雪\\", "x".repeat(padding)))];
                messages.extend(values.iter().map(|(n,s)| json!({"nested": [[n,s], {"k,":"a\\\"b,c"}], "empty": {}})));
                messages.extend([json!([null, false, [1,2]]), json!(true), json!(-13.25)]);
                assert_eq!(issue(&mut machine,"POST","/source",1001,ct(),
                    serde_json::to_vec(&messages).unwrap()).await.status,204);
                let cut = cut_seed % messages.len() + 1;
                let headers = |count: u64| vec![("stream-forked-from","/source".into()),
                    ("stream-fork-offset",anchor.clone()), ("stream-fork-sub-offset",count.to_string())];
                let child_value = json!({"child-only": [3,7]});
                assert_eq!(issue(&mut machine,"PUT","/child",1002,headers(cut as u64),
                    serde_json::to_vec(&child_value).unwrap()).await.status,201);
                let mut expected = vec![anchor_value];
                expected.extend_from_slice(&messages[..cut]);
                expected.push(child_value);
                for restart in [false,true] {
                    if restart { machine = reopen(machine,snapshot).await; }
                    let (status, data) = bytes(&machine,"/child").await;
                    assert_eq!(status,200);
                    assert_eq!(serde_json::from_slice::<serde_json::Value>(&data).unwrap(),json!(expected));
                    assert_eq!(issue(&mut machine,"PUT","/overshoot",1003,
                        headers(messages.len() as u64 + 1),vec![]).await.status,400);
                    assert_eq!(issue(&mut machine,"PUT","/overflow",1003,
                        headers(u64::MAX),vec![]).await.status,400);
                }
            });
        }

        #[test]
        fn binary_fork_suboffset_cannot_wrap(
            prefix in prop::collection::vec(any::<u8>(),1..41),
            suffix in prop::collection::vec(any::<u8>(),1..53),
            sub in prop_oneof![Just(u64::MAX),0u64..56],
        ) {
            let _mode = crate::handlers::test_support::DurabilityGuard::memory();
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let dir = tempfile::tempdir().unwrap();
                let mut machine = machine::Machine::open(dir.path().join("state"),
                    Journal::open(dir.path().join("wal"),256*1024).unwrap()).await.unwrap();
                let created = issue(&mut machine,"PUT","/source",1000,vec![],prefix.clone()).await;
                assert_eq!(created.status,201);
                let anchor = created.headers.iter().find(|(k,_)| k == "stream-next-offset").unwrap().1.clone();
                assert_eq!(issue(&mut machine,"POST","/source",1001,
                    vec![("content-type","application/octet-stream".into())],suffix.clone()).await.status,204);
                let reply = issue(&mut machine,"PUT","/child",1002,
                    vec![("stream-forked-from","/source".into()),("stream-fork-offset",anchor),
                        ("stream-fork-sub-offset",sub.to_string())],vec![197,12,91]).await;
                if sub <= suffix.len() as u64 {
                    assert_eq!(reply.status,201);
                    let mut expected = prefix;
                    expected.extend_from_slice(&suffix[..sub as usize]);
                    expected.extend_from_slice(&[197,12,91]);
                    machine = reopen(machine,true).await;
                    assert_eq!(bytes(&machine,"/child").await,(200,expected));
                } else {
                    assert_eq!(reply.status,400);
                    assert_eq!(bytes(&machine,"/child").await.0,404);
                }
            });
        }

        #[test]
        fn native_wal_suffix_replacement_survives_two_restarts(
            sizes in prop::collection::vec(1usize..500, 2..24), split in 0usize..24,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let dir = tempfile::tempdir().unwrap();
                let keep = split % (sizes.len()+1);
                let make = |i, n, term| Entry {
                    log_id: LogId::new(CommittedLeaderId::new(term, 1), i),
                    payload: EntryPayload::Normal(vec![Command { method: "POST".into(), path: "/p".into(),
                        headers: vec![], body: vec![i as u8 + term as u8; n], time: 0 }].into()),
                };
                let mut journal = Journal::open(dir.path().into(), 1024).unwrap();
                for (i, n) in sizes.iter().enumerate() {
                    journal.persist(Event::Entry(make(i as u64, *n, 1))).await.unwrap();
                }
                let last = keep.checked_sub(1).map(|i| LogId::new(CommittedLeaderId::new(1,1),i as u64));
                journal.persist(Event::Commit(last)).await.unwrap();
                journal.truncate_after(last).await.unwrap();
                assert_eq!(journal.try_get_log_entries(..).await.unwrap().len(),keep);
                assert_eq!(journal.index.lock().unwrap().committed,last,"truncation preserves committed boundary");
                journal.persist(Event::Entry(make(keep as u64, 31, 2))).await.unwrap();
                drop(journal);
                for _ in 0..2 {
                    let mut journal = Journal::open(dir.path().into(), 1024).unwrap();
                    let entries = journal.try_get_log_entries(0..=keep as u64).await.unwrap();
                    assert_eq!(entries.len(), keep + 1);
                    for (i, entry) in entries.iter().enumerate() {
                        let expected = if i == keep { vec![i as u8 + 2; 31] } else { vec![i as u8 + 1; sizes[i]] };
                        let EntryPayload::Normal(batch) = &entry.payload else { panic!("missing command") };
                        let commands = &batch.commands;
                        assert_eq!(commands.len(),1);
                        assert_eq!(commands[0].body, expected);
                    }
                }
            });
        }
    }
}
