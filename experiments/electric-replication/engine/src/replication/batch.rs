//! Ordered admission and native-fsync amortization; see BATCHING.md.
use super::{machine::Machine, receipts::Position, Command, LogId, Raft, Reply};
use openraft::CommittedLeaderId;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};

pub(super) const CAPACITY: usize = 256;
pub(super) const MAX_COMMANDS: usize = 64;
const MAX_BYTES: usize = 2 * 1024 * 1024;
const MAX_INFLIGHT_BATCHES: usize = 2;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Batch {
    pub commands: Vec<Command>,
    /// Never serialized into the WAL or replicated to another process.
    #[serde(skip)]
    pub durable: Option<Arc<Mutex<Option<oneshot::Sender<LogId<u64>>>>>>,
}
impl From<Vec<Command>> for Batch {
    fn from(commands: Vec<Command>) -> Self {
        Self { commands, durable: None }
    }
}

pub(super) struct Committed {
    pub log_id: LogId<u64>,
    pub data: Reply,
}
pub(super) enum Outcome {
    Committed(Committed),
    Accepted(Position),
}
struct Completion {
    result: Option<oneshot::Sender<Result<Outcome, u16>>>,
    local: bool,
    _count: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}

pub(super) struct Pending {
    pub command: Command,
    pub result: oneshot::Sender<Result<Outcome, u16>>,
    pub permit: OwnedSemaphorePermit,
    pub bytes: OwnedSemaphorePermit,
}

impl Command {
    /// Exact bincode-1 fixed-width size, without traversing every payload byte.
    /// A property compares this accounting against the actual serializer.
    pub(super) fn encoded_len(&self) -> usize {
        40 + self.method.len() + self.path.len() + self.body.len()
            + self.headers.iter().map(|(k, v)| 16 + k.len() + v.len()).sum::<usize>()
    }

    pub(super) fn local(&self) -> bool {
        self.method == "POST" && self.headers.iter()
            .find(|(k, _)| k == "stream-durability")
            .is_some_and(|(_, v)| v == "local-fsync")
    }
}

async fn take_ready(
    first: Pending,
    receive: &mut mpsc::Receiver<Pending>,
    carry: &mut Option<Pending>,
) -> (Vec<Command>, Vec<Completion>) {
    let mut next = Some(first);
    let mut commands = Vec::new();
    let mut completions = Vec::new();
    let mut yielded = false;
    let mut bytes = 8; // Vec length; the journal adds its fixed entry/frame header.
    while let Some(pending) = next.take() {
        let size = pending.command.encoded_len();
        if !commands.is_empty()
            && (pending.command.method != "POST" || bytes + size > MAX_BYTES)
        {
            *carry = Some(pending);
            break;
        }
        let append = pending.command.method == "POST";
        bytes += size;
        completions.push(Completion {
            local: pending.command.local(), result: Some(pending.result),
            _count: pending.permit, _bytes: pending.bytes,
        });
        commands.push(pending.command);
        if !append || commands.len() == MAX_COMMANDS {
            break;
        }
        next = receive.try_recv().ok();
        if next.is_none() && !yielded {
            // Keep the collected prefix and its credits while ready senders
            // join. Yield once only, and never delay a full/metadata batch.
            yielded = true;
            tokio::task::yield_now().await;
            next = receive.try_recv().ok();
        }
    }
    (commands, completions)
}

async fn prepare_epoch(raft: &Raft, machine: &Machine, admitted_term: &AtomicU64) -> Result<CommittedLeaderId<u64>, u16> {
    let metrics = raft.metrics().borrow().clone();
    if metrics.current_leader != Some(metrics.id) { return Err(503); }
    let expected = CommittedLeaderId::new(metrics.current_term, metrics.id);
    if admitted_term.load(Ordering::Acquire) != metrics.current_term {
        let prepare = async {
            // Once per leadership epoch, not once per append. The election
            // entry must be applied before newly owned admission can proceed.
            raft.ensure_linearizable().await.map_err(|_| 503u16)?;
            let inherited = machine.journal.uncommitted();
            loop {
                let changed = machine.journal.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let now = raft.metrics().borrow().clone();
                if now.current_term != expected.term || now.current_leader != Some(now.id) {
                    return Err(503u16);
                }
                if !machine.unresolved(&inherited).await {
                    admitted_term.store(expected.term, Ordering::Release);
                    return Ok(());
                }
                changed.await;
            }
        };
        tokio::time::timeout(Duration::from_secs(3), prepare).await.map_err(|_| 429u16)??;
    }
    Ok(expected)
}

pub(super) fn start(raft: Raft, machine: Arc<Machine>, capacity: usize, admitted_term: Arc<AtomicU64>) -> mpsc::Sender<Pending> {
    let (send, mut receive) = mpsc::channel(capacity);
    tokio::spawn(async move {
        let mut carry = None;
        let pipeline = Arc::new(Semaphore::new(MAX_INFLIGHT_BATCHES));
        loop {
            // Bound not-yet-flushed batches as well as admitted commands.
            // Waiting before draining lets arrivals coalesce without a timer.
            let flight = pipeline.clone().acquire_owned().await.unwrap();
            let first: Pending = match carry.take() {
                Some(pending) => pending,
                None => match receive.recv().await {
                    Some(pending) => pending,
                    None => break,
                },
            };
            let (commands, mut completions) = take_ready(first, &mut receive, &mut carry).await;
            let expected = match prepare_epoch(&raft, &machine, &admitted_term).await {
                Ok(expected) => expected,
                Err(status) => {
                    for mut completion in completions {
                        let _ = completion.result.take().unwrap().send(Err(status));
                    }
                    continue;
                }
            };
            let (durable, flushed) = oneshot::channel();
            let batch = Batch { commands, durable: Some(Arc::new(Mutex::new(Some(durable)))) };
            let resolve_probe = super::timing::RESOLVE.start();
            // Enqueue from this single dispatcher BEFORE spawning a waiter.
            // Concurrent client_write tasks would reorder FIFO submissions.
            // The epoch is also checked atomically inside RaftCore at assignment.
            let receive_commit = raft.client_write_ff_with_leader(batch, expected).await;
            let machine = machine.clone();
            tokio::spawn(async move {
                let _resolve_probe = resolve_probe;
                let log_id = if receive_commit.is_ok() { flushed.await.ok() } else { None };
                // Strong requests ahead in FIFO must not block local-fsync
                // dispatch on quorum. All unresolved request credits stay held.
                drop(flight);
                if let Some(log_id) = log_id {
                    for (ordinal, completion) in completions.iter_mut().enumerate() {
                        if completion.local {
                            let _ = completion.result.take().unwrap().send(
                                Ok(Outcome::Accepted(Position { log_id, ordinal })));
                        }
                    }
                }
                let committed = match receive_commit {
                    Ok(receive) => receive.await.ok().and_then(Result::ok),
                    Err(_) => None,
                };
                match committed {
                    Some(committed) => {
                        assert_eq!(Some(committed.log_id), log_id, "local flush identity");
                        assert_eq!(committed.data.len(), completions.len(), "per-command apply replies");
                        for (data, mut completion) in committed.data.into_iter().zip(completions) {
                            if let Some(result) = completion.result.take() {
                                let _ = result.send(Ok(Outcome::Committed(Committed { log_id: committed.log_id, data })));
                            }
                        }
                    }
                    None => {
                        for completion in &mut completions {
                            if let Some(result) = completion.result.take() { let _ = result.send(Err(503)); }
                        }
                        // Leadership loss is not resolution of a durable entry.
                        // Keep credits while this exact uncommitted entry stays
                        // retained, even though HTTP callers have gone away.
                        if let Some(id) = log_id {
                            loop {
                                let changed = machine.journal.changed.notified();
                                tokio::pin!(changed);
                                changed.as_mut().enable();
                                if !machine.unresolved(&[id]).await { break; }
                                changed.await;
                            }
                        }
                        drop(completions);
                    }
                }
            });
        }
    });
    send
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::{journal::Journal, network, BasicNode};
    use openraft::error::ClientWriteError;
    use std::future::Future;
    use proptest::prelude::*;
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    #[tokio::test]
    async fn held_prefix_survives_arrival_during_yield_and_metadata_never_yields() {
        use std::future::poll_fn;
        use std::task::Poll;
        let slots = Arc::new(Semaphore::new(4));
        let bytes = Arc::new(Semaphore::new(400));
        let pending = |n: u8, method: &str| {
            let (result, receiver) = oneshot::channel();
            drop(receiver); // HTTP cancellation does not drop the held prefix.
            Pending { command:Command {method:method.into(),path:format!("/held/{n}"),
                headers:vec![],body:vec![n; n as usize],time:n as u64},result,
                permit:slots.clone().try_acquire_owned().unwrap(),
                bytes:bytes.clone().try_acquire_many_owned(100).unwrap() }
        };
        let (send, mut receive) = mpsc::channel(4);
        let first = pending(1,"POST");
        send.try_send(pending(2,"POST")).unwrap_or_else(|_|panic!("queue full"));
        let mut carry = None;
        let mut forming = Box::pin(take_ready(first,&mut receive,&mut carry));
        assert!(poll_fn(|cx|Poll::Ready(forming.as_mut().poll(cx))).await.is_pending());
        assert_eq!(send.capacity(),4,"ready prefix was drained BEFORE the yield");
        assert_eq!(slots.available_permits(),2);
        send.try_send(pending(3,"POST")).unwrap_or_else(|_|panic!("queue full"));
        send.try_send(pending(4,"DELETE")).unwrap_or_else(|_|panic!("queue full"));
        let (commands, completions) = forming.await;
        assert_eq!(commands.iter().map(|c|c.body.clone()).collect::<Vec<_>>(),vec![vec![1],vec![2,2],vec![3,3,3]]);
        assert_eq!(slots.available_permits(),0);
        assert_eq!(bytes.available_permits(),0);
        let mut metadata = Box::pin(take_ready(carry.take().unwrap(),&mut receive,&mut carry));
        let Poll::Ready((commands, metadata_completion)) = poll_fn(|cx|Poll::Ready(metadata.as_mut().poll(cx))).await
            else {panic!("metadata must not yield")};
        assert_eq!(commands.len(),1);
        assert_eq!(commands[0].path,"/held/4");
        assert_eq!(commands[0].method,"DELETE");
        drop(completions);
        assert_eq!(slots.available_permits(),3);
        drop(metadata_completion);
        assert_eq!(slots.available_permits(),4);
        assert_eq!(bytes.available_permits(),400);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn queued_epoch_fence_rejects_without_native_wal_or_receipt() {
        let dir = tempfile::tempdir().unwrap();
        let journal = Journal::open(dir.path().join("wal"), 256 * 1024).unwrap();
        let machine = Machine::open(dir.path().join("state"), journal.clone()).await.unwrap();
        let config = openraft::Config {
            enable_tick: false, heartbeat_interval: 1,
            election_timeout_min: 5, election_timeout_max: 10,
            ..Default::default()
        }.validate().unwrap();
        let raft = Raft::new(7, Arc::new(config), network::Network {
            group: 0, cluster: "epoch-test".into(), client: reqwest::Client::new(),
            faults: Arc::new(std::sync::RwLock::new(network::Faults::default())),
        }, journal.clone(), machine.clone()).await.unwrap();
        raft.initialize(std::collections::BTreeMap::from([(7, BasicNode::new("127.0.0.1:1"))])).await.unwrap();
        raft.wait(Some(Duration::from_secs(3))).current_leader(7, "initial leader").await.unwrap();
        let prepared = AtomicU64::new(0);
        let mut leader = prepare_epoch(&raft, &machine, &prepared).await.unwrap();
        let command = |n: u64| Command {method: "PUT".into(), path: format!("/epoch/{n}"),
            headers: vec![], body: vec![], time: 1000+n};
        let mut prior = raft.client_write_ff_with_leader(vec![command(0)].into(), leader)
            .await.unwrap().await.unwrap().unwrap().log_id;

        // Matching numeric index cannot excuse a stale term or a different node.
        for wrong in [CommittedLeaderId::new(leader.term-1,7), CommittedLeaderId::new(leader.term,8)] {
            let before = journal.shard.tail_lsn();
            let (send, flushed) = oneshot::channel();
            let rejected = raft.client_write_ff_with_leader(Batch {commands:vec![command(99)],
                durable:Some(Arc::new(Mutex::new(Some(send))))},wrong).await.unwrap().await.unwrap();
            assert!(matches!(rejected, Err(ClientWriteError::ForwardToLeader(_))));
            assert!(flushed.await.is_err(), "no local-durable receipt for a rejected proposal");
            assert_eq!(journal.shard.tail_lsn(), before, "no native WAL frame for epoch rejection");
        }

        for n in 1..=3 {
            // Tick is disabled; let the previous vote lease expire, then hold
            // the actual core while enqueuing a higher vote, election and
            // old-ticket write. No timing race decides the API queue order.
            tokio::time::sleep(Duration::from_millis(20)).await;
            let (entered, reached) = oneshot::channel();
            let (resume, blocked) = std::sync::mpsc::channel();
            raft.external_request(move |_| {let _=entered.send(()); let _=blocked.recv();});
            reached.await.unwrap();
            let vote = raft.vote(openraft::raft::VoteRequest::new(openraft::Vote::new(leader.term+1,8),Some(prior)));
            tokio::pin!(vote);
            std::future::poll_fn(|cx| {
                assert!(vote.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            }).await;
            raft.trigger().elect().await.unwrap();
            let (send, flushed) = oneshot::channel();
            let stale = raft.client_write_ff_with_leader(Batch {commands:vec![command(99)],
                durable:Some(Arc::new(Mutex::new(Some(send))))},leader).await.unwrap();
            resume.send(()).unwrap();
            assert!(vote.await.unwrap().vote_granted);
            assert!(matches!(stale.await.unwrap(), Err(ClientWriteError::ForwardToLeader(_))));
            assert!(flushed.await.is_err());
            raft.wait(Some(Duration::from_secs(3))).metrics(
                |m| m.current_leader == Some(7) && m.current_term > leader.term, "reelected in newer term",
            ).await.unwrap();
            let next = prepare_epoch(&raft, &machine, &prepared).await.unwrap();
            assert!(next.term > leader.term);
            assert_eq!(prepared.load(Ordering::Acquire),next.term);
            let committed = raft.client_write_ff_with_leader(vec![command(n)].into(),next)
                .await.unwrap().await.unwrap().unwrap();
            assert_eq!(committed.log_id.index,prior.index+2,"only election blank plus good write, no stale index");
            assert_eq!(committed.data[0].status,201);
            prior=committed.log_id;
            leader=next;
        }
        raft.shutdown().await.unwrap();
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(48))]
        #[test]
        fn fifo_bounds_singletons_and_canceled_receivers_keep_permits(
            count in 130usize..256, body_bytes in prop_oneof![0usize..512, 17000usize..80000],
            metadata_at in 1usize..69, cancel_every in 1usize..9,
            metadata_kind in 0usize..5,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let slots = Arc::new(Semaphore::new(CAPACITY));
                let bytes = Arc::new(Semaphore::new(64 * 1024 * 1024));
                let (send, mut receive) = mpsc::channel(CAPACITY);
                let mut receivers = Vec::new();
                let mut admitted_bytes = 0;
                for i in 0..count {
                    let command = Command {
                        method: if i == metadata_at || i == metadata_at+1 {
                            ["PUT","DELETE","SUB","FORK","TOUCH"][metadata_kind]
                        } else {"POST"}.into(),
                        path: format!("/fifo/{i}"), headers: vec![("content-type".into(),"application/octet-stream".into()),
                            ("stream-durability".into(),if i%2==0 {"local-fsync"} else {"quorum-fsync"}.into())],
                        body: vec![(i % 251) as u8; body_bytes+i], time: 1000+i as u64,
                    };
                    assert_eq!(command.encoded_len() as u64,bincode::serialized_size(&command).unwrap());
                    let size = command.encoded_len();
                    admitted_bytes += size;
                    let (result, receiver) = oneshot::channel();
                    send.try_send(Pending {command,result,permit:slots.clone().try_acquire_owned().unwrap(),
                        bytes:bytes.clone().try_acquire_many_owned(size as u32).unwrap()}).unwrap_or_else(|_|panic!("queue full"));
                    // Dropping HTTP receivers must not cancel queued appends.
                    receivers.push(if i % cancel_every == 0 {drop(receiver);None} else {Some(receiver)});
                }
                let mut carry = None;
                let mut offset = 0;
                while offset < count {
                    let first = carry.take().unwrap_or_else(||receive.try_recv().unwrap());
                    let (commands, mut completions) = take_ready(first,&mut receive,&mut carry).await;
                    for (ordinal, completion) in completions.iter_mut().enumerate() {
                        if completion.local {
                            let _ = completion.result.take().unwrap().send(Ok(Outcome::Accepted(Position {
                                log_id:LogId::new(openraft::CommittedLeaderId::new(7,2),offset as u64),ordinal,
                            })));
                        }
                    }
                    assert_eq!(slots.available_permits(),CAPACITY-count+offset,
                        "dequeue, 202 acceptance and HTTP cancellation do not free admission");
                    assert_eq!(bytes.available_permits(),64 * 1024 * 1024-admitted_bytes);
                    assert!(commands.len()<=MAX_COMMANDS);
                    assert!(bincode::serialized_size(&commands).unwrap()<=MAX_BYTES as u64);
                    if body_bytes < 512 && offset > metadata_at+1 {
                        // Remaining small appends hit the count ceiling, not
                        // metadata or bytes. Premature sealing/off-by-one fails.
                        assert_eq!(commands.len(),(count-offset).min(64));
                    }
                    let (durable, _receiver) = oneshot::channel();
                    let batch = Batch {commands:commands.clone(),durable:Some(Arc::new(Mutex::new(Some(durable))))};
                    let encoded=bincode::serialize(&batch).unwrap();
                    assert_eq!(encoded,bincode::serialize(&commands).unwrap(),"hook adds no WAL/network bytes");
                    assert!(bincode::deserialize::<Batch>(&encoded).unwrap().durable.is_none());
                    assert!(commands.len()==1 || commands.iter().all(|c|c.method=="POST"));
                    for (position,command) in commands.iter().enumerate() {
                        assert_eq!(command.path,format!("/fifo/{}",offset+position));
                        assert_eq!(command.body,vec![((offset+position)%251) as u8;body_bytes+offset+position]);
                    }
                    offset+=commands.len();
                    admitted_bytes-=commands.iter().map(Command::encoded_len).sum::<usize>();
                    drop(completions); // Consensus resolution, not HTTP lifetime.
                }
                assert_eq!(slots.available_permits(),CAPACITY);
                assert_eq!(bytes.available_permits(),64 * 1024 * 1024);
                assert!(carry.is_none() && receive.try_recv().is_err());
            });
        }
    }
}
