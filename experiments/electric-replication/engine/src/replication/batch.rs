//! Ordered admission and native-fsync amortization; see BATCHING.md.
use super::{Command, LogId, Raft, Reply};
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit};

pub(super) const CAPACITY: usize = 256;
const MAX_COMMANDS: usize = 64;
const MAX_BYTES: usize = 2 * 1024 * 1024;

pub(super) struct Committed {
    pub log_id: LogId<u64>,
    pub data: Reply,
}
type Completion = (oneshot::Sender<Result<Committed, u16>>, OwnedSemaphorePermit);

pub(super) struct Pending {
    pub command: Command,
    pub result: oneshot::Sender<Result<Committed, u16>>,
    pub permit: OwnedSemaphorePermit,
}

impl Command {
    /// Exact bincode-1 fixed-width size, without traversing every payload byte.
    /// A property compares this accounting against the actual serializer.
    fn encoded_len(&self) -> usize {
        40 + self.method.len() + self.path.len() + self.body.len()
            + self.headers.iter().map(|(k, v)| 16 + k.len() + v.len()).sum::<usize>()
    }
}

fn take_ready(
    first: Pending,
    receive: &mut mpsc::Receiver<Pending>,
    carry: &mut Option<Pending>,
) -> (Vec<Command>, Vec<Completion>) {
    let mut next = Some(first);
    let mut commands = Vec::new();
    let mut completions = Vec::new();
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
        commands.push(pending.command);
        completions.push((pending.result, pending.permit));
        if !append || commands.len() == MAX_COMMANDS {
            break;
        }
        next = receive.try_recv().ok();
    }
    (commands, completions)
}

pub(super) fn start(raft: Raft) -> mpsc::Sender<Pending> {
    let (send, mut receive) = mpsc::channel(CAPACITY);
    tokio::spawn(async move {
        let mut carry = None;
        loop {
            let first = match carry.take() {
                Some(pending) => pending,
                None => match receive.recv().await {
                    Some(pending) => pending,
                    None => break,
                },
            };
            let (commands, completions) = take_ready(first, &mut receive, &mut carry);
            match raft.client_write(commands).await {
                Ok(committed) => {
                    assert_eq!(committed.data.len(), completions.len(), "per-command apply replies");
                    for (data, (result, _permit)) in committed.data.into_iter().zip(completions) {
                        let _ = result.send(Ok(Committed { log_id: committed.log_id, data }));
                    }
                }
                Err(_) => {
                    for (result, _permit) in completions {
                        let _ = result.send(Err(503));
                    }
                }
            }
        }
    });
    send
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(48))]
        #[test]
        fn fifo_bounds_singletons_and_canceled_receivers_keep_permits(
            count in 70usize..160, body_bytes in 1000usize..80000,
            metadata_at in 1usize..69, cancel_every in 1usize..9,
            metadata_kind in 0usize..5,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let slots = Arc::new(Semaphore::new(CAPACITY));
                let (send, mut receive) = mpsc::channel(CAPACITY);
                let mut receivers = Vec::new();
                for i in 0..count {
                    let command = Command {
                        method: if i == metadata_at || i == metadata_at+1 {
                            ["PUT","DELETE","SUB","FORK","TOUCH"][metadata_kind]
                        } else {"POST"}.into(),
                        path: format!("/fifo/{i}"), headers: vec![("content-type".into(),"application/octet-stream".into())],
                        body: vec![(i % 251) as u8; body_bytes+i], time: 1000+i as u64,
                    };
                    assert_eq!(command.encoded_len() as u64,bincode::serialized_size(&command).unwrap());
                    let (result, receiver) = oneshot::channel();
                    send.try_send(Pending {command,result,permit:slots.clone().try_acquire_owned().unwrap()}).unwrap_or_else(|_|panic!("queue full"));
                    // Dropping HTTP receivers must not cancel queued appends.
                    receivers.push(if i % cancel_every == 0 {drop(receiver);None} else {Some(receiver)});
                }
                let mut carry = None;
                let mut offset = 0;
                while offset < count {
                    let first = carry.take().unwrap_or_else(||receive.try_recv().unwrap());
                    let (commands, completions) = take_ready(first,&mut receive,&mut carry);
                    assert_eq!(slots.available_permits(),CAPACITY-count+offset,
                        "dequeue and HTTP cancellation do not free admission");
                    assert!(commands.len()<=MAX_COMMANDS);
                    assert!(bincode::serialized_size(&commands).unwrap()<=MAX_BYTES as u64);
                    assert!(commands.len()==1 || commands.iter().all(|c|c.method=="POST"));
                    for (position,command) in commands.iter().enumerate() {
                        assert_eq!(command.path,format!("/fifo/{}",offset+position));
                        assert_eq!(command.body,vec![((offset+position)%251) as u8;body_bytes+offset+position]);
                    }
                    offset+=commands.len();
                    drop(completions); // Consensus resolution, not HTTP lifetime.
                }
                assert_eq!(slots.available_permits(),CAPACITY);
                assert!(carry.is_none() && receive.try_recv().is_err());
            });
        }
    }
}
