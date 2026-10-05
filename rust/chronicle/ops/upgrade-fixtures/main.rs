use chronicle_raft::{TypeConfig, model::*, storage::SqliteStore};
use openraft::storage::{RaftLogStorage, RaftLogStorageExt, RaftStateMachine};
use openraft::{
    BasicNode, CommittedLeaderId, Entry, EntryPayload, LogId, Membership, RaftSnapshotBuilder, Vote,
};

fn entry(index: u64, payload: EntryPayload<TypeConfig>) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(7, 9), index),
        payload,
    }
}
fn append(seq: u64, data: &[u8]) -> Command {
    Command::Append {
        key: "fixture".into(),
        incarnation: 1,
        data: data.into(),
        producer: Some(Producer {
            id: "p".into(),
            epoch: 3,
            seq,
        }),
        close: false,
        empty_body: false,
        stream_seq: None,
        now_ms: None,
    }
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let output = std::env::args().nth(1).unwrap();
    for name in ["replay", "snapshot-joint", "absent-vote", "zero-vote"] {
        let mut store = SqliteStore::open(format!("{output}/{name}.sqlite")).await?;
        if name.ends_with("vote") {
            if name == "zero-vote" {
                store.save_vote(&Vote::new(0, 0)).await?;
            }
            store.close().await;
            continue;
        }
        let nodes = [1, 2, 3, 9]
            .into_iter()
            .map(|id| (id, BasicNode::new(format!("fixture-{id}.invalid"))));
        let membership = Membership::new(
            vec![[1, 2, 9].into()],
            nodes.collect::<std::collections::BTreeMap<_, _>>(),
        );
        let mut entries = vec![
            entry(0, EntryPayload::Membership(membership)),
            entry(
                1,
                EntryPayload::Normal(Command::Create {
                    key: "fixture".into(),
                    expected_incarnation: None,
                    config: StreamConfig {
                        content_type: "application/octet-stream".into(),
                        track_boundaries: true,
                        json_framing: Some(false),
                        expiry: None,
                    },
                    data: b"base".to_vec(),
                    closed: false,
                    now_ms: None,
                }),
            ),
            entry(2, EntryPayload::Normal(append(0, b"-ack"))),
            entry(3, EntryPayload::Normal(append(1, b"-replay"))),
        ];
        if name == "snapshot-joint" {
            let nodes = [1, 2, 3, 9]
                .into_iter()
                .map(|id| (id, BasicNode::new(format!("fixture-{id}.invalid"))));
            entries.push(entry(
                4,
                EntryPayload::Membership(Membership::new(
                    vec![[1, 2, 9].into(), [1, 3, 9].into()],
                    nodes.collect::<std::collections::BTreeMap<_, _>>(),
                )),
            ));
        }
        let committed = entries.last().unwrap().log_id;
        entries.push(entry(
            committed.index + 1,
            EntryPayload::Normal(append(2, b"-UNCOMMITTED")),
        ));
        store.blocking_append(entries.clone()).await?;
        store.save_vote(&Vote::new_committed(7, 9)).await?;
        store.apply(entries[..3].to_vec()).await?;
        if name == "snapshot-joint" {
            store.build_snapshot().await?;
            store.purge(entries[2].log_id).await?;
        }
        store.save_committed(Some(committed)).await?;
        store.close().await;
    }
    Ok(())
}
