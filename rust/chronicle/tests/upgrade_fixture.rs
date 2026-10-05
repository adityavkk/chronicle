//! Bounded old-serializer qualification, not mixed-version wire or WAL testing.
use chronicle_raft::network::RpcError;
use chronicle_raft::{Raft, TypeConfig, Vote, model::*, storage::SqliteStore};
use openraft::network::RPCOption;
use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse};
use openraft::storage::{RaftLogStorage, RaftStateMachine};
use openraft::{BasicNode, RaftLogReader, RaftNetworkFactory, RaftSnapshotBuilder};
use openraft_legacy::network_v1::{
    Adapter, InstallSnapshotError, InstallSnapshotRequest, InstallSnapshotResponse, RaftNetwork,
};
use std::{sync::Arc, time::Duration};

// No HTTP client, sockets, resolver, or live cluster addresses exist here.
// Pending RPCs are cancellable; no quorum is fabricated for the joint fixture.
struct NoNetwork;
impl RaftNetworkFactory<TypeConfig> for NoNetwork {
    type Network = Adapter<TypeConfig, Self, std::io::Cursor<Vec<u8>>>;
    async fn new_client(&mut self, _: u64, _: &BasicNode) -> Self::Network {
        Adapter::new(Self)
    }
}
impl RaftNetwork<TypeConfig> for NoNetwork {
    async fn append_entries(
        &mut self,
        _: AppendEntriesRequest<TypeConfig>,
        _: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RpcError> {
        std::future::pending().await
    }
    async fn vote(
        &mut self,
        _: VoteRequest<TypeConfig>,
        _: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RpcError> {
        std::future::pending().await
    }
    async fn install_snapshot(
        &mut self,
        _: InstallSnapshotRequest<TypeConfig>,
        _: RPCOption,
    ) -> Result<InstallSnapshotResponse<TypeConfig>, RpcError<InstallSnapshotError>> {
        std::future::pending().await
    }
}

fn restore(path: &std::path::Path, sql: &str) {
    rusqlite::Connection::open(path)
        .unwrap()
        .execute_batch(sql)
        .unwrap();
}
fn config() -> Arc<openraft::Config> {
    Arc::new(
        openraft::Config {
            enable_tick: false,
            enable_elect: false,
            enable_heartbeat: false,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    )
}
fn old_sequence() -> Command {
    Command::Append {
        key: "fixture".into(),
        incarnation: 1,
        data: b"-ack".to_vec(),
        producer: Some(Producer {
            id: "p".into(),
            epoch: 3,
            seq: 0,
        }),
        close: false,
        empty_body: false,
        stream_seq: None,
        now_ms: None,
    }
}
async fn assert_state(store: &SqliteStore, replayed: bool) {
    let mut state = store.read_state().await.unwrap();
    let stream = &state.streams["fixture"];
    assert_eq!(
        stream.data,
        if replayed {
            b"base-ack-replay".as_slice()
        } else {
            b"base-ack".as_slice()
        }
    );
    assert_eq!(stream.incarnation, 1);
    assert_eq!(stream.producers["p"].results[&0], 8);
    assert_eq!(stream.producers["p"].seq, u64::from(replayed));
    assert_eq!(stream.producers["p"].end, if replayed { 15 } else { 8 });
    assert_eq!(
        stream.append_ends,
        if replayed { vec![4, 8, 15] } else { vec![4, 8] }
    );
    assert!(!stream.producers["p"].results.contains_key(&2));
    // Local deterministic probe only: this is NOT a quorum-backed fresh read/write.
    let cached = state.apply(&old_sequence());
    assert_eq!(
        cached,
        Outcome {
            end: 8,
            incarnation: 1,
            duplicate: true,
            closed: false,
            producer: Some(ProducerPosition {
                epoch: 3,
                seq: u64::from(replayed)
            }),
            content_type: None,
            error: None
        }
    );
    assert_eq!(state.streams["fixture"].data, stream_bytes(replayed));
}
fn stream_bytes(replayed: bool) -> Vec<u8> {
    if replayed {
        b"base-ack-replay".to_vec()
    } else {
        b"base-ack".to_vec()
    }
}

#[tokio::test]
async fn old_committed_prefix_replays_without_committing_suffix_or_fabricating_quorum() {
    for (sql, joint, committed, last) in [
        (include_str!("fixtures/openraft09/replay.sql"), false, 3, 4),
        (
            include_str!("fixtures/openraft09/snapshot-joint.sql"),
            true,
            4,
            5,
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft.sqlite");
        restore(&path, sql);
        let mut store = SqliteStore::open_existing(&path).await.unwrap();
        assert_state(&store, false).await;
        assert_eq!(
            store.read_vote().await.unwrap(),
            Some(Vote::new_committed(7, 9))
        );
        let (applied, membership) = store.applied_state().await.unwrap();
        assert_eq!(applied.unwrap().index, 2);
        assert_eq!(membership.log_id().unwrap().index, 0);
        let bounds = store.get_log_state().await.unwrap();
        assert_eq!(
            bounds.last_purged_log_id.map(|id| id.index),
            joint.then_some(2)
        );
        assert_eq!(bounds.last_log_id.unwrap().index, last);
        assert_eq!(
            store.read_committed().await.unwrap().unwrap().index,
            committed
        );
        let entries = store.try_get_log_entries(..).await.unwrap();
        assert_eq!(
            entries.first().unwrap().log_id.index,
            if joint { 3 } else { 0 }
        );
        assert!(
            entries
                .iter()
                .all(|entry| entry.log_id.leader_id == applied.unwrap().leader_id)
        );
        if joint {
            let old = store.get_current_snapshot().await.unwrap().unwrap();
            assert_eq!(old.meta.last_log_id.unwrap().index, 2);
            let mut installed = SqliteStore::open(dir.path().join("old-snapshot"))
                .await
                .unwrap();
            installed
                .install_snapshot(&old.meta, old.snapshot)
                .await
                .unwrap();
            assert_state(&installed, false).await;
            installed.close().await;
        }
        // Recover node 1, whereas the persisted committed leader is node 9.
        let raft = Raft::new(1, config(), NoNetwork, store.clone(), store.clone())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if store.applied_state().await.unwrap().0.map(|id| id.index) == Some(committed) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_state(&store, true).await;
        let (applied, membership) = store.applied_state().await.unwrap();
        assert_eq!(
            applied.unwrap().leader_id,
            Vote::new_committed(7, 9).leader_id
        );
        assert_eq!(
            membership.log_id().unwrap().index,
            if joint { 4 } else { 0 }
        );
        let expected = if joint {
            vec![vec![1, 2, 9], vec![1, 3, 9]]
        } else {
            vec![vec![1, 2, 9]]
        };
        assert_eq!(
            membership
                .membership()
                .get_joint_config()
                .iter()
                .map(|set| set.iter().copied().collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            store.read_committed().await.unwrap().unwrap().index,
            committed
        );
        assert_eq!(
            store
                .get_log_state()
                .await
                .unwrap()
                .last_log_id
                .unwrap()
                .index,
            last
        );
        raft.shutdown().await.unwrap();
        drop(raft);
        assert_eq!(store.get_log_state().await.unwrap(), bounds);
        assert_eq!(
            serde_json::to_value(store.try_get_log_entries(..).await.unwrap()).unwrap(),
            serde_json::to_value(entries).unwrap()
        );
        // Candidate snapshot installation and reopen must preserve the same state
        // and applied/membership envelope as replay, not the uncommitted suffix.
        let snapshot = store.build_snapshot().await.unwrap();
        let installed_path = dir.path().join("candidate-snapshot");
        let mut installed = SqliteStore::open(&installed_path).await.unwrap();
        installed
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .unwrap();
        installed.close().await;
        let mut installed = SqliteStore::open_existing(&installed_path).await.unwrap();
        assert_state(&installed, true).await;
        assert_eq!(
            installed.applied_state().await.unwrap(),
            (applied, membership)
        );
        assert_eq!(
            serde_json::to_value(installed.read_state().await.unwrap()).unwrap(),
            serde_json::to_value(store.read_state().await.unwrap()).unwrap()
        );
        installed.close().await;
        store.close().await;
        let mut reopened = SqliteStore::open_existing(&path).await.unwrap();
        assert_state(&reopened, true).await;
        assert_eq!(reopened.applied_state().await.unwrap().0, applied);
        reopened.close().await;
    }
}

#[tokio::test]
async fn absent_vote_is_distinct_from_persisted_uncommitted_zero_vote() {
    for (sql, expected) in [
        (include_str!("fixtures/openraft09/absent-vote.sql"), None),
        (
            include_str!("fixtures/openraft09/zero-vote.sql"),
            Some(Vote::new(0, 0)),
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft.sqlite");
        restore(&path, sql);
        let mut store = SqliteStore::open_existing(&path).await.unwrap();
        assert_eq!(store.read_vote().await.unwrap(), expected);
        let raft = Raft::new(1, config(), NoNetwork, store.clone(), store.clone())
            .await
            .unwrap();
        assert_eq!(store.read_vote().await.unwrap(), expected);
        assert_eq!(store.applied_state().await.unwrap().0, None);
        assert_eq!(store.get_log_state().await.unwrap().last_log_id, None);
        raft.shutdown().await.unwrap();
        drop(raft);
        store.close().await;
        let mut reopened = SqliteStore::open_existing(&path).await.unwrap();
        assert_eq!(reopened.read_vote().await.unwrap(), expected);
        reopened.close().await;
    }
}
