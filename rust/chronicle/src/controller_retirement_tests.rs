use super::*;
use crate::{App, Group, Network, Raft, identity, telemetry::Telemetry};
use axum::{
    Router,
    routing::{get, post},
};
use chronicle_raft::{model::Node, storage::SqliteStore};
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};

async fn write(app: &Shared, command: Command) {
    let response = app.groups[&0].raft.client_write(command).await.unwrap();
    assert!(response.data.error.is_none(), "{:?}", response.data.error);
}

async fn place(app: &Shared, shard: u64, generation: u64, voters: BTreeSet<u64>) {
    write(
        app,
        Command::Place {
            shard,
            expected_generation: generation,
            voters,
            now_ms: now_ms(),
            eligible_only: false,
            repair_pending: false,
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removed_control_leader_completes_data_placement_and_retires() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let dir = tempfile::tempdir().unwrap();
        let client = reqwest::Client::new();
        let mut listeners = Vec::new();
        let mut nodes = BTreeMap::new();
        for id in 1..=4 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            nodes.insert(
                id,
                Node {
                    addr: listener.local_addr().unwrap().to_string(),
                    zone: id.to_string(),
                    draining: false,
                },
            );
            listeners.push((id, listener));
        }
        let (logs, _guard) = tracing_appender::non_blocking(std::io::sink());
        let mut apps = BTreeMap::new();
        let mut servers = tokio::task::JoinSet::new();
        for (id, listener) in listeners {
            let mut groups = BTreeMap::new();
            for shard in 0..=SHARDS {
                let store = SqliteStore::open(dir.path().join(format!("{id}-{shard}.sqlite")))
                    .await
                    .unwrap();
                let raft = Raft::new(
                    id,
                    Arc::new(openraft::Config::default().validate().unwrap()),
                    Network {
                        client: client.clone(),
                        cluster: "retirement-test".into(),
                        group: shard,
                    },
                    store.clone(),
                    store.clone(),
                )
                .await
                .unwrap();
                groups.insert(
                    shard,
                    Group {
                        raft,
                        store,
                        movement: Mutex::new(()),
                    },
                );
            }
            let app = Arc::new(App {
                id,
                stream_tenant: None,
                identity: identity::Identity {
                    node: id,
                    cluster: "retirement-test".into(),
                    genesis: true,
                },
                nodes: nodes.clone(),
                groups,
                client: client.clone(),
                admission: Arc::new(Semaphore::new(16)),
                live_admission: Arc::new(Semaphore::new(16)),
                telemetry: Arc::new(Telemetry::new(id, logs.error_counter(), String::new())),
            });
            let router = Router::new()
                .route("/raft/{group}/append", post(crate::append_rpc))
                .route("/raft/{group}/vote", post(crate::vote_rpc))
                .route("/raft/{group}/snapshot", post(crate::snapshot_rpc))
                .route("/raft/{group}/transfer", post(crate::transfer_rpc))
                .route("/admin/status", get(crate::status))
                .route("/admin/control", get(crate::control))
                .route("/admin/placed", post(crate::placed))
                .route("/admin/retirement-state", get(crate::retirement_state))
                .with_state(app.clone());
            servers.spawn(async move { axum::serve(listener, router).await.unwrap() });
            apps.insert(id, app);
        }
        let old = &apps[&1];
        // Single-node bootstrap fixes the original leader without campaigns.
        // All subsequent membership changes use real replication and SQLite apply.
        for group in old.groups.values() {
            group
                .raft
                .initialize(BTreeMap::from([(
                    1,
                    BasicNode::new(nodes[&1].addr.clone()),
                )]))
                .await
                .unwrap();
            group
                .raft
                .wait(Some(Duration::from_secs(5)))
                .state(openraft::ServerState::Leader, "bootstrap")
                .await
                .unwrap();
            for id in 2..=4 {
                group
                    .raft
                    .add_learner(id, BasicNode::new(nodes[&id].addr.clone()), true)
                    .await
                    .unwrap();
            }
            let vote = group.raft.metrics().borrow_watched().vote;
            commit_membership(group, &BTreeSet::from([1, 2, 3]), vote)
                .await
                .unwrap();
        }
        for (id, node) in &nodes {
            write(
                old,
                Command::Register {
                    id: *id,
                    node: node.clone(),
                },
            )
            .await;
        }
        for shard in 0..=SHARDS {
            place(old, shard, 0, BTreeSet::from([1, 2, 3])).await;
            let group = &old.groups[&shard];
            let vote = group.raft.metrics().borrow_watched().vote;
            let boundary = commit_membership(group, &BTreeSet::from([1, 2, 3]), vote)
                .await
                .unwrap();
            write(
                old,
                Command::Placed {
                    shard,
                    generation: 1,
                    membership: Some(boundary),
                },
            )
            .await;
        }
        let mut draining = nodes[&1].clone();
        draining.draining = true;
        write(
            old,
            Command::Register {
                id: 1,
                node: draining,
            },
        )
        .await;
        place(old, 0, 1, BTreeSet::from([2, 3, 4])).await;
        let group = &old.groups[&0];
        let vote = group.raft.metrics().borrow_watched().vote;
        let boundary = commit_membership(group, &BTreeSet::from([2, 3, 4]), vote)
            .await
            .unwrap();
        write(
            old,
            Command::Placed {
                shard: 0,
                generation: 2,
                membership: Some(boundary),
            },
        )
        .await;
        let state = control(old).await.unwrap();
        let metrics = group.raft.metrics().borrow_watched().clone();
        assert_eq!(metrics.state, openraft::ServerState::Leader);
        assert!(demotion_applied(&metrics, boundary));
        assert!(!replica_retired(&state.placements[&0], &metrics));
        assert!(retire_replica(old, 0, &state, &mut 0).await.unwrap());
        group
            .raft
            .wait(Some(Duration::from_secs(5)))
            .state(openraft::ServerState::Learner, "self removal")
            .await
            .unwrap();
        assert_eq!(
            group.raft.metrics().borrow_watched().current_leader,
            Some(1)
        );
        assert_eq!(
            old.groups[&1].raft.metrics().borrow_watched().state,
            openraft::ServerState::Leader
        );

        // Let the surviving control voters elect naturally; node 1 no longer
        // receives their log and must discover this leader through HTTP status.
        let leader = loop {
            if let Some(app) = apps.values().find(|a| {
                a.id != 1
                    && a.groups[&0].raft.metrics().borrow_watched().state
                        == openraft::ServerState::Leader
            }) {
                break app;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        leader.groups[&0]
            .raft
            .ensure_linearizable(openraft::ReadPolicy::ReadIndex)
            .await
            .unwrap();
        let mut campaigns = BTreeMap::new();
        let mut retirement = Retirement::default();
        for shard in 1..=SHARDS {
            place(leader, shard, 1, BTreeSet::from([2, 3, 4])).await;
            tick(
                old,
                &mut campaigns,
                false,
                &mut retirement,
                &mut chronicle_raft::balance::Window::default(),
            )
            .await
            .unwrap();
            let remote = leader.groups[&0].store.read_state().await.unwrap();
            assert!(remote.placements[&shard].retirement_known());
            assert_eq!(remote.placements[&shard].generation, 2);
            // The completion cannot have come from node 1's stale local state.
            assert_eq!(
                group.store.read_state().await.unwrap().placements[&shard].generation,
                1
            );
            let metrics = old.groups[&shard].raft.metrics().borrow_watched().clone();
            assert_eq!(metrics.state, openraft::ServerState::Leader);
            assert!(!replica_retired(&remote.placements[&shard], &metrics));
        }
        assert!(!retired(leader, 1).await.unwrap());
        let state = control(old).await.unwrap();
        for shard in 1..=SHARDS {
            assert!(retire_replica(old, shard, &state, &mut 0).await.unwrap());
            old.groups[&shard]
                .raft
                .wait(Some(Duration::from_secs(5)))
                .state(openraft::ServerState::Learner, "data self removal")
                .await
                .unwrap();
        }
        assert!(retired(leader, 1).await.unwrap());
        let mut stores = Vec::new();
        for app in apps.values() {
            for group in app.groups.values() {
                group.raft.shutdown().await.unwrap();
                stores.push(group.store.clone());
            }
        }
        servers.abort_all();
        while servers.join_next().await.is_some() {}
        drop(apps);
        for store in stores {
            store.close().await;
        }
    })
    .await
    .unwrap();
}
