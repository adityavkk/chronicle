use super::*;
use crate::{App, Group, Network, Raft, identity, telemetry::Telemetry};
use axum::{
    Router,
    routing::{get, post},
};
use chronicle_raft::{model::Node, storage::SqliteStore};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
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
            // Fixture placements have already cooled; exercise real elections
            // without spending a wall-clock minute on advisory policy timing.
            now_ms: now_ms().saturating_sub(60_001),
            eligible_only: false,
            repair_pending: false,
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removed_control_leader_completes_data_placement_and_retires() {
    regression(Scenario::Retirement).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removed_control_replica_discovers_claim_owner_and_transfers_once() {
    regression(Scenario::Transfer).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_claim_reply_never_reconstructs_permission() {
    regression(Scenario::LostReply).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn changed_admission_after_claim_spends_permission_without_submission() {
    regression(Scenario::AfterClaimDrain).await;
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Scenario {
    Retirement,
    Transfer,
    LostReply,
    AfterClaimDrain,
}

async fn regression(scenario: Scenario) {
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
        let transfers: Arc<[AtomicUsize; 4]> = Arc::new(std::array::from_fn(|_| AtomicUsize::new(0)));
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
            let count = transfers.clone();
            let router = Router::new()
                .route("/raft/{group}/append", post(crate::append_rpc))
                .route("/raft/{group}/vote", post(crate::vote_rpc))
                .route("/raft/{group}/snapshot", post(crate::snapshot_rpc))
                .route("/raft/{group}/transfer", post(move |
                    axum::extract::State(a): axum::extract::State<Shared>,
                    axum::extract::Path(group): axum::extract::Path<u64>,
                    headers: axum::http::HeaderMap,
                    axum::Json(request): axum::Json<openraft::raft::TransferLeaderRequest<TypeConfig>>,
                | {
                    if group == 1 { count[id as usize - 1].fetch_add(1, Ordering::SeqCst); }
                    eprintln!("transfer delivery: group={group}, recipient={id}, source={}, target={}, boundary={:?}",
                        request.from_leader(), request.to_node_id(), request.last_log_id());
                    async move { crate::transfer_rpc(axum::extract::State(a), axum::extract::Path(group), headers, axum::Json(request)).await }
                }))
                .route("/admin/status", get(crate::status))
                .route("/admin/control", get(crate::control))
                .route("/admin/resources", get(crate::resources))
                .route("/admin/leadership/{group}", get(crate::leader_balance::observe))
                .route("/admin/leadership/claim", post(move |
                    axum::extract::State(a): axum::extract::State<Shared>,
                    headers: axum::http::HeaderMap,
                    axum::Json(request): axum::Json<crate::leader_balance::Claim>,
                | async move {
                    let response = crate::leader_balance::claim(axum::extract::State(a.clone()), headers, axum::Json(request)).await?;
                    if scenario == Scenario::LostReply { return Err(crate::unavailable("claim committed; reply discarded")) }
                    if scenario == Scenario::AfterClaimDrain {
                        let mut node = a.nodes[&2].clone();
                        node.draining = true;
                        write(&a, Command::Register { id: 2, node }).await;
                    }
                    Ok(response)
                }))
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

        // The pinned upstream step-down watcher may broadcast a transfer when
        // node 1 leaves the effective membership. No application campaign is
        // requested. Node 1 must discover the successor through HTTP status.
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
        if scenario != Scenario::Retirement {
            use chronicle_raft::leadership::{Operation, Phase, Proposal};
            write(leader, Command::Register { id: 1, node: nodes[&1].clone() }).await;
            let view = crate::leader_balance::view(&old.groups[&1]).await.unwrap().unwrap();
            apps[&2].groups[&1].raft.wait(Some(Duration::from_secs(3))).metrics(
                |m| m.last_applied >= view.applied, "target caught up",
            ).await.unwrap();
            write(leader, Command::Leadership(Operation::Plan { expected_id: 0,
                proposal: Proposal { shard: 1, generation: 1, source_vote: view.vote,
                    membership: view.membership, target: 2, created_ms: now_ms() },
            })).await;
            let state = control(old).await.unwrap();
            let result = crate::leader_balance::reconcile(old, &state, true).await;
            assert_eq!(result.is_err(), scenario == Scenario::LostReply, "{result:?}");
            let state = control(old).await.unwrap();
            assert_eq!(state.leadership.attempt.as_ref().unwrap().phase, Phase::Claimed);
            assert_eq!(state.leadership.consumed[&1], view.vote);
            // Fresh invocations hold no executor-local permission, including
            // concurrent reconciliation after an ambiguous response.
            let (one, two) = tokio::join!(
                crate::leader_balance::reconcile(old, &state, true),
                crate::leader_balance::reconcile(old, &state, true),
            );
            one.unwrap(); two.unwrap();
            if scenario == Scenario::Transfer {
                let target = &apps[&2].groups[&1].raft;
                target.wait(Some(Duration::from_secs(5))).metrics(
                    |m| m.state == openraft::ServerState::Leader && m.last_quorum_acked.is_some(),
                    "directed target quorum ready",
                ).await.unwrap();
                target.ensure_linearizable(openraft::ReadPolicy::ReadIndex).await.unwrap();
                let state = control(leader).await.unwrap();
                crate::leader_balance::reconcile(leader, &state, true).await.unwrap();
                let state = control(leader).await.unwrap();
                assert_eq!(state.leadership.attempt.unwrap().phase, Phase::ObservedTarget);
                assert_eq!(old.telemetry.leadership_submissions[1].load(Ordering::Relaxed), 1);
                assert_eq!(transfers.each_ref().map(|n| n.load(Ordering::SeqCst)), [0, 1, 1, 0]);
            } else {
                tokio::time::sleep(Duration::from_millis(400)).await;
                assert_eq!(old.telemetry.leadership_submissions[1].load(Ordering::Relaxed), 0);
                assert_eq!(transfers.each_ref().map(|n| n.load(Ordering::SeqCst)), [0; 4]);
                assert_eq!(old.groups[&1].raft.metrics().borrow_watched().state, openraft::ServerState::Leader);
            }
        } else {
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
                false,
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
        }
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
