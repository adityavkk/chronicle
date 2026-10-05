//! Reused HTTP pool; RPC timeouts and bounded message bodies belong to the caller.
use crate::{TypeConfig, metrics::Histogram};
use openraft::error::{RPCError, RaftError, RemoteError, Unreachable};
use openraft::errors::decompose::DecomposeResult;
use openraft::network::RPCOption;
use openraft::raft::*;
use openraft::{BasicNode, RaftNetworkFactory};
use openraft_legacy::network_v1::{
    Adapter, InstallSnapshotError, InstallSnapshotRequest, InstallSnapshotResponse, RaftNetwork,
};
use serde::{Serialize, de::DeserializeOwned};
use std::time::Instant;

static RPC: Histogram = Histogram::new();

pub fn timing_metrics(text: &mut String) {
    RPC.render("chronicle_rpc_duration_seconds", text);
}

pub type RpcError<E = openraft::error::Infallible> = RPCError<TypeConfig, RaftError<TypeConfig, E>>;

#[derive(Clone)]
pub struct Network {
    pub client: reqwest::Client,
    pub cluster: String,
    pub group: u64,
}
pub struct Connection {
    network: Network,
    id: u64,
    node: BasicNode,
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = Adapter<TypeConfig, Connection, std::io::Cursor<Vec<u8>>>;
    async fn new_client(&mut self, id: u64, node: &BasicNode) -> Self::Network {
        Adapter::new(Connection {
            network: self.clone(),
            id,
            node: node.clone(),
        })
    }
}

impl Connection {
    async fn rpc<Q: Serialize, R: DeserializeOwned, E: std::error::Error + DeserializeOwned>(
        &self,
        method: &str,
        req: Q,
    ) -> Result<R, RPCError<TypeConfig, E>> {
        let url = format!(
            "http://{}/raft/{}/{}",
            self.node.addr, self.network.group, method
        );
        let started = Instant::now();
        let result = async {
            let response = self
                .network
                .client
                .post(url)
                .header("x-chronicle-cluster", &self.network.cluster)
                .header("x-chronicle-recipient", self.id)
                .json(&req)
                .send()
                .await;
            // Network means immediate retry in OpenRaft. An unusable endpoint
            // must activate its per-replication-worker backoff instead.
            let response = response.map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
            let response = response
                .error_for_status()
                .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
            let result: Result<R, E> = response
                .json()
                .await
                .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
            result.map_err(|e| RPCError::RemoteError(RemoteError::new(self.id, e)))
        }
        .await;
        RPC.observe(started.elapsed());
        result
    }
}

impl RaftNetwork<TypeConfig> for Connection {
    fn backoff(&self) -> openraft::network::Backoff {
        // Keep the existing retry floor rather than the legacy adapter's new
        // shorter default; unavailable endpoints must not produce retry bursts.
        openraft::network::Backoff::new(std::iter::repeat(std::time::Duration::from_millis(500)))
    }

    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RpcError> {
        self.rpc("append", req).await
    }
    async fn vote(
        &mut self,
        req: VoteRequest<TypeConfig>,
        _: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RpcError> {
        self.rpc("vote", req).await
    }
    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _: RPCOption,
    ) -> Result<InstallSnapshotResponse<TypeConfig>, RpcError<InstallSnapshotError>> {
        self.rpc("snapshot", req).await
    }

    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest<TypeConfig>,
        _: RPCOption,
    ) -> Result<TransferLeaderResponse<TypeConfig>, RPCError<TypeConfig>> {
        // Unlike append/vote, the v1 transfer method already returns the v2
        // error type. Preserve API rejection in the response, back off on fatal.
        self.rpc::<_, _, RaftError<TypeConfig>>("transfer", req)
            .await
            .decompose_infallible()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Raft, storage::SqliteStore};
    use axum::{Json, Router, http::StatusCode, response::IntoResponse, routing::post};
    use openraft::type_config::async_runtime::WatchReceiver;
    use std::{
        collections::BTreeMap,
        sync::{
            Arc, Mutex,
            atomic::{AtomicU8, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    // Actual protocol recipients, not canned consensus responses. Faults are
    // injected only at the HTTP boundary before append/transfer executes.
    #[derive(Default)]
    struct Traffic {
        mode: AtomicU8,
        attempts: Mutex<Vec<Instant>>,
        active: AtomicUsize,
        peak: AtomicUsize,
        transfers: AtomicUsize,
    }

    struct InFlight(Arc<Traffic>);
    impl Drop for InFlight {
        fn drop(&mut self) {
            self.0.active.fetch_sub(1, Ordering::SeqCst);
        }
    }

    struct Peers {
        raft: Vec<Raft>,
        stores: Vec<SqliteStore>,
        nodes: BTreeMap<u64, BasicNode>,
        traffic: Vec<Arc<Traffic>>,
        servers: Vec<tokio::task::JoinHandle<()>>,
        network: Network,
        _dir: tempfile::TempDir,
    }

    impl Peers {
        async fn new() -> Self {
            Self::configured(
                2,
                openraft::Config {
                    heartbeat_interval: 200,
                    election_timeout_min: 2000,
                    election_timeout_max: 3000,
                    // No periodic campaigning: transfer must cause the election.
                    enable_elect: false,
                    ..Default::default()
                },
            )
            .await
        }

        async fn configured(count: u64, config: openraft::Config) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let network = Network {
                client: reqwest::Client::builder().no_proxy().build().unwrap(),
                cluster: "real-transfer".into(),
                group: 1,
            };
            let config = Arc::new(config.validate().unwrap());
            let mut peers = Self {
                raft: Vec::new(),
                stores: Vec::new(),
                nodes: BTreeMap::new(),
                traffic: Vec::new(),
                servers: Vec::new(),
                network,
                _dir: dir,
            };
            for id in 1..=count {
                let store = SqliteStore::open(peers._dir.path().join(id.to_string()))
                    .await
                    .unwrap();
                let raft = Raft::new(
                    id,
                    config.clone(),
                    peers.network.clone(),
                    store.clone(),
                    store.clone(),
                )
                .await
                .unwrap();
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                peers.nodes.insert(
                    id,
                    BasicNode::new(listener.local_addr().unwrap().to_string()),
                );
                let traffic = Arc::new(Traffic::default());
                let (append_peer, vote_peer, transfer_peer) =
                    (raft.clone(), raft.clone(), raft.clone());
                let (append_traffic, transfer_traffic) = (traffic.clone(), traffic.clone());
                let router = Router::new()
                    .route("/raft/1/append", post(move |headers: axum::http::HeaderMap, Json(req): Json<AppendEntriesRequest<TypeConfig>>| {
                        let (peer, traffic) = (append_peer.clone(), append_traffic.clone());
                        async move {
                            assert_eq!(headers["x-chronicle-cluster"], "real-transfer");
                            assert_eq!(headers["x-chronicle-recipient"], id.to_string());
                            traffic.attempts.lock().unwrap().push(Instant::now());
                            let active = traffic.active.fetch_add(1, Ordering::SeqCst) + 1;
                            traffic.peak.fetch_max(active, Ordering::SeqCst);
                            let _guard = InFlight(traffic.clone());
                            match traffic.mode.load(Ordering::SeqCst) {
                                1 => StatusCode::SERVICE_UNAVAILABLE.into_response(),
                                2 => {
                                    tokio::time::sleep(Duration::from_millis(350)).await;
                                    StatusCode::SERVICE_UNAVAILABLE.into_response()
                                }
                                _ => Json(peer.append_entries(req).await).into_response(),
                            }
                        }
                    }))
                    .route("/raft/1/vote", post(move |headers: axum::http::HeaderMap, Json(req): Json<VoteRequest<TypeConfig>>| {
                        let peer = vote_peer.clone();
                        async move {
                            assert_eq!(headers["x-chronicle-cluster"], "real-transfer");
                            assert_eq!(headers["x-chronicle-recipient"], id.to_string());
                            Json(peer.vote(req).await)
                        }
                    }))
                    .route("/raft/1/transfer", post(move |headers: axum::http::HeaderMap, Json(req): Json<TransferLeaderRequest<TypeConfig>>| {
                        let (peer, traffic) = (transfer_peer.clone(), transfer_traffic.clone());
                        async move {
                            assert_eq!(headers["x-chronicle-cluster"], "real-transfer");
                            assert_eq!(headers["x-chronicle-recipient"], id.to_string());
                            traffic.transfers.fetch_add(1, Ordering::SeqCst);
                            if traffic.mode.load(Ordering::SeqCst) == 3 {
                                return StatusCode::SERVICE_UNAVAILABLE.into_response();
                            }
                            Json(peer.handle_transfer_leader(req).await.map_err(RaftError::<TypeConfig>::Fatal)).into_response()
                        }
                    }));
                peers.servers.push(tokio::spawn(async move {
                    axum::serve(listener, router).await.unwrap()
                }));
                peers.raft.push(raft);
                peers.stores.push(store);
                peers.traffic.push(traffic);
            }
            peers.raft[0]
                .initialize(BTreeMap::from([(1, peers.nodes[&1].clone())]))
                .await
                .unwrap();
            peers.raft[0]
                .wait(Some(Duration::from_secs(3)))
                .state(openraft::ServerState::Leader, "bootstrap")
                .await
                .unwrap();
            for id in 2..=count {
                peers.raft[0]
                    .add_learner(id, peers.nodes[&id].clone(), true)
                    .await
                    .unwrap();
            }
            peers
        }

        async fn voters(&self) {
            self.raft[0]
                .change_membership(
                    self.nodes
                        .keys()
                        .copied()
                        .collect::<std::collections::BTreeSet<_>>(),
                    false,
                )
                .await
                .unwrap();
            self.write_and_read(0, 100).await;
        }

        async fn write_and_read(&self, leader: usize, id: u64) {
            let node = crate::model::Node {
                addr: format!("written-{id}"),
                zone: "test".into(),
                draining: false,
            };
            let written = self.raft[leader]
                .client_write(crate::model::Command::Register {
                    id,
                    node: node.clone(),
                })
                .await
                .unwrap();
            self.raft[leader]
                .ensure_linearizable(openraft::ReadPolicy::ReadIndex)
                .await
                .unwrap();
            assert_eq!(
                self.stores[leader]
                    .read_state()
                    .await
                    .unwrap()
                    .nodes
                    .get(&id),
                Some(&node)
            );
            for replica in 0..self.raft.len() {
                if replica == leader {
                    continue;
                }
                self.raft[replica]
                    .wait(Some(Duration::from_secs(3)))
                    .metrics(
                        |m| m.last_applied >= Some(written.log_id),
                        "durable write replicated",
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    self.stores[replica]
                        .read_state()
                        .await
                        .unwrap()
                        .nodes
                        .get(&id),
                    Some(&node)
                );
            }
        }

        async fn close(self) {
            for raft in &self.raft {
                raft.shutdown().await.unwrap();
            }
            for server in self.servers {
                server.abort();
                let _ = server.await;
            }
            drop(self.raft);
            for store in self.stores {
                store.close().await;
            }
        }
    }

    #[tokio::test]
    async fn real_transfer_elects_successor_and_serves_strict_reads_and_new_writes() {
        tokio::time::timeout(Duration::from_secs(12), async {
            let peers = Peers::new().await;
            peers.voters().await;
            let before = peers.raft[0].metrics().borrow_watched().current_term;
            // Submission is not success: wait for the successor and exercise it.
            peers.raft[0].trigger().transfer_leader(2).await.unwrap();
            peers.raft[1].wait(Some(Duration::from_secs(4))).state(openraft::ServerState::Leader, "directed successor").await.unwrap();
            // Leader state alone raced client_write with LeaseExpired in the
            // first qualification run: quorum contact is a separate readiness
            // condition, not evidence that transfer submission completed.
            peers.raft[1].wait(Some(Duration::from_secs(3))).metrics(
                |m| m.last_quorum_acked.is_some(), "successor quorum contact",
            ).await.unwrap();
            peers.write_and_read(1, 101).await;
            tokio::time::sleep(Duration::from_millis(600)).await;
            assert_eq!(peers.raft[1].metrics().borrow_watched().current_term, before + 1);
            assert_eq!(peers.raft[1].metrics().borrow_watched().current_leader, Some(2));
            assert_eq!(peers.traffic[1].transfers.load(Ordering::SeqCst), 1);
            eprintln!("real transfer: leader=2, term={} -> {}, one transfer, strict read + replicated write passed", before, before + 1);
            peers.close().await;
        }).await.unwrap();
    }

    #[tokio::test]
    async fn real_transfer_rejects_stale_vote_and_unflushed_boundary() {
        use openraft::network::RaftNetworkV2;
        tokio::time::timeout(Duration::from_secs(12), async {
            let mut peers = Peers::new().await;
            peers.voters().await;
            let vote = peers.raft[0].metrics().borrow_watched().vote;
            let actual = peers.raft[1].metrics().borrow_watched().last_applied;
            let stale = crate::Vote::new_committed(0, 1);
            let mut connection = peers.network.new_client(2, &peers.nodes[&2]).await;
            let rejected = connection.transfer_leader(TransferLeaderRequest::new(stale, 2, actual), RPCOption::new(Duration::from_secs(3))).await.unwrap();
            assert_eq!(rejected, Err(TransferLeaderError::VoteChanged { expected: stale, actual: vote }));
            // Withhold a real proposed entry at the HTTP boundary. The target
            // cannot flush this boundary; no invented consensus/log state.
            peers.traffic[1].mode.store(1, Ordering::SeqCst);
            let leader = peers.raft[0].clone();
            let pending = tokio::spawn(async move {
                leader.client_write(crate::model::Command::Register {
                    id: 107, node: crate::model::Node { addr: "withheld-entry".into(), zone: "test".into(), draining: false },
                }).await
            });
            let missing = crate::LogId::new(vote.leader_id, actual.unwrap().index + 1);
            peers.raft[0].wait(Some(Duration::from_secs(2))).metrics(
                |m| m.last_log_index == Some(missing.index), "source proposed withheld entry",
            ).await.unwrap();
            let rejected = connection.transfer_leader(TransferLeaderRequest::new(vote, 2, Some(missing)), RPCOption::new(Duration::from_secs(3))).await.unwrap();
            assert_eq!(rejected, Err(TransferLeaderError::LogNotFlushed { expected: Some(missing), actual }));
            assert_eq!(peers.raft[1].metrics().borrow_watched().current_term, vote.leader_id.term);
            assert_eq!(peers.raft[1].metrics().borrow_watched().current_leader, Some(1));
            peers.traffic[1].mode.store(0, Ordering::SeqCst);
            assert_eq!(pending.await.unwrap().unwrap().log_id, missing);
            peers.write_and_read(0, 102).await;
            assert_eq!(peers.stores[1].read_state().await.unwrap().nodes[&107].addr, "withheld-entry");
            eprintln!("real rejection: VoteChanged and LogNotFlushed preserved; original leader still writes/strict-reads");
            peers.close().await;
        }).await.unwrap();
    }

    #[tokio::test]
    async fn unavailable_transfer_recipient_is_not_success_or_an_automatic_retry() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let peers = Peers::new().await;
            peers.voters().await;
            let before = peers.raft[0].metrics().borrow_watched().current_term;
            peers.traffic[1].mode.store(3, Ordering::SeqCst);
            peers.raft[0].trigger().transfer_leader(2).await.unwrap();
            while peers.traffic[1].transfers.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            peers.traffic[1].mode.store(0, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(600)).await;
            assert_eq!(peers.traffic[1].transfers.load(Ordering::SeqCst), 1);
            assert_eq!(peers.raft[1].metrics().borrow_watched().current_term, before);
            assert_ne!(peers.raft[1].metrics().borrow_watched().state, openraft::ServerState::Leader);
            let result = peers.raft[0].client_write(crate::model::Command::Register {
                id: 106, node: crate::model::Node { addr: "must-not-commit".into(), zone: "test".into(), draining: false },
            }).await;
            let error = result.unwrap_err();
            assert!(matches!(error, RaftError::APIError(openraft::error::ClientWriteError::ForwardToLeader(_))), "{error:?}");
            assert!(!peers.stores[0].read_state().await.unwrap().nodes.contains_key(&106));
            assert!(!peers.stores[1].read_state().await.unwrap().nodes.contains_key(&106));
            eprintln!("unavailable transfer: submission Ok, one HTTP 503, no successor/no retry after recovery (700ms), write rejected: {error}");
            peers.close().await;
        }).await.unwrap();
    }

    #[tokio::test]
    async fn dead_transfer_target_recovers_only_through_surviving_voter_election() {
        tokio::time::timeout(Duration::from_secs(25), async {
            for elections in [false, true] {
                let peers = Peers::configured(3, openraft::Config {
                    heartbeat_interval: 200,
                    election_timeout_min: 800,
                    election_timeout_max: 1600,
                    enable_elect: elections,
                    ..Default::default()
                }).await;
                peers.voters().await;
                let before = peers.raft[0].metrics().borrow_watched().vote;
                // Shut down the target core, not merely its incoming endpoint:
                // otherwise it could still campaign through outgoing vote RPCs.
                peers.raft[1].shutdown().await.unwrap();
                peers.traffic[1].mode.store(3, Ordering::SeqCst);
                let start = Instant::now();
                peers.raft[0].trigger().transfer_leader(2).await.unwrap();
                while peers.traffic[2].transfers.load(Ordering::SeqCst) == 0 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                let command = crate::model::Command::Register {
                    id: 108,
                    node: crate::model::Node {
                        addr: "surviving-quorum".into(), zone: "test".into(), draining: false,
                    },
                };
                let rejected = peers.raft[0].client_write(command.clone()).await.unwrap_err();
                let RaftError::APIError(openraft::error::ClientWriteError::ForwardToLeader(to)) = rejected else {
                    panic!("transfer did not fence writes: {rejected:?}");
                };
                assert_eq!(to.leader_id, Some(2));
                let source = peers.raft[0].clone();
                let reads = tokio::spawn(async move {
                    loop {
                        let _ = source.ensure_linearizable(openraft::ReadPolicy::ReadIndex).await;
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                });
                if elections {
                    peers.raft[2].wait(Some(Duration::from_secs(10))).state(
                        openraft::ServerState::Leader, "remaining voter must recover quorum",
                    ).await.unwrap();
                    peers.raft[2].ensure_linearizable(openraft::ReadPolicy::ReadIndex).await.unwrap();
                    let written = peers.raft[2].client_write(command).await.unwrap();
                    assert!(written.data.error.is_none());
                    peers.raft[0].wait(Some(Duration::from_secs(3))).metrics(
                        |m| m.current_leader == Some(3) && m.last_applied >= Some(written.log_id),
                        "old source follows surviving voter and applies new write",
                    ).await.unwrap();
                    for replica in [0, 2] {
                        let state = peers.stores[replica].read_state().await.unwrap();
                        assert_eq!(state.nodes[&100].addr, "written-100");
                        assert_eq!(state.nodes[&108].addr, "surviving-quorum");
                    }
                    assert!(peers.raft[2].metrics().borrow_watched().vote > before);
                    eprintln!("dead target: surviving quorum recovered strict read + write in {:?} with source ReadIndex probes", start.elapsed());
                } else {
                    // More than two maximum election windows: transfer failure
                    // has no source-side rollback timer, even with healthy quorum.
                    tokio::time::sleep(Duration::from_secs(4)).await;
                    assert_eq!(peers.raft[0].metrics().borrow_watched().vote, before);
                    assert_ne!(peers.raft[2].metrics().borrow_watched().state, openraft::ServerState::Leader);
                    assert!(peers.raft[0].client_write(command).await.is_err());
                    for replica in [0, 2] {
                        assert!(!peers.stores[replica].read_state().await.unwrap().nodes.contains_key(&108));
                    }
                    eprintln!("dead target negative control: no recovery after4s with elections disabled");
                }
                reads.abort();
                let _ = reads.await;
                assert_eq!(peers.traffic[1].transfers.load(Ordering::SeqCst), 1);
                assert_eq!(peers.traffic[2].transfers.load(Ordering::SeqCst), 1);
                peers.close().await;
            }
        }).await.unwrap();
    }

    #[tokio::test]
    async fn heartbeat_enabled_unavailable_replica_has_bounded_aggregate_traffic_and_recovers() {
        tokio::time::timeout(Duration::from_secs(15), async {
            let peers = Peers::new().await;
            let traffic = &peers.traffic[1];
            traffic.mode.store(1, Ordering::SeqCst);
            // A learner outage cannot prevent the healthy single-voter quorum
            // from committing, but keeps the real replication worker busy.
            peers.raft[0]
                .client_write(crate::model::Command::Register {
                    id: 103,
                    node: crate::model::Node {
                        addr: "during-outage".into(),
                        zone: "test".into(),
                        draining: false,
                    },
                })
                .await
                .unwrap();
            for mode in [1, 2] {
                traffic.mode.store(mode, Ordering::SeqCst);
                traffic.attempts.lock().unwrap().clear();
                traffic
                    .peak
                    .store(traffic.active.load(Ordering::SeqCst), Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(1600)).await;
                let attempts = traffic.attempts.lock().unwrap().clone();
                let peak = traffic.peak.load(Ordering::SeqCst);
                let shortest = attempts
                    .windows(2)
                    .map(|p| p[1].duration_since(p[0]))
                    .min()
                    .unwrap();
                eprintln!(
                    "heartbeat=200ms mode={mode}: attempts={}, peak={peak}, shortest={shortest:?}",
                    attempts.len()
                );
                assert!(
                    (3..=20).contains(&attempts.len()),
                    "unbounded/stalled RPC traffic: {}",
                    attempts.len()
                );
                // Independent heartbeat and replication workers can overlap;
                // canceled HTTP requests may still finish in the server.
                assert!(peak <= 4, "unbounded in-flight RPCs: {peak}");
                if mode == 1 {
                    assert!(
                        shortest < Duration::from_millis(450),
                        "heartbeats must not inherit the replication retry floor"
                    );
                }
                peers.raft[0]
                    .ensure_linearizable(openraft::ReadPolicy::ReadIndex)
                    .await
                    .unwrap();
            }
            traffic.mode.store(0, Ordering::SeqCst);
            peers.write_and_read(0, 104).await;
            assert_eq!(
                peers.stores[1].read_state().await.unwrap().nodes[&103].addr,
                "during-outage"
            );
            peers.voters().await;
            peers.write_and_read(0, 105).await; // ReadIndex now requires the recovered peer.
            peers.close().await;
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn adapter_forwards_directed_transfer_and_preserves_rejection() {
        use openraft::network::RaftNetworkV2;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let vote = crate::Vote::new_committed(7, 1);
        let boundary = crate::LogId::new(vote.leader_id, 42);
        let router = Router::new().route(
            "/raft/3/transfer",
            post(
                move |headers: axum::http::HeaderMap,
                      Json(req): Json<TransferLeaderRequest<TypeConfig>>| async move {
                    assert_eq!(headers["x-chronicle-cluster"], "transfer-test");
                    assert_eq!(headers["x-chronicle-recipient"], "2");
                    assert_eq!(req.from_leader(), &vote);
                    assert_eq!(req.to_node_id(), &2);
                    let result: Result<TransferLeaderResponse<TypeConfig>, RaftError<TypeConfig>> =
                        if req.last_log_id().is_some() {
                            assert_eq!(req.last_log_id(), Some(&boundary));
                            Ok(Err(TransferLeaderError::LogNotFlushed {
                                expected: Some(boundary),
                                actual: None,
                            }))
                        } else {
                            Err(RaftError::Fatal(openraft::error::Fatal::Stopped))
                        };
                    Json(result)
                },
            ),
        );
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let mut network = Network {
            client: reqwest::Client::new(),
            cluster: "transfer-test".into(),
            group: 3,
        };
        let mut connection = network.new_client(2, &BasicNode::new(address)).await;
        let response = connection
            .transfer_leader(
                TransferLeaderRequest::new(vote, 2, Some(boundary)),
                RPCOption::new(Duration::from_secs(1)),
            )
            .await
            .unwrap();
        assert_eq!(
            response,
            Err(TransferLeaderError::LogNotFlushed {
                expected: Some(boundary),
                actual: None,
            })
        );
        let fatal = connection
            .transfer_leader(
                TransferLeaderRequest::new(vote, 2, None),
                RPCOption::new(Duration::from_secs(1)),
            )
            .await;
        assert!(matches!(fatal, Err(RPCError::Unreachable(_))));
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn unavailable_replica_backs_off_and_recovers() {
        tokio::time::timeout(Duration::from_secs(15), async {
            let dir = tempfile::tempdir().unwrap();
            let client = reqwest::Client::new();
            let network = Network {
                client,
                cluster: "retry-test".into(),
                group: 1,
            };
            let config = Arc::new(
                openraft::Config {
                    heartbeat_interval: 2000,
                    // Heartbeats now use an independent RPC path; measure only
                    // replication retries, not interleaved liveness probes.
                    enable_heartbeat: false,
                    election_timeout_min: 5000,
                    election_timeout_max: 6000,
                    ..Default::default()
                }
                .validate()
                .unwrap(),
            );
            let store = SqliteStore::open(dir.path().join("leader")).await.unwrap();
            let follower_store = SqliteStore::open(dir.path().join("follower"))
                .await
                .unwrap();
            let raft = Raft::new(
                1,
                config.clone(),
                network.clone(),
                store.clone(),
                store.clone(),
            )
            .await
            .unwrap();
            let follower = Raft::new(
                2,
                config,
                network.clone(),
                follower_store.clone(),
                follower_store.clone(),
            )
            .await
            .unwrap();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap().to_string();
            let mode = Arc::new(AtomicU8::new(0));
            let attempts = Arc::new(Mutex::new(Vec::new()));
            let (fault, observed, peer) = (mode.clone(), attempts.clone(), follower.clone());
            let router = Router::new().route(
                "/raft/1/append",
                post(
                    move |Json(request): Json<AppendEntriesRequest<TypeConfig>>| {
                        let (fault, observed, peer) =
                            (fault.clone(), observed.clone(), peer.clone());
                        async move {
                            observed.lock().unwrap().push(Instant::now());
                            match fault.load(Ordering::SeqCst) {
                                0 => StatusCode::SERVICE_UNAVAILABLE.into_response(),
                                1 => "invalid RPC response".into_response(),
                                _ => Json(peer.append_entries(request).await).into_response(),
                            }
                        }
                    },
                ),
            );
            let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
            raft.initialize(BTreeMap::from([(1, BasicNode::new("unused"))]))
                .await
                .unwrap();
            raft.wait(Some(Duration::from_secs(2)))
                .state(openraft::ServerState::Leader, "bootstrap")
                .await
                .unwrap();
            let added = raft
                .add_learner(2, BasicNode::new(address.clone()), false)
                .await
                .unwrap();
            for (target, next_mode) in [(3, 1), (6, 2)] {
                while attempts.lock().unwrap().len() < target {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                mode.store(next_mode, Ordering::SeqCst);
            }
            let observed = attempts.lock().unwrap().clone();
            let shortest = observed
                .windows(2)
                .map(|pair| pair[1].duration_since(pair[0]))
                .min()
                .unwrap();
            eprintln!(
                "{} failed attempts; minimum spacing {shortest:?}",
                observed.len()
            );
            assert!(
                shortest >= Duration::from_millis(450),
                "retry burst: {} attempts, minimum spacing {shortest:?}",
                observed.len()
            );
            follower
                .wait(Some(Duration::from_secs(3)))
                .metrics(
                    |m| m.last_applied >= Some(added.log_id),
                    "caught up after recovery",
                )
                .await
                .unwrap();
            raft.shutdown().await.unwrap();
            follower.shutdown().await.unwrap();
            server.abort();
            let _ = server.await;
            drop((raft, follower));
            store.close().await;
            follower_store.close().await;

            // A connection refused before HTTP also activates the same policy.
            // A fresh pool cannot reuse a server connection still shutting down.
            let mut network = Network {
                client: reqwest::Client::builder().no_proxy().build().unwrap(),
                ..network
            };
            let mut connection = network
                .new_client(2, &BasicNode::new(address))
                .await
                .into_inner();
            let result = connection
                .vote(
                    VoteRequest {
                        vote: crate::Vote::new(1, 1),
                        last_log_id: None,
                        leadership_transfer: false,
                    },
                    RPCOption::new(Duration::from_secs(1)),
                )
                .await;
            assert!(matches!(result, Err(RPCError::Unreachable(_))));
        })
        .await
        .unwrap();
    }
}
