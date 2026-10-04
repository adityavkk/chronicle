//! Reused HTTP pool; RPC timeouts and bounded message bodies belong to the caller.
use crate::{TypeConfig, metrics::Histogram};
use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::*;
use openraft::{BasicNode, RaftNetwork, RaftNetworkFactory};
use serde::{Serialize, de::DeserializeOwned};
use std::time::Instant;

static RPC: Histogram = Histogram::new();

pub fn timing_metrics(text: &mut String) {
    RPC.render("chronicle_rpc_duration_seconds", text);
}

pub type RpcError<E = openraft::error::Infallible> = RPCError<u64, BasicNode, RaftError<u64, E>>;

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
    type Network = Connection;
    async fn new_client(&mut self, id: u64, node: &BasicNode) -> Connection {
        Connection {
            network: self.clone(),
            id,
            node: node.clone(),
        }
    }
}

impl Connection {
    async fn rpc<Q: Serialize, R: DeserializeOwned, E: std::error::Error + DeserializeOwned>(
        &self,
        method: &str,
        req: Q,
    ) -> Result<R, RPCError<u64, BasicNode, E>> {
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
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RpcError> {
        self.rpc("append", req).await
    }
    async fn vote(
        &mut self,
        req: VoteRequest<u64>,
        _: RPCOption,
    ) -> Result<VoteResponse<u64>, RpcError> {
        self.rpc("vote", req).await
    }
    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _: RPCOption,
    ) -> Result<InstallSnapshotResponse<u64>, RpcError<InstallSnapshotError>> {
        self.rpc("snapshot", req).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Raft, storage::SqliteStore};
    use axum::{Json, Router, http::StatusCode, response::IntoResponse, routing::post};
    use std::{
        collections::BTreeMap,
        sync::{
            Arc, Mutex,
            atomic::{AtomicU8, Ordering},
        },
        time::Duration,
    };

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
            let mut connection = network.new_client(2, &BasicNode::new(address)).await;
            let result = connection
                .vote(
                    VoteRequest {
                        vote: openraft::Vote::new(1, 1),
                        last_log_id: None,
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
