use super::*;
use openraft::errors::{
    Infallible, NetworkError, RPCError, RaftError, RemoteError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse,
    VoteRequest, VoteResponse,
};
use openraft_legacy::network_v1::{Adapter, RaftNetwork, InstallSnapshotRequest, InstallSnapshotResponse, InstallSnapshotError};
use serde::de::DeserializeOwned;
use std::sync::RwLock;

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Faults {
    pub blocked: BTreeSet<u64>,
    pub delay_ms: u64,
    #[serde(default)]
    pub pause_fork_import: bool,
}

#[derive(Clone)]
pub struct Network {
    pub group: usize,
    pub cluster: String,
    pub client: reqwest::Client,
    pub faults: Arc<RwLock<Faults>>,
}
pub struct Connection {
    network: Network,
    target: u64,
    node: BasicNode,
}
type RpcError<E = Infallible> = RPCError<Types, RaftError<Types, E>>;

impl Network {
    async fn send<Q: Serialize, R: DeserializeOwned, E: std::error::Error + DeserializeOwned>(
        &self,
        target: u64,
        node: &BasicNode,
        rpc: &str,
        request: Q,
    ) -> Result<R, RPCError<Types, E>> {
        let faults = self.faults.read().unwrap().clone();
        if faults.blocked.contains(&target) {
            return Err(RPCError::Unreachable(Unreachable::new(&io::Error::other(
                "injected link partition",
            ))));
        }
        if faults.delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(faults.delay_ms)).await;
        }
        let body =
            bincode::serialize(&request).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let response = self
            .client
            .post(format!("http://{}/_raft10/{}/{rpc}", node.addr, self.group))
            .header("x-electric-cluster", &self.cluster)
            .body(body)
            .send()
            .await
            .map_err(|e| {
                // OpenRaft retries Network immediately. A dead peer must use
                // Unreachable to engage its built-in per-target backoff.
                if e.is_connect() {
                    RPCError::Unreachable(Unreachable::new(&e))
                } else {
                    RPCError::Network(NetworkError::new(&e))
                }
            })?
            .error_for_status()
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?
            .bytes()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let result: Result<R, E> = bincode::deserialize(&response)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        result.map_err(|e| RPCError::RemoteError(RemoteError::new(target, e)))
    }
}
impl RaftNetworkFactory<Types> for Network {
    type Network = Adapter<Types, Connection, tokio::fs::File>;
    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        Connection {
            network: self.clone(),
            target,
            node: node.clone(),
        }.into_v2()
    }
}
impl RaftNetwork<Types> for Connection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<Types>,
        _: RPCOption,
    ) -> Result<AppendEntriesResponse<Types>, RpcError> {
        self.network
            .send(self.target, &self.node, "append", req)
            .await
    }
    async fn vote(
        &mut self,
        req: VoteRequest<Types>,
        _: RPCOption,
    ) -> Result<VoteResponse<Types>, RpcError> {
        self.network
            .send(self.target, &self.node, "vote", req)
            .await
    }
    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<Types>,
        _: RPCOption,
    ) -> Result<InstallSnapshotResponse<Types>, RpcError<InstallSnapshotError>> {
        self.network
            .send(self.target, &self.node, "snapshot", req)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dead_peer_and_partition_enable_openraft_backoff() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let peer = BasicNode {
            addr: listener.local_addr().unwrap().to_string(),
        };
        drop(listener);
        let network = Network {
            group: 0,
            cluster: "backoff-test".into(),
            client: reqwest::Client::builder().no_proxy().build().unwrap(),
            faults: Arc::new(RwLock::new(Faults::default())),
        };
        for blocked in [false, true] {
            if blocked {
                network.faults.write().unwrap().blocked.insert(2);
            }
            let result = network
                .send::<_, (), Infallible>(2, &peer, "append", ())
                .await;
            assert!(
                matches!(result, Err(RPCError::Unreachable(_))),
                "{result:?}"
            );
        }
    }
}
