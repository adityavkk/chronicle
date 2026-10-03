//! Reused HTTP pool; RPC timeouts and bounded message bodies belong to the caller.
use crate::{TypeConfig, metrics::Histogram};
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError};
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
            let response = response.map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
            let response = response
                .error_for_status()
                .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
            let result: Result<R, E> = response
                .json()
                .await
                .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
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
