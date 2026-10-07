//! Experimental source-integrated partition replication; see ../../CONTRACT.md.
pub(crate) mod clock;
mod fork_io;
mod forks;
mod journal;
mod machine;
mod network;
mod subscription_io;
mod subscriptions;

use crate::api::{Body, Method, Req, Resp};
use crate::store::Store;
use openraft::{
    BasicNode, EntryPayload, LogId, SnapshotMeta, StorageError, StorageIOError, StoredMembership,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::Semaphore;

openraft::declare_raft_types!(pub Types: D = Command, R = Reply, SnapshotData = tokio::fs::File);
type Entry = openraft::Entry<Types>;
type Raft = openraft::Raft<Types>;
pub static CLUSTER: OnceLock<Cluster> = OnceLock::new();

#[derive(Clone, Serialize, Deserialize)]
pub struct Command {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub time: u64,
}

impl std::fmt::Debug for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Subscription commands can contain keys/tokens. OpenRaft diagnostics
        // must never format request headers or payload bytes.
        f.debug_struct("Command")
            .field("method", &self.method)
            .field("path", &self.path)
            .field("bytes", &self.body.len())
            .finish()
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Reply {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}
impl std::fmt::Debug for Reply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reply")
            .field("status", &self.status)
            .field("bytes", &self.body.len())
            .finish()
    }
}
impl Reply {
    fn from_resp(resp: Resp) -> Self {
        let body = match resp.body {
            Body::Empty => vec![],
            Body::Full(b) => b.to_vec(),
            _ => unreachable!("mutation returned a streaming body"),
        };
        Self {
            status: resp.status,
            headers: resp
                .headers
                .into_iter()
                .map(|(k, v)| (k.into(), v))
                .collect(),
            body,
        }
    }
    fn into_resp(self) -> Resp {
        const NAMES: &[&str] = &[
            "content-type",
            "cache-control",
            "location",
            "stream-next-offset",
            "stream-closed",
            "producer-epoch",
            "producer-seq",
            "producer-expected-seq",
            "producer-received-seq",
        ];
        Resp {
            status: self.status,
            body: Body::Full(self.body.into()),
            headers: self
                .headers
                .into_iter()
                .map(|(k, v)| {
                    (
                        *NAMES
                            .iter()
                            .find(|name| **name == k)
                            .expect("native mutation header"),
                        v,
                    )
                })
                .collect(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    cluster: String,
    node: u64,
    listen: std::net::SocketAddr,
    dir: PathBuf,
    partitions: usize,
    /// Immutable genesis config, shared even by nodes added later as learners.
    /// Membership after bootstrap is authoritative only in the Raft log.
    genesis: BTreeMap<u64, BasicNode>,
    #[serde(default = "workers")]
    workers: usize,
    #[serde(default = "long_poll_ms")]
    long_poll_ms: u64,
    #[serde(default)]
    fault_testing: bool,
    /// Native WAL/appender diagnostics; zero keeps clock probes disabled.
    #[serde(default)]
    stats_secs: u64,
}
fn long_poll_ms() -> u64 {
    30_000
}
fn workers() -> usize {
    4
}

struct Group {
    raft: Raft,
    machine: Arc<machine::Machine>,
    slots: Arc<Semaphore>,
    reads: ReadBarrier,
}

#[derive(Default)]
struct ReadBarrier {
    started: AtomicU64,
    completed: tokio::sync::Mutex<(u64, bool)>,
}
impl ReadBarrier {
    async fn confirm(&self, round: impl Future<Output = bool>) -> bool {
        let observed = self.started.load(Ordering::SeqCst);
        let mut completed = self.completed.lock().await;
        if completed.0 > observed {
            return completed.1;
        }
        // Publish START before polling the Raft future. New arrivals cannot
        // reuse this round even if it completes while they wait for the lock.
        let generation = self.started.fetch_add(1, Ordering::SeqCst) + 1;
        let result = round.await;
        *completed = (generation, result);
        result
    }
}

impl Group {
    async fn propose(
        &self,
        command: Command,
    ) -> Result<openraft::raft::ClientWriteResponse<Types>, u16> {
        let permit = self.slots.clone().try_acquire_owned().map_err(|_| 429u16)?;
        let raft = self.raft.clone();
        // Charge admission until consensus resolves, even after HTTP timeout.
        let proposal = tokio::spawn(async move {
            let _permit = permit;
            raft.client_write(command).await
        });
        match tokio::time::timeout(Duration::from_secs(3), proposal).await {
            Ok(Ok(Ok(result))) => Ok(result),
            _ => Err(503),
        }
    }
}
pub struct Cluster {
    config: Config,
    groups: Vec<Group>,
    faults: Arc<std::sync::RwLock<network::Faults>>,
    client: reqwest::Client,
    _lock: std::fs::File,
}

fn storage_error(error: io::Error) -> StorageError<u64> {
    // Fail-stop avoids continuing reads against a partially materialized
    // committed command. No error is converted into a successful fsync retry.
    eprintln!("FATAL replication storage: {error}");
    if CLUSTER.get().is_some() {
        std::process::abort();
    }
    StorageIOError::write_logs(&error).into()
}

fn response(status: u16, text: impl Into<String>) -> Resp {
    Resp {
        status,
        headers: vec![
            ("content-type", "text/plain".into()),
            ("cache-control", "no-store".into()),
        ],
        body: Body::Full(text.into().into()),
    }
}
fn json(value: &impl Serialize) -> Resp {
    Resp {
        status: 200,
        headers: vec![("content-type", "application/json".into())],
        body: Body::Full(serde_json::to_vec(value).unwrap().into()),
    }
}
fn rpc(value: &impl Serialize) -> Resp {
    Resp {
        status: 200,
        headers: vec![],
        body: Body::Full(bincode::serialize(value).unwrap().into()),
    }
}

pub fn partition(path: &str, count: usize) -> usize {
    let hash = path.as_bytes().iter().fold(0xcbf29ce484222325u64, |h, b| {
        (h ^ *b as u64).wrapping_mul(0x100000001b3)
    });
    (hash % count as u64) as usize
}

impl Cluster {
    fn token(&self, group: usize, index: u64) -> String {
        format!("{}:{group}:{index}", self.config.cluster)
    }

    fn parse_token(&self, token: &str, group: usize) -> Option<u64> {
        let mut parts = token.split(':');
        if parts.next()? != self.config.cluster || parts.next()?.parse::<usize>().ok()? != group {
            return None;
        }
        let index = parts.next()?.parse().ok()?;
        if parts.next().is_some() {
            return None;
        }
        Some(index)
    }

    fn unavailable(&self, group: usize, message: &str) -> Resp {
        let mut resp = response(503, message);
        let metrics = self.groups[group].raft.metrics().borrow().clone();
        if let Some(leader) = metrics
            .current_leader
            .and_then(|id| metrics.membership_config.membership().get_node(&id))
        {
            resp.headers
                .push(("stream-leader", format!("http://{}", leader.addr)));
        }
        resp
    }

    pub async fn handle(&self, req: Req) -> Resp {
        if req.path == "/health" {
            return response(200, "experimental electric replica");
        }
        if req.path.starts_with("/_raft/") || req.path.starts_with("/_admin/") {
            return match tokio::time::timeout(Duration::from_secs(10), self.internal(req)).await {
                Ok(resp) => resp,
                Err(_) => response(503, "admin/RPC timeout: outcome unknown"),
            };
        }
        if subscription_io::reserved(&req.path) {
            return self.subscriptions(req).await;
        }
        let group = partition(&req.path, self.groups.len());
        let g = &self.groups[group];
        // The native parser caps raw bodies; this tighter admitted command cap
        // also bounds each Raft entry and its encoded WAL frame.
        if req.body.len() > 1024 * 1024 {
            return response(413, "replicated command limit: 1 MiB");
        }
        if matches!(req.method, Method::Put | Method::Post | Method::Delete) {
            if req
                .header("stream-durability")
                .is_some_and(|v| v != "quorum-fsync")
            {
                return response(400, "only quorum-fsync writes are supported");
            }
            if g.raft.metrics().borrow().current_leader != Some(self.config.node) {
                return self.unavailable(group, "not leader; mutation was not forwarded");
            }
            if req.method == Method::Put
                && req
                    .header("stream-forked-from")
                    .is_some_and(|source| partition(source, self.groups.len()) != group)
            {
                return self.remote_fork(group, req).await;
            }
            let command = Command {
                method: match req.method {
                    Method::Put => "PUT",
                    Method::Post => "POST",
                    _ => "DELETE",
                }
                .into(),
                path: req.path,
                headers: req.headers,
                body: req.body.to_vec(),
                time: clock::millis(std::time::SystemTime::now()),
            };
            return match g.propose(command).await {
                Ok(result) => {
                    let mut resp = result.data.into_resp();
                    resp.headers
                        .push(("stream-session", self.token(group, result.log_id.index)));
                    resp.headers
                        .push(("stream-durability", "quorum-fsync".into()));
                    resp
                }
                Err(429) => response(429, "pending proposal bound reached"),
                _ => self.unavailable(group, "write outcome unknown; retry with producer identity"),
            };
        }
        if !matches!(req.method, Method::Get | Method::Head) {
            return crate::handlers::handle(g.machine.view.read().await.store.clone(), req).await;
        }
        let consistency = req
            .header("stream-consistency")
            .unwrap_or("linearizable")
            .to_string();
        let session = match req.header("stream-session") {
            Some(raw) => match self.parse_token(raw, group) {
                Some(token) => Some(token),
                None => return response(400, "invalid session cluster/partition/index"),
            },
            None => None,
        };
        match consistency.as_str() {
            "linearizable" => {
                if self.barrier(group).await.is_err() {
                    return self.unavailable(group, "linearizable barrier unavailable");
                }
                let timed = g
                    .machine
                    .view
                    .read()
                    .await
                    .store
                    .streams
                    .get(&req.path)
                    .is_some_and(|s| {
                        s.config.ttl_seconds.is_some() || s.config.expires_at.is_some()
                    });
                if timed {
                    let command = Command {
                        method: if req.method == Method::Get {
                            "TOUCH"
                        } else {
                            "TICK"
                        }
                        .into(),
                        path: req.path.clone(),
                        headers: vec![],
                        body: vec![],
                        time: clock::millis(std::time::SystemTime::now()),
                    };
                    if g.propose(command).await.is_err() {
                        return self.unavailable(
                            group,
                            "timed read barrier unavailable; touch outcome unknown",
                        );
                    }
                }
            }
            "session" if session.is_none() => {
                return response(400, "session consistency requires Stream-Session")
            }
            "prefix" | "session" => {}
            _ => return response(400, "unknown Stream-Consistency"),
        }
        if let Some(index) = session {
            if g.raft
                .wait(Some(Duration::from_secs(3)))
                .applied_index_at_least(Some(index), "session")
                .await
                .is_err()
            {
                return self.unavailable(group, "session position not applied");
            }
        }
        let view = g.machine.view.read().await;
        if view.forks.pending(&req.path) {
            return self.unavailable(
                group,
                "fork materialization pending; absence is not established",
            );
        }
        let index = view.applied.map(|id| id.index).unwrap_or(0);
        let store = view.store.clone();
        let live = req
            .query
            .as_deref()
            .is_some_and(|q| q.split('&').any(|p| p == "live=long-poll"));
        let mut resp = if live {
            // Initialize the native stream lookup and tail observation while
            // apply is excluded (PUT can insert before writing its initial body).
            // Release after the first poll: a parked long-poll must not block
            // the committed append that will wake its watch::Receiver.
            let mut guard = Some(view);
            let mut pending = Box::pin(crate::handlers::handle(store, req));
            std::future::poll_fn(|cx| {
                let result = pending.as_mut().poll(cx);
                drop(guard.take());
                result
            })
            .await
        } else {
            let resp = crate::handlers::handle(store, req).await;
            drop(view);
            resp
        };
        // A long-poll may have observed a later committed apply. Wait for apply
        // to finish before issuing a token that covers those observed bytes.
        let index = if live {
            g.machine
                .view
                .read()
                .await
                .applied
                .map(|id| id.index)
                .unwrap_or(index)
        } else {
            index
        };
        resp.headers.retain(|(k, _)| *k != "cache-control");
        resp.headers
            .push(("cache-control", "no-cache, no-store".into()));
        resp.headers.push(("stream-consistency", consistency));
        resp.headers
            .push(("stream-session", self.token(group, index)));
        resp
    }

    async fn internal(&self, req: Req) -> Resp {
        if req.header("x-electric-cluster") != Some(&self.config.cluster) {
            return response(403, "cluster identity required");
        }
        let parts: Vec<_> = req.path.split('/').collect();
        if parts.as_slice() == ["", "_admin", "network"] && self.config.fault_testing {
            let Ok(faults) = serde_json::from_slice(&req.body) else {
                return response(400, "invalid faults");
            };
            *self.faults.write().unwrap() = faults;
            return response(200, "faults installed");
        }
        if parts.len() != 4 {
            return response(404, "unknown control path");
        }
        let Some(group) = parts[2]
            .parse::<usize>()
            .ok()
            .and_then(|g| self.groups.get(g))
        else {
            return response(404, "unknown group");
        };
        let raft = &group.raft;
        macro_rules! decode {
            () => {
                match bincode::deserialize(&req.body) {
                    Ok(req) => req,
                    Err(_) => return response(400, "invalid RPC"),
                }
            };
        }
        match (parts[1], parts[3]) {
            ("_raft", "append") => rpc(&raft.append_entries(decode!()).await),
            ("_raft", "vote") => rpc(&raft.vote(decode!()).await),
            ("_raft", "snapshot") => rpc(&raft.install_snapshot(decode!()).await),
            ("_admin", "metrics") => json(&raft.metrics().borrow().clone()),
            ("_admin", "keys") => self.public_group_keys(parts[2].parse().unwrap()).await,
            ("_admin", "fork") => self.fork_control(parts[2].parse().unwrap(), req).await,
            ("_admin", "fork-state") if self.config.fault_testing => {
                let view = group.machine.view.read().await;
                let transactions: Vec<_> = view
                    .forks
                    .destinations
                    .iter()
                    .map(|(tx, d)| {
                        let imported = d
                            .grant
                            .as_ref()
                            .and_then(|g| view.store.streams.get(&g.source))
                            .map_or(0, |s| s.tail().bytes);
                        serde_json::json!({"tx":tx,"path":d.path,"source":d.config.forked_from,
                        "granted":d.grant.is_some(),"imported":imported,"created":d.created,
                        "retired":d.retired,"released":d.released})
                    })
                    .collect();
                json(
                    &serde_json::json!({"applied":view.applied.map(|i|i.index),"transactions":transactions}),
                )
            }
            ("_admin", "catalog") => {
                let Ok(filters) = serde_json::from_slice::<Vec<subscription_io::Filter>>(&req.body)
                else {
                    return response(400, "invalid catalog filters");
                };
                self.catalog_local(parts[2].parse().unwrap(), &filters)
                    .await
            }
            ("_admin", "init") => {
                let Ok(nodes) = serde_json::from_slice::<BTreeMap<u64, BasicNode>>(&req.body)
                else {
                    return response(400, "invalid initial membership");
                };
                if nodes != self.config.genesis {
                    return response(409, "initial membership differs from pinned genesis");
                }
                json(&raft.initialize(nodes).await)
            }
            ("_admin", "learner") => {
                let Ok((id, node)) = serde_json::from_slice::<(u64, BasicNode)>(&req.body) else {
                    return response(400, "invalid learner");
                };
                json(&raft.add_learner(id, node, true).await)
            }
            ("_admin", "membership") => {
                let Ok(nodes) = serde_json::from_slice::<BTreeSet<u64>>(&req.body) else {
                    return response(400, "invalid voters");
                };
                if nodes.is_empty() {
                    return response(400, "empty voters");
                }
                json(&raft.change_membership(nodes, false).await)
            }
            ("_admin", "snapshot") => json(&raft.trigger().snapshot().await),
            _ => response(404, "unknown control operation"),
        }
    }
}

pub fn run() {
    crate::raise_nofile_limit();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .init();
    let file = std::env::args()
        .nth(2)
        .expect("--cluster-config needs a JSON file");
    assert_eq!(
        std::env::args().len(),
        3,
        "cluster configuration does not accept native tier/durability flags"
    );
    let config: Config =
        serde_json::from_slice(&std::fs::read(file).expect("read config")).expect("parse config");
    assert!(
        config.listen.ip().is_loopback(),
        "experimental control APIs are loopback-only"
    );
    assert!(config.node > 0 && (1..=64).contains(&config.partitions) && config.workers > 0);
    assert!(!config.genesis.is_empty() && config.genesis.keys().all(|id| *id > 0));
    assert!(
        !config.cluster.is_empty()
            && config
                .cluster
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
    );
    // Process-wide native apply mode: no duplicate WAL; do not expose this as a
    // client option. Original standalone mode uses main.rs unchanged.
    crate::handlers::set_durability(crate::handlers::DurabilityMode::Memory);
    crate::handlers::set_long_poll_timeout(config.long_poll_ms);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.workers)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        use std::io::Write;
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::PermissionsExt;
        std::fs::create_dir_all(&config.dir).unwrap();
        std::fs::set_permissions(&config.dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(config.dir.join("LOCK"))
            .unwrap();
        assert_eq!(
            unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            0,
            "data directory already owned"
        );
        let identity = serde_json::to_vec(&(
            4u32,
            &config.cluster,
            config.node,
            config.partitions,
            &config.genesis,
        ))
        .unwrap();
        let marker = config.dir.join("IDENTITY");
        if marker.exists() {
            assert_eq!(
                std::fs::read(&marker).unwrap(),
                identity,
                "node/cluster/partition identity mismatch"
            );
        } else {
            assert_eq!(
                std::fs::read_dir(&config.dir).unwrap().count(),
                1,
                "refuse unmarked existing data directory"
            );
            let mut out = std::fs::File::create(&marker).unwrap();
            out.write_all(&identity).unwrap();
            out.sync_all().unwrap();
            crate::store::fsync_parent_dir(&marker).unwrap();
        }
        crate::store::fsync_parent_dir(&config.dir).unwrap();
        let faults = Arc::new(std::sync::RwLock::new(network::Faults::default()));
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let mut groups = Vec::new();
        for group in 0..config.partitions {
            let dir = config.dir.join(group.to_string());
            let journal = journal::Journal::open(dir.join("wal"), 8 * 1024 * 1024)
                .expect("native journal recovery");
            let machine = machine::Machine::open(dir.join("state"), journal.clone())
                .await
                .expect("materialization recovery");
            crate::store::fsync_parent_dir(&dir.join("wal")).unwrap();
            crate::store::fsync_parent_dir(&dir).unwrap();
            let raft_config = openraft::Config {
                cluster_name: format!("{}-{group}", config.cluster),
                heartbeat_interval: 100,
                election_timeout_min: 350,
                election_timeout_max: 700,
                max_payload_entries: 64,
                snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(10_000),
                max_in_snapshot_log_to_keep: 64,
                snapshot_max_chunk_size: 64 * 1024,
                ..Default::default()
            }
            .validate()
            .unwrap();
            let raft = Raft::new(
                config.node,
                Arc::new(raft_config),
                network::Network {
                    group,
                    cluster: config.cluster.clone(),
                    client: client.clone(),
                    faults: faults.clone(),
                },
                journal,
                machine.clone(),
            )
            .await
            .expect("start Raft");
            groups.push(Group {
                raft,
                machine,
                slots: Arc::new(Semaphore::new(256)),
                reads: ReadBarrier::default(),
            });
        }
        if config.stats_secs > 0 {
            crate::wal::telemetry::set_stats_enabled(true);
            crate::wal::telemetry::spawn_stats_emitter(
                groups.iter().map(|g| g.machine.journal.shard.clone()).collect(),
                Duration::from_secs(config.stats_secs),
            );
            crate::srvstats::spawn(config.stats_secs);
        }
        let listener = tokio::net::TcpListener::bind(config.listen)
            .await
            .expect("listen");
        let placeholder = groups[0].machine.view.read().await.store.clone();
        let cluster = Cluster {
            config,
            groups,
            faults,
            client,
            _lock: lock,
        };
        assert!(CLUSTER.set(cluster).is_ok());
        for group in 0..CLUSTER.get().unwrap().groups.len() {
            tokio::spawn(CLUSTER.get().unwrap().subscription_worker(group));
            tokio::spawn(CLUSTER.get().unwrap().fork_worker(group));
        }
        println!("replicated Electric engine ready");
        crate::engine_raw::serve(placeholder, listener).await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::future::poll_fn;
    use std::task::Poll;

    proptest! {
        #[test]
        fn read_cohorts_exclude_inflight_round_even_after_cancel(
            count in 1usize..64, first_result in any::<bool>(), cancel in any::<bool>(),
        ) {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let gate = ReadBarrier::default();
                let calls = AtomicU64::new(0);
                let (release, wait) = tokio::sync::oneshot::channel();
                let mut first = Box::pin(gate.confirm(async {
                    calls.fetch_add(1,Ordering::SeqCst);
                    wait.await.unwrap()
                }));
                assert!(poll_fn(|cx|Poll::Ready(first.as_mut().poll(cx))).await.is_pending());
                let mut late = Vec::new();
                for _ in 0..count {
                    let mut request = Box::pin(gate.confirm(async {
                        calls.fetch_add(1,Ordering::SeqCst);
                        !first_result
                    }));
                    // Poll explicitly to establish arrival DURING round 1,
                    // without relying on scheduler sleeps or yield counts.
                    assert!(poll_fn(|cx|Poll::Ready(request.as_mut().poll(cx))).await.is_pending());
                    late.push(request);
                }
                if cancel { drop(first); } else {
                    release.send(first_result).unwrap();
                    assert_eq!(first.await,first_result);
                }
                for request in late { assert_eq!(request.await,!first_result); }
                assert_eq!(calls.load(Ordering::SeqCst),2,"late arrivals share only the NEW round");
                assert_eq!(gate.confirm(async {
                    calls.fetch_add(1,Ordering::SeqCst);
                    first_result
                }).await,first_result);
                assert_eq!(calls.load(Ordering::SeqCst),3,"a later invocation needs a fresh confirmation");
            });
        }
    }
}
