//! One process hosts a control group and fixed virtual data shards.
mod controller;
mod failure;
mod fork_http;
mod forks;
mod identity;
mod leader_balance;
mod sse;
mod telemetry;

use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Extension, Path, Query, Request, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, Uri},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{any, get, post},
};
use bytes::Bytes;
use chronicle_raft::{
    Raft, TypeConfig,
    model::{self, Command, Node, Producer, StreamConfig},
    network::Network,
    storage::SqliteStore,
    wire::{self, ParsedOffset},
};
use openraft::raft::{AppendEntriesRequest, TransferLeaderRequest, VoteRequest};
use openraft::type_config::async_runtime::WatchReceiver;
use openraft::{BasicNode, Config, Instant as _, SnapshotPolicy};
use openraft_legacy::network_v1::{ChunkedSnapshotReceiver, InstallSnapshotRequest};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, Semaphore};

pub struct Group {
    pub raft: Raft,
    pub store: SqliteStore,
    pub movement: Mutex<()>,
}
pub struct App {
    pub id: u64,
    /// Fixed-tenant mount for a dedicated API origin; None keeps canonical URLs.
    pub stream_tenant: Option<String>,
    pub identity: identity::Identity,
    pub nodes: BTreeMap<u64, Node>,
    pub groups: BTreeMap<u64, Group>,
    pub client: reqwest::Client,
    pub admission: Arc<Semaphore>,
    pub live_admission: Arc<Semaphore>,
    pub telemetry: Arc<telemetry::Telemetry>,
}
type Shared = Arc<App>;
type ApiResult = Result<Response, (StatusCode, String)>;
fn unavailable(e: impl std::fmt::Display) -> (StatusCode, String) {
    (StatusCode::SERVICE_UNAVAILABLE, e.to_string())
}
fn bad(e: impl std::fmt::Display) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, e.to_string())
}
fn error_status(error: &model::Error) -> StatusCode {
    match error {
        model::Error::Missing => StatusCode::NOT_FOUND,
        model::Error::EmptyBody | model::Error::InvalidFork => StatusCode::BAD_REQUEST,
        model::Error::EpochFenced => StatusCode::FORBIDDEN,
        model::Error::Capacity => StatusCode::TOO_MANY_REQUESTS,
        model::Error::PendingFork => StatusCode::SERVICE_UNAVAILABLE,
        model::Error::LegacyFork => StatusCode::NOT_IMPLEMENTED,
        model::Error::Gone => StatusCode::GONE,
        _ => StatusCode::CONFLICT,
    }
}
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (stdout, _log_guard) = tracing_appender::non_blocking::NonBlockingBuilder::default()
        .buffered_lines_limit(4096)
        .lossy(true)
        .finish(std::io::stdout());
    let log_errors = stdout.error_counter();
    tracing_subscriber::fmt()
        .json()
        .with_writer(stdout)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "chronicle_raft=info,warn".into()),
        )
        .init();
    let id: u64 = std::env::var("NODE_ID")?.parse()?;
    let nodes: BTreeMap<u64, Node> = serde_json::from_str(&std::env::var("CLUSTER_NODES")?)?;
    identity::validate_seeds(&nodes)?;
    let cluster = std::env::var("CLUSTER_ID")?;
    let dir = std::env::var("DATA_DIR").unwrap_or_else(|_| ".data".into());
    let listen = std::env::var("LISTEN").unwrap_or_else(|_| "0.0.0.0:8080".into());
    let stream_tenant = match std::env::var("STREAM_TENANT") {
        Ok(tenant) => {
            anyhow::ensure!(
                !tenant.is_empty() && tenant.len() <= 1024 && !tenant.contains(['/', '\0']),
                "invalid fixed stream tenant"
            );
            Some(tenant)
        }
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => return Err(error.into()),
    };
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let path = std::path::PathBuf::from(&dir);
    let directory = tokio::task::spawn_blocking(move || identity::Directory::lock(&path)).await??;
    let mode = std::env::args().nth(1);
    if let Some(mode) = mode {
        anyhow::ensure!(
            mode == "--init-genesis" || mode == "--init-learner",
            "unknown argument"
        );
        directory.require_empty()?;
        let genesis = mode == "--init-genesis";
        if genesis {
            anyhow::ensure!(nodes.contains_key(&id), "genesis ID must be a seed");
        } else {
            anyhow::ensure!(
                !nodes.contains_key(&id),
                "replacement must use a fresh non-seed ID"
            );
            let node = Node {
                addr: std::env::var("ADVERTISE")?,
                zone: std::env::var("ZONE").unwrap_or_else(|_| "unknown".into()),
                draining: false,
            };
            // Send exactly once: transport failure leaves admission unknown. Reusing that ID
            // automatically after an ambiguous admission would defeat lost-disk fencing.
            let seed = nodes
                .values()
                .next()
                .ok_or_else(|| anyhow::anyhow!("no seed"))?;
            client
                .post(format!("http://{}/admin/admit", seed.addr))
                .json(&(cluster.clone(), id, node))
                .send()
                .await?
                .error_for_status()?;
        }
        for group in 0..=model::SHARDS {
            SqliteStore::open(format!("{dir}/group-{group}.sqlite"))
                .await?
                .close()
                .await;
        }
        tokio::task::spawn_blocking(move || {
            directory.persist(&identity::Identity {
                cluster,
                node: id,
                genesis,
            })
        })
        .await??;
        return Ok(());
    }
    let local_identity = directory.restart(id, &cluster)?;
    let config = Arc::new(
        Config {
            heartbeat_interval: 200,
            election_timeout_min: 800,
            election_timeout_max: 1600,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(256),
            max_in_snapshot_log_to_keep: 64,
            snapshot_max_chunk_size: 512 * 1024,
            max_payload_entries: 1,
            replication_lag_threshold: 0,
            ..Config::default()
        }
        .validate()?,
    );
    let mut stores = BTreeMap::new();
    for group in 0..=model::SHARDS {
        let store = SqliteStore::open_existing(format!("{dir}/group-{group}.sqlite")).await?;
        stores.insert(group, store);
    }
    let mut groups = BTreeMap::new();
    for (group, store) in stores {
        let raft = Raft::new(
            id,
            config.clone(),
            Network {
                client: client.clone(),
                cluster: cluster.clone(),
                group,
            },
            store.clone(),
            store.clone(),
        )
        .await?;
        tokio::spawn(failure::monitor(id, group, raft.metrics()));
        groups.insert(
            group,
            Group {
                raft,
                store,
                movement: Mutex::new(()),
            },
        );
    }
    let app = Arc::new(App {
        id,
        stream_tenant,
        identity: local_identity,
        nodes,
        groups,
        client,
        admission: Arc::new(Semaphore::new(128)),
        live_admission: Arc::new(Semaphore::new(32)),
        telemetry: Arc::new(telemetry::Telemetry::new(
            id,
            log_errors,
            std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").unwrap_or_default(),
        )),
    });
    tokio::spawn(controller::run(app.clone()));
    tokio::spawn(forks::run(app.clone()));
    let router = Router::new()
        .route("/healthz", get(health))
        .route("/metrics", get(metrics))
        .route("/admin/status", get(status))
        .route(
            "/admin/resources",
            get(resources).layer(middleware::from_fn_with_state(
                Arc::new(Semaphore::new(8)),
                admit_stream,
            )),
        )
        .route(
            "/admin/fork",
            post(forks::rpc).layer(middleware::from_fn_with_state(
                Arc::new(Semaphore::new(16)),
                admit_stream,
            )),
        )
        .route("/admin/bootstrap", post(bootstrap))
        .route("/admin/register", post(register))
        .route("/admin/admit", post(admit))
        .route("/admin/placed", post(placed))
        .route("/admin/control", get(control))
        .route("/admin/leadership/{group}", get(leader_balance::observe))
        .route("/admin/leadership/claim", post(leader_balance::claim))
        .route("/admin/retirement/{id}", get(retirement))
        .route("/admin/retirement-state", get(retirement_state))
        .route("/admin/snapshot/{group}", post(snapshot))
        .route("/raft/{group}/append", post(append_rpc))
        .route("/raft/{group}/vote", post(vote_rpc))
        .route("/raft/{group}/snapshot", post(snapshot_rpc))
        .route("/raft/{group}/transfer", post(transfer_rpc))
        .route(
            if app.stream_tenant.is_some() {
                "/v1/stream/{*path}"
            } else {
                "/v1/stream/{tenant}/{*path}"
            },
            any(stream)
                .layer(DefaultBodyLimit::max(1024 * 1024))
                .layer(middleware::from_fn_with_state(
                    app.admission.clone(),
                    admit_stream,
                ))
                .layer(middleware::from_fn(browser_headers)),
        )
        .layer(DefaultBodyLimit::max(8 * 1024 * 1024))
        .with_state(app);
    axum::serve(tokio::net::TcpListener::bind(listen).await?, router).await?;
    Ok(())
}

async fn health(State(a): State<Shared>) -> impl IntoResponse {
    if a.groups
        .values()
        .any(|g| failure::storage_error(&g.raft.metrics().borrow_watched()).is_some())
    {
        (StatusCode::SERVICE_UNAVAILABLE, "fatal storage error")
    } else {
        (StatusCode::OK, "ok")
    }
}

async fn append_rpc(
    State(a): State<Shared>,
    Path(g): Path<u64>,
    headers: HeaderMap,
    Json(q): Json<AppendEntriesRequest<TypeConfig>>,
) -> ApiResult {
    rpc_recipient(&a.identity, &headers)?;
    Ok(Json(
        a.groups
            .get(&g)
            .ok_or_else(|| bad("group"))?
            .raft
            .append_entries(q)
            .await,
    )
    .into_response())
}
async fn vote_rpc(
    State(a): State<Shared>,
    Path(g): Path<u64>,
    headers: HeaderMap,
    Json(q): Json<VoteRequest<TypeConfig>>,
) -> ApiResult {
    rpc_recipient(&a.identity, &headers)?;
    Ok(Json(
        a.groups
            .get(&g)
            .ok_or_else(|| bad("group"))?
            .raft
            .vote(q)
            .await,
    )
    .into_response())
}
// Private recipient-bound protocol RPC, never an administrative transfer API.
async fn transfer_rpc(
    State(a): State<Shared>,
    Path(g): Path<u64>,
    headers: HeaderMap,
    Json(q): Json<TransferLeaderRequest<TypeConfig>>,
) -> ApiResult {
    rpc_recipient(&a.identity, &headers)?;
    Ok(Json(
        a.groups
            .get(&g)
            .ok_or_else(|| bad("group"))?
            .raft
            .handle_transfer_leader(q)
            .await
            .map_err(openraft::error::RaftError::<TypeConfig>::Fatal),
    )
    .into_response())
}

async fn snapshot_rpc(
    State(a): State<Shared>,
    Path(g): Path<u64>,
    headers: HeaderMap,
    Json(q): Json<InstallSnapshotRequest<TypeConfig>>,
) -> ApiResult {
    rpc_recipient(&a.identity, &headers)?;
    snapshot_range(q.offset, q.data.len())?;
    Ok(Json(
        a.groups
            .get(&g)
            .ok_or_else(|| bad("group"))?
            .raft
            .install_snapshot(q)
            .await,
    )
    .into_response())
}
fn snapshot_range(offset: u64, len: usize) -> Result<(), (StatusCode, String)> {
    // Bound assembly, not just the final decoded snapshot. Checked addition also
    // rejects malformed offsets without delegating allocation decisions to Raft.
    if offset
        .checked_add(len as u64)
        .is_none_or(|end| end > chronicle_raft::storage::MAX_SNAPSHOT_BYTES as u64)
    {
        return Err((StatusCode::PAYLOAD_TOO_LARGE, "snapshot too large".into()));
    }
    Ok(())
}
fn rpc_recipient(
    identity: &identity::Identity,
    headers: &HeaderMap,
) -> Result<(), (StatusCode, String)> {
    let cluster = headers
        .get("x-chronicle-cluster")
        .and_then(|h| h.to_str().ok());
    let recipient = headers
        .get("x-chronicle-recipient")
        .and_then(|h| h.to_str().ok())
        .and_then(|id| id.parse::<u64>().ok());
    if cluster != Some(identity.cluster.as_str()) || recipient != Some(identity.node) {
        return Err((
            StatusCode::MISDIRECTED_REQUEST,
            "Raft recipient identity mismatch".into(),
        ));
    }
    Ok(())
}

async fn status(State(a): State<Shared>) -> Response {
    Json(
        a.groups
            .iter()
            .map(|(id, g)| (*id, g.raft.metrics().borrow_watched().clone()))
            .collect::<BTreeMap<_, _>>(),
    )
    .into_response()
}
async fn resources(State(a): State<Shared>, headers: HeaderMap) -> ApiResult {
    rpc_recipient(&a.identity, &headers)?;
    let mut loads = BTreeMap::new();
    for (id, group) in &a.groups {
        let mut load = group.store.load().await.map_err(unavailable)?;
        load.view = leader_balance::view(group).await.map_err(unavailable)?;
        loads.insert(*id, load);
    }
    Ok(Json(loads).into_response())
}

async fn retirement_state(State(a): State<Shared>, headers: HeaderMap) -> ApiResult {
    rpc_recipient(&a.identity, &headers)?;
    // This core request is processed only after storage recovery. Ordinary
    // routing status stays nonblocking; retirement must not trust startup defaults.
    for group in a.groups.values() {
        group.raft.is_initialized().await.map_err(unavailable)?;
    }
    Ok(status(State(a)).await)
}
async fn metrics(State(a): State<Shared>) -> String {
    let mut text = a.telemetry.metrics();
    text.push_str(&format!(
        "# TYPE chronicle_live_read_available_slots gauge\nchronicle_live_read_available_slots {}\n\
         # TYPE chronicle_request_available_slots gauge\nchronicle_request_available_slots {}\n",
        a.live_admission.available_permits(),
        a.admission.available_permits()
    ));
    for (id, g) in &a.groups {
        let m = g.raft.metrics().borrow_watched().clone();
        text.push_str(&format!("chronicle_raft_leader{{group=\"{id}\"}} {}\nchronicle_raft_applied{{group=\"{id}\"}} {}\n", u8::from(m.state == openraft::ServerState::Leader), m.last_applied.map_or(0, |l| l.index())));
        text.push_str(&format!("chronicle_raft_quorum_available{{group=\"{id}\"}} {}\nchronicle_snapshot_index{{group=\"{id}\"}} {}\n",u8::from(m.state == openraft::ServerState::Leader && m.last_quorum_acked.is_some_and(|acked| acked.into_inner().elapsed() < Duration::from_millis(800))),m.snapshot.map_or(0,|l|l.index())));
        if let Some(replication) = m.replication {
            for (node, matched) in replication {
                text.push_str(&format!(
                    "chronicle_replica_lag{{group=\"{id}\",replica=\"{node}\"}} {}\n",
                    m.last_log_index
                        .unwrap_or(0)
                        .saturating_sub(matched.map_or(0, |l| l.index()))
                ));
            }
        }
    }
    text.push_str(&format!(
        "chronicle_backpressure_queue_depth {}\n",
        128 - a.admission.available_permits()
    ));
    if let Ok(state) = a.groups[&0].store.read_state().await {
        for (group, p) in state.placements {
            text.push_str(&format!(
                "chronicle_migration_age_seconds{{group=\"{group}\"}} {}\n",
                if p.complete {
                    0
                } else {
                    now_ms().saturating_sub(p.changed_ms) / 1000
                }
            ));
        }
    }
    text
}
async fn bootstrap(State(a): State<Shared>) -> ApiResult {
    if !a.identity.genesis || a.nodes.keys().next() != Some(&a.id) {
        return Err(bad(
            "bootstrap is restricted to the first genesis seed; never use it for recovery",
        ));
    }
    let members: BTreeMap<_, _> = a
        .nodes
        .iter()
        .map(|(id, n)| (*id, BasicNode::new(n.addr.clone())))
        .collect();
    if members.len() != 3 {
        return Err(bad("bootstrap requires exactly three seed nodes"));
    }
    for g in a.groups.values() {
        if !g.raft.is_initialized().await.map_err(unavailable)? {
            g.raft
                .initialize(members.clone())
                .await
                .map_err(unavailable)?;
        }
    }
    Ok("initialized".into_response())
}
pub async fn proxy(
    a: &App,
    group: u64,
    method: Method,
    uri: &str,
    mut headers: HeaderMap,
    body: Bytes,
) -> ApiResult {
    if headers.contains_key("x-chronicle-forwarded") {
        return Err(unavailable("leadership changed; retry"));
    }
    let mut nodes = a.groups[&0]
        .store
        .read_state()
        .await
        .map_err(unavailable)?
        .nodes;
    for (id, node) in &a.nodes {
        nodes.entry(*id).or_insert_with(|| node.clone());
    }
    let candidates = nodes.into_iter().filter(|(id, _)| *id != a.id).collect();
    let (_, address) = discover_leader(a, group, candidates).await?;
    headers.insert("x-chronicle-forwarded", "1".parse().map_err(bad)?);
    // Keep the caller's authority for Location; routing uses the explicit URL.
    // Admin handlers reserialize JSON before forwarding. The original framing
    // may describe different bytes; let reqwest frame the supplied body.
    headers.remove("content-length");
    headers.remove("transfer-encoding");
    let url = reqwest::Url::parse(&format!("http://{address}{uri}")).map_err(bad)?;
    let sse = method == Method::GET
        && url
            .query_pairs()
            .any(|(key, value)| key == "live" && value == "sse");
    let mut request = a.client.request(method, url).headers(headers).body(body);
    if sse {
        // Override the pool's 10s total deadline, including body delivery. The
        // destination caps subscriptions at 60s; leave time for setup and EOF.
        request = request.timeout(Duration::from_secs(65));
    }
    let r = request.send().await.map_err(unavailable)?;
    let status = r.status();
    let headers = r.headers().clone();
    let chunks = futures_util::stream::try_unfold(r, |mut response| async move {
        Ok::<_, reqwest::Error>(response.chunk().await?.map(|bytes| (bytes, response)))
    });
    Ok((status, headers, Body::from_stream(chunks)).into_response())
}
/// Bounded read-only routing discovery. A result is only a hint; the receiver
/// must still perform its barrier/client_write. Never retry mutations here.
pub async fn discover_leader(
    a: &App,
    group: u64,
    mut candidates: Vec<(u64, Node)>,
) -> Result<(u64, String), (StatusCode, String)> {
    let hint = a.groups[&group]
        .raft
        .metrics()
        .borrow_watched()
        .current_leader;
    candidates.sort_by_key(|(id, _)| Some(*id) != hint);
    // Removed replicas can retain an absent/stale local hint indefinitely.
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut candidates: VecDeque<_> =
            candidates.into_iter().map(|(id, n)| (id, n.addr)).collect();
        let mut seen = BTreeSet::new();
        for _ in 0..32 {
            let (id, address) = candidates.pop_front()?;
            if !seen.insert(id) {
                continue;
            }
            let response = a
                .client
                .get(format!("http://{address}/admin/status"))
                .timeout(Duration::from_millis(500))
                .send()
                .await;
            if let Ok(response) = response
                && response.status().is_success()
                && let Ok(status) = response.json::<serde_json::Value>().await
                && status[group.to_string()]["id"].as_u64() == Some(id)
            {
                let status = &status[group.to_string()];
                let leader = status["current_leader"].as_u64();
                if leader == Some(id) && status["state"].as_str() == Some("Leader") {
                    return Some((id, address));
                }
                // A surviving peer may know a leader absent from our retired
                // registry. Probe that hint; never treat membership as a read barrier.
                if let Some(leader) = leader
                    && !seen.contains(&leader)
                    && let Some(address) = status["membership_config"]["membership"]["nodes"]
                        [leader.to_string()]["addr"]
                        .as_str()
                {
                    candidates.push_front((leader, address.to_owned()));
                }
            }
        }
        None
    })
    .await
    .map_err(unavailable)?
    .ok_or_else(|| unavailable("no leader; retry"))
}

async fn control(State(a): State<Shared>, headers: HeaderMap) -> ApiResult {
    let g = &a.groups[&0];
    if g.raft.metrics().borrow_watched().state != openraft::ServerState::Leader {
        return proxy(&a, 0, Method::GET, "/admin/control", headers, Bytes::new()).await;
    }
    g.raft
        .ensure_linearizable(openraft::ReadPolicy::ReadIndex)
        .await
        .map_err(unavailable)?;
    Ok(Json(g.store.read_state().await.map_err(unavailable)?).into_response())
}
async fn retirement(State(a): State<Shared>, Path(id): Path<u64>) -> ApiResult {
    Ok(Json(controller::retired(&a, id).await.map_err(unavailable)?).into_response())
}
async fn register(
    State(a): State<Shared>,
    headers: HeaderMap,
    Json((id, node)): Json<(u64, Node)>,
) -> ApiResult {
    let g = &a.groups[&0];
    if g.raft.metrics().borrow_watched().state != openraft::ServerState::Leader {
        return proxy(
            &a,
            0,
            Method::POST,
            "/admin/register",
            headers,
            serde_json::to_vec(&(id, node)).map_err(bad)?.into(),
        )
        .await;
    }
    // Admin API is private to the trusted cluster network. Not an unauthenticated public API.
    if node.addr.len() > 256 || node.zone.len() > 64 {
        return Err(bad("node metadata too long"));
    }
    let result = g
        .raft
        .client_write(Command::Register { id, node })
        .await
        .map_err(unavailable)?;
    if let Some(error) = result.data.error {
        return Err((StatusCode::CONFLICT, format!("{error:?}")));
    }
    Ok(Json(result).into_response())
}
async fn admit(
    State(a): State<Shared>,
    headers: HeaderMap,
    Json((cluster, id, node)): Json<(String, u64, Node)>,
) -> ApiResult {
    if cluster != a.identity.cluster
        || a.nodes.contains_key(&id)
        || a.nodes.values().any(|seed| seed.addr == node.addr)
        || node.addr.len() > 256
        || node.zone.len() > 64
    {
        return Err(bad("invalid learner admission"));
    }
    let g = &a.groups[&0];
    if g.raft.metrics().borrow_watched().state != openraft::ServerState::Leader {
        return proxy(
            &a,
            0,
            Method::POST,
            "/admin/admit",
            headers,
            serde_json::to_vec(&(cluster, id, node))
                .map_err(bad)?
                .into(),
        )
        .await;
    }
    let result = g
        .raft
        .client_write(Command::Admit { id, node })
        .await
        .map_err(unavailable)?;
    if let Some(error) = result.data.error {
        return Err((
            StatusCode::CONFLICT,
            format!("{error:?}; choose a fresh ID after unknown admission"),
        ));
    }
    Ok(Json(result).into_response())
}
async fn snapshot(State(a): State<Shared>, Path(id): Path<u64>) -> ApiResult {
    a.groups
        .get(&id)
        .ok_or_else(|| bad("group"))?
        .raft
        .trigger()
        .snapshot()
        .await
        .map_err(unavailable)?;
    Ok("snapshot requested".into_response())
}

async fn placed(
    State(a): State<Shared>,
    Json((shard, generation, membership)): Json<(u64, u64, Option<chronicle_raft::LogId>)>,
) -> ApiResult {
    let result = a.groups[&0]
        .raft
        .client_write(Command::Placed {
            shard,
            generation,
            membership,
        })
        .await
        .map_err(unavailable)?;
    if result.data.error.is_some() {
        return Err(unavailable("placement generation changed"));
    }
    Ok(Json(result).into_response())
}

async fn browser_headers(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    response.headers_mut().insert(
        "cross-origin-resource-policy",
        HeaderValue::from_static("cross-origin"),
    );
    response
}

async fn admit_stream(
    State(admission): State<Arc<Semaphore>>,
    mut request: Request,
    next: Next,
) -> ApiResult {
    let context = telemetry::RequestContext::from_headers(request.headers());
    context.inject(request.headers_mut());
    request.extensions_mut().insert(context.clone());
    if request.method() == Method::PUT
        && !request.headers().contains_key("stream-forked-from")
        && ["stream-fork-offset", "stream-fork-sub-offset"]
            .iter()
            .any(|name| request.headers().contains_key(*name))
    {
        let mut response = (
            StatusCode::BAD_REQUEST,
            "fork offset requires Stream-Forked-From",
        )
            .into_response();
        context.inject(response.headers_mut());
        return Ok(response);
    }
    // Acquire before the Bytes extractor reads the body, not after allocation.
    let Ok(permit) = admission.try_acquire_owned() else {
        let mut response = (StatusCode::TOO_MANY_REQUESTS, "admission full").into_response();
        context.inject(response.headers_mut());
        return Ok(response);
    };
    let permit = Arc::new(permit);
    request.extensions_mut().insert(permit.clone());
    let mut response = next.run(request).await;
    context.inject(response.headers_mut());
    // Keep admission through delivery: slow readers must not accumulate unbounded
    // file handles and detached cache generations after the handler returns.
    Ok(
        response.map(|body| {
            use futures_util::{StreamExt, stream};
            Body::from_stream(stream::unfold(
                (body.into_data_stream(), permit),
                |(mut body, permit)| async move {
                    body.next().await.map(|chunk| (chunk, (body, permit)))
                },
            ))
        }),
    )
}

#[allow(clippy::too_many_arguments)]
async fn stream(
    State(a): State<Shared>,
    Extension(context): Extension<telemetry::RequestContext>,
    Extension(admission): Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    Path(mut params): Path<BTreeMap<String, String>>,
    Query(query): Query<BTreeMap<String, String>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult {
    let tenant = a
        .stream_tenant
        .clone()
        .or_else(|| params.remove("tenant"))
        .ok_or_else(|| bad("stream tenant required"))?;
    let path = params
        .remove("path")
        .ok_or_else(|| bad("stream path required"))?;
    let key = format!("{}:{tenant}{path}", tenant.len());
    let shard = model::shard(&key);
    let m = a.groups[&shard].raft.metrics().borrow_watched().clone();
    let mut observation = a.telemetry.observe(
        context,
        telemetry::Completion {
            method: method.to_string(),
            shard,
            term: m.current_term,
            applied: m.last_applied.map_or(0, |l| l.index()),
            bytes_in: body.len(),
            ..Default::default()
        },
    );
    let result = stream_inner(
        &a,
        shard,
        key,
        query,
        method.clone(),
        uri,
        headers,
        body,
        &mut observation.completion.timings,
        admission,
    )
    .await;
    let m = a.groups[&shard].raft.metrics().borrow_watched().clone();
    observation.completion.term = m.current_term;
    observation.completion.applied = m.last_applied.map_or(0, |l| l.index());
    Ok(observation.response(result.unwrap_or_else(IntoResponse::into_response)))
}

#[allow(clippy::too_many_arguments)]
async fn stream_inner(
    a: &Shared,
    shard: u64,
    key: String,
    query: BTreeMap<String, String>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
    timings: &mut telemetry::PhaseTimings,
    admission: Arc<tokio::sync::OwnedSemaphorePermit>,
) -> ApiResult {
    if key.len() > 2048 || key.contains('\0') {
        return Err(bad("invalid stream identity"));
    }
    let g = &a.groups[&shard];
    let stale = query.get("consistency").is_some_and(|v| v == "stale") && method == Method::GET;
    let live = match query.get("live").map(String::as_str) {
        None => None,
        Some(mode @ ("long-poll" | "sse"))
            if method == Method::GET && !stale && query.contains_key("offset") =>
        {
            Some(mode)
        }
        _ => {
            return Err(bad(
                "live reads require strict GET, offset and live=long-poll or sse",
            ));
        }
    };
    // Reserve fewer waiters than the overall request limit, including on ingress
    // forwarders, so quiet long polls cannot occupy every writer admission slot.
    let live_permit = if live.is_some() {
        Some(Arc::new(
            a.live_admission.clone().try_acquire_owned().map_err(|_| {
                (
                    StatusCode::TOO_MANY_REQUESTS,
                    "live read admission full".into(),
                )
            })?,
        ))
    } else {
        None
    };
    if !stale && g.raft.metrics().borrow_watched().state != openraft::ServerState::Leader {
        let started = Instant::now();
        let result = proxy(a, shard, method, &uri.to_string(), headers, body).await;
        timings.forward_us = started.elapsed().as_micros() as u64;
        // Forwarded SSE headers arrive before the stream ends. Keep its live
        // reservation with the body, not just with the header-producing future.
        return result.map(|response| {
            response.map(|body| {
                use futures_util::{StreamExt, stream};
                Body::from_stream(stream::unfold(
                    (body.into_data_stream(), live_permit),
                    |(mut body, permit)| async move {
                        body.next().await.map(|chunk| (chunk, (body, permit)))
                    },
                ))
            })
        });
    }
    if method == Method::PUT && headers.contains_key("stream-forked-from") {
        return tokio::time::timeout(
            Duration::from_secs(8),
            fork_http::create(a, key, headers, body, uri),
        )
        .await
        .map_err(|_| unavailable("fork timeout: outcome unknown"))?;
    }
    let changes = live_permit
        .as_ref()
        .map(|permit| (g.store.applied_changes(), permit.clone()));
    let existing = read_visible_info(
        g,
        &key,
        stale,
        timings,
        (admission.clone(), live_permit.clone()),
    )
    .await
    .map_err(|(status, message)| {
        (
            if method == Method::PUT && status == StatusCode::GONE {
                StatusCode::CONFLICT
            } else {
                status
            },
            message,
        )
    })?;
    if (method == Method::PUT || (method == Method::POST && !body.is_empty()))
        && let Some(value) = headers.get("content-type")
    {
        value.to_str().map_err(bad)?;
    }
    let h = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    let close = h("stream-closed").is_some_and(|v| v.eq_ignore_ascii_case("true"));
    let content_type = h("content-type")
        .unwrap_or("application/octet-stream")
        .to_string();
    if method == Method::GET || method == Method::HEAD {
        let mut s = existing.ok_or_else(|| (StatusCode::NOT_FOUND, "stream missing".into()))?;
        if method == Method::GET
            && !stale
            && matches!(
                s.config.expiry,
                Some(chronicle_raft::expiry::Expiry::Ttl(_))
            )
        {
            let started = Instant::now();
            let result = admitted_write(
                g.raft.clone(),
                Command::Touch {
                    key: key.clone(),
                    incarnation: s.incarnation,
                    now_ms: now_ms(),
                },
                (admission.clone(), live_permit.clone()),
            )
            .await?;
            telemetry::commit_apply(started.elapsed());
            timings.proposal_us += started.elapsed().as_micros() as u64;
            if let Some(error @ (model::Error::PendingFork | model::Error::Gone)) =
                &result.data.error
            {
                return Err((error_status(error), format!("{error:?}")));
            }
            if result.data.error.is_some() {
                return Err((StatusCode::NOT_FOUND, "stream expired or replaced".into()));
            }
        }
        let offset =
            match wire::parse_offset(query.get("offset").map(String::as_str)).map_err(bad)? {
                ParsedOffset::Start => 0,
                ParsedOffset::Now => s.end,
                // Electric treats a future live cursor as caught up at this tail.
                ParsedOffset::At(n) if live.is_some() => n.min(s.end),
                ParsedOffset::At(n) => n,
            };
        if offset > s.end {
            return Err((
                StatusCode::RANGE_NOT_SATISFIABLE,
                "offset beyond committed tail".into(),
            ));
        }
        let cursor = if let Some((changes, live_permit)) = changes {
            let client = query
                .get("cursor")
                .map(|v| v.parse::<u64>().map_err(bad))
                .transpose()?;
            if live == Some("sse") {
                return sse::response(
                    a.clone(),
                    key,
                    offset,
                    s,
                    changes,
                    client,
                    [admission, live_permit],
                )
                .await;
            }
            s = wait_for_data(
                g,
                &key,
                offset,
                s,
                changes,
                timings,
                [admission.clone(), live_permit],
            )
            .await?;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            Some(wire::compute_cursor(client, now).map_err(bad)?)
        } else {
            None
        };
        let json = s.config.is_json();
        let length = (s.end - offset).saturating_sub(u64::from(json));
        let mut r = Response::builder()
            .status(200)
            .header("content-type", &s.config.content_type)
            .header("stream-next-offset", wire::format_offset(s.end))
            .header("stream-up-to-date", "true")
            .header("stream-incarnation", s.incarnation.to_string())
            .header("cache-control", "no-store")
            .header("stream-consistency", if stale { "stale" } else { "strict" });
        if s.closed {
            r = r.header("stream-closed", "true");
        }
        match s.config.expiry {
            Some(chronicle_raft::expiry::Expiry::Ttl(seconds)) => {
                r = r.header("stream-ttl", seconds.to_string());
            }
            Some(policy) => {
                if let Some(value) = policy.absolute_header() {
                    r = r.header("stream-expires-at", value);
                }
            }
            None => {}
        }
        if let Some(cursor) = cursor {
            r = r.header("stream-cursor", cursor.to_string());
            if offset == s.end {
                return r.status(204).body(Body::empty()).map_err(unavailable);
            }
        }
        let started = Instant::now();
        let tag = (live.is_none() && method == Method::GET)
            .then(|| wire::etag(&key, s.incarnation, offset, s.end, s.closed, json));
        let file = g
            .store
            .read_file(key, &s, offset, (admission.clone(), live_permit))
            .await;
        timings.read_us += started.elapsed().as_micros() as u64;
        let file = file.map_err(|e| match e {
            chronicle_raft::storage::ReadError::Offset => bad(e),
            _ => unavailable(e),
        })?;
        if let Some(tag) = tag {
            r = r.header("etag", &tag);
            if headers.get_all("if-none-match").iter().any(|value| {
                value
                    .to_str()
                    .is_ok_and(|value| wire::matches_etag(value, &tag))
            }) {
                return r.status(304).body(Body::empty()).map_err(unavailable);
            }
        }
        return r
            .header("content-length", length + if json { 2 } else { 0 })
            .body(if method == Method::HEAD {
                Body::empty()
            } else {
                wire::file_body(file, length, json, admission)
            })
            .map_err(unavailable);
    }
    // Validate response metadata before accepting a durable create.
    let location = if method == Method::PUT {
        let authority = uri
            .authority()
            .map(|v| v.as_str())
            .or_else(|| h("host"))
            .ok_or_else(|| bad("request authority required"))?
            .parse::<axum::http::uri::Authority>()
            .map_err(bad)?;
        Some(
            format!(
                "{}://{}{}",
                uri.scheme_str().unwrap_or("http"),
                authority,
                uri.path()
            )
            .parse::<HeaderValue>()
            .map_err(bad)?,
        )
    } else {
        None
    };
    let mut incarnations = headers.get_all("stream-incarnation").iter();
    let requested_incarnation = incarnations
        .next()
        .map(|value| value.to_str().map_err(bad)?.parse::<u64>().map_err(bad))
        .transpose()?;
    if incarnations.next().is_some() {
        return Err(bad("repeated incarnation header"));
    }
    let command = if method == Method::PUT {
        for name in ["stream-ttl", "stream-expires-at"] {
            let mut values = headers.get_all(name).iter();
            if let Some(value) = values.next() {
                value.to_str().map_err(bad)?;
            }
            if values.next().is_some() {
                return Err(bad("repeated expiry header"));
            }
        }
        let expiry = chronicle_raft::expiry::Expiry::parse(h("stream-ttl"), h("stream-expires-at"))
            .map_err(bad)?;
        let wire = if body.is_empty() {
            Bytes::new()
        } else {
            wire::encode_wire(
                &body,
                model::content_type_matches(&content_type, "application/json"),
                true,
            )
            .map_err(bad)?
        };
        let incarnation = if let Some(incarnation) = requested_incarnation {
            incarnation
        } else {
            // Preserve tombstones when binding a fresh request. Never retarget
            // this incarnation during apply if a concurrent lifecycle change wins.
            let started = Instant::now();
            let previous = g.store.read_info(key.clone(), admission.clone()).await;
            timings.read_us += started.elapsed().as_micros() as u64;
            previous
                .map_err(unavailable)?
                .map_or(Some(1), |s| {
                    if s.deleted {
                        s.incarnation.checked_add(1)
                    } else {
                        Some(s.incarnation)
                    }
                })
                .ok_or_else(|| (StatusCode::CONFLICT, "incarnation exhausted".into()))?
        };
        Command::Create {
            key,
            expected_incarnation: Some(incarnation),
            config: StreamConfig {
                content_type: content_type.clone(),
                track_boundaries: true,
                json_framing: Some(model::content_type_matches(
                    &content_type,
                    "application/json",
                )),
                expiry,
            },
            data: wire.to_vec(),
            closed: close,
            now_ms: Some(now_ms()),
        }
    } else if method == Method::POST || method == Method::DELETE {
        let s = existing.ok_or_else(|| (StatusCode::NOT_FOUND, "stream missing".into()))?;
        let incarnation = requested_incarnation.unwrap_or(s.incarnation);
        if method == Method::DELETE {
            Command::Delete {
                key,
                incarnation,
                expired_at: None,
            }
        } else {
            if body.is_empty() && !close && h("producer-id").is_none() {
                return Err(bad("empty append"));
            }
            if !body.is_empty() {
                if h("content-type").is_none_or(str::is_empty) {
                    return Err(bad("Content-Type required for append body"));
                }
                if !model::content_type_matches(&content_type, &s.config.content_type) {
                    return Err((StatusCode::CONFLICT, "content-type mismatch".into()));
                }
            }
            for name in ["producer-id", "producer-epoch", "producer-seq"] {
                if let Some(value) = headers.get(name) {
                    value.to_str().map_err(bad)?;
                }
            }
            let producer = match (h("producer-id"), h("producer-epoch"), h("producer-seq")) {
                (None, None, None) => None,
                (Some(id), Some(epoch), Some(seq)) if !id.is_empty() && id.len() <= 256 => {
                    Some(Producer {
                        id: id.into(),
                        epoch: epoch.parse().map_err(bad)?,
                        seq: seq.parse().map_err(bad)?,
                    })
                }
                _ => return Err(bad("all producer headers are required")),
            };
            let mut tokens = headers.get_all("stream-seq").iter();
            let stream_seq = tokens
                .next()
                .map(|value| value.to_str().map(str::to_owned))
                .transpose()
                .map_err(bad)?;
            if tokens.next().is_some() {
                return Err(bad("at most one Stream-Seq field is allowed"));
            }
            let wire = if body.is_empty() {
                Bytes::new()
            } else {
                wire::encode_wire(&body, s.config.is_json(), false).map_err(bad)?
            };
            Command::Append {
                key,
                incarnation,
                data: wire.to_vec(),
                producer,
                close,
                empty_body: body.is_empty(),
                stream_seq,
                now_ms: Some(now_ms()),
            }
        }
    } else {
        return Err((StatusCode::METHOD_NOT_ALLOWED, "unsupported method".into()));
    };
    let requested_producer = match &command {
        Command::Append { producer, .. } => producer.as_ref().map(|p| model::ProducerPosition {
            epoch: p.epoch,
            seq: p.seq,
        }),
        _ => None,
    };
    // No duplicate shortcut: every response follows durable-majority apply, even retries.
    // This measures the caller-visible proposal-through-apply path. It deliberately is not
    // labelled as replication latency: queueing, persistence and state-machine apply are included.
    let commit_apply_started = Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(8),
        admitted_write(g.raft.clone(), command, admission),
    )
    .await;
    telemetry::commit_apply(commit_apply_started.elapsed());
    timings.proposal_us += commit_apply_started.elapsed().as_micros() as u64;
    let result = result.map_err(|_| unavailable("timeout: outcome unknown"))??;
    let mut response = Response::builder();
    if let Some(position) = result.data.producer {
        response = response
            .header("producer-epoch", position.epoch.to_string())
            .header("producer-seq", position.seq.to_string());
    }
    if let Some(error) = result.data.error {
        let mut status = error_status(&error);
        if error == model::Error::SequenceGap
            && let Some(requested) = requested_producer
        {
            let expected = match result.data.producer {
                Some(current) if current.epoch == requested.epoch => current.seq.checked_add(1),
                Some(_) => {
                    status = StatusCode::BAD_REQUEST;
                    Some(0)
                }
                None => Some(0),
            };
            if let Some(expected) = expected {
                response = response.header("producer-expected-seq", expected.to_string());
            }
            response = response.header("producer-received-seq", requested.seq.to_string());
        }
        if error == model::Error::StreamSequenceConflict {
            response = response
                .header("stream-next-offset", wire::format_offset(result.data.end))
                .header("stream-incarnation", result.data.incarnation.to_string());
        }
        if error == model::Error::Closed {
            return response
                .status(status)
                .header("stream-closed", "true")
                .header("stream-next-offset", wire::format_offset(result.data.end))
                .header("stream-incarnation", result.data.incarnation.to_string())
                .body(Body::empty())
                .map_err(unavailable);
        }
        return response
            .status(status)
            .header("content-type", "text/plain; charset=utf-8")
            .body(Body::from(format!("{error:?}")))
            .map_err(unavailable);
    }
    let status = if method == Method::PUT {
        if result.data.duplicate { 200 } else { 201 }
    } else if method == Method::DELETE
        || result.data.duplicate
        || h("producer-id").is_none()
        || body.is_empty()
    {
        204
    } else {
        200
    };
    response = response
        .status(status)
        .header("stream-next-offset", wire::format_offset(result.data.end))
        .header("stream-incarnation", result.data.incarnation.to_string())
        .header("stream-commit-index", result.log_id.index().to_string())
        .header("stream-duplicate", result.data.duplicate.to_string());
    if result.data.closed {
        response = response.header("stream-closed", "true");
    }
    if method == Method::PUT {
        // A successful create outcome has atomically validated this config,
        // including on a retry. Do not sample mutable stream metadata afterward.
        response = response.header(
            "content-type",
            result
                .data
                .content_type
                .ok_or_else(|| unavailable("create outcome missing stored content type"))?,
        );
        if status == 201
            && let Some(location) = location
        {
            response = response.header("location", location);
        }
    }
    response.body(Body::empty()).map_err(unavailable)
}

async fn admitted_write(
    raft: Raft,
    command: Command,
    admission: impl Send + 'static,
) -> Result<openraft::raft::ClientWriteResponse<TypeConfig>, (StatusCode, String)> {
    // Dropping a JoinHandle detaches rather than aborts. HTTP cancellation must
    // not free capacity while the local client_write waiter remains pending.
    tokio::spawn(async move {
        let result = raft.client_write(command).await;
        drop(admission);
        result
    })
    .await
    .map_err(unavailable)?
    .map_err(unavailable)
}

async fn wait_for_data(
    g: &Group,
    key: &str,
    offset: u64,
    mut view: chronicle_raft::storage::StreamInfo,
    mut changes: tokio::sync::watch::Receiver<()>,
    timings: &mut telemetry::PhaseTimings,
    admission: [Arc<tokio::sync::OwnedSemaphorePermit>; 2],
) -> Result<chronicle_raft::storage::StreamInfo, (StatusCode, String)> {
    let incarnation = view.incarnation;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while view.end == offset && !view.closed && tokio::time::Instant::now() < deadline {
        let waiting = Instant::now();
        tokio::select! {
            result = changes.changed() => { result.map_err(unavailable)?; }
            _ = tokio::time::sleep_until(deadline) => {
                #[cfg(feature = "storage-faults")]
                chronicle_raft::faults::before_live_recheck(model::shard(key), admission.clone())
                    .await
                    .map_err(unavailable)?;
            }
        }
        timings.wait_us += waiting.elapsed().as_micros() as u64;
        // Recheck after a deadline too: never return an empty response carrying
        // the offset of bytes that arrived during the wait but were not delivered.
        view = read_visible_info(g, key, false, timings, admission.clone())
            .await?
            .ok_or_else(|| (StatusCode::NOT_FOUND, "stream missing".into()))?;
        if view.incarnation != incarnation {
            return Err((StatusCode::CONFLICT, "stream incarnation changed".into()));
        }
    }
    Ok(view)
}

/// Strict visibility and expiration are shared by initial reads and subsequent
/// live-read wakeups. Local notifications cannot authorize a successful read.
async fn read_visible_info(
    g: &Group,
    key: &str,
    stale: bool,
    timings: &mut telemetry::PhaseTimings,
    admission: impl Clone + Send + 'static,
) -> Result<Option<chronicle_raft::storage::StreamInfo>, (StatusCode, String)> {
    if !stale {
        let started = Instant::now();
        let raft = g.raft.clone();
        let guard = admission.clone();
        // The barrier also submits to OpenRaft's unbounded API queue. Keep its
        // waiter admitted if the HTTP caller disconnects while the core stalls.
        let result = tokio::spawn(async move {
            let result = raft
                .ensure_linearizable(openraft::ReadPolicy::ReadIndex)
                .await;
            drop(guard);
            result
        })
        .await
        .map_err(unavailable)?;
        timings.barrier_us += started.elapsed().as_micros() as u64;
        result.map_err(unavailable)?;
    }
    for _ in 0..8 {
        let started = Instant::now();
        let existing = g.store.read_info(key.to_owned(), admission.clone()).await;
        timings.read_us += started.elapsed().as_micros() as u64;
        let existing = existing.map_err(unavailable)?;
        if existing.as_ref().is_some_and(|s| s.fork_pending) {
            return Err(unavailable("fork preparation pending; retry"));
        }
        if existing.as_ref().is_some_and(|s| s.soft_deleted) {
            return Err((StatusCode::GONE, "source retained by forks".into()));
        }
        let existing = existing.filter(|s| !s.deleted);
        let now = now_ms();
        let Some(s) = existing
            .as_ref()
            .filter(|s| !stale && s.config.expiry.is_some_and(|p| p.expired(s.access_ms, now)))
        else {
            return Ok(existing);
        };
        let started = Instant::now();
        let result = admitted_write(
            g.raft.clone(),
            Command::Expire {
                key: key.to_owned(),
                incarnation: s.incarnation,
                access_ms: s.access_ms,
                now_ms: now,
            },
            admission.clone(),
        )
        .await;
        telemetry::commit_apply(started.elapsed());
        timings.proposal_us += started.elapsed().as_micros() as u64;
        if result?.data.error.is_none() {
            continue; // Re-read: expiration can soft-delete a retained source.
        }
        // A concurrent renewal or recreation invalidates the observed expiry.
        // Re-read instead of reporting that a still-live stream was deleted.
    }
    Err(unavailable("expiry changed repeatedly; retry"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancelled_write_and_expiry_retain_admission_until_raft_completes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft.sqlite");
        let store = SqliteStore::open(&path).await.unwrap();
        let raft = Raft::new(
            1,
            Arc::new(Config::default().validate().unwrap()),
            Network {
                client: reqwest::Client::new(),
                cluster: "admission-test".into(),
                group: 1,
            },
            store.clone(),
            store.clone(),
        )
        .await
        .unwrap();
        raft.initialize(BTreeMap::from([(1, BasicNode::new("unused"))]))
            .await
            .unwrap();
        raft.wait(Some(Duration::from_secs(3)))
            .state(openraft::ServerState::Leader, "bootstrap")
            .await
            .unwrap();
        raft.ensure_linearizable(openraft::ReadPolicy::ReadIndex)
            .await
            .unwrap();
        // Hold SQLite's writer lock, not an artificial completion future. The
        // actual Raft write cannot persist until this transaction rolls back.
        let blocker = rusqlite::Connection::open(&path).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
        let admission = Arc::new(Semaphore::new(1));
        let permit = admission.clone().try_acquire_owned().unwrap();
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            admitted_write(
                raft.clone(),
                Command::Create {
                    key: "cancelled".into(),
                    expected_incarnation: None,
                    config: StreamConfig {
                        content_type: "application/octet-stream".into(),
                        track_boundaries: false,
                        json_framing: None,
                        expiry: None,
                    },
                    data: b"accepted after cancellation".to_vec(),
                    closed: false,
                    now_ms: None,
                },
                permit,
            ),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(admission.available_permits(), 0);
        assert!(admission.clone().try_acquire_owned().is_err());
        blocker.execute_batch("ROLLBACK").unwrap();
        let released = tokio::time::timeout(Duration::from_secs(3), admission.acquire())
            .await
            .unwrap()
            .unwrap();
        let stream = store
            .read_info("cancelled".into(), ())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stream.end, 27);
        assert_eq!(stream.incarnation, 1);
        drop(released);

        raft.client_write(Command::Create {
            key: "expired".into(),
            expected_incarnation: None,
            config: StreamConfig {
                content_type: "application/octet-stream".into(),
                track_boundaries: false,
                json_framing: None,
                expiry: Some(chronicle_raft::expiry::Expiry::At {
                    seconds: 0,
                    nanos: 0,
                }),
            },
            data: vec![7],
            closed: false,
            now_ms: None,
        })
        .await
        .unwrap();
        let live = Arc::new(Semaphore::new(1));
        let guards = (
            Arc::new(admission.clone().try_acquire_owned().unwrap()),
            Arc::new(live.clone().try_acquire_owned().unwrap()),
        );
        let group = Group {
            raft: raft.clone(),
            store: store.clone(),
            movement: Mutex::new(()),
        };
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
        let mut timings = telemetry::PhaseTimings::default();
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            read_visible_info(&group, "expired", false, &mut timings, guards),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(admission.available_permits(), 0);
        assert_eq!(live.available_permits(), 0);
        blocker.execute_batch("ROLLBACK").unwrap();
        let released = tokio::time::timeout(Duration::from_secs(3), admission.acquire())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(live.available_permits(), 1);
        assert!(
            store
                .read_info("expired".into(), ())
                .await
                .unwrap()
                .unwrap()
                .deleted
        );
        drop(released);
        drop(group);
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
        let guards = (
            Arc::new(admission.clone().try_acquire_owned().unwrap()),
            Arc::new(live.clone().try_acquire_owned().unwrap()),
        );
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            admitted_write(
                raft.clone(),
                Command::Touch {
                    key: "cancelled".into(),
                    incarnation: 1,
                    now_ms: 1,
                },
                guards,
            ),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(admission.available_permits(), 0);
        assert_eq!(live.available_permits(), 0);
        blocker.execute_batch("ROLLBACK").unwrap();
        let released = tokio::time::timeout(Duration::from_secs(3), admission.acquire())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(live.available_permits(), 1);
        drop(released);
        raft.shutdown().await.unwrap();
        drop(raft);
        store.close().await;
    }

    #[tokio::test]
    async fn orphan_fork_headers_reject_before_admission_or_body_extraction() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        // Exhausted admission distinguishes this gate from entering the handler.
        let admission = Arc::new(Semaphore::new(0));
        let router = Router::new()
            .route("/", any(|body: Bytes| async move { body }))
            .layer(middleware::from_fn_with_state(admission, admit_stream));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        for (method, header, status) in [
            ("PUT", "Stream-Forked-From", "429"),
            ("PUT", "Stream-Fork-Offset", "400"),
            ("PUT", "Stream-Fork-Sub-Offset", "400"),
            ("PUT", "Stream-Expires-At", "429"),
            ("POST", "Stream-Seq", "429"),
            ("POST", "X-Unrelated", "429"),
        ] {
            let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
            client
                .write_all(
                    format!("{method} / HTTP/1.1\r\nHost: test\r\n{header}: value\r\nContent-Length: 1\r\n\r\n")
                        .as_bytes(),
                )
                .await
                .unwrap();
            // Deliberately never send the body byte. Reading it would deadlock.
            let mut prefix = [0; 12];
            tokio::time::timeout(Duration::from_secs(2), client.read_exact(&mut prefix))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(prefix.as_slice(), format!("HTTP/1.1 {status}").as_bytes());
        }
        server.abort();
    }

    #[tokio::test]
    async fn admission_lasts_until_streaming_response_is_dropped() {
        use futures_util::{StreamExt, stream};
        let admission = Arc::new(Semaphore::new(1));
        let router = Router::new()
            .route(
                "/",
                get(|| async {
                    Body::from_stream(
                        stream::once(async {
                            Ok::<_, std::io::Error>(Bytes::from_static(b"first"))
                        })
                        .chain(stream::pending()),
                    )
                }),
            )
            .layer(middleware::from_fn_with_state(
                admission.clone(),
                admit_stream,
            ))
            .layer(middleware::from_fn(browser_headers));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = reqwest::Client::new();
        let response = client
            .get(format!("http://{address}/"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["x-content-type-options"], "nosniff");
        assert_eq!(
            response.headers()["cross-origin-resource-policy"],
            "cross-origin"
        );
        assert_eq!(admission.available_permits(), 0);
        let rejected = client
            .get(format!("http://{address}/"))
            .send()
            .await
            .unwrap();
        assert_eq!(rejected.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(rejected.headers()["x-content-type-options"], "nosniff");
        assert_eq!(
            rejected.headers()["cross-origin-resource-policy"],
            "cross-origin"
        );
        drop(response);
        tokio::time::timeout(Duration::from_secs(2), async {
            while admission.available_permits() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        server.abort();
    }

    #[tokio::test]
    async fn browser_headers_cover_early_rejection_and_extraction_failure() {
        let router = Router::new()
            .route("/", any(|_: Bytes| async { StatusCode::NO_CONTENT }))
            .layer(DefaultBodyLimit::max(1))
            .layer(middleware::from_fn_with_state(
                Arc::new(Semaphore::new(1)),
                admit_stream,
            ))
            .layer(middleware::from_fn(browser_headers));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = reqwest::Client::new();
        for (body, orphan_offset, status) in [
            ("x", false, StatusCode::NO_CONTENT),
            ("xx", false, StatusCode::PAYLOAD_TOO_LARGE),
            ("xx", true, StatusCode::BAD_REQUEST),
        ] {
            let mut request = client.post(format!("http://{address}/")).body(body);
            if orphan_offset {
                request = client
                    .put(format!("http://{address}/"))
                    .header("stream-fork-offset", "-1")
                    .body(body);
            }
            let response = request.send().await.unwrap();
            assert_eq!(response.status(), status);
            assert_eq!(response.headers()["x-content-type-options"], "nosniff");
            assert_eq!(
                response.headers()["cross-origin-resource-policy"],
                "cross-origin"
            );
        }
        server.abort();
    }

    #[tokio::test]
    async fn admission_is_reserved_before_reading_a_slow_body() {
        use tokio::io::AsyncWriteExt;
        let admission = Arc::new(Semaphore::new(1));
        let router = Router::new()
            .route("/", post(|body: Bytes| async move { body }))
            .layer(middleware::from_fn_with_state(
                admission.clone(),
                admit_stream,
            ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let mut slow = tokio::net::TcpStream::connect(address).await.unwrap();
        slow.write_all(b"POST / HTTP/1.1\r\nHost: test\r\nContent-Length: 1\r\n\r\n")
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while admission.available_permits() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let response = reqwest::Client::new()
            .post(format!("http://{address}/"))
            .body("second")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(response.headers().contains_key("x-request-id"));
        assert!(response.headers().contains_key("traceparent"));
        slow.write_all(b"x").await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while admission.available_permits() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        server.abort();
    }

    #[test]
    fn snapshot_chunks_are_bounded_before_assembly() {
        let limit = chronicle_raft::storage::MAX_SNAPSHOT_BYTES as u64;
        assert!(snapshot_range(limit - 1, 1).is_ok());
        assert!(snapshot_range(limit, 0).is_ok());
        for (offset, len) in [(limit - 1, 2), (limit + 1, 0), (u64::MAX, 1)] {
            assert_eq!(
                snapshot_range(offset, len).unwrap_err().0,
                StatusCode::PAYLOAD_TOO_LARGE
            );
        }
    }

    #[test]
    fn rpc_rejects_missing_wrong_node_and_wrong_cluster_identity() {
        let identity = identity::Identity {
            node: 2,
            cluster: "c".into(),
            genesis: true,
        };
        let mut headers = HeaderMap::new();
        assert!(rpc_recipient(&identity, &headers).is_err());
        headers.insert("x-chronicle-cluster", "c".parse().unwrap());
        headers.insert("x-chronicle-recipient", "4".parse().unwrap());
        assert!(rpc_recipient(&identity, &headers).is_err());
        headers.insert("x-chronicle-recipient", "2".parse().unwrap());
        assert!(rpc_recipient(&identity, &headers).is_ok());
        headers.insert("x-chronicle-cluster", "other".parse().unwrap());
        assert!(rpc_recipient(&identity, &headers).is_err());
    }
}
