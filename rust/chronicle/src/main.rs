//! One process hosts a control group and fixed virtual data shards.
mod controller;
mod identity;
mod telemetry;

use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Path, Query, Request, State},
    http::{HeaderMap, Method, StatusCode, Uri},
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
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use openraft::{BasicNode, Config, SnapshotPolicy};
use std::{
    collections::BTreeMap,
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
    pub identity: identity::Identity,
    pub nodes: BTreeMap<u64, Node>,
    pub groups: BTreeMap<u64, Group>,
    pub client: reqwest::Client,
    pub admission: Arc<Semaphore>,
    pub telemetry: telemetry::Telemetry,
}
type Shared = Arc<App>;
type ApiResult = Result<Response, (StatusCode, String)>;
fn unavailable(e: impl std::fmt::Display) -> (StatusCode, String) {
    (StatusCode::SERVICE_UNAVAILABLE, e.to_string())
}
fn bad(e: impl std::fmt::Display) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, e.to_string())
}
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .json()
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
        identity: local_identity,
        nodes,
        groups,
        client,
        admission: Arc::new(Semaphore::new(128)),
        telemetry: telemetry::Telemetry::new(id),
    });
    tokio::spawn(controller::run(app.clone()));
    let router = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/metrics", get(metrics))
        .route("/admin/status", get(status))
        .route("/admin/bootstrap", post(bootstrap))
        .route("/admin/register", post(register))
        .route("/admin/admit", post(admit))
        .route("/admin/placed", post(placed))
        .route("/admin/control", get(control))
        .route("/admin/snapshot/{group}", post(snapshot))
        .route("/raft/{group}/append", post(append_rpc))
        .route("/raft/{group}/vote", post(vote_rpc))
        .route("/raft/{group}/snapshot", post(snapshot_rpc))
        .route(
            "/v1/stream/{tenant}/{*path}",
            any(stream).layer(DefaultBodyLimit::max(1024 * 1024)).layer(
                middleware::from_fn_with_state(app.admission.clone(), admit_stream),
            ),
        )
        .layer(DefaultBodyLimit::max(8 * 1024 * 1024))
        .with_state(app);
    axum::serve(tokio::net::TcpListener::bind(listen).await?, router).await?;
    Ok(())
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
    Json(q): Json<VoteRequest<u64>>,
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
async fn snapshot_rpc(
    State(a): State<Shared>,
    Path(g): Path<u64>,
    headers: HeaderMap,
    Json(q): Json<InstallSnapshotRequest<TypeConfig>>,
) -> ApiResult {
    rpc_recipient(&a.identity, &headers)?;
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
            .map(|(id, g)| (*id, g.raft.metrics().borrow().clone()))
            .collect::<BTreeMap<_, _>>(),
    )
    .into_response()
}
async fn metrics(State(a): State<Shared>) -> String {
    let mut text = a.telemetry.metrics();
    for (id, g) in &a.groups {
        let m = g.raft.metrics().borrow().clone();
        text.push_str(&format!("chronicle_raft_leader{{group=\"{id}\"}} {}\nchronicle_raft_applied{{group=\"{id}\"}} {}\n", u8::from(m.current_leader == Some(a.id)), m.last_applied.map_or(0, |l| l.index)));
        text.push_str(&format!("chronicle_raft_quorum_available{{group=\"{id}\"}} {}\nchronicle_snapshot_index{{group=\"{id}\"}} {}\n",u8::from(m.current_leader==Some(a.id) && m.millis_since_quorum_ack.is_some_and(|n|n<800)),m.snapshot.map_or(0,|l|l.index)));
        if let Some(replication) = m.replication {
            for (node, matched) in replication {
                text.push_str(&format!(
                    "chronicle_replica_lag{{group=\"{id}\",replica=\"{node}\"}} {}\n",
                    m.last_log_index
                        .unwrap_or(0)
                        .saturating_sub(matched.map_or(0, |l| l.index))
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
    let hint = a.groups[&group].raft.metrics().borrow().current_leader;
    let mut nodes = a.groups[&0]
        .store
        .read_state()
        .await
        .map_err(unavailable)?
        .nodes;
    for (id, node) in &a.nodes {
        nodes.entry(*id).or_insert_with(|| node.clone());
    }
    let mut candidates: Vec<_> = nodes.into_iter().filter(|(id, _)| *id != a.id).collect();
    candidates.sort_by_key(|(id, _)| Some(*id) != hint);
    // Removed replicas stop receiving heartbeats, so their local leader hint can stay
    // absent/stale forever. Probe bounded routing hints; only the destination's read
    // barrier and client_write authorize the operation. Never retry the mutation here.
    let address = tokio::time::timeout(Duration::from_secs(3), async {
        for (id, node) in candidates {
            let response = a
                .client
                .get(format!("http://{}/admin/status", node.addr))
                .timeout(Duration::from_millis(500))
                .send()
                .await;
            if let Ok(response) = response
                && response.status().is_success()
                && let Ok(status) = response.json::<serde_json::Value>().await
                && status[group.to_string()]["id"].as_u64() == Some(id)
                && status[group.to_string()]["current_leader"].as_u64() == Some(id)
            {
                return Some(node.addr);
            }
        }
        None
    })
    .await
    .map_err(unavailable)?
    .ok_or_else(|| unavailable("no leader; retry"))?;
    headers.insert("x-chronicle-forwarded", "1".parse().map_err(bad)?);
    headers.remove("host");
    // Admin handlers reserialize JSON before forwarding. The original framing
    // may describe different bytes; let reqwest frame the supplied body.
    headers.remove("content-length");
    headers.remove("transfer-encoding");
    let r = a
        .client
        .request(method, format!("http://{address}{uri}"))
        .headers(headers)
        .body(body)
        .send()
        .await
        .map_err(unavailable)?;
    let status = r.status();
    let headers = r.headers().clone();
    let bytes = r.bytes().await.map_err(unavailable)?;
    Ok((status, headers, bytes).into_response())
}
async fn control(State(a): State<Shared>, headers: HeaderMap) -> ApiResult {
    let g = &a.groups[&0];
    if g.raft.metrics().borrow().current_leader != Some(a.id) {
        return proxy(&a, 0, Method::GET, "/admin/control", headers, Bytes::new()).await;
    }
    g.raft.ensure_linearizable().await.map_err(unavailable)?;
    Ok(Json(g.store.read_state().await.map_err(unavailable)?).into_response())
}
async fn register(
    State(a): State<Shared>,
    headers: HeaderMap,
    Json((id, node)): Json<(u64, Node)>,
) -> ApiResult {
    let g = &a.groups[&0];
    if g.raft.metrics().borrow().current_leader != Some(a.id) {
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
    if g.raft.metrics().borrow().current_leader != Some(a.id) {
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

async fn placed(State(a): State<Shared>, Json((shard, generation)): Json<(u64, u64)>) -> ApiResult {
    let result = a.groups[&0]
        .raft
        .client_write(Command::Placed { shard, generation })
        .await
        .map_err(unavailable)?;
    if result.data.error.is_some() {
        return Err(unavailable("placement generation changed"));
    }
    Ok(Json(result).into_response())
}

async fn admit_stream(
    State(admission): State<Arc<Semaphore>>,
    request: Request,
    next: Next,
) -> ApiResult {
    // Acquire before the Bytes extractor reads the body, not after allocation.
    let _permit = admission
        .try_acquire()
        .map_err(|_| (StatusCode::TOO_MANY_REQUESTS, "admission full".into()))?;
    Ok(next.run(request).await)
}

async fn stream(
    State(a): State<Shared>,
    Path((tenant, path)): Path<(String, String)>,
    Query(query): Query<BTreeMap<String, String>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult {
    let started = Instant::now();
    let key = format!("{}:{tenant}{path}", tenant.len());
    let shard = model::shard(&key);
    let bytes_in = body.len();
    let result = stream_inner(&a, shard, key, query, method.clone(), uri, headers, body).await;
    let status = result
        .as_ref()
        .map_or_else(|(s, _)| s.as_u16(), |r| r.status().as_u16());
    let m = a.groups[&shard].raft.metrics().borrow().clone();
    a.telemetry.complete(
        method.as_str(),
        shard,
        m.current_term,
        m.last_applied.map_or(0, |l| l.index),
        status,
        bytes_in,
        started.elapsed(),
    );
    result
}

#[allow(clippy::too_many_arguments)]
async fn stream_inner(
    a: &App,
    shard: u64,
    key: String,
    query: BTreeMap<String, String>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult {
    if key.len() > 2048 || key.contains('\0') {
        return Err(bad("invalid stream identity"));
    }
    let g = &a.groups[&shard];
    let stale = query.get("consistency").is_some_and(|v| v == "stale") && method == Method::GET;
    if !stale {
        if g.raft.metrics().borrow().current_leader != Some(a.id) {
            return proxy(a, shard, method, &uri.to_string(), headers, body).await;
        }
        g.raft.ensure_linearizable().await.map_err(unavailable)?;
    }
    let mut existing = g
        .store
        .read_stream(key.clone())
        .await
        .map_err(unavailable)?
        .filter(|s| !s.deleted);
    if !stale
        && let Some(s) = &existing
        && s.config.expires_ms.is_some_and(|t| t <= now_ms())
    {
        g.raft
            .client_write(Command::Delete {
                key: key.clone(),
                incarnation: s.incarnation,
                expired_at: s.config.expires_ms,
            })
            .await
            .map_err(unavailable)?;
        existing = None;
    }
    let h = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    let close = h("stream-closed") == Some("true");
    let content_type = h("content-type")
        .unwrap_or("application/octet-stream")
        .to_string();
    if method == Method::GET || method == Method::HEAD {
        let s = existing.ok_or_else(|| (StatusCode::NOT_FOUND, "stream missing".into()))?;
        let offset =
            match wire::parse_offset(query.get("offset").map(String::as_str)).map_err(bad)? {
                ParsedOffset::Start => 0,
                ParsedOffset::Now => s.data.len() as u64,
                ParsedOffset::At(n) => n,
            };
        if offset > s.data.len() as u64 {
            return Err((
                StatusCode::RANGE_NOT_SATISFIABLE,
                "offset beyond committed tail".into(),
            ));
        }
        let mut data = s.data[offset as usize..].to_vec();
        if s.config.content_type.starts_with("application/json") {
            if offset > 0 && s.data[offset as usize - 1] != b',' {
                return Err(bad("offset is not a JSON boundary"));
            }
            if data.last() == Some(&b',') {
                data.pop();
            }
            data.insert(0, b'[');
            data.push(b']');
        }
        let mut r = Response::builder()
            .status(200)
            .header("content-type", &s.config.content_type)
            .header(
                "stream-next-offset",
                wire::format_offset(s.data.len() as u64),
            )
            .header("stream-up-to-date", "true")
            .header("stream-incarnation", s.incarnation.to_string())
            .header("cache-control", "no-store")
            .header("stream-consistency", if stale { "stale" } else { "strict" });
        if s.closed {
            r = r.header("stream-closed", "true");
        }
        return r
            .body(Body::from(if method == Method::HEAD {
                Vec::new()
            } else {
                data
            }))
            .map_err(unavailable);
    }
    let command = if method == Method::PUT {
        let expires_ms = h("stream-ttl")
            .map(|v| v.parse::<u64>().map_err(bad))
            .transpose()?
            .map(|s| now_ms().saturating_add(s.saturating_mul(1000)));
        let wire = if body.is_empty() {
            Bytes::new()
        } else {
            wire::encode_wire(&body, content_type.starts_with("application/json"), true)
                .map_err(bad)?
        };
        Command::Create {
            key,
            expected_incarnation: h("stream-incarnation")
                .map(|v| v.parse::<u64>().map_err(bad))
                .transpose()?,
            config: StreamConfig {
                content_type,
                expires_ms,
            },
            data: wire.to_vec(),
            closed: close,
        }
    } else if method == Method::POST || method == Method::DELETE {
        let s = existing.ok_or_else(|| (StatusCode::NOT_FOUND, "stream missing".into()))?;
        let incarnation = h("stream-incarnation")
            .map(|v| v.parse::<u64>().map_err(bad))
            .transpose()?
            .unwrap_or(1);
        if method == Method::DELETE {
            Command::Delete {
                key,
                incarnation,
                expired_at: None,
            }
        } else {
            if body.is_empty() && !close {
                return Err(bad("empty append"));
            }
            if content_type != s.config.content_type && !body.is_empty() {
                return Err((
                    StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    "content-type mismatch".into(),
                ));
            }
            let producer = match (h("producer-id"), h("producer-epoch"), h("producer-seq")) {
                (None, None, None) => None,
                (Some(id), Some(epoch), Some(seq)) if id.len() <= 256 => Some(Producer {
                    id: id.into(),
                    epoch: epoch.parse().map_err(bad)?,
                    seq: seq.parse().map_err(bad)?,
                }),
                _ => return Err(bad("all producer headers are required")),
            };
            let wire = if body.is_empty() {
                Bytes::new()
            } else {
                wire::encode_wire(
                    &body,
                    s.config.content_type.starts_with("application/json"),
                    false,
                )
                .map_err(bad)?
            };
            Command::Append {
                key,
                incarnation,
                data: wire.to_vec(),
                producer,
                close,
            }
        }
    } else {
        return Err((StatusCode::METHOD_NOT_ALLOWED, "unsupported method".into()));
    };
    // No duplicate shortcut: every response follows durable-majority apply, even retries.
    let result = tokio::time::timeout(Duration::from_secs(8), g.raft.client_write(command))
        .await
        .map_err(|_| unavailable("timeout: outcome unknown"))?
        .map_err(unavailable)?;
    if let Some(error) = result.data.error {
        let status = match error {
            model::Error::Missing => StatusCode::NOT_FOUND,
            model::Error::Capacity => StatusCode::TOO_MANY_REQUESTS,
            _ => StatusCode::CONFLICT,
        };
        return Err((status, format!("{error:?}")));
    }
    let status = if method == Method::PUT && !result.data.duplicate {
        201
    } else if method == Method::DELETE || result.data.duplicate {
        204
    } else {
        200
    };
    Response::builder()
        .status(status)
        .header("stream-next-offset", wire::format_offset(result.data.end))
        .header("stream-incarnation", result.data.incarnation.to_string())
        .header("stream-commit-index", result.log_id.index.to_string())
        .body(Body::empty())
        .map_err(unavailable)
}

#[cfg(test)]
mod tests {
    use super::*;

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
