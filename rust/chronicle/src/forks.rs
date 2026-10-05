//! Trusted internal fork RPCs and idempotent cross-group reconciliation.
//!
//! This endpoint belongs on the private consensus network, never public ingress.
//! The cluster header prevents misrouting; it is not an authentication secret.
use crate::{ApiResult, Shared, bad, now_ms, proxy, unavailable};
use axum::{
    Json,
    body::to_bytes,
    extract::State,
    http::{HeaderMap, Method, StatusCode},
    response::IntoResponse,
};
use bytes::Bytes;
use chronicle_raft::{
    fork::{Decision, Id, Offer, Operation, View},
    model::{self, Command, Error, Outcome},
};
use openraft::type_config::async_runtime::WatchReceiver;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

type Result<T> = std::result::Result<T, (StatusCode, String)>;
const RPC_PATH: &str = "/admin/fork";
static RPC_SLOTS: Semaphore = Semaphore::const_new(16);

#[cfg(test)]
#[path = "forks_tests.rs"]
mod tests;

#[derive(Clone, Serialize, Deserialize)]
pub enum Request {
    Read {
        key: String,
        sequence: Option<u64>,
    },
    Chunk {
        id: Id,
        offset: u64,
    },
    Apply(Box<Operation>),
    Expire {
        key: String,
        incarnation: u64,
        access_ms: u64,
    },
}

impl Request {
    fn key(&self) -> &str {
        match self {
            Self::Read { key, .. } | Self::Expire { key, .. } => key,
            Self::Chunk { id, .. } => &id.source,
            Self::Apply(operation) => operation.key(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub enum Reply {
    View(Box<View>),
    Chunk(std::result::Result<Box<Operation>, Error>),
    Applied(Outcome),
}

pub async fn rpc(
    State(a): State<Shared>,
    headers: HeaderMap,
    Json(request): Json<Request>,
) -> ApiResult {
    if headers
        .get("x-chronicle-cluster")
        .and_then(|v| v.to_str().ok())
        != Some(&a.identity.cluster)
    {
        return Err((
            StatusCode::MISDIRECTED_REQUEST,
            "fork cluster mismatch".into(),
        ));
    }
    let group = model::shard(request.key());
    if a.groups[&group].raft.metrics().borrow_watched().state != openraft::ServerState::Leader {
        let body = serde_json::to_vec(&request).map_err(bad)?;
        return proxy(&a, group, Method::POST, RPC_PATH, headers, body.into()).await;
    }
    Ok(Json(dispatch(a, request).await?).into_response())
}

async fn dispatch(a: Shared, request: Request) -> Result<Reply> {
    if request.key().len() > 2048 || request.key().contains('\0') {
        return Err(bad("invalid fork identity"));
    }
    let permit = RPC_SLOTS.try_acquire().map_err(unavailable)?;
    // Keep the slot with the operation, even if its remote caller abandons it.
    tokio::spawn(async move {
        let _permit = permit;
        let group = &a.groups[&model::shard(request.key())];
        match request {
            Request::Apply(operation) => {
                let result = group
                    .raft
                    .client_write(Command::Fork(operation))
                    .await
                    .map_err(unavailable)?;
                Ok(Reply::Applied(result.data))
            }
            Request::Expire {
                key,
                incarnation,
                access_ms,
            } => {
                let result = group
                    .raft
                    .client_write(Command::Expire {
                        key,
                        incarnation,
                        access_ms,
                        now_ms: now_ms(),
                    })
                    .await
                    .map_err(unavailable)?;
                Ok(Reply::Applied(result.data))
            }
            Request::Read { key, sequence } => {
                group
                    .raft
                    .ensure_linearizable(openraft::ReadPolicy::ReadIndex)
                    .await
                    .map_err(unavailable)?;
                Ok(Reply::View(Box::new(
                    group
                        .store
                        .fork_view(key, sequence)
                        .await
                        .map_err(unavailable)?,
                )))
            }
            Request::Chunk { id, offset } => {
                group
                    .raft
                    .ensure_linearizable(openraft::ReadPolicy::ReadIndex)
                    .await
                    .map_err(unavailable)?;
                Ok(Reply::Chunk(
                    group
                        .store
                        .fork_chunk(id, offset)
                        .await
                        .map_err(unavailable)?
                        .map(Box::new),
                ))
            }
        }
    })
    .await
    .map_err(unavailable)?
}

async fn call(a: &Shared, request: Request) -> Result<Reply> {
    let group = model::shard(request.key());
    if a.groups[&group].raft.metrics().borrow_watched().state == openraft::ServerState::Leader {
        return dispatch(a.clone(), request).await;
    }
    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json".parse().map_err(bad)?);
    headers.insert(
        "x-chronicle-cluster",
        a.identity.cluster.parse().map_err(bad)?,
    );
    let response = proxy(
        a,
        group,
        Method::POST,
        RPC_PATH,
        headers,
        Bytes::from(serde_json::to_vec(&request).map_err(bad)?),
    )
    .await?;
    if !response.status().is_success() {
        // Even a failed response is not permission to retry a mutation under a new ID.
        return Err(unavailable(format!(
            "fork RPC returned {}",
            response.status()
        )));
    }
    let body = to_bytes(response.into_body(), 8 * 1024 * 1024)
        .await
        .map_err(unavailable)?;
    serde_json::from_slice(&body).map_err(unavailable)
}

pub async fn read(a: &Shared, key: &str, sequence: Option<u64>) -> Result<View> {
    match call(
        a,
        Request::Read {
            key: key.into(),
            sequence,
        },
    )
    .await?
    {
        Reply::View(view) => Ok(*view),
        _ => Err(unavailable("unexpected fork read reply")),
    }
}

pub async fn apply(a: &Shared, operation: Operation) -> Result<Outcome> {
    match call(a, Request::Apply(Box::new(operation))).await? {
        Reply::Applied(outcome) => Ok(outcome),
        _ => Err(unavailable("unexpected fork apply reply")),
    }
}

/// HTTP admission needs expiry materialized; recovery must still see tombstones.
pub async fn visible(a: &Shared, key: &str) -> Result<View> {
    for _ in 0..8 {
        let view = read(a, key, None).await?;
        let Some(stream) = view.stream.as_ref().filter(|s| {
            !s.deleted
                && s.config
                    .expiry
                    .is_some_and(|p| p.expired(s.access_ms, now_ms()))
        }) else {
            return Ok(view);
        };
        let result = call(
            a,
            Request::Expire {
                key: key.into(),
                incarnation: stream.incarnation,
                access_ms: stream.access_ms,
            },
        )
        .await?;
        match result {
            Reply::Applied(outcome) if outcome.error == Some(Error::PendingFork) => {
                return Err(unavailable("fork preparation pending"));
            }
            Reply::Applied(_) => (), // A concurrent renewal/recreation needs a fresh view.
            _ => return Err(unavailable("unexpected expiry reply")),
        }
    }
    Err(unavailable("expiry changed repeatedly"))
}

fn accepted(outcome: Outcome) -> Result<Outcome> {
    match &outcome.error {
        Some(error) => Err((crate::error_status(error), format!("{error:?}"))),
        None => Ok(outcome),
    }
}

/// A retry runs under the original ID. Transport failure never decides abort.
pub async fn reconcile(a: &Shared, offer: &Offer) -> Result<Outcome> {
    let id = &offer.id;
    let source = read(a, &id.source, Some(id.sequence)).await?;
    let stream = source
        .stream
        .ok_or_else(|| unavailable("source decision missing"))?;
    let mut decision = if stream.incarnation > id.incarnation {
        // Strict retirement receipt: an unresolved committed child would pin it.
        Decision::Aborted(Error::StaleIncarnation)
    } else {
        let transaction = source
            .transaction
            .ok_or_else(|| unavailable("source transaction missing"))?;
        if transaction.offer != *offer || stream.incarnation != id.incarnation {
            return Err(unavailable("source transaction mismatch"));
        }
        transaction.decision
    };
    if decision == Decision::Preparing {
        let preparation = apply(a, Operation::Prepare(offer.clone())).await?;
        if let Some(error) = preparation.error {
            accepted(
                apply(
                    a,
                    Operation::Decide {
                        id: id.clone(),
                        commit: false,
                        now_ms: now_ms(),
                        rejection: Some(error),
                    },
                )
                .await?,
            )?;
        } else {
            // A full stream may exceed the RPC body limit. Each stage is bounded.
            loop {
                let target = read(a, &offer.request.target, None).await?;
                let Some(prepared) = target.prepared else {
                    // Another reconciler may already have published this transaction.
                    if target
                        .stream
                        .and_then(|s| s.origin)
                        .is_some_and(|origin| origin == *offer)
                    {
                        break;
                    }
                    return Err(unavailable("target reservation missing"));
                };
                if prepared.offer != *offer {
                    return Err(unavailable("target reservation changed"));
                }
                if prepared.ready {
                    accepted(
                        apply(
                            a,
                            Operation::Decide {
                                id: id.clone(),
                                commit: true,
                                now_ms: now_ms(),
                                rejection: None,
                            },
                        )
                        .await?,
                    )?;
                    break;
                }
                let chunk = match call(
                    a,
                    Request::Chunk {
                        id: id.clone(),
                        offset: prepared.offset,
                    },
                )
                .await?
                {
                    Reply::Chunk(Ok(chunk)) => *chunk,
                    Reply::Chunk(Err(error)) => {
                        // A concurrent reconciler may have finalized and released
                        // the source body. Its immutable decision resolves this.
                        tracing::debug!(?error, "fork chunk changed during reconciliation");
                        break;
                    }
                    _ => return Err(unavailable("unexpected fork chunk reply")),
                };
                if let Some(error) = apply(a, chunk).await?.error {
                    tracing::debug!(?error, "fork stage changed during reconciliation");
                    break;
                }
            }
        }
        // Decide's successful application can have chosen abort (expiry/race).
        // Read the actual immutable decision; never reconstruct it from intent.
        let source = read(a, &id.source, Some(id.sequence)).await?;
        if source
            .stream
            .as_ref()
            .is_some_and(|s| s.incarnation > id.incarnation)
        {
            decision = Decision::Aborted(Error::StaleIncarnation);
        } else {
            let transaction = source
                .transaction
                .ok_or_else(|| unavailable("source decision missing"))?;
            if transaction.offer != *offer {
                return Err(unavailable("source decision mismatch"));
            }
            decision = transaction.decision;
        }
    }
    let outcome = accepted(
        apply(
            a,
            Operation::Finish {
                target: offer.request.target.clone(),
                id: id.clone(),
                decision: decision.clone(),
            },
        )
        .await?,
    )?;
    // Failure here leaves a retryable source cleanup obligation, not a lost fork.
    let _ = apply(a, Operation::Finalized(id.clone())).await?;
    match decision {
        Decision::Committed { .. } => Ok(outcome),
        Decision::Aborted(error) => Err((crate::error_status(&error), format!("{error:?}"))),
        Decision::Preparing => Err(unavailable("fork decision pending")),
    }
}

pub async fn release(a: &Shared, target: &str) -> Result<()> {
    let view = read(a, target, None).await?;
    let Some(stream) = view.stream.filter(|s| s.deleted && !s.retained) else {
        return Ok(());
    };
    let Some(origin) = stream.origin else {
        return Ok(());
    };
    accepted(apply(a, Operation::Release(origin.id.clone())).await?)?;
    accepted(
        apply(
            a,
            Operation::Released {
                target: target.into(),
                incarnation: stream.incarnation,
                id: origin.id,
            },
        )
        .await?,
    )?;
    Ok(())
}

pub async fn run(a: Shared) {
    use chronicle_raft::fork::Work;
    use futures_util::{StreamExt, stream};
    use std::{collections::BTreeMap, time::Duration};
    let mut cursors = BTreeMap::<u64, String>::new();
    loop {
        let mut work = Vec::new();
        for (&id, group) in a.groups.iter().filter(|(id, _)| **id != 0) {
            if group.raft.metrics().borrow_watched().state != openraft::ServerState::Leader {
                continue;
            }
            let cursor = cursors.entry(id).or_default();
            match group.store.fork_work(cursor.clone()).await {
                Ok(batch) => {
                    *cursor = batch
                        .last()
                        .map_or_else(String::new, |(key, _)| key.clone());
                    work.extend(batch.into_iter().map(|(_, item)| item));
                }
                Err(error) => tracing::warn!(group = id, %error, "fork work scan failed"),
            }
        }
        stream::iter(work)
            .for_each_concurrent(4, |item| {
                let a = a.clone();
                async move {
                    let result = tokio::time::timeout(Duration::from_secs(10), async {
                        match item {
                            Work::Reconcile(offer) => reconcile(&a, &offer).await.map(|_| ()),
                            Work::Release(key) => release(&a, &key).await,
                        }
                    })
                    .await;
                    if !matches!(result, Ok(Ok(()))) {
                        // Aborted application results are terminal too; scans stop once
                        // target cleanup and source finalization are durably recorded.
                        tracing::debug!(?result, "fork reconciliation deferred or aborted");
                    }
                }
            })
            .await;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
