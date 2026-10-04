//! Fork PUT admission: canonical identities and immutable retry configuration.
use crate::{ApiResult, Shared, bad, forks, now_ms, unavailable};
use axum::{
    body::Body,
    http::{HeaderMap, HeaderValue, StatusCode, Uri},
    response::Response,
};
use bytes::Bytes;
use chronicle_raft::{
    expiry::Expiry,
    fork::{Id, Operation, Request},
    model::Outcome,
    wire::{self, ParsedOffset},
};

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Result<Option<&'a str>, (StatusCode, String)> {
    let mut values = headers.get_all(name).iter();
    let value = values.next().map(|v| v.to_str().map_err(bad)).transpose()?;
    if values.next().is_some() {
        return Err(bad(format!("repeated {name}")));
    }
    Ok(value)
}

fn source_key(
    fixed_tenant: Option<&str>,
    target: &str,
    path: &str,
) -> Result<String, (StatusCode, String)> {
    if path.contains(['?', '#']) {
        return Err(bad(
            "fork source must be a stream path without query or fragment",
        ));
    }
    let path = path
        .strip_prefix("/v1/stream/")
        .ok_or_else(|| bad("fork source must be a stream path"))?;
    // Axum matches literal path separators before decoding individual captures.
    let (tenant, path) = if let Some(tenant) = fixed_tenant {
        (std::borrow::Cow::Borrowed(tenant), path)
    } else {
        let (tenant, path) = path
            .split_once('/')
            .ok_or_else(|| bad("source tenant required"))?;
        (
            percent_encoding::percent_decode_str(tenant)
                .decode_utf8()
                .map_err(bad)?,
            path,
        )
    };
    let path = percent_encoding::percent_decode_str(path)
        .decode_utf8()
        .map_err(bad)?;
    let prefix = format!("{}:{tenant}", tenant.len());
    if path.is_empty() || !target.starts_with(&prefix) || path.contains('\0') {
        return Err(bad("fork source must be in the target tenant"));
    }
    Ok(format!("{prefix}{path}"))
}

pub async fn create(
    a: &Shared,
    key: String,
    headers: HeaderMap,
    body: Bytes,
    uri: Uri,
) -> ApiResult {
    let source = source_key(
        a.stream_tenant.as_deref(),
        &key,
        header(&headers, "stream-forked-from")?.ok_or_else(|| bad("fork source required"))?,
    )?;
    if source == key {
        return Err(bad("cannot fork a stream to itself"));
    }
    let anchor = match header(&headers, "stream-fork-offset")? {
        None => None,
        Some(value) => match wire::parse_offset(Some(value)).map_err(bad)? {
            ParsedOffset::Start => Some(0),
            ParsedOffset::At(offset) => Some(offset),
            ParsedOffset::Now => return Err(bad("fork offset must be an explicit offset")),
        },
    };
    let sub = header(&headers, "stream-fork-sub-offset")?
        .map(|value| {
            if value.is_empty()
                || !value.bytes().all(|b| b.is_ascii_digit())
                || (value.len() > 1 && value.starts_with('0'))
            {
                return Err(bad("invalid fork sub-offset"));
            }
            value.parse::<u64>().map_err(bad)
        })
        .transpose()?
        .unwrap_or(0);
    if sub > 0 && anchor.is_none() {
        return Err(bad("sub-offset requires an anchor"));
    }
    let expiry = Expiry::parse(
        header(&headers, "stream-ttl")?,
        header(&headers, "stream-expires-at")?,
    )
    .map_err(bad)?;
    let content_type = header(&headers, "content-type")?.map(str::to_owned);
    let closed = header(&headers, "stream-closed")?.is_some_and(|v| v.eq_ignore_ascii_case("true"));
    let explicit_incarnation = header(&headers, "stream-incarnation")?
        .map(|v| v.parse::<u64>().map_err(bad))
        .transpose()?;
    // Validate response metadata before accepting durable work.
    let authority = uri
        .authority()
        .map(|v| v.as_str())
        .or_else(|| headers.get("host").and_then(|v| v.to_str().ok()))
        .ok_or_else(|| bad("request authority required"))?
        .parse::<axum::http::uri::Authority>()
        .map_err(bad)?;
    let location = format!(
        "{}://{authority}{}",
        uri.scheme_str().unwrap_or("http"),
        uri.path()
    )
    .parse::<HeaderValue>()
    .map_err(bad)?;
    let target = forks::visible(a, &key).await?;
    #[cfg(test)]
    gates::pause(&key, "after_target_read").await;
    let incarnation = explicit_incarnation
        .or_else(|| {
            target
                .prepared
                .as_ref()
                .map(|p| p.offer.request.incarnation)
        })
        .or_else(|| target.stream.as_ref().map(|s| s.incarnation))
        .unwrap_or(1);
    let incarnation = if explicit_incarnation.is_none()
        && target.prepared.is_none()
        && target.stream.as_ref().is_some_and(|s| s.deleted)
    {
        incarnation
            .checked_add(1)
            .ok_or_else(|| bad("incarnation exhausted"))?
    } else {
        incarnation
    };
    let request = Request {
        target: key.clone(),
        incarnation,
        anchor,
        sub,
        content_type,
        expiry,
        closed,
    };
    if let Some(prepared) = target.prepared {
        if prepared.offer.id.source != source || prepared.offer.request != request {
            return Err((StatusCode::CONFLICT, "different fork preparation".into()));
        }
        let outcome = forks::reconcile(a, &prepared.offer).await?;
        return response(outcome, true, location);
    }
    if let Some(stream) = target.stream {
        if !stream.deleted {
            let origin = stream
                .origin
                .ok_or_else(|| (StatusCode::CONFLICT, "target is not this fork".into()))?;
            if origin.id.source != source || origin.request != request {
                return Err((StatusCode::CONFLICT, "fork configuration mismatch".into()));
            }
            return response(
                Outcome {
                    end: stream.end,
                    incarnation: stream.incarnation,
                    duplicate: true,
                    closed: stream.closed,
                    producer: None,
                    content_type: Some(stream.config.content_type),
                    error: None,
                },
                true,
                location,
            );
        }
        if stream.retained {
            return Err((StatusCode::CONFLICT, "target retained by forks".into()));
        }
        if stream.origin.is_some() {
            forks::release(a, &key).await?;
        }
    }
    let source_view = forks::visible(a, &source).await?;
    if source_view.prepared.is_some() {
        return Err(unavailable("source preparation pending"));
    }
    let stream = source_view
        .stream
        .ok_or_else(|| (StatusCode::NOT_FOUND, "source missing".into()))?;
    if stream.deleted {
        return Err((
            if stream.retained {
                StatusCode::CONFLICT
            } else {
                StatusCode::NOT_FOUND
            },
            "source deleted".into(),
        ));
    }
    let id = Id {
        source,
        incarnation: stream.incarnation,
        sequence: stream
            .sequence
            .checked_add(1)
            .ok_or_else(|| unavailable("fork sequence exhausted"))?,
    };
    let begun = forks::apply(
        a,
        Operation::Begin {
            id: id.clone(),
            request: request.clone(),
            body: body.to_vec(),
            now_ms: now_ms(),
        },
    )
    .await?;
    if let Some(error) = begun.error {
        let status = if error == chronicle_raft::model::Error::Gone {
            StatusCode::CONFLICT
        } else {
            crate::error_status(&error)
        };
        return Err((status, format!("{error:?}")));
    }
    #[cfg(test)]
    gates::pause(&key, "after_begin").await;
    let source = forks::read(a, &id.source, Some(id.sequence)).await?;
    let transaction = source
        .transaction
        .ok_or_else(|| unavailable("fork transaction unavailable; outcome unknown"))?;
    if transaction.offer.id != id || transaction.offer.request != request {
        return Err(unavailable(
            "source recreated after fork admission; original outcome unknown",
        ));
    }
    let outcome = match forks::reconcile(a, &transaction.offer).await {
        Ok(outcome) => outcome,
        Err(error) if error.0 == StatusCode::CONFLICT => {
            // Another identical PUT may have won after our initial target read.
            // Settle our transaction first; never weaken Prepare's ownership ID.
            let source = forks::read(a, &id.source, Some(id.sequence)).await?;
            let settled = source.transaction.is_some_and(|t| {
                t.offer.id == id
                    && t.finalized
                    && matches!(t.decision, chronicle_raft::fork::Decision::Aborted(_))
            });
            if settled
                && let Some(stream) = forks::visible(a, &key).await?.stream
                && !stream.deleted
                && stream.origin.as_ref().is_some_and(|origin| {
                    origin.id.source == id.source
                        && origin.id.incarnation == id.incarnation
                        && origin.request == request
                })
            {
                return response(
                    Outcome {
                        end: stream.end,
                        incarnation: stream.incarnation,
                        duplicate: true,
                        closed: stream.closed,
                        producer: None,
                        content_type: Some(stream.config.content_type),
                        error: None,
                    },
                    true,
                    location,
                );
            }
            return Err(error);
        }
        Err(error) => return Err(error),
    };
    response(outcome, begun.duplicate, location)
}

fn response(outcome: Outcome, duplicate: bool, location: HeaderValue) -> ApiResult {
    let mut response = Response::builder()
        .status(if duplicate { 200 } else { 201 })
        .header("stream-next-offset", wire::format_offset(outcome.end))
        .header("stream-incarnation", outcome.incarnation.to_string())
        .header("stream-duplicate", duplicate.to_string())
        .header(
            "content-type",
            outcome
                .content_type
                .ok_or_else(|| unavailable("fork outcome missing content type"))?,
        );
    if !duplicate {
        response = response.header("location", location);
    }
    if outcome.closed {
        response = response.header("stream-closed", "true");
    }
    response.body(Body::empty()).map_err(unavailable)
}

#[cfg(test)]
pub(crate) mod gates {
    use tokio::sync::oneshot;
    struct Gate {
        key: String,
        phase: &'static str,
        arrived: oneshot::Sender<()>,
        resume: oneshot::Receiver<()>,
    }
    static GATE: std::sync::Mutex<Option<Gate>> = std::sync::Mutex::new(None);
    pub fn next(key: String, phase: &'static str) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (arrived, arrival) = oneshot::channel();
        let (release, resume) = oneshot::channel();
        let mut gate = GATE.lock().unwrap();
        assert!(gate.is_none());
        *gate = Some(Gate {
            key,
            phase,
            arrived,
            resume,
        });
        (arrival, release)
    }
    pub async fn pause(key: &str, phase: &str) {
        let gate = {
            let mut gate = GATE.lock().unwrap();
            if gate
                .as_ref()
                .is_some_and(|g| g.key == key && g.phase == phase)
            {
                gate.take()
            } else {
                None
            }
        };
        if let Some(gate) = gate {
            gate.arrived.send(()).unwrap();
            gate.resume.await.unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_path_matches_axum_capture_decoding_not_whole_path_splitting() {
        assert_eq!(
            source_key(None, "3:a/btarget", "/v1/stream/a%2Fb/s").unwrap(),
            "3:a/bs"
        );
        assert!(source_key(None, "1:atarget", "/v1/stream/a%2Fb/s").is_err());
        assert_eq!(
            source_key(None, "1:atarget", "/v1/stream/a/b%2Fs").unwrap(),
            "1:ab/s"
        );
        assert_eq!(
            source_key(Some("a"), "1:atarget", "/v1/stream/b%2Fs").unwrap(),
            "1:ab/s"
        );
    }
}
