//! Bounded completion telemetry. Export and stdout backpressure never await Raft.
use axum::http::{HeaderMap, HeaderValue};
use chronicle_raft::metrics::Histogram;
use opentelemetry::{propagation::TextMapPropagator, trace::TraceContextExt};
use opentelemetry_sdk::{
    propagation::TraceContextPropagator,
    trace::{IdGenerator, RandomIdGenerator},
};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::mpsc;

const QUEUE_DEPTH: usize = 1024;

#[derive(Clone, Debug)]
pub struct RequestContext {
    pub started: Instant,
    trace_id: String,
    span_id: String,
    parent_span_id: String,
    flags: String,
    sampled: bool,
    trace_state: String,
    request_id: String,
}

impl RequestContext {
    pub fn from_headers(headers: &HeaderMap) -> Self {
        let mut carrier = HashMap::new();
        let mut parents = headers.get_all("traceparent").iter();
        // The SDK tolerates future-version extensions, so enforce singleton
        // cardinality here rather than relying on comma-joining to fail parsing.
        if let Some(value) = parents.next()
            && parents.next().is_none()
            && let Ok(value) = value.to_str()
            && !value.contains(',')
        {
            carrier.insert("traceparent".to_owned(), value.to_owned());
        }
        if let Ok(values) = headers
            .get_all("tracestate")
            .iter()
            .map(|v| v.to_str())
            .collect::<Result<Vec<_>, _>>()
        {
            carrier.insert("tracestate".to_owned(), values.join(","));
        }
        let parent = TraceContextPropagator::new().extract(&carrier);
        let parent = parent.span().span_context().clone();
        let ids = RandomIdGenerator::default();
        let trace_id = if parent.is_valid() {
            parent.trace_id()
        } else {
            ids.new_trace_id()
        }
        .to_string();
        let span_id = ids.new_span_id().to_string();
        let request_id = headers
            .get("x-request-id")
            .and_then(|v| v.to_str().ok())
            .filter(|v| valid_request_id(v))
            .map(str::to_owned)
            .unwrap_or_else(|| format!("chronicle-{trace_id}-{span_id}"));
        Self {
            started: Instant::now(),
            trace_id,
            span_id,
            parent_span_id: if parent.is_valid() {
                parent.span_id().to_string()
            } else {
                String::new()
            },
            flags: if parent.is_valid() {
                format!("{:02x}", parent.trace_flags().to_u8())
            } else {
                "01".into()
            },
            sampled: !parent.is_valid() || parent.is_sampled(),
            trace_state: if parent.is_valid() {
                parent.trace_state().header()
            } else {
                String::new()
            },
            request_id,
        }
    }

    pub fn inject(&self, headers: &mut HeaderMap) {
        let traceparent = format!("00-{}-{}-{}", self.trace_id, self.span_id, self.flags);
        if let Ok(value) = HeaderValue::from_str(&traceparent) {
            headers.insert("traceparent", value);
        }
        if let Ok(value) = HeaderValue::from_str(&self.request_id) {
            headers.insert("x-request-id", value);
        }
        headers.remove("tracestate");
        if !self.trace_state.is_empty()
            && let Ok(value) = HeaderValue::from_str(&self.trace_state)
        {
            headers.insert("tracestate", value);
        }
    }
}

fn valid_request_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

static COMMIT_APPLY: Histogram = Histogram::new();
pub fn commit_apply(elapsed: Duration) {
    COMMIT_APPLY.observe(elapsed);
}

#[derive(Default, Serialize)]
pub struct PhaseTimings {
    pub barrier_us: u64,
    pub read_us: u64,
    pub proposal_us: u64,
    pub forward_us: u64,
}

pub struct Completion<'a> {
    pub method: &'a str,
    pub shard: u64,
    pub term: u64,
    pub applied: u64,
    pub commit_index: Option<u64>,
    pub frontier: Option<&'a str>,
    pub duplicate: Option<bool>,
    pub status: u16,
    pub bytes_in: usize,
    pub bytes_out: u64,
    pub timings: PhaseTimings,
}

pub struct Telemetry {
    tx: mpsc::Sender<(Value, Value)>,
    dropped: Arc<AtomicU64>,
    export_errors: Arc<AtomicU64>,
    log_errors: tracing_appender::non_blocking::ErrorCounter,
    requests: Histogram,
    errors: AtomicU64,
}

impl Telemetry {
    pub fn new(
        node: u64,
        log_errors: tracing_appender::non_blocking::ErrorCounter,
        endpoint: String,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<(Value, Value)>(QUEUE_DEPTH);
        let dropped = Arc::new(AtomicU64::new(0));
        let export_errors = Arc::new(AtomicU64::new(0));
        let worker_errors = export_errors.clone();
        let endpoint = endpoint.replace(":4317", ":4318");
        tokio::spawn(async move {
            let Ok(client) = reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .build()
            else {
                return;
            };
            while let Some((log, span)) = rx.recv().await {
                if endpoint.is_empty() {
                    continue;
                }
                let resource = json!({"attributes":[{"key":"service.name","value":{"stringValue":"chronicle-raft"}},{"key":"service.instance.id","value":{"stringValue":node.to_string()}}]});
                let sampled = !span.is_null();
                for (path, value) in [
                    (
                        "logs",
                        json!({"resourceLogs":[{"resource":resource,"scopeLogs":[{"logRecords":[log]}]}]}),
                    ),
                    (
                        "traces",
                        json!({"resourceSpans":[{"resource":resource,"scopeSpans":[{"spans":[span]}]}]}),
                    ),
                ] {
                    if path == "traces" && !sampled {
                        continue;
                    }
                    if !client
                        .post(format!("{endpoint}/v1/{path}"))
                        .json(&value)
                        .send()
                        .await
                        .is_ok_and(|r| r.status().is_success())
                    {
                        worker_errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
        Self {
            tx,
            dropped,
            export_errors,
            log_errors,
            requests: Histogram::new(),
            errors: AtomicU64::new(0),
        }
    }

    pub fn complete(&self, context: &RequestContext, completion: Completion<'_>) {
        let Completion {
            method,
            shard,
            term,
            applied,
            commit_index,
            frontier,
            duplicate,
            status,
            bytes_in,
            bytes_out,
            timings,
        } = completion;
        let elapsed = context.started.elapsed();
        self.requests.observe(elapsed);
        let us = elapsed.as_micros().min(u64::MAX as u128) as u64;
        if status >= 400 {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
        let end = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let event = json!({
            "event": "request.complete",
            "http.request.method": method,
            "http.response.status_code": status,
            "error.type": (status >= 400).then(|| status.to_string()),
            "shard": shard,
            "raft.term": term,
            "raft.applied_index": applied,
            "raft.commit_index": commit_index,
            "frontier": frontier,
            "duplicate": duplicate,
            "duration_us": us,
            "phase_us": timings,
            "bytes_in": bytes_in,
            "bytes_out": bytes_out,
            "outcome": if status >= 500 { "unknown" } else if status >= 400 { "rejected" } else { "ok" },
            "trace_id": context.trace_id,
            "span_id": context.span_id,
            "request_id": context.request_id
        });
        tracing::info!(event=%event, "request.complete");
        let log = json!({"timeUnixNano":end.to_string(),"severityNumber":9,"severityText":"INFO","body":{"stringValue":event.to_string()},"traceId":context.trace_id,"spanId":context.span_id});
        let span = if context.sampled {
            json!({"traceId":context.trace_id,"spanId":context.span_id,"parentSpanId":context.parent_span_id,"name":"durable_stream.request","kind":2,"startTimeUnixNano":end.saturating_sub(elapsed.as_nanos()).to_string(),"endTimeUnixNano":end.to_string(),"attributes":[{"key":"http.request.method","value":{"stringValue":method}},{"key":"http.response.status_code","value":{"intValue":status.to_string()}},{"key":"chronicle.shard","value":{"intValue":shard.to_string()}}],"status":{"code":if status>=500{2}else{0}}})
        } else {
            Value::Null
        };
        if self.tx.try_send((log, span)).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn metrics(&self) -> String {
        let mut text = format!(
            "chronicle_requests_total {}\nchronicle_errors_total {}\nchronicle_telemetry_dropped_total {}\nchronicle_telemetry_export_errors_total {}\nchronicle_log_dropped_total {}\n",
            self.requests.count(),
            self.errors.load(Ordering::Relaxed),
            self.dropped.load(Ordering::Relaxed),
            self.export_errors.load(Ordering::Relaxed),
            self.log_errors.dropped_lines()
        );
        self.requests
            .render("chronicle_request_duration_seconds", &mut text);
        COMMIT_APPLY.render("chronicle_commit_apply_duration_seconds", &mut text);
        chronicle_raft::storage::timing_metrics(&mut text);
        chronicle_raft::network::timing_metrics(&mut text);
        text
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn invalid_context_is_replaced_and_valid_context_continues() {
        let mut h = HeaderMap::new();
        h.insert(
            "traceparent",
            "00-00000000000000000000000000000000-0000000000000000-01"
                .parse()
                .unwrap(),
        );
        h.insert("x-request-id", "secret/request".parse().unwrap());
        let invalid = RequestContext::from_headers(&h);
        assert_ne!(invalid.trace_id, "00000000000000000000000000000000");
        assert_ne!(invalid.request_id, "secret/request");
        h.insert(
            "traceparent",
            "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
                .parse()
                .unwrap(),
        );
        h.insert("x-request-id", "safe-123".parse().unwrap());
        let valid = RequestContext::from_headers(&h);
        assert_eq!(valid.trace_id, "0123456789abcdef0123456789abcdef");
        assert_eq!(valid.request_id, "safe-123");
        assert_eq!(valid.parent_span_id, "0123456789abcdef");
        assert_ne!(valid.span_id, valid.parent_span_id);
        valid.inject(&mut h);
        let forwarded = RequestContext::from_headers(&h);
        assert_eq!(forwarded.trace_id, valid.trace_id);
        assert_eq!(forwarded.parent_span_id, valid.span_id);
        assert_eq!(forwarded.request_id, valid.request_id);
    }

    #[test]
    fn forwarding_preserves_unsampled_context() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "traceparent",
            "00-0123456789abcdef0123456789abcdef-0123456789abcdef-00"
                .parse()
                .unwrap(),
        );
        headers.insert("tracestate", "vendor=value".parse().unwrap());
        headers.append("tracestate", "other=second".parse().unwrap());
        let context = RequestContext::from_headers(&headers);
        assert!(!context.sampled);
        context.inject(&mut headers);
        let forwarded = RequestContext::from_headers(&headers);
        assert!(!forwarded.sampled);
        assert_eq!(forwarded.trace_state, "vendor=value,other=second");
        assert_eq!(forwarded.parent_span_id, context.span_id);
    }

    #[test]
    fn duplicate_and_coalesced_future_traceparents_are_rejected() {
        let first = "01-0123456789abcdef0123456789abcdef-0123456789abcdef-00-extra";
        let second = "00-11111111111111111111111111111111-1111111111111111-01";
        for coalesced in [false, true] {
            let mut headers = HeaderMap::new();
            if coalesced {
                headers.insert("traceparent", format!("{first},{second}").parse().unwrap());
            } else {
                headers.append("traceparent", first.parse().unwrap());
                headers.append("traceparent", second.parse().unwrap());
            }
            let context = RequestContext::from_headers(&headers);
            assert!(context.parent_span_id.is_empty());
            assert!(context.sampled);
            assert_ne!(context.trace_id, "0123456789abcdef0123456789abcdef");
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unavailable_exporter_cannot_block_completion_and_overflow_is_counted() {
        use axum::{Router, http::StatusCode, routing::post};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/v1/{kind}",
                    post(|| async { StatusCode::SERVICE_UNAVAILABLE }),
                ),
            )
            .await
            .unwrap();
        });
        let (writer, _guard) = tracing_appender::non_blocking(std::io::sink());
        let telemetry = Telemetry::new(1, writer.error_counter(), endpoint);
        let context = RequestContext::from_headers(&HeaderMap::new());
        // No yield: the worker cannot consume until every completion has returned.
        for _ in 0..QUEUE_DEPTH + 7 {
            telemetry.complete(
                &context,
                Completion {
                    method: "POST",
                    shard: 1,
                    term: 2,
                    applied: 3,
                    commit_index: Some(3),
                    frontier: Some("000000000000000b"),
                    duplicate: Some(false),
                    status: 200,
                    bytes_in: 11,
                    bytes_out: 0,
                    timings: PhaseTimings::default(),
                },
            );
        }
        assert_eq!(telemetry.dropped.load(Ordering::Relaxed), 7);
        assert_eq!(telemetry.requests.count(), (QUEUE_DEPTH + 7) as u64);
        tokio::time::timeout(Duration::from_secs(5), async {
            while telemetry.export_errors.load(Ordering::Relaxed) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(telemetry.dropped.load(Ordering::Relaxed), 7);
        server.abort();
    }
}
