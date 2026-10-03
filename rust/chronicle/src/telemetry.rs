//! Bounded completion telemetry. Export and stdout backpressure never await Raft.
use axum::{
    body::{Body, HttpBody},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::Response,
};
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
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
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

#[derive(Default)]
pub struct Completion {
    pub method: String,
    pub shard: u64,
    pub term: u64,
    pub applied: u64,
    pub commit_index: Option<u64>,
    pub frontier: Option<String>,
    pub duplicate: Option<bool>,
    pub status: Option<u16>,
    pub bytes_in: usize,
    pub bytes_out: u64,
    pub expected_bytes: Option<u64>,
    pub delivery: Delivery,
    pub timings: PhaseTimings,
}

impl Completion {
    fn body_end(&mut self) {
        if self.delivery != Delivery::BodyError {
            self.delivery = if self.expected_bytes.is_none_or(|n| n == self.bytes_out) {
                Delivery::Complete
            } else {
                Delivery::BodyError
            };
        }
    }
}

#[derive(Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Delivery {
    Complete,
    BodyError,
    #[default]
    Cancelled,
}

/// One event even when the handler or response body is cancelled. Publication is
/// bounded and nonblocking; this records server observation, not client receipt.
pub struct Observation {
    telemetry: Arc<Telemetry>,
    context: RequestContext,
    pub completion: Completion,
}

impl Observation {
    pub fn response(mut self, response: Response) -> Response {
        let header = |name: &str| response.headers().get(name)?.to_str().ok();
        self.completion.status = Some(response.status().as_u16());
        self.completion.commit_index = header("stream-commit-index").and_then(|v| v.parse().ok());
        self.completion.frontier = header("stream-next-offset").map(str::to_owned);
        self.completion.duplicate = header("stream-duplicate").and_then(|v| v.parse().ok());
        self.completion.expected_bytes = if self.completion.method == "HEAD"
            || matches!(
                response.status(),
                StatusCode::NO_CONTENT | StatusCode::NOT_MODIFIED
            ) {
            Some(0)
        } else {
            header("content-length")
                .and_then(|v| v.parse().ok())
                .or_else(|| response.body().size_hint().exact())
        };
        if response.body().is_end_stream() || self.completion.expected_bytes == Some(0) {
            self.completion.body_end();
        }
        response.map(|inner| {
            Body::new(ObservedBody {
                inner,
                observation: self,
            })
        })
    }
}

impl Drop for Observation {
    fn drop(&mut self) {
        self.telemetry.complete(&self.context, &self.completion);
    }
}

struct ObservedBody {
    inner: Body,
    observation: Observation,
}

impl HttpBody for ObservedBody {
    type Data = bytes::Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        let frame = Pin::new(&mut this.inner).poll_frame(cx);
        let completion = &mut this.observation.completion;
        match &frame {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    completion.bytes_out += data.len() as u64;
                }
                if this.inner.is_end_stream()
                    || completion
                        .expected_bytes
                        .is_some_and(|n| completion.bytes_out >= n)
                {
                    completion.body_end();
                }
            }
            Poll::Ready(Some(Err(_))) => completion.delivery = Delivery::BodyError,
            Poll::Ready(None) => completion.body_end(),
            _ => {}
        }
        frame
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

pub struct Telemetry {
    tx: mpsc::Sender<(Value, Value)>,
    dropped: Arc<AtomicU64>,
    export_errors: Arc<AtomicU64>,
    log_errors: tracing_appender::non_blocking::ErrorCounter,
    requests: Histogram,
    errors: AtomicU64,
    body_errors: AtomicU64,
    cancellations: AtomicU64,
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
            body_errors: AtomicU64::new(0),
            cancellations: AtomicU64::new(0),
        }
    }

    pub fn observe(
        self: &Arc<Self>,
        context: RequestContext,
        completion: Completion,
    ) -> Observation {
        Observation {
            telemetry: self.clone(),
            context,
            completion,
        }
    }

    fn complete(&self, context: &RequestContext, completion: &Completion) {
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
            expected_bytes,
            delivery,
            timings,
        } = completion;
        let elapsed = context.started.elapsed();
        self.requests.observe(elapsed);
        let us = elapsed.as_micros().min(u64::MAX as u128) as u64;
        let error_type = match delivery {
            Delivery::BodyError => {
                self.body_errors.fetch_add(1, Ordering::Relaxed);
                Some("response_body".to_owned())
            }
            Delivery::Cancelled => {
                self.cancellations.fetch_add(1, Ordering::Relaxed);
                Some("cancelled".to_owned())
            }
            Delivery::Complete => status.filter(|s| *s >= 400).map(|s| s.to_string()),
        };
        if error_type.is_some() {
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
            "error.type": error_type,
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
            "expected_bytes": expected_bytes,
            "delivery": delivery,
            "outcome": if *delivery != Delivery::Complete || status.is_none_or(|s| s >= 500) { "unknown" } else if status.is_some_and(|s| s >= 400) { "rejected" } else { "ok" },
            "trace_id": context.trace_id,
            "span_id": context.span_id,
            "request_id": context.request_id
        });
        tracing::info!(event=%event, "request.complete");
        let log = json!({"timeUnixNano":end.to_string(),"severityNumber":9,"severityText":"INFO","body":{"stringValue":event.to_string()},"traceId":context.trace_id,"spanId":context.span_id});
        let span = if context.sampled {
            let mut attributes = vec![
                json!({"key":"http.request.method","value":{"stringValue":method}}),
                json!({"key":"chronicle.shard","value":{"intValue":shard.to_string()}}),
            ];
            if let Some(status) = status {
                attributes.push(json!({"key":"http.response.status_code","value":{"intValue":status.to_string()}}));
            }
            if let Some(error) = error_type {
                attributes.push(json!({"key":"error.type","value":{"stringValue":error}}));
            }
            json!({"traceId":context.trace_id,"spanId":context.span_id,"parentSpanId":context.parent_span_id,"name":"durable_stream.request","kind":2,"startTimeUnixNano":end.saturating_sub(elapsed.as_nanos()).to_string(),"endTimeUnixNano":end.to_string(),"attributes":attributes,"status":{"code":if *delivery != Delivery::Complete || status.is_some_and(|s| s>=500){2}else{0}}})
        } else {
            Value::Null
        };
        if self.tx.try_send((log, span)).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn metrics(&self) -> String {
        let mut text = format!(
            "chronicle_requests_total {}\nchronicle_errors_total {}\nchronicle_response_body_errors_total {}\nchronicle_request_cancellations_total {}\nchronicle_telemetry_dropped_total {}\nchronicle_telemetry_export_errors_total {}\nchronicle_log_dropped_total {}\n",
            self.requests.count(),
            self.errors.load(Ordering::Relaxed),
            self.body_errors.load(Ordering::Relaxed),
            self.cancellations.load(Ordering::Relaxed),
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

    fn captured() -> (
        Arc<Telemetry>,
        mpsc::Receiver<(Value, Value)>,
        tracing_appender::non_blocking::WorkerGuard,
    ) {
        let (writer, guard) = tracing_appender::non_blocking(std::io::sink());
        let mut telemetry = Telemetry::new(1, writer.error_counter(), String::new());
        let (tx, rx) = mpsc::channel(16);
        telemetry.tx = tx;
        (Arc::new(telemetry), rx, guard)
    }

    fn observation(telemetry: &Arc<Telemetry>, method: &str) -> Observation {
        let mut headers = HeaderMap::new();
        headers.insert(
            "traceparent",
            "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
                .parse()
                .unwrap(),
        );
        headers.insert("x-request-id", "body-test".parse().unwrap());
        telemetry.observe(
            RequestContext::from_headers(&headers),
            Completion {
                method: method.into(),
                shard: 2,
                ..Default::default()
            },
        )
    }

    fn event(rx: &mut mpsc::Receiver<(Value, Value)>) -> (Value, Value) {
        let (log, span) = rx.try_recv().unwrap();
        let event: Value =
            serde_json::from_str(log["body"]["stringValue"].as_str().unwrap()).unwrap();
        assert_eq!(event["trace_id"], "0123456789abcdef0123456789abcdef");
        assert_eq!(span["parentSpanId"], "0123456789abcdef");
        assert_eq!(event["span_id"], span["spanId"]);
        assert_eq!(event["request_id"], "body-test");
        assert!(
            rx.try_recv().is_err(),
            "completion must be emitted exactly once"
        );
        (event, span)
    }

    #[tokio::test]
    async fn body_error_keeps_http_status_but_records_unknown_partial_delivery() {
        let (telemetry, mut rx, _logging) = captured();
        let body = Body::from_stream(futures_util::stream::iter([
            Ok(bytes::Bytes::from_static(b"NEVER_LOG_THIS")),
            Err(std::io::Error::other("private fault detail")),
        ]));
        let response = observation(&telemetry, "GET").response(
            Response::builder()
                .header("content-length", 20)
                .body(body)
                .unwrap(),
        );
        assert!(rx.try_recv().is_err());
        assert!(
            axum::body::to_bytes(response.into_body(), 100)
                .await
                .is_err()
        );
        let (event, span) = event(&mut rx);
        assert_eq!(event["http.response.status_code"], 200);
        assert_eq!(event["bytes_out"], 14);
        assert_eq!(event["expected_bytes"], 20);
        assert_eq!(event["delivery"], "body_error");
        assert_eq!(event["outcome"], "unknown");
        assert_eq!(span["status"]["code"], 2);
        assert!(!event.to_string().contains("NEVER_LOG_THIS"));
        assert!(!event.to_string().contains("private fault detail"));
        assert_eq!(telemetry.body_errors.load(Ordering::Relaxed), 1);
        assert_eq!(telemetry.cancellations.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn completion_waits_for_body_and_includes_delivery_time() {
        let (telemetry, mut rx, _logging) = captured();
        let body = Body::from_stream(futures_util::stream::once(async {
            tokio::time::sleep(Duration::from_millis(40)).await;
            Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"done"))
        }));
        let response = observation(&telemetry, "GET").response(
            Response::builder()
                .header("content-length", 4)
                .body(body)
                .unwrap(),
        );
        assert!(rx.try_recv().is_err());
        assert_eq!(
            axum::body::to_bytes(response.into_body(), 100)
                .await
                .unwrap(),
            "done"
        );
        let (event, span) = event(&mut rx);
        assert_eq!(event["delivery"], "complete");
        assert_eq!(event["bytes_out"], 4);
        assert!(event["duration_us"].as_u64().unwrap() >= 40_000);
        assert_eq!(span["status"]["code"], 0);
    }

    #[tokio::test]
    async fn cancellation_before_headers_and_after_partial_body_are_unknown() {
        use futures_util::{StreamExt, stream};
        let (telemetry, mut rx, _logging) = captured();
        drop(observation(&telemetry, "POST"));
        let (before, _) = event(&mut rx);
        assert!(before["http.response.status_code"].is_null());
        assert_eq!(before["outcome"], "unknown");
        assert_eq!(before["delivery"], "cancelled");
        let body = Body::from_stream(
            stream::once(async { Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"abc")) })
                .chain(stream::pending()),
        );
        let response = observation(&telemetry, "GET").response(Response::new(body));
        let mut chunks = response.into_body().into_data_stream();
        assert_eq!(chunks.next().await.unwrap().unwrap(), "abc");
        drop(chunks);
        let (after, _) = event(&mut rx);
        assert_eq!(after["bytes_out"], 3);
        assert_eq!(after["delivery"], "cancelled");
        assert_eq!(after["outcome"], "unknown");
        assert_eq!(telemetry.cancellations.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn forwarded_bodyless_status_is_complete_without_length_or_polling() {
        let (telemetry, mut rx, _logging) = captured();
        for status in [204, 304] {
            let body = Body::from_stream(futures_util::stream::pending::<
                Result<bytes::Bytes, std::io::Error>,
            >());
            let response = Response::builder().status(status).body(body).unwrap();
            drop(observation(&telemetry, "GET").response(response));
            let (event, _) = event(&mut rx);
            assert_eq!(event["delivery"], "complete");
            assert_eq!(event["outcome"], "ok");
            assert_eq!(event["bytes_out"], 0);
        }
        assert_eq!(telemetry.cancellations.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn head_and_no_content_need_no_poll_but_length_mismatches_are_errors() {
        let (telemetry, mut rx, _logging) = captured();
        for (method, status) in [("HEAD", 200), ("POST", 204)] {
            let response = Response::builder()
                .status(status)
                .header("content-length", if method == "HEAD" { 37 } else { 0 })
                .body(Body::empty())
                .unwrap();
            drop(observation(&telemetry, method).response(response));
            let (event, _) = event(&mut rx);
            assert_eq!(event["delivery"], "complete");
            assert_eq!(event["bytes_out"], 0);
        }
        for data in ["", "short", "too long"] {
            let response = Response::builder()
                .header("content-length", 6)
                .body(Body::from(data))
                .unwrap();
            let response = observation(&telemetry, "GET").response(response);
            let _ = axum::body::to_bytes(response.into_body(), 100)
                .await
                .unwrap();
            let (event, _) = event(&mut rx);
            assert_eq!(event["delivery"], "body_error");
        }
    }

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
                &Completion {
                    method: "POST".into(),
                    shard: 1,
                    term: 2,
                    applied: 3,
                    commit_index: Some(3),
                    frontier: Some("000000000000000b".into()),
                    duplicate: Some(false),
                    status: Some(200),
                    bytes_in: 11,
                    bytes_out: 0,
                    expected_bytes: Some(0),
                    delivery: Delivery::Complete,
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
