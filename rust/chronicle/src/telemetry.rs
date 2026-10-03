//! Bounded OTLP/HTTP completion events and spans. Export failures never await Raft.
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::mpsc;

pub struct Telemetry {
    tx: mpsc::Sender<(Value, Value)>,
    dropped: Arc<AtomicU64>,
    count: AtomicU64,
    errors: AtomicU64,
    micros: AtomicU64,
    buckets: [AtomicU64; 8],
    node: u64,
}
const BOUNDS: [u64; 8] = [1000, 5000, 10000, 25000, 50000, 100000, 500000, u64::MAX];

impl Telemetry {
    pub fn new(node: u64) -> Self {
        let (tx, mut rx) = mpsc::channel::<(Value, Value)>(1024);
        let dropped = Arc::new(AtomicU64::new(0));
        let errors = dropped.clone();
        let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
            .unwrap_or_default()
            .replace(":4317", ":4318");
        tokio::spawn(async move {
            let client = match reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .build()
            {
                Ok(c) => c,
                Err(_) => return,
            };
            while let Some((log, span)) = rx.recv().await {
                if endpoint.is_empty() {
                    continue;
                }
                let resource = json!({"attributes":[{"key":"service.name","value":{"stringValue":"chronicle-raft"}},{"key":"service.instance.id","value":{"stringValue":node.to_string()}}]});
                let logs = json!({"resourceLogs":[{"resource":resource,"scopeLogs":[{"logRecords":[log]}]}]});
                let traces = json!({"resourceSpans":[{"resource":resource,"scopeSpans":[{"spans":[span]}]}]});
                for (path, value) in [("logs", logs), ("traces", traces)] {
                    if !client
                        .post(format!("{endpoint}/v1/{path}"))
                        .json(&value)
                        .send()
                        .await
                        .is_ok_and(|r| r.status().is_success())
                    {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
        Self {
            tx,
            dropped,
            count: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            micros: AtomicU64::new(0),
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            node,
        }
    }
    #[allow(clippy::too_many_arguments)]
    pub fn complete(
        &self,
        method: &str,
        shard: u64,
        term: u64,
        applied: u64,
        status: u16,
        bytes: usize,
        elapsed: Duration,
    ) {
        let sequence = self.count.fetch_add(1, Ordering::Relaxed);
        let us = elapsed.as_micros() as u64;
        self.micros.fetch_add(us, Ordering::Relaxed);
        for (bound, bucket) in BOUNDS.iter().zip(&self.buckets) {
            if us <= *bound {
                bucket.fetch_add(1, Ordering::Relaxed);
            }
        }
        if status >= 400 {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
        let end = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let trace = format!(
            "{:016x}{:016x}",
            end as u64,
            self.node.wrapping_shl(48) | sequence
        );
        let span = format!("{:016x}", sequence + 1);
        let event = json!({"event":"request.complete","http.request.method":method,"http.response.status_code":status,
            "shard":shard,"raft.term":term,"raft.applied_index":applied,"duration_us":us,"bytes_in":bytes,
            "outcome":if status>=500 {"unknown"} else if status>=400 {"rejected"} else {"ok"},"trace_id":trace,"span_id":span});
        tracing::info!(event = %event, "request.complete");
        let log = json!({"timeUnixNano":end.to_string(),"severityNumber":9,"severityText":"INFO","body":{"stringValue":event.to_string()},"traceId":trace,"spanId":span});
        let attrs = vec![
            json!({"key":"http.request.method","value":{"stringValue":method}}),
            json!({"key":"http.response.status_code","value":{"intValue":status.to_string()}}),
            json!({"key":"chronicle.shard","value":{"intValue":shard.to_string()}}),
        ];
        let span = json!({"traceId":trace,"spanId":span,"name":"durable_stream.request","kind":2,"startTimeUnixNano":end.saturating_sub(elapsed.as_nanos()).to_string(),"endTimeUnixNano":end.to_string(),"attributes":attrs,"status":{"code":if status>=500 {2}else{1}}});
        if self.tx.try_send((log, span)).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub fn metrics(&self) -> String {
        let mut text = format!(
            "chronicle_requests_total {}\nchronicle_errors_total {}\nchronicle_telemetry_dropped_total {}\nchronicle_request_duration_seconds_sum {}\nchronicle_request_duration_seconds_count {}\n",
            self.count.load(Ordering::Relaxed),
            self.errors.load(Ordering::Relaxed),
            self.dropped.load(Ordering::Relaxed),
            self.micros.load(Ordering::Relaxed) as f64 / 1e6,
            self.count.load(Ordering::Relaxed)
        );
        for (bound, bucket) in BOUNDS.iter().zip(&self.buckets) {
            text.push_str(&format!(
                "chronicle_request_duration_seconds_bucket{{le=\"{}\"}} {}\n",
                if *bound == u64::MAX {
                    "+Inf".into()
                } else {
                    format!("{}", *bound as f64 / 1e6)
                },
                bucket.load(Ordering::Relaxed)
            ));
        }
        text
    }
}
