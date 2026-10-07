//! Subscription ingress, committed catalog reads and recoverable external I/O.
//! No network call holds Machine::view. See SUBSCRIPTIONS.md for failure cases.
use super::*;
use serde_json::{json, Value};
use std::net::{IpAddr, SocketAddr};
use subscriptions::{
    error, Action, Config as SubConfig, Keys, Observation, Subscription, MAX_LINKS,
};

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Filter {
    root: String,
    pattern: Option<String>,
    paths: BTreeSet<String>,
}
impl Filter {
    fn config(root: &str, config: &SubConfig) -> Self {
        Self {
            root: root.into(),
            pattern: config.pattern.clone(),
            paths: config.streams.iter().cloned().collect(),
        }
    }
    fn subscription(sub: &Subscription) -> Self {
        Self {
            root: sub.root.clone(),
            pattern: sub.config.pattern.clone(),
            paths: sub.links.keys().cloned().collect(),
        }
    }
    fn matches(&self, full: &str) -> bool {
        full.strip_prefix(&self.root).is_some_and(|p| {
            self.paths.contains(p)
                || self
                    .pattern
                    .as_deref()
                    .is_some_and(|pattern| subscriptions::glob(pattern, p))
        })
    }
}

pub fn reserved(path: &str) -> bool {
    path.contains("/__ds/") || path.ends_with("/__ds")
}

fn now() -> u64 {
    clock::millis(std::time::SystemTime::now())
}

impl Cluster {
    async fn sub_propose(
        &self,
        group: usize,
        path: &str,
        action: Action,
    ) -> Result<batch::Committed, u16> {
        let body = serde_json::to_vec(&action).unwrap();
        if body.len() > 1024 * 1024 {
            return Err(413);
        }
        self.groups[group]
            .propose(Command {
                method: "SUB".into(),
                path: path.into(),
                headers: vec![],
                body,
                time: now(),
            })
            .await
    }

    async fn sub_reply(&self, group: usize, path: &str, action: Action) -> Resp {
        match self.sub_propose(group, path, action).await {
            Ok(result) => {
                let mut resp = result.data.into_resp();
                resp.headers
                    .push(("stream-session", self.token(group, result.log_id.index)));
                resp.headers
                    .push(("stream-durability", "quorum-fsync".into()));
                resp.headers.push(("cache-control", "no-store".into()));
                resp
            }
            Err(413) => error(413, "COMMAND_LIMIT", "subscription command exceeds 1 MiB"),
            Err(429) => error(429, "BUSY", "pending proposal bound reached"),
            _ => self.unavailable(
                group,
                "subscription write outcome unknown; reread before retry",
            ),
        }
    }

    pub(super) async fn barrier(&self, group: usize) -> Result<(), u16> {
        let g = &self.groups[group];
        match tokio::time::timeout(
            Duration::from_secs(3),
            g.reads.confirm(async { g.raft.ensure_linearizable().await.is_ok() }),
        )
        .await
        {
            Ok(true) => Ok(()),
            _ => Err(503),
        }
    }

    async fn subscription_read(&self, group: usize, path: &str, req: &Req) -> Resp {
        let consistency = req.header("stream-consistency").unwrap_or("linearizable");
        let session = match req.header("stream-session") {
            Some(t) => match self.parse_token(t, group) {
                Some(i) => Some(i),
                None => {
                    return error(
                        400,
                        "INVALID_SESSION",
                        "wrong cluster or subscription owner partition",
                    )
                }
            },
            None => None,
        };
        match consistency {
            "linearizable" => {
                if self.barrier(group).await.is_err() {
                    return self.unavailable(group, "subscription read barrier unavailable");
                }
            }
            "prefix" => {}
            "session" if session.is_some() => {}
            _ => {
                return error(
                    400,
                    "INVALID_CONSISTENCY",
                    "linearizable, prefix, or session with token required",
                )
            }
        }
        if let Some(i) = session {
            if self.groups[group]
                .raft
                .wait(Some(Duration::from_secs(3)))
                .applied_index_at_least(Some(i), "subscription session")
                .await
                .is_err()
            {
                return self.unavailable(group, "subscription session not applied");
            }
        }
        let view = self.groups[group].machine.view.read().await;
        let mut reply = match view.subscriptions.subscriptions.get(path) {
            Some(sub) => super::json(&sub.public(path, view.subscriptions.keys.as_ref().unwrap())),
            None => error(404, "NOT_FOUND", "subscription not found"),
        };
        reply.headers.push((
            "stream-session",
            self.token(group, view.applied.map_or(0, |i| i.index)),
        ));
        reply
            .headers
            .push(("stream-consistency", consistency.into()));
        reply.headers.push(("cache-control", "no-store".into()));
        reply
    }

    pub async fn subscriptions(&self, req: Req) -> Resp {
        if req.body.len() > 1024 * 1024 {
            return error(413, "COMMAND_LIMIT", "request exceeds 1 MiB");
        }
        let Some((mount, control)) = req.path.split_once("/__ds/") else {
            return error(404, "NOT_FOUND", "reserved namespace");
        };
        let root = format!("{mount}/");
        if control == "jwks.json" && req.method == Method::Get {
            let mut keys = Vec::new();
            for group in 0..self.groups.len() {
                match self.control_read(group, "keys", &json!(null)).await {
                    Ok(Value::Array(group_keys)) => keys.extend(group_keys),
                    _ => return self.unavailable(group, "JWKS owner unavailable"),
                }
            }
            let mut reply = super::json(&json!({"keys":keys}));
            reply.headers = vec![
                ("content-type", "application/jwk-set+json".into()),
                ("cache-control", "public, max-age=300".into()),
            ];
            return reply;
        }
        let Some(tail) = control.strip_prefix("subscriptions/") else {
            return error(404, "NOT_FOUND", "unknown control path");
        };
        let (id, operation) = tail.split_once('/').unwrap_or((tail, ""));
        if id.is_empty()
            || id.len() > 256
            || !id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
            || id == "."
            || id == ".."
        {
            return error(
                400,
                "INVALID_REQUEST",
                "subscription id must be a URL-safe segment",
            );
        }
        let path = format!("{root}__ds/subscriptions/{id}");
        let group = partition(&path, self.groups.len());
        if req.method == Method::Get && operation.is_empty() {
            return self.subscription_read(group, &path, &req).await;
        }
        if req
            .header("stream-durability")
            .is_some_and(|s| s != "quorum-fsync")
        {
            return error(400, "INVALID_DURABILITY", "only quorum-fsync is supported");
        }
        if self.groups[group].raft.metrics().borrow().current_leader != Some(self.config.node) {
            return self.unavailable(
                group,
                "subscription owner is not this leader; mutation not forwarded",
            );
        }
        if req.method == Method::Delete && operation.is_empty() {
            return self.sub_reply(group, &path, Action::Delete).await;
        }
        let action = if req.method == Method::Put && operation.is_empty() {
            let config = match serde_json::from_slice::<SubConfig>(&req.body)
                .map_err(|_| "invalid configuration JSON")
                .and_then(SubConfig::normalize)
            {
                Ok(c) => c,
                Err(m) => return error(400, "INVALID_REQUEST", m),
            };
            if let Some(webhook) = &config.webhook {
                if let Err(m) = webhook_target(&webhook.url).await {
                    return error(400, "WEBHOOK_URL_REJECTED", m);
                }
            }
            if self.groups[group]
                .machine
                .view
                .read()
                .await
                .subscriptions
                .keys
                .is_none()
            {
                let keys = match Keys::generate() {
                    Ok(k) => k,
                    Err(_) => {
                        return error(
                            503,
                            "ENTROPY_UNAVAILABLE",
                            "cannot initialize subscription keys",
                        )
                    }
                };
                if self
                    .sub_propose(group, &path, Action::Keys(keys))
                    .await
                    .is_err()
                {
                    return self.unavailable(group, "key initialization outcome unknown");
                }
            }
            let observations = match self.catalog(&[Filter::config(&root, &config)]).await {
                Ok(o) => o,
                Err(code) => {
                    return error(
                        code,
                        "CATALOG_UNAVAILABLE",
                        "complete creation catalog required",
                    )
                }
            };
            let Some(host) = req.header("host") else {
                return error(400, "INVALID_REQUEST", "Host required for callback URLs");
            };
            let base_url = format!("http://{host}{root}");
            if reqwest::Url::parse(&base_url).is_err() {
                return error(400, "INVALID_REQUEST", "invalid Host or mount");
            }
            Action::Create {
                config,
                root,
                base_url,
                observations,
            }
        } else {
            let (operation, body) = if req.method == Method::Delete {
                let Some(encoded) = operation.strip_prefix("streams/") else {
                    return error(405, "METHOD_NOT_ALLOWED", "unsupported subscription method");
                };
                let Ok(p) = percent_encoding::percent_decode_str(encoded).decode_utf8() else {
                    return error(400, "INVALID_REQUEST", "invalid encoded path");
                };
                ("unlink", Value::String(p.into()))
            } else if req.method == Method::Post
                && matches!(
                    operation,
                    "streams" | "claim" | "ack" | "callback" | "release"
                )
            {
                let Ok(body) = serde_json::from_slice(&req.body) else {
                    return error(400, "INVALID_REQUEST", "JSON body required");
                };
                (operation, body)
            } else {
                return error(405, "METHOD_NOT_ALLOWED", "unsupported subscription method");
            };
            let (mut filter, incarnation) = {
                let view = self.groups[group].machine.view.read().await;
                let Some(sub) = view.subscriptions.subscriptions.get(&path) else {
                    return error(404, "NOT_FOUND", "subscription not found");
                };
                (Filter::subscription(sub), sub.incarnation)
            };
            if operation == "streams" {
                if let Some(paths) = body.get("streams").and_then(Value::as_array) {
                    for p in paths {
                        if let Some(p) = p.as_str().filter(|p| subscriptions::valid_path(p)) {
                            filter.paths.insert(p.into());
                        }
                    }
                }
            }
            let observations = match self.catalog(&[filter]).await {
                Ok(o) => o,
                Err(code) => {
                    return error(
                        code,
                        "CATALOG_UNAVAILABLE",
                        "linked stream owner unavailable",
                    )
                }
            };
            Action::Request {
                incarnation,
                operation: operation.into(),
                body,
                observations,
                token: req
                    .header("authorization")
                    .and_then(|v| v.strip_prefix("Bearer "))
                    .unwrap_or("")
                    .into(),
            }
        };
        self.sub_reply(group, &path, action).await
    }

    /// Trusted internal endpoint. A catalog is one applied prefix, not local
    /// file sizes; linearizable barrier and apply read lock surround capture.
    pub async fn catalog_local(&self, group: usize, filters: &[Filter]) -> Resp {
        if filters.len() > 32 || filters.iter().any(|f| f.paths.len() > MAX_LINKS) {
            return response(413, "catalog filter bound");
        }
        if self.barrier(group).await.is_err() {
            return self.unavailable(group, "catalog barrier unavailable");
        }
        let timed = self.groups[group]
            .machine
            .view
            .read()
            .await
            .store
            .streams
            .iter()
            .any(|s| s.config.ttl_seconds.is_some() || s.config.expires_at.is_some());
        if timed
            && self.groups[group]
                .propose(Command {
                    method: "TICK".into(),
                    path: String::new(),
                    headers: vec![],
                    body: vec![],
                    time: now(),
                })
                .await
                .is_err()
        {
            return self.unavailable(group, "catalog TTL clock unavailable");
        }
        let view = self.groups[group].machine.view.read().await;
        let index = view.applied.map_or(0, |i| i.index);
        if view
            .forks
            .reservations
            .keys()
            .any(|path| filters.iter().any(|f| f.matches(path)))
        {
            return response(
                503,
                "catalog includes a pending fork; absence not established",
            );
        }
        let mut found = BTreeMap::new();
        for stream in &view.store.streams {
            if filters.iter().any(|f| f.matches(&stream.path))
                && !stream.is_expired()
                && !stream.shared.read().unwrap().soft_deleted
            {
                found.insert(
                    stream.path.clone(),
                    Observation {
                        path: stream.path.clone(),
                        group,
                        index,
                        incarnation: Some(stream.id),
                        tail: stream.tail().bytes,
                    },
                );
                if found.len() > MAX_LINKS * 32 {
                    return response(413, "catalog result bound; narrow patterns");
                }
            }
        }
        for filter in filters {
            for path in &filter.paths {
                let path = format!("{}{path}", filter.root);
                if partition(&path, self.groups.len()) == group {
                    found.entry(path.clone()).or_insert(Observation {
                        path,
                        group,
                        index,
                        incarnation: None,
                        tail: 0,
                    });
                }
            }
        }
        super::json(&found.into_values().collect::<Vec<_>>())
    }

    fn candidates(&self, group: usize) -> Vec<String> {
        let metrics = self.groups[group].raft.metrics().borrow().clone();
        let members = metrics.membership_config.membership();
        let mut nodes: Vec<_> = members
            .nodes()
            .map(|(id, n)| (*id, n.addr.clone()))
            .collect();
        nodes.sort_by_key(|(id, _)| (Some(*id) != metrics.current_leader, *id));
        nodes.into_iter().map(|(_, addr)| addr).collect()
    }

    pub(super) async fn control_read(
        &self,
        group: usize,
        operation: &str,
        body: &impl Serialize,
    ) -> Result<Value, u16> {
        // Routing retries are safe only for reads or transaction-ID-fenced
        // internal fork decisions. Never pass an external mutation here.
        for address in self.candidates(group) {
            let url = format!("http://{address}/_admin/{group}/{operation}");
            if let Ok(mut reply) = self
                .client
                .post(url)
                .header("x-electric-cluster", &self.config.cluster)
                .json(body)
                .send()
                .await
            {
                if reply.status().as_u16() == 413 {
                    return Err(413);
                }
                if reply.status().is_success() {
                    let mut bytes = Vec::new();
                    while let Some(chunk) = reply.chunk().await.map_err(|_| 503u16)? {
                        if bytes.len() + chunk.len() > 16 * 1024 * 1024 {
                            return Err(413);
                        }
                        bytes.extend_from_slice(&chunk);
                    }
                    return serde_json::from_slice(&bytes).map_err(|_| 503);
                }
            }
        }
        Err(503)
    }

    async fn catalog(&self, filters: &[Filter]) -> Result<Vec<Observation>, u16> {
        let mut all = Vec::new();
        for group in 0..self.groups.len() {
            let value = self.control_read(group, "catalog", &filters).await?;
            all.extend(serde_json::from_value::<Vec<Observation>>(value).map_err(|_| 503u16)?);
        }
        Ok(all)
    }

    pub async fn public_group_keys(&self, group: usize) -> Resp {
        if self.barrier(group).await.is_err() {
            return self.unavailable(group, "key read barrier unavailable");
        }
        let view = self.groups[group].machine.view.read().await;
        super::json(
            &view
                .subscriptions
                .keys
                .iter()
                .map(Keys::jwk)
                .collect::<Vec<_>>(),
        )
    }

    pub async fn subscription_worker(&'static self, group: usize) {
        let mut cursor = String::new();
        loop {
            tokio::time::sleep(Duration::from_millis(150)).await;
            if self.groups[group].raft.metrics().borrow().current_leader != Some(self.config.node) {
                continue;
            }
            let batch: Vec<_> = {
                let view = self.groups[group].machine.view.read().await;
                view.subscriptions
                    .subscriptions
                    .range((
                        std::ops::Bound::Excluded(cursor.clone()),
                        std::ops::Bound::Unbounded,
                    ))
                    .take(32)
                    .map(|(p, s)| (p.clone(), s.incarnation, Filter::subscription(s)))
                    .collect()
            };
            if batch.is_empty() {
                cursor.clear();
                continue;
            }
            cursor = batch.last().unwrap().0.clone();
            let filters: Vec<_> = batch.iter().map(|(_, _, f)| f.clone()).collect();
            let targets: BTreeMap<_, _> =
                batch.iter().map(|(p, inc, _)| (p.clone(), *inc)).collect();
            match self.catalog(&filters).await {
                Ok(observations) => {
                    // Each metadata journal entry stays bounded independently of
                    // the payload size of the linked streams.
                    let mut failed = false;
                    for chunk in observations.chunks(128) {
                        match self
                            .sub_propose(
                                group,
                                "",
                                Action::Observe {
                                    targets: targets.clone(),
                                    observations: chunk.to_vec(),
                                },
                            )
                            .await
                        {
                            Ok(r) if r.data.status == 204 => {}
                            _ => {
                                failed = true;
                                break;
                            }
                        }
                    }
                    if observations.is_empty()
                        && self
                            .sub_propose(
                                group,
                                "",
                                Action::Observe {
                                    targets,
                                    observations: vec![],
                                },
                            )
                            .await
                            .is_err()
                    {
                        failed = true;
                    }
                    if failed {
                        tracing::warn!(group, "subscription catalog commit failed");
                        continue;
                    }
                }
                Err(code) => {
                    tracing::warn!(group, code, "subscription catalog unavailable");
                    continue;
                }
            }
            let mut deliveries = tokio::task::JoinSet::new();
            for (path, _, _) in batch {
                let candidate = {
                    let view = self.groups[group].machine.view.read().await;
                    view.subscriptions
                        .subscriptions
                        .get(&path)
                        .filter(|s| {
                            s.wake
                                .as_ref()
                                .is_some_and(|w| !w.delivered && w.reserved_until <= now())
                                && s.next_attempt_at <= now()
                        })
                        .map(|s| (s.incarnation, s.generation))
                };
                if let Some((inc, generation)) = candidate {
                    deliveries.spawn(self.deliver(group, path, inc, generation));
                }
            }
            while let Some(result) = deliveries.join_next().await {
                if let Err(e) = result {
                    tracing::warn!(group,error=%e,"subscription delivery task failed");
                }
            }
        }
    }

    async fn deliver(&self, group: usize, path: String, incarnation: u64, generation: u64) {
        let Ok(reserved) = self
            .sub_propose(
                group,
                &path,
                Action::Reserve {
                    incarnation,
                    generation,
                },
            )
            .await
        else {
            return;
        };
        if reserved.data.status != 200 {
            return;
        }
        let attempt = reserved.log_id.index;
        let (sub, keys) = {
            let view = self.groups[group].machine.view.read().await;
            let Some(s) = view
                .subscriptions
                .subscriptions
                .get(&path)
                .filter(|s| s.incarnation == incarnation && s.generation == generation)
            else {
                return;
            };
            (s.clone(), view.subscriptions.keys.as_ref().unwrap().clone())
        };
        let (ok, done) = if let Some(webhook) = &sub.config.webhook {
            let body = reserved.data.body;
            let delivered = async {
                let (url, addresses) = webhook_target(&webhook.url).await.ok()?;
                let host = url.host_str()?;
                let client = reqwest::Client::builder()
                    .no_proxy()
                    .redirect(reqwest::redirect::Policy::none())
                    .timeout(Duration::from_secs(4))
                    .resolve_to_addrs(host, &addresses)
                    .build()
                    .ok()?;
                let mut reply = client
                    .post(url.clone())
                    .header("content-type", "application/json")
                    .header("webhook-signature", keys.signature(&body, now()))
                    .body(body)
                    .send()
                    .await
                    .ok()?;
                if !reply.status().is_success() {
                    return None;
                }
                let mut bytes = Vec::new();
                while let Some(chunk) = reply.chunk().await.ok()? {
                    if bytes.len() + chunk.len() > 16 * 1024 {
                        return None;
                    }
                    bytes.extend_from_slice(&chunk);
                }
                let done = serde_json::from_slice::<Value>(&bytes)
                    .ok()
                    .is_some_and(|v| v["done"] == true);
                Some(done)
            }
            .await;
            (delivered.is_some(), delivered.unwrap_or(false))
        } else {
            let target = format!("{}{}", sub.root, sub.config.wake_stream.as_ref().unwrap());
            let wake = sub.wake.as_ref().unwrap();
            let event = json!({"type":"wake","subscription_id":path.rsplit('/').next().unwrap(),
                "stream":wake.snapshot.iter().find(|(_,l)|l.observation.tail>l.acked).map(|(p,_)|p),
                "generation":generation,"ts":wake.created_at});
            let owner = partition(&target, self.groups.len());
            let Some(address) = self.candidates(owner).into_iter().next() else {
                return;
            };
            // Unknown outcomes retry the same producer generation later. This
            // engine's producer epoch fences delayed notifications from old wakes.
            let result = self
                .client
                .post(format!("http://{address}{target}"))
                .header("content-type", "application/json")
                .header("producer-id", format!("__wake_{path}_{incarnation}"))
                .header("producer-epoch", generation.to_string())
                .header("producer-seq", "0")
                .json(&event)
                .send()
                .await;
            (result.is_ok_and(|r| r.status().is_success()), false)
        };
        let mut random = [0; 2];
        if getrandom::fill(&mut random).is_err() {
            return;
        } // reservation remains recoverable
        let _ = self
            .sub_propose(
                group,
                &path,
                Action::Delivered {
                    incarnation,
                    generation,
                    attempt,
                    ok,
                    done,
                    jitter: u16::from_le_bytes(random) % 1000,
                },
            )
            .await;
    }
}

fn public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v) => {
            let [a, b, c, _] = v.octets();
            !(v.is_private()
                || v.is_loopback()
                || v.is_link_local()
                || v.is_unspecified()
                || a == 0
                || a >= 224
                || (a == 100 && (64..=127).contains(&b))
                || (a == 192 && ((b == 0 && (c == 0 || c == 2)) || (b == 88 && c == 99)))
                || (a == 198 && (b == 18 || b == 19 || (b == 51 && c == 100)))
                || (a == 203 && b == 0 && c == 113))
        }
        IpAddr::V6(v) => {
            let s = v.segments();
            s[0] & 0xe000 == 0x2000
                && s[0] != 0x2002
                && !(s[0] == 0x2001 && (s[1] == 0 || s[1] == 0xdb8))
        }
    }
}

async fn webhook_target(raw: &str) -> Result<(reqwest::Url, Vec<SocketAddr>), &'static str> {
    let url = reqwest::Url::parse(raw).map_err(|_| "invalid URL")?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err("http(s) URL without credentials or fragment required");
    }
    let host = url.host_str().ok_or("missing host")?;
    let host = host.trim_start_matches('[').trim_end_matches(']');
    let literal = host.parse::<IpAddr>().ok();
    let localhost = host.eq_ignore_ascii_case("localhost");
    let development =
        localhost || matches!(literal,Some(IpAddr::V4(v)) if v.octets()[..3] == [127,0,0]);
    if !development && url.scheme() != "https" {
        return Err("production webhooks require HTTPS");
    }
    let port = url.port_or_known_default().ok_or("missing port")?;
    let addresses: Vec<_> = if let Some(ip) = literal {
        vec![SocketAddr::new(ip, port)]
    } else {
        tokio::time::timeout(
            Duration::from_secs(2),
            tokio::net::lookup_host((host, port)),
        )
        .await
        .map_err(|_| "DNS timeout")?
        .map_err(|_| "DNS failed")?
        .collect()
    };
    if addresses.is_empty()
        || addresses.iter().any(|a| {
            if development {
                !a.ip().is_loopback()
            } else {
                !public_ip(a.ip())
            }
        })
    {
        return Err("target resolves to a private, local, reserved or unsafe address");
    }
    Ok((url, addresses))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ssrf_rejects_private_reserved_mapped_and_non_https_targets() {
        for url in [
            "http://10.0.0.1/hook",
            "https://169.254.169.254/metadata",
            "https://100.64.0.1/",
            "https://198.19.0.1/",
            "https://224.0.0.1/",
            "https://[::ffff:10.0.0.1]/",
            "https://[fd00::1]/",
            "https://[fe80::1]/",
            "https://[2002:0a00:0001::]/",
            "https://[2001:db8::1]/",
            "http://8.8.8.8/",
            "https://user:password@8.8.8.8/",
            "ftp://127.0.0.1/",
            "http://127.1.0.1/",
        ] {
            assert!(webhook_target(url).await.is_err(), "{url}");
        }
        for url in [
            "http://127.0.0.2:8080/hook",
            "http://localhost:8080/hook",
            "https://8.8.8.8/hook",
        ] {
            let (parsed, addresses) = webhook_target(url).await.unwrap();
            assert!(!addresses.is_empty());
            assert!(addresses
                .iter()
                .all(|a| a.port() == parsed.port_or_known_default().unwrap()));
        }
    }
}
