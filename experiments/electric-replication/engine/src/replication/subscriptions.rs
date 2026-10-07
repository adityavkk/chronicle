//! Deterministic, partition-owned control state. No network or local clock here.
use super::*;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64, Engine};
use ed25519_dalek::{Signer, SigningKey};
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub const MAX_LINKS: usize = 2048;

#[derive(Clone, Serialize, Deserialize)]
pub struct Keys {
    signing: [u8; 32],
    token: [u8; 32],
}
impl Keys {
    pub fn generate() -> io::Result<Self> {
        let mut keys = Self {
            signing: [0; 32],
            token: [0; 32],
        };
        getrandom::fill(&mut keys.signing).map_err(|e| io::Error::other(e.to_string()))?;
        getrandom::fill(&mut keys.token).map_err(|e| io::Error::other(e.to_string()))?;
        Ok(keys)
    }
    pub fn jwk(&self) -> Value {
        let x = B64.encode(
            SigningKey::from_bytes(&self.signing)
                .verifying_key()
                .as_bytes(),
        );
        let thumb = format!("{{\"crv\":\"Ed25519\",\"kty\":\"OKP\",\"x\":\"{x}\"}}");
        json!({"kty":"OKP", "crv":"Ed25519", "use":"sig", "alg":"EdDSA",
            "x":x, "kid":format!("ds_{}", B64.encode(Sha256::digest(thumb)))})
    }
    pub fn signature(&self, body: &[u8], time: u64) -> String {
        let timestamp = time / 1000;
        let mut signed = format!("{timestamp}.").into_bytes();
        signed.extend_from_slice(body);
        let signature = SigningKey::from_bytes(&self.signing).sign(&signed);
        format!(
            "t={timestamp},kid={},ed25519={}",
            self.jwk()["kid"].as_str().unwrap(),
            B64.encode(signature.to_bytes())
        )
    }
    fn token(&self, claims: &Claims) -> String {
        let payload = B64.encode(serde_json::to_vec(claims).unwrap());
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.token).unwrap();
        mac.update(payload.as_bytes());
        format!("{payload}.{}", B64.encode(mac.finalize().into_bytes()))
    }
    fn verify(&self, raw: &str) -> Option<Claims> {
        if raw.len() > 8192 {
            return None;
        }
        let (payload, signature) = raw.split_once('.')?;
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.token).unwrap();
        mac.update(payload.as_bytes());
        mac.verify_slice(&B64.decode(signature).ok()?).ok()?;
        serde_json::from_slice(&B64.decode(payload).ok()?).ok()
    }
}

#[derive(Serialize, Deserialize)]
struct Claims {
    path: String,
    incarnation: u64,
    generation: u64,
    wake: String,
    kind: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Delivery {
    pub url: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub pattern: Option<String>,
    #[serde(default)]
    pub streams: Vec<String>,
    #[serde(default)]
    pub webhook: Option<Delivery>,
    #[serde(default)]
    pub wake_stream: Option<String>,
    #[serde(default = "lease_default")]
    pub lease_ttl_ms: u64,
    #[serde(default)]
    pub description: String,
}
fn lease_default() -> u64 {
    30_000
}

pub fn valid_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 2048
        && !path.starts_with('/')
        && !path
            .split('/')
            .any(|s| s.is_empty() || s == "." || s == "..")
        && path.split('/').next() != Some("__ds")
        && !path
            .bytes()
            .any(|b| b <= 32 || matches!(b, b'?' | b'#' | b'%' | b'\\'))
}

impl Config {
    pub fn normalize(mut self) -> Result<Self, &'static str> {
        self.streams.sort();
        self.streams.dedup();
        if self.streams.len() > MAX_LINKS || self.streams.iter().any(|s| !valid_path(s)) {
            return Err("invalid explicit paths or link limit exceeded");
        }
        if self.pattern.as_deref() == Some("") {
            self.pattern = None;
        }
        if let Some(pattern) = &self.pattern {
            if !valid_path(pattern)
                || pattern
                    .split('/')
                    .any(|p| p.contains('*') && p != "*" && p != "**")
            {
                return Err("pattern uses whole-segment * and ** only");
            }
        }
        if self.pattern.is_none() && self.streams.is_empty() {
            return Err("pattern or streams required");
        }
        if !(1000..=600_000).contains(&self.lease_ttl_ms) {
            return Err("lease_ttl_ms must be 1000..600000");
        }
        match self.kind.as_str() {
            "webhook" if self.webhook.is_some() && self.wake_stream.is_none() => {}
            "pull-wake"
                if self.webhook.is_none()
                    && self.wake_stream.as_deref().is_some_and(valid_path) => {}
            _ => return Err("type and delivery configuration disagree"),
        }
        Ok(self)
    }
    fn matches(&self, path: &str) -> bool {
        self.pattern.as_deref().is_some_and(|p| glob(p, path))
    }
    fn hash(&self) -> String {
        B64.encode(Sha256::digest(serde_json::to_vec(self).unwrap()))
    }
}

/// Segment DP: ** matches zero or more segments, * exactly one, including the
/// middle of a pattern. No exponential recursive matcher on untrusted patterns.
pub fn glob(pattern: &str, path: &str) -> bool {
    let segments: Vec<_> = path.split('/').collect();
    let mut matched = vec![false; segments.len() + 1];
    matched[0] = true;
    for p in pattern.split('/') {
        if p == "**" {
            for j in 1..matched.len() {
                matched[j] |= matched[j - 1];
            }
        } else {
            for j in (1..matched.len()).rev() {
                matched[j] = matched[j - 1] && (p == "*" || p == segments[j - 1]);
            }
            matched[0] = false;
        }
    }
    matched[segments.len()]
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Observation {
    pub path: String, // Full native path, not a public relative link.
    pub group: usize,
    pub index: u64,
    pub incarnation: Option<u64>,
    pub tail: u64,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Link {
    pub explicit: bool,
    pub observation: Observation,
    pub acked: u64,
}
impl Link {
    fn pending(&self) -> bool {
        self.observation.incarnation.is_some() && self.observation.tail > self.acked
    }
    fn public(&self, path: &str, snapshot: bool) -> Value {
        let mut value = json!({"path":path, "link_type":if self.explicit {"explicit"} else {"glob"},
            "acked_offset":crate::store::format_offset(self.acked)});
        if snapshot {
            value["tail_offset"] =
                crate::store::format_offset(if self.observation.incarnation.is_some() {
                    self.observation.tail
                } else {
                    self.acked
                })
                .into();
            value["has_pending"] = self.pending().into();
        }
        value
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Wake {
    pub id: String,
    pub snapshot: BTreeMap<String, Link>,
    pub holder: Option<String>,
    pub lease_until: u64,
    pub notify_after: u64,
    pub delivered: bool,
    pub attempt: u64,
    pub reserved_until: u64,
    pub created_at: u64,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Subscription {
    pub config: Config,
    pub config_hash: String,
    pub root: String,
    pub base_url: String,
    pub incarnation: u64,
    pub created_at: u64,
    pub generation: u64,
    pub links: BTreeMap<String, Link>,
    pub wake: Option<Wake>,
    pub next_attempt_at: u64,
    pub retries: u32,
}
impl Subscription {
    pub fn pending(&self) -> bool {
        self.links.values().any(Link::pending)
    }
    fn claims(&self, path: &str) -> Claims {
        Claims {
            path: path.into(),
            incarnation: self.incarnation,
            generation: self.generation,
            wake: self.wake.as_ref().unwrap().id.clone(),
            kind: self.config.kind.clone(),
        }
    }
    fn advance(&mut self, path: &str, now: u64) {
        if self.wake.as_ref().is_some_and(|w| {
            (w.lease_until != 0 && now >= w.lease_until)
                || (w.holder.is_none() && w.delivered && now >= w.notify_after)
        }) {
            self.wake = None;
        }
        if self.wake.is_none() && self.pending() && now >= self.next_attempt_at {
            self.generation = self
                .generation
                .checked_add(1)
                .expect("generation exhausted");
            let identity = serde_json::to_vec(&(path, self.incarnation, self.generation)).unwrap();
            let webhook = self.config.kind == "webhook";
            self.wake = Some(Wake {
                id: format!("w_{}", B64.encode(Sha256::digest(identity))),
                snapshot: self.links.clone(),
                holder: webhook.then(|| "webhook".into()),
                lease_until: if webhook {
                    now + self.config.lease_ttl_ms
                } else {
                    0
                },
                notify_after: now + self.config.lease_ttl_ms,
                delivered: false,
                attempt: 0,
                reserved_until: 0,
                created_at: now,
            });
        }
    }
    fn observe(&mut self, observations: &[Observation]) -> Result<(), &'static str> {
        let mut additions = BTreeSet::new();
        for o in observations {
            if let Some(path) = o.path.strip_prefix(&self.root) {
                if o.incarnation.is_some()
                    && self.config.matches(path)
                    && !self.links.contains_key(path)
                {
                    additions.insert(path);
                }
            }
        }
        if self.links.len() + additions.len() > MAX_LINKS {
            return Err("subscription link limit exceeded");
        }
        for o in observations {
            let Some(path) = o.path.strip_prefix(&self.root) else {
                continue;
            };
            if !self.links.contains_key(path)
                && self.config.matches(path)
                && o.incarnation.is_some()
            {
                self.links.insert(
                    path.into(),
                    Link {
                        explicit: false,
                        observation: o.clone(),
                        acked: 0,
                    },
                );
            }
            if let Some(link) = self.links.get_mut(path) {
                if o.index >= link.observation.index {
                    // A missing/recreated stream cannot inherit the old cursor.
                    if o.incarnation != link.observation.incarnation {
                        link.acked = 0;
                    }
                    link.observation = o.clone();
                }
            }
        }
        Ok(())
    }
    pub fn public(&self, path: &str, keys: &Keys) -> Value {
        let id = path.rsplit('/').next().unwrap();
        let webhook = self.config.webhook.as_ref().map(|w| json!({"url":w.url,
            "signing":{"alg":"ed25519", "kid":keys.jwk()["kid"], "jwks_url":format!("{}__ds/jwks.json", self.base_url)}}));
        json!({"id":id, "subscription_id":id, "type":self.config.kind, "pattern":self.config.pattern,
            "streams":self.links.iter().map(|(p,l)| l.public(p,false)).collect::<Vec<_>>(),
            "webhook":webhook, "wake_stream":self.config.wake_stream, "lease_ttl_ms":self.config.lease_ttl_ms,
            "created_at":timestamp(self.created_at), "status":if self.retries == 0 {"active"} else {"failed"},
            "description":self.config.description})
    }
    pub fn envelope(&self, path: &str, keys: &Keys) -> Value {
        let wake = self.wake.as_ref().unwrap();
        let id = path.rsplit('/').next().unwrap();
        json!({"subscription_id":id, "wake_id":wake.id, "generation":self.generation,
            "streams":wake.snapshot.iter().map(|(p,l)| l.public(p,true)).collect::<Vec<_>>(),
            "callback_url":format!("{}__ds/subscriptions/{id}/callback", self.base_url),
            "callback_token":keys.token(&self.claims(path))})
    }
}

fn timestamp(ms: u64) -> String {
    let seconds = (ms / 1000) as libc::time_t;
    let mut tm = std::mem::MaybeUninit::<libc::tm>::uninit();
    // All inputs originate in the committed wall-clock samples, not strings.
    let tm = unsafe {
        libc::gmtime_r(&seconds, tm.as_mut_ptr());
        tm.assume_init()
    };
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        tm.tm_year + 1900,
        tm.tm_mon + 1,
        tm.tm_mday,
        tm.tm_hour,
        tm.tm_min,
        tm.tm_sec,
        ms % 1000
    )
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct State {
    pub keys: Option<Keys>,
    pub subscriptions: BTreeMap<String, Subscription>,
}

#[derive(Serialize, Deserialize)]
pub enum Action {
    Keys(Keys),
    Create {
        config: Config,
        root: String,
        base_url: String,
        observations: Vec<Observation>,
    },
    Observe {
        targets: BTreeMap<String, u64>,
        observations: Vec<Observation>,
    },
    Request {
        incarnation: u64,
        operation: String,
        body: Value,
        token: String,
        observations: Vec<Observation>,
    },
    Reserve {
        incarnation: u64,
        generation: u64,
    },
    Delivered {
        incarnation: u64,
        generation: u64,
        attempt: u64,
        ok: bool,
        done: bool,
        jitter: u16,
    },
    Delete,
}

pub fn error(status: u16, code: &str, message: &str) -> Resp {
    let mut r = super::json(&json!({"error":{"code":code,"message":message}}));
    r.status = status;
    r
}

impl State {
    pub fn apply(&mut self, path: &str, action: Action, index: u64, now: u64) -> Resp {
        if let Action::Keys(keys) = action {
            self.keys.get_or_insert(keys);
            return Resp::new(204);
        }
        if let Action::Create {
            config,
            root,
            base_url,
            observations,
        } = action
        {
            let config = match config.normalize() {
                Ok(c) => c,
                Err(m) => return error(400, "INVALID_REQUEST", m),
            };
            let hash = config.hash();
            if let Some(sub) = self.subscriptions.get(path) {
                return if sub.config_hash == hash {
                    super::json(&sub.public(path, self.keys.as_ref().unwrap()))
                } else {
                    error(
                        409,
                        "CONFIG_MISMATCH",
                        "immutable subscription configuration differs",
                    )
                };
            }
            let mut sub = Subscription {
                config,
                config_hash: hash,
                root,
                base_url,
                incarnation: index,
                created_at: now,
                generation: 0,
                links: BTreeMap::new(),
                wake: None,
                next_attempt_at: 0,
                retries: 0,
            };
            for o in &observations {
                let Some(p) = o.path.strip_prefix(&sub.root) else {
                    continue;
                };
                let explicit = sub.config.streams.iter().any(|s| s == p);
                if explicit || (o.incarnation.is_some() && sub.config.matches(p)) {
                    sub.links.insert(
                        p.into(),
                        Link {
                            explicit,
                            observation: o.clone(),
                            acked: o.tail,
                        },
                    );
                }
            }
            if sub.links.len() > MAX_LINKS {
                return error(400, "LINK_LIMIT", "narrow pattern or shard subscriptions");
            }
            // Explicit absent paths must be represented by the catalog too.
            if sub
                .config
                .streams
                .iter()
                .any(|p| !sub.links.contains_key(p))
            {
                return error(
                    400,
                    "INCOMPLETE_CATALOG",
                    "missing explicit path observation",
                );
            }
            let mut reply = super::json(&sub.public(path, self.keys.as_ref().unwrap()));
            reply.status = 201;
            self.subscriptions.insert(path.into(), sub);
            return reply;
        }
        if let Action::Observe {
            targets,
            observations,
        } = action
        {
            // Validate every affected subscription before modifying any of them.
            for (p, incarnation) in &targets {
                let Some(sub) = self
                    .subscriptions
                    .get(p)
                    .filter(|s| &s.incarnation == incarnation)
                else {
                    continue;
                };
                let extra = observations
                    .iter()
                    .filter_map(|o| {
                        o.path.strip_prefix(&sub.root).filter(|p| {
                            o.incarnation.is_some()
                                && sub.config.matches(p)
                                && !sub.links.contains_key(*p)
                        })
                    })
                    .collect::<BTreeSet<_>>()
                    .len();
                if sub.links.len() + extra > MAX_LINKS {
                    return error(400, "LINK_LIMIT", "narrow pattern or shard subscriptions");
                }
            }
            for (p, incarnation) in &targets {
                let Some(sub) = self
                    .subscriptions
                    .get_mut(p)
                    .filter(|s| &s.incarnation == incarnation)
                else {
                    continue;
                };
                sub.observe(&observations).unwrap();
                sub.advance(p, now);
            }
            return Resp::new(204);
        }
        if matches!(action, Action::Delete) {
            self.subscriptions.remove(path);
            return Resp::new(204);
        }
        let Some(sub) = self.subscriptions.get_mut(path) else {
            return error(404, "NOT_FOUND", "subscription not found");
        };
        sub.advance(path, now);
        let keys = self.keys.as_ref().unwrap();
        match action {
            Action::Request {
                incarnation,
                operation,
                body,
                token,
                observations,
            } => {
                if sub.incarnation != incarnation {
                    return error(409, "FENCED", "subscription recreated during catalog read");
                }
                if let Err(m) = sub.observe(&observations) {
                    return error(400, "LINK_LIMIT", m);
                }
                sub.advance(path, now);
                match operation.as_str() {
                    "streams" => {
                        let Some(paths) = body.get("streams").and_then(Value::as_array) else {
                            return error(400, "INVALID_REQUEST", "streams array required");
                        };
                        let paths: Option<BTreeSet<_>> = paths
                            .iter()
                            .map(|p| p.as_str().filter(|p| valid_path(p)))
                            .collect();
                        let Some(paths) = paths else {
                            return error(400, "INVALID_REQUEST", "invalid stream paths");
                        };
                        if sub.links.len()
                            + paths
                                .iter()
                                .filter(|p| !sub.links.contains_key(**p))
                                .count()
                            > MAX_LINKS
                        {
                            return error(400, "LINK_LIMIT", "link limit exceeded");
                        }
                        for p in &paths {
                            if !sub.links.contains_key(*p)
                                && !observations
                                    .iter()
                                    .any(|o| o.path == format!("{}{p}", sub.root))
                            {
                                return error(
                                    400,
                                    "INCOMPLETE_CATALOG",
                                    "missing explicit path observation",
                                );
                            }
                        }
                        for p in paths {
                            if let Some(l) = sub.links.get_mut(p) {
                                l.explicit = true;
                            } else {
                                let o = observations
                                    .iter()
                                    .find(|o| o.path == format!("{}{p}", sub.root))
                                    .unwrap();
                                sub.links.insert(
                                    p.into(),
                                    Link {
                                        explicit: true,
                                        observation: o.clone(),
                                        acked: o.tail,
                                    },
                                );
                            }
                        }
                        Resp::new(204)
                    }
                    "unlink" => {
                        let Some(p) = body.as_str().filter(|p| valid_path(p)) else {
                            return error(400, "INVALID_REQUEST", "invalid path");
                        };
                        if sub.config.matches(p) {
                            if let Some(l) = sub.links.get_mut(p) {
                                l.explicit = false;
                            }
                        } else {
                            sub.links.remove(p);
                        }
                        Resp::new(204)
                    }
                    "claim" => {
                        if sub.config.kind != "pull-wake" {
                            return error(400, "INVALID_REQUEST", "claim requires pull-wake");
                        }
                        let Some(worker) = body
                            .get("worker")
                            .and_then(Value::as_str)
                            .filter(|w| !w.trim().is_empty() && w.len() <= 256)
                        else {
                            return error(400, "INVALID_REQUEST", "worker name required");
                        };
                        let Some(w) = &mut sub.wake else {
                            return error(409, "NO_PENDING_WORK", "no pending events");
                        };
                        if let Some(holder) = &w.holder {
                            let mut reply = super::json(
                                &json!({"error":{"code":"ALREADY_CLAIMED","current_holder":holder,"generation":sub.generation}}),
                            );
                            reply.status = 409;
                            return reply;
                        }
                        w.holder = Some(worker.into());
                        w.lease_until = now + sub.config.lease_ttl_ms;
                        w.snapshot = sub.links.clone();
                        let streams: Vec<_> =
                            w.snapshot.iter().map(|(p, l)| l.public(p, true)).collect();
                        let id = w.id.clone();
                        super::json(&json!({"wake_id":id,"generation":sub.generation,
                            "token":keys.token(&sub.claims(path)),"streams":streams,"lease_ttl_ms":sub.config.lease_ttl_ms}))
                    }
                    "callback" | "ack" | "release" => {
                        let Some(claims) = keys.verify(&token).filter(|c| c.path == path) else {
                            return error(
                                401,
                                "INVALID_TOKEN",
                                "subscription-scoped bearer token required",
                            );
                        };
                        let Some(w) = &sub.wake else {
                            return error(409, "FENCED", "wake retired");
                        };
                        if claims.incarnation != sub.incarnation
                            || claims.generation != sub.generation
                            || claims.wake != w.id
                            || claims.kind != sub.config.kind
                            || w.holder.is_none()
                            || now >= w.lease_until
                            || body.get("generation").and_then(Value::as_u64)
                                != Some(sub.generation)
                            || body.get("wake_id").and_then(Value::as_str) != Some(&w.id)
                        {
                            return error(
                                409,
                                "FENCED",
                                "stale incarnation, generation, wake or lease",
                            );
                        }
                        if (operation == "callback") != (sub.config.kind == "webhook")
                            && operation != "release"
                        {
                            return error(400, "INVALID_REQUEST", "wrong acknowledgement endpoint");
                        }
                        if operation == "release" {
                            sub.wake = None;
                            sub.next_attempt_at = 0;
                            sub.retries = 0;
                            sub.advance(path, now);
                            return Resp::new(204);
                        }
                        let acks = match body.get("acks") {
                            None => &[][..],
                            Some(Value::Array(a)) => a.as_slice(),
                            _ => return error(400, "INVALID_REQUEST", "acks must be an array"),
                        };
                        let mut validated = Vec::new();
                        for ack in acks {
                            let Some(p) = ack.get("stream").and_then(Value::as_str) else {
                                return error(400, "INVALID_REQUEST", "ack stream required");
                            };
                            let Some(offset) = ack
                                .get("offset")
                                .and_then(Value::as_str)
                                .and_then(parse_cursor)
                            else {
                                return error(
                                    400,
                                    "INVALID_OFFSET",
                                    "invalid native stream cursor",
                                );
                            };
                            let (Some(link), Some(issued)) = (sub.links.get(p), w.snapshot.get(p))
                            else {
                                return error(409, "FENCED", "link changed since wake");
                            };
                            if link.observation.incarnation != issued.observation.incarnation {
                                return error(409, "FENCED", "stream recreated since wake");
                            }
                            if offset > link.observation.tail {
                                return error(400, "INVALID_OFFSET", "ack beyond observed tail");
                            }
                            validated.push((p.to_string(), offset));
                        }
                        for (p, offset) in validated {
                            let link = sub.links.get_mut(&p).unwrap();
                            link.acked = link.acked.max(offset);
                        }
                        let done = body.get("done").and_then(Value::as_bool).unwrap_or(false);
                        sub.next_attempt_at = 0;
                        sub.retries = 0;
                        if done {
                            sub.wake = None;
                            sub.advance(path, now);
                        } else {
                            sub.wake.as_mut().unwrap().lease_until = now + sub.config.lease_ttl_ms;
                        }
                        super::json(&json!({"ok":true,"next_wake":done && sub.pending()}))
                    }
                    _ => error(404, "NOT_FOUND", "unknown subscription action"),
                }
            }
            Action::Reserve {
                incarnation,
                generation,
            } => {
                if sub.incarnation != incarnation || sub.generation != generation {
                    return error(409, "FENCED", "dispatch wake changed");
                }
                let Some(w) = &mut sub.wake else {
                    return Resp::new(204);
                };
                if w.delivered || w.reserved_until > now || sub.next_attempt_at > now {
                    return Resp::new(204);
                }
                w.attempt = index;
                w.reserved_until = now + 6000;
                // Only the committed reservation can authorize this envelope.
                super::json(&sub.envelope(path, keys))
            }
            Action::Delivered {
                incarnation,
                generation,
                attempt,
                ok,
                done,
                jitter,
            } => {
                if sub.incarnation != incarnation
                    || sub.generation != generation
                    || sub.wake.as_ref().is_none_or(|w| w.attempt != attempt)
                {
                    return error(409, "FENCED", "stale delivery result");
                }
                if ok {
                    sub.next_attempt_at = 0;
                    sub.retries = 0;
                    if done && sub.config.kind == "webhook" {
                        let wake = sub.wake.take().unwrap();
                        for (p, issued) in wake.snapshot {
                            if let Some(link) = sub.links.get_mut(&p) {
                                if link.observation.incarnation == issued.observation.incarnation {
                                    link.acked = link.acked.max(issued.observation.tail);
                                }
                            }
                        }
                        sub.advance(path, now);
                    } else {
                        sub.wake.as_mut().unwrap().delivered = true;
                    }
                } else {
                    sub.retries = sub.retries.saturating_add(1);
                    let base = (1000u64 << (sub.retries - 1).min(6)).min(60_000);
                    sub.next_attempt_at = now + base + base * u64::from(jitter.min(999)) / 5000;
                    sub.wake.as_mut().unwrap().reserved_until = 0;
                }
                Resp::new(204)
            }
            _ => unreachable!(),
        }
    }
}

fn parse_cursor(raw: &str) -> Option<u64> {
    if raw == "-1" {
        return Some(0);
    }
    match crate::store::parse_offset(Some(raw)).ok()? {
        crate::store::ParsedOffset::At(n) if crate::store::format_offset(n) == raw => Some(n),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    const PATH: &str = "/r/__ds/subscriptions/a";

    fn configured(kind: &str) -> State {
        let mut state = State::default();
        state.apply(PATH, Action::Keys(Keys::generate().unwrap()), 1, 0);
        let mut config = json!({"type":kind,"pattern":"data/**","lease_ttl_ms":1000});
        if kind == "webhook" {
            config["webhook"] = json!({"url":"http://localhost/hook"});
        } else {
            config["wake_stream"] = "wake/pool".into();
        }
        let response = state.apply(
            PATH,
            Action::Create {
                config: serde_json::from_value(config).unwrap(),
                root: "/r/".into(),
                base_url: "http://localhost/r/".into(),
                observations: vec![],
            },
            42,
            0,
        );
        assert_eq!(response.status, 201);
        state
    }
    fn observe(state: &mut State, time: u64, index: u64, incarnation: u64, tail: u64) {
        let response = state.apply(
            "",
            Action::Observe {
                targets: BTreeMap::from([(PATH.into(), 42)]),
                observations: vec![Observation {
                    path: "/r/data/x".into(),
                    group: 1,
                    index,
                    incarnation: Some(incarnation),
                    tail,
                }],
            },
            index,
            time,
        );
        assert_eq!(response.status, 204);
    }
    fn body(response: Resp) -> Value {
        let Body::Full(bytes) = response.body else {
            panic!("expected JSON");
        };
        serde_json::from_slice(&bytes).unwrap()
    }
    fn request(operation: &str, value: Value, token: &str) -> Action {
        Action::Request {
            incarnation: 42,
            operation: operation.into(),
            body: value,
            token: token.into(),
            observations: vec![],
        }
    }

    proptest! {
        #[test]
        fn exact_lease_boundary_fences_and_ack_batches_are_atomic(delta in 999u64..1003, a in 1u64..10000, b in 1u64..1000) {
            let mut state = configured("pull-wake");
            observe(&mut state,2000,2,7,a);
            let claim = state.apply(PATH,request("claim",json!({"worker":"first"}),""),3,2001);
            assert_eq!(claim.status,200);
            let claim = body(claim);
            let token = claim["token"].as_str().unwrap();
            let envelope = json!({"generation":1,"wake_id":claim["wake_id"],"done":true,
                "acks":[{"stream":"data/x","offset":crate::store::format_offset(a)},
                        {"stream":"data/x","offset":crate::store::format_offset(a+b)}]});
            let result = state.apply(PATH,request("ack",envelope,token),4,2001+delta);
            assert_eq!(result.status,if delta < 1000 {400} else {409});
            assert_eq!(state.subscriptions[PATH].links["data/x"].acked,0);
            if delta < 1000 {
                // Heartbeat just before expiry extends the same lease; no ack.
                let result = state.apply(PATH,request("ack",json!({"generation":1,"wake_id":claim["wake_id"]}),token),5,3000);
                assert_eq!(result.status,200);
                assert_eq!(state.subscriptions[PATH].wake.as_ref().unwrap().lease_until,4000);
                let result = state.apply(PATH,request("claim",json!({"worker":"second"}),""),6,3002);
                assert_eq!(result.status,409);
                assert_eq!(body(result)["error"]["current_holder"],"first");
            } else {
                let newer = body(state.apply(PATH,request("claim",json!({"worker":"second"}),""),5,3004));
                assert_eq!(newer["generation"],2);
                assert_ne!(newer["wake_id"],claim["wake_id"]);
            }
        }

        #[test]
        fn retry_deadline_survives_serialization_and_expiry(jitter in 0u16..1000) {
            let mut state = configured("webhook");
            observe(&mut state,1000,1,7,19);
            assert_eq!(state.apply(PATH,Action::Reserve {incarnation:42,generation:1},10,1001).status,200);
            assert_eq!(state.apply(PATH,Action::Delivered {incarnation:42,generation:1,attempt:10,ok:false,done:false,jitter},11,1002).status,204);
            let deadline = 2002 + u64::from(jitter)/5;
            let mut restored: State = serde_json::from_slice(&serde_json::to_vec(&state).unwrap()).unwrap();
            observe(&mut restored,deadline-1,12,7,19);
            assert_eq!(restored.subscriptions[PATH].next_attempt_at,deadline);
            assert!(restored.subscriptions[PATH].wake.is_none());
            observe(&mut restored,deadline,13,7,19);
            assert_eq!(restored.subscriptions[PATH].generation,2);
            assert!(restored.subscriptions[PATH].wake.is_some());
        }
    }

    #[test]
    fn snapshot_ack_and_old_workers_do_not_consume_recreated_stream() {
        let mut state = configured("webhook");
        observe(&mut state, 1, 1, 7, 19);
        let envelope = body(state.apply(
            PATH,
            Action::Reserve {
                incarnation: 42,
                generation: 1,
            },
            10,
            2,
        ));
        observe(&mut state, 3, 2, 8, 53); // A different stream incarnation, same path.
        assert_eq!(
            state
                .apply(
                    PATH,
                    Action::Delivered {
                        incarnation: 42,
                        generation: 1,
                        attempt: 10,
                        ok: true,
                        done: true,
                        jitter: 0
                    },
                    11,
                    4
                )
                .status,
            204
        );
        assert_eq!(state.subscriptions[PATH].links["data/x"].acked, 0);
        assert_eq!(state.subscriptions[PATH].generation, 2);
        let callback = json!({"generation":1,"wake_id":envelope["wake_id"],"done":true,
            "acks":[{"stream":"data/x","offset":crate::store::format_offset(53)}]});
        assert_eq!(
            state
                .apply(
                    PATH,
                    request(
                        "callback",
                        callback,
                        envelope["callback_token"].as_str().unwrap()
                    ),
                    12,
                    5
                )
                .status,
            409
        );
        let old = state.subscriptions[PATH].clone();
        state.apply(PATH, Action::Delete, 13, 6);
        assert_eq!(
            state
                .apply(
                    PATH,
                    Action::Create {
                        config: old.config,
                        root: old.root,
                        base_url: old.base_url,
                        observations: vec![]
                    },
                    99,
                    7
                )
                .status,
            201
        );
        observe(&mut state, 8, 4, 8, 53); // captured for incarnation 42, not 99
        assert!(state.subscriptions[PATH].links.is_empty());
    }

    #[test]
    fn explicit_precedence_and_unlink_preserve_glob_cursor() {
        let mut state = configured("pull-wake");
        observe(&mut state, 1, 1, 7, 19);
        assert_eq!(
            state
                .apply(
                    PATH,
                    request("streams", json!({"streams":["data/x","data/x"]}), ""),
                    2,
                    2
                )
                .status,
            204
        );
        assert!(state.subscriptions[PATH].links["data/x"].explicit);
        assert_eq!(
            state
                .apply(PATH, request("unlink", json!("data/x"), ""), 3, 3)
                .status,
            204
        );
        assert!(!state.subscriptions[PATH].links["data/x"].explicit);
        assert_eq!(state.subscriptions[PATH].links["data/x"].acked, 0);
        for (pattern, path, want) in [
            ("data/**/end", "data/end", true),
            ("data/*/end", "data/end", false),
            ("data/**/end", "data/a/b/end", true),
            ("data/*", "data/a/b", false),
            ("data/**/end", "data/a/end/x", false),
            ("data/*/end", "data/a/end", true),
        ] {
            assert_eq!(glob(pattern, path), want, "{pattern} {path}");
        }
    }

    #[test]
    fn real_ed25519_signature_covers_raw_bytes_and_keys_stay_private() {
        let keys = Keys::generate().unwrap();
        let raw = br#"{"a":  17,"b":4}"#;
        let header = keys.signature(raw, 1700000000123);
        let signature = header.split("ed25519=").nth(1).unwrap();
        let signature =
            ed25519_dalek::Signature::from_slice(&B64.decode(signature).unwrap()).unwrap();
        let public = B64.decode(keys.jwk()["x"].as_str().unwrap()).unwrap();
        let verifier =
            ed25519_dalek::VerifyingKey::from_bytes(public.as_slice().try_into().unwrap()).unwrap();
        let mut message = b"1700000000.".to_vec();
        message.extend_from_slice(raw);
        assert!(verifier.verify_strict(&message, &signature).is_ok());
        message[16] ^= 1;
        assert!(verifier.verify_strict(&message, &signature).is_err());
        let public = serde_json::to_string(&keys.jwk()).unwrap();
        assert!(!public.contains("\"d\""));
        let state = configured("webhook");
        let public = state.subscriptions[PATH].public(PATH, state.keys.as_ref().unwrap());
        assert!(public.get("webhook_secret").is_none());
    }
}
