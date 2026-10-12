//! Bounded external I/O and local restart certificates for cold ownership.
//! The offloader and collector do not hold apply/snapshot locks during remote
//! I/O. Native JSON fork replay still resolves remote ranges under apply; a
//! provider failure fail-stops that replica, never commits a different result.
use super::*;
use crate::blobstore::{BlobStore, BoxFuture, SharedBlobStore};
use bytes::Bytes;
use cold::{Kind, Object};
use sha2::{Digest, Sha256};
use std::io::{Read, Seek, Write};
use std::sync::Mutex;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    pub backend: String,
    pub local_dir: Option<PathBuf>,
    pub bucket: Option<String>,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    #[serde(default)]
    pub allow_http: bool,
    #[serde(default = "segment_bytes")]
    pub segment_bytes: u64,
    #[serde(default = "inventory_interval_ms")]
    pub inventory_interval_ms: u64,
}
fn segment_bytes() -> u64 {
    cold::MAX_SEGMENT
}
fn inventory_interval_ms() -> u64 {
    30_000
}

impl Config {
    pub fn tier(&self, prefix: String) -> io::Result<crate::tier::TierConfig> {
        if !(1024..=cold::MAX_SEGMENT).contains(&self.segment_bytes)
            || self.inventory_interval_ms < 100
        {
            return Err(io::Error::other("invalid cold segment/interval bounds"));
        }
        let kind = match self.backend.as_str() {
            "local"
                if self.local_dir.as_ref().is_some_and(|p| p.is_absolute())
                    && self.bucket.is_none() =>
            {
                crate::tier::TierKind::Local
            }
            "s3" if self.bucket.is_some() && self.local_dir.is_none() => crate::tier::TierKind::S3,
            _ => {
                return Err(io::Error::other(
                    "cold requires absolute local_dir or an S3 bucket",
                ))
            }
        };
        Ok(crate::tier::TierConfig {
            kind,
            consensus_managed: true,
            segment_bytes: self.segment_bytes,
            // Publication already serializes reclamation under committed apply.
            // Pressure/closed tails can be shorter than a segment: retaining
            // those prefixes would prevent admission from ever recovering.
            compact_bytes: 1,
            key_prefix: prefix,
            local_dir: self.local_dir.clone(),
            bucket: self.bucket.clone(),
            endpoint: self.endpoint.clone(),
            region: self.region.clone(),
            allow_http: self.allow_http,
            // Credentials never appear in serialized node configuration/evidence.
            access_key_id: std::env::var("ELECTRIC_COLD_ACCESS_KEY_ID").ok(),
            secret_access_key: std::env::var("ELECTRIC_COLD_SECRET_ACCESS_KEY").ok(),
            ..Default::default()
        })
    }
}

#[derive(Clone)]
pub(super) struct Options {
    pub node: u64,
    pub genesis: Vec<u64>,
    pub tier: crate::tier::TierConfig,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(super) struct Certificate {
    pub floor: u64,
    pub registered: bool,
    pub bootstrap: bool,
}

pub(super) struct Runtime {
    pub options: Options,
    pub pins: Arc<cold::Pins>,
    pub ready: tokio::sync::Notify,
    work: tokio::sync::Mutex<()>,
    counters: Mutex<BTreeMap<&'static str, u64>>,
    certificate: Mutex<Certificate>,
    certificate_path: PathBuf,
}

impl Runtime {
    pub fn open(dir: &std::path::Path, options: Options) -> io::Result<Self> {
        let path = dir.join("cold-certificate");
        let certificate = match std::fs::read(&path) {
            Ok(bytes) => {
                if bytes.len() < 40
                    || &bytes[..8] != b"ERCC0001"
                    || Sha256::digest(&bytes[40..])[..] != bytes[8..40]
                {
                    return Err(io::Error::other(
                        "cold restart certificate checksum/version",
                    ));
                }
                let (node, prefix, certificate): (u64, String, Certificate) =
                    serde_json::from_slice(&bytes[40..]).map_err(io::Error::other)?;
                if node != options.node || prefix != options.tier.key_prefix {
                    return Err(io::Error::other("cold restart certificate identity"));
                }
                certificate
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                // Only genesis identities can start without an online bootstrap
                // authorization. A fresh learner must register before transfer.
                if std::fs::read_dir(dir)?.next().is_some() {
                    return Err(io::Error::other(
                        "existing cold state lacks restart certificate",
                    ));
                }
                Certificate {
                    floor: 0,
                    registered: options.genesis.contains(&options.node),
                    bootstrap: false,
                }
            }
            Err(e) => return Err(e),
        };
        let runtime = Self {
            options,
            pins: Arc::default(),
            ready: tokio::sync::Notify::new(),
            work: tokio::sync::Mutex::new(()),
            counters: Mutex::default(),
            certificate: Mutex::new(certificate),
            certificate_path: path,
        };
        runtime.save(certificate)?;
        Ok(runtime)
    }

    pub fn certificate(&self) -> Certificate {
        *self.certificate.lock().unwrap()
    }

    pub(super) fn count(&self, name: &'static str) {
        self.add(name, 1);
    }

    fn add(&self, name: &'static str, value: u64) {
        *self.counters.lock().unwrap().entry(name).or_default() += value;
    }

    /// The caller owns snapshots -> exclusive view. Complete file and directory
    /// fsync before publishing this certificate locally or through consensus.
    pub fn save(&self, certificate: Certificate) -> io::Result<()> {
        let mut current = self.certificate.lock().unwrap();
        if certificate.floor < current.floor {
            return Err(io::Error::other("cold certificate regression"));
        }
        let bytes = serde_json::to_vec(&(
            self.options.node,
            &self.options.tier.key_prefix,
            certificate,
        ))
        .map_err(io::Error::other)?;
        let temporary = self.certificate_path.with_extension("next");
        let mut file = std::fs::File::create(&temporary)?;
        file.write_all(b"ERCC0001")?;
        file.write_all(&Sha256::digest(&bytes))?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(&temporary, &self.certificate_path)?;
        crate::store::fsync_parent_dir(&self.certificate_path)?;
        *current = certificate;
        Ok(())
    }

    pub fn admits(&self, view: &machine::View) -> bool {
        let certificate = self.certificate();
        certificate.registered
            && view.applied.map_or(0, |id| id.index) >= certificate.floor
            && view
                .cold
                .as_ref()
                .is_some_and(|s| s.participants.contains_key(&self.options.node))
    }
}

/// Verify every payload range before exposing bytes. The immutable object key
/// binds the complete segment digest. Reading a full (<=8 MiB) object to verify
/// a subrange is an explicit cost; native local reads remain fd/sendfile based.
/// Inventories are instead streamed to a local file and verified in full there.
pub(crate) struct VerifiedBlob {
    inner: SharedBlobStore,
    prefix: String,
}
impl VerifiedBlob {
    pub fn new(inner: SharedBlobStore, prefix: String) -> Self {
        Self { inner, prefix }
    }
}

pub(super) fn parse_key<'a>(prefix: &str, key: &'a str) -> Option<(u64, bool, u64, &'a str)> {
    let rest = key.strip_prefix(prefix)?.strip_prefix('/')?;
    let fields: Vec<_> = rest.split('/').collect();
    if fields.len() != 6
        || !matches!(fields[1], "p" | "i")
        || [0, 2, 3, 4]
            .iter()
            .any(|i| fields[*i].len() != 20 || !fields[*i].bytes().all(|b| b.is_ascii_digit()))
        || fields[5].len() != 64
        || !fields[5]
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return None;
    }
    fields[2].parse::<u64>().ok()?;
    fields[3].parse::<u64>().ok()?;
    Some((
        fields[0].parse().ok()?,
        fields[1] == "i",
        fields[4].parse().ok()?,
        fields[5],
    ))
}

impl BlobStore for VerifiedBlob {
    fn put<'a>(&'a self, key: &'a str, body: Bytes) -> BoxFuture<'a, io::Result<()>> {
        self.inner.put(key, body)
    }
    fn put_file<'a>(&'a self, key: &'a str, file: std::fs::File) -> BoxFuture<'a, io::Result<()>> {
        self.inner.put_file(key, file)
    }
    fn head<'a>(&'a self, key: &'a str) -> BoxFuture<'a, io::Result<Option<u64>>> {
        self.inner.head(key)
    }
    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, io::Result<()>> {
        self.inner.delete(key)
    }
    fn list_page<'a>(
        &'a self,
        prefix: &'a str,
        after: &'a str,
        limit: usize,
    ) -> BoxFuture<'a, io::Result<Vec<String>>> {
        self.inner.list_page(prefix, after, limit)
    }
    fn get_range<'a>(
        &'a self,
        key: &'a str,
        start: u64,
        len: u64,
    ) -> BoxFuture<'a, io::Result<Bytes>> {
        Box::pin(async move {
            let (_, inventory, total, hash) =
                parse_key(&self.prefix, key).ok_or_else(|| io::Error::other("invalid cold key"))?;
            if start.checked_add(len).is_none_or(|end| end > total) {
                return Err(io::Error::other("cold range exceeds object"));
            }
            if inventory {
                return self.inner.get_range(key, start, len).await;
            }
            if total > cold::MAX_SEGMENT {
                return Err(io::Error::other("cold object exceeds segment bound"));
            }
            let bytes = self.inner.get_range(key, 0, total).await?;
            if bytes.len() as u64 != total || format!("{:x}", Sha256::digest(&bytes)) != hash {
                return Err(io::Error::other("cold object length/checksum mismatch"));
            }
            Ok(bytes.slice(start as usize..(start + len) as usize))
        })
    }
}

pub(super) fn digest(mut file: &std::fs::File) -> io::Result<String> {
    file.rewind()?;
    let mut hash = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    file.rewind()?;
    Ok(format!("{:x}", hash.finalize()))
}

/// An unlinked local spool is reclaimed even if its owner task is cancelled.
pub(super) fn spool(dir: &std::path::Path) -> io::Result<std::fs::File> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    loop {
        let path = dir.join(format!(
            "cold-spool-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(file) => {
                std::fs::remove_file(path)?;
                return Ok(file);
            }
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => (),
            Err(e) => return Err(e),
        }
    }
}

pub(super) async fn fetch_inventory(
    blob: &SharedBlobStore,
    prefix: &str,
    object: Object,
    dir: &std::path::Path,
) -> io::Result<cold::Inventory> {
    if object.cut().is_none() || !(16..=cold::MAX_INVENTORY).contains(&object.len) {
        return Err(io::Error::other("invalid inventory descriptor"));
    }
    let key = object.key(prefix);
    if blob.head(&key).await? != Some(object.len) {
        return Err(io::Error::other("inventory not present at expected length"));
    }
    let mut file = spool(dir)?;
    let mut at = 0;
    while at < object.len {
        let want = (object.len - at).min(1024 * 1024);
        let bytes = blob.get_range(&key, at, want).await?;
        if bytes.len() as u64 != want {
            return Err(io::Error::other("short inventory read"));
        }
        file.write_all(&bytes)?;
        at += want;
    }
    cold::Inventory::verify(object, file)
}

#[derive(Serialize, Deserialize)]
pub(super) enum Call {
    Status,
    Seal {
        path: String,
    },
    Inventory,
    Certificate {
        floor: u64,
    },
    Bootstrap {
        floor: u64,
    },
    /// Administrative cancellation consumes a Begin, not proof of remote
    /// absence. A delayed issued PUT can still arrive and needs repeated GC.
    Abort {
        birth: u64,
    },
    Resume {
        birth: u64,
    },
}

impl Cluster {
    async fn cold_propose(&self, group: usize, action: cold::Action) -> Result<Reply, u16> {
        let result = self.groups[group]
            .propose(Command {
                method: "COLD".into(),
                path: String::new(),
                headers: vec![],
                body: bincode::serialize(&action).unwrap(),
                time: clock::millis(std::time::SystemTime::now()),
            })
            .await?
            .data;
        if result.status >= 300 {
            return Err(result.status);
        }
        Ok(result)
    }

    async fn cold_call(
        &self,
        group: usize,
        node: &BasicNode,
        call: &Call,
    ) -> Result<serde_json::Value, u16> {
        self.client
            .post(format!("http://{}/_admin/{group}/cold", node.addr))
            .header("x-electric-cluster", &self.config.cluster)
            .json(call)
            .send()
            .await
            .map_err(|_| 503u16)?
            .error_for_status()
            .map_err(|_| 503u16)?
            .json()
            .await
            .map_err(|_| 503)
    }

    pub(super) async fn cold_register(
        &self,
        group: usize,
        id: u64,
        node: &BasicNode,
    ) -> Result<(), u16> {
        let reply = self
            .cold_propose(group, cold::Action::Register { node: id })
            .await?;
        let floor = serde_json::from_slice(&reply.body).map_err(|_| 503u16)?;
        if self
            .cold_call(group, node, &Call::Bootstrap { floor })
            .await?
            != true
        {
            return Err(409);
        }
        // Ensure a learner's first snapshot includes its storage registration.
        // The registration itself already blocks GC if this request is lost.
        let g = &self.groups[group];
        let registered_at = g.machine.view.read().await.applied.unwrap().index;
        g.raft.trigger().snapshot().await.map_err(|_| 503u16)?;
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if g.machine
                    .journal
                    .index
                    .lock()
                    .unwrap()
                    .snapshot
                    .as_ref()
                    .and_then(|s| s.meta.last_log_id)
                    .is_some_and(|id| id.index >= registered_at)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| 503u16)
    }

    async fn cold_certificate(&self, group: usize, floor: u64) -> Result<bool, u16> {
        let g = &self.groups[group];
        // Avoid snapshot/purge work that cannot earn a certificate yet. In
        // particular, do not displace a held snapshot's replay suffix merely
        // to discover its transfer pin at the final certificate check. This
        // is only a preflight: certify_cold still checks pins under the view
        // lock after joining the actual durable snapshot/purge boundary.
        if g.machine
            .cold
            .as_ref()
            .is_some_and(|r| r.pins.blocks(floor))
        {
            return Ok(false);
        }
        let (snapshot, purged) = {
            let index = g.machine.journal.index.lock().unwrap();
            (
                index
                    .snapshot
                    .as_ref()
                    .and_then(|s| s.meta.last_log_id)
                    .map(|i| i.index),
                index.purged.map(|i| i.index),
            )
        };
        if snapshot.is_none_or(|i| i < floor) {
            g.raft.trigger().snapshot().await.map_err(|_| 503u16)?;
            return Ok(false);
        }
        if purged.is_none_or(|i| i < floor) {
            g.raft
                .trigger()
                .purge_log(floor)
                .await
                .map_err(|_| 503u16)?;
            return Ok(false);
        }
        g.machine.certify_cold(floor).await.map_err(|_| 503)
    }

    pub(super) async fn cold_control(&self, group: usize, req: Req) -> Resp {
        let g = &self.groups[group];
        let Some(runtime) = &g.machine.cold else {
            return response(409, "cold storage disabled");
        };
        let Ok(call) = serde_json::from_slice::<Call>(&req.body) else {
            return response(400, "invalid cold control");
        };
        match call {
            Call::Status => {
                let view = g.machine.view.read().await;
                let state = view.cold.as_ref().unwrap();
                let objects: Vec<_> = state.live.values().take(128).map(|o| serde_json::json!({
                    "birth":o.birth,"key":o.key(&runtime.options.tier.key_prefix),"len":o.len,
                })).collect();
                let local_bytes: u64 = view
                    .store
                    .streams
                    .iter()
                    .map(|s| {
                        let shared = s.shared.read().unwrap();
                        shared.tail.saturating_sub(shared.file_base)
                    })
                    .sum();
                let eligible: u64 = view.store.streams.iter().map(|s| {
                    let sealed = s.tier.manifest.lock().unwrap().sealed_offset.max(s.base_offset);
                    let tail = s.tail();
                    let bytes = tail.bytes.saturating_sub(sealed);
                    if tail.closed || bytes >= runtime.options.tier.segment_bytes { bytes } else { 0 }
                }).sum();
                json(
                    &serde_json::json!({"version":1,"applied":view.applied.map(|i|i.index),
                    "floor":state.floor(),"participants":state.participants,"pending":state.pending,
                    "inventory":state.inventory,"objects":objects,"local_payload_bytes":local_bytes,
                    "live_objects":state.live.len(),"objects_truncated":state.live.len()>128,
                    "eligible_payload_bytes":eligible,"worker_busy":runtime.work.try_lock().is_err(),
                    "pending_bytes":state.pending.values().map(|o|o.len).sum::<u64>(),
                    "oldest_pin":runtime.pins.oldest(),
                    "certificate":runtime.certificate(),"counters":*runtime.counters.lock().unwrap()}),
                )
            }
            Call::Certificate { floor } => match self.cold_certificate(group, floor).await {
                Ok(done) => json(&done),
                Err(status) => response(status, "cold certificate unavailable"),
            },
            Call::Bootstrap { floor } => match g.machine.bootstrap_cold(floor).await {
                Ok(done) => json(&done),
                Err(_) => response(503, "cold bootstrap unavailable"),
            },
            Call::Abort { birth } => match self
                .cold_propose(group, cold::Action::Abort { birth })
                .await
            {
                Ok(reply) => reply.into_resp(),
                Err(status) => response(status, "cold abort outcome unknown"),
            },
            Call::Seal { path } => {
                let _work = runtime.work.lock().await;
                match self.cold_seal(group, &path, true).await {
                    Ok(done) => json(&done),
                    Err(_) => response(503, "cold seal outcome unknown; inspect pending Begins"),
                }
            }
            Call::Inventory => {
                let _work = runtime.work.lock().await;
                match self.cold_inventory(group).await {
                    Ok(()) => json(&true),
                    Err(_) => response(503, "cold inventory pending or unavailable"),
                }
            }
            Call::Resume { birth } => {
                let _work = runtime.work.lock().await;
                let object = g
                    .machine
                    .view
                    .read()
                    .await
                    .cold
                    .as_ref()
                    .unwrap()
                    .pending
                    .get(&birth)
                    .cloned();
                let Some(object) = object else {
                    return response(409, "no pending Begin; retired identities cannot reattach");
                };
                match self.cold_finish(group, object, None).await {
                    Ok(()) => json(&true),
                    Err(_) => response(503, "cold publication remains pending"),
                }
            }
        }
    }

    /// Capture native bytes from a pinned descriptor. No network I/O while the
    /// view is held, and never a payload copy in the replicated metadata log.
    async fn cold_payload(
        &self,
        group: usize,
        path: &str,
        force: bool,
        pending: Option<&Object>,
    ) -> io::Result<Option<(Kind, Bytes)>> {
        let g = &self.groups[group];
        let view = g.machine.view.read().await;
        let runtime = g.machine.cold.as_ref().unwrap();
        if !runtime.admits(&view) {
            return Err(io::Error::other("cold participant not ready"));
        }
        let Some(st) = view.store.get(path) else {
            return Ok(None);
        };
        let start = st
            .tier
            .manifest
            .lock()
            .unwrap()
            .sealed_offset
            .max(st.base_offset);
        let kind = Kind::Payload {
            path: path.into(),
            incarnation: st.id,
            start,
        };
        if pending.is_some_and(|p| p.kind != kind) {
            return Ok(None);
        }
        let (file, base, tail) = {
            let shared = st.shared.read().unwrap();
            (shared.file.clone(), shared.file_base, shared.durable_tail)
        };
        let available = tail.saturating_sub(start);
        if available == 0
            || (!force && !st.tail().closed && available < runtime.options.tier.segment_bytes)
        {
            return Ok(None);
        }
        if start < base {
            return Err(io::Error::other("cold payload below native file base"));
        }
        // A retry after a short forced seal or concurrent appends must use the
        // original immutable range, not recalculate a longer segment boundary.
        let mut want = pending.map_or_else(
            || available.min(runtime.options.tier.segment_bytes),
            |p| p.len,
        );
        if want > available {
            return Err(io::Error::other("pending cold payload beyond durable tail"));
        }
        let bytes = tokio::task::block_in_place(|| -> io::Result<Bytes> {
            use std::os::unix::fs::FileExt;
            loop {
                let mut bytes = vec![0; want as usize];
                file.read_exact_at(&mut bytes, start - base)?;
                let cut = if st.is_json && pending.is_none() {
                    crate::tier::last_json_value_boundary(&bytes, want)
                } else {
                    want
                };
                if cut > 0 {
                    bytes.truncate(cut as usize);
                    return Ok(bytes.into());
                }
                let next = (want * 2).min(available).min(cold::MAX_SEGMENT);
                if next <= want {
                    return Err(io::Error::other(
                        "no JSON value boundary within cold segment bound",
                    ));
                }
                want = next;
            }
        })?;
        Ok(Some((kind, bytes)))
    }

    async fn cold_seal(&self, group: usize, path: &str, force: bool) -> io::Result<bool> {
        let Some((kind, bytes)) = self.cold_payload(group, path, force, None).await? else {
            return Ok(false);
        };
        let reply = self
            .cold_propose(
                group,
                cold::Action::Begin {
                    kind,
                    len: bytes.len() as u64,
                    sha256: format!("{:x}", Sha256::digest(&bytes)),
                },
            )
            .await
            .map_err(|s| io::Error::other(format!("cold Begin rejected: {s}")))?;
        let object = serde_json::from_slice(&reply.body).map_err(io::Error::other)?;
        self.cold_finish(group, object, Some(bytes)).await?;
        Ok(true)
    }

    async fn cold_finish(
        &self,
        group: usize,
        object: Object,
        bytes: Option<Bytes>,
    ) -> io::Result<()> {
        let g = &self.groups[group];
        let runtime = g.machine.cold.as_ref().unwrap();
        let blob = g.machine.view.read().await.store.blobstore.clone().unwrap();
        let key = object.key(&runtime.options.tier.key_prefix);
        if let Kind::Payload {
            path, incarnation, ..
        } = &object.kind
        {
            let view = g.machine.view.read().await;
            let retired = view.store.get(path).is_none_or(|s| s.id != *incarnation);
            drop(view);
            if retired {
                self.cold_propose(
                    group,
                    cold::Action::Abort {
                        birth: object.birth,
                    },
                )
                .await
                .map_err(|s| io::Error::other(format!("retired source Abort unavailable: {s}")))?;
                return Ok(());
            }
        }
        match blob.head(&key).await? {
            Some(len) if len == object.len => (),
            Some(_) => return Err(io::Error::other("cold immutable object length mismatch")),
            None => match &object.kind {
                Kind::Inventory { .. } => {
                    // Its capture is not reconstructible from a newer cut.
                    // Abort is safe even if an older unknown PUT arrives later.
                    self.cold_propose(
                        group,
                        cold::Action::Abort {
                            birth: object.birth,
                        },
                    )
                    .await
                    .map_err(|s| io::Error::other(format!("cold Abort unavailable: {s}")))?;
                    return Ok(());
                }
                Kind::Payload { path, .. } => {
                    let bytes = if let Some(bytes) = bytes {
                        bytes
                    } else {
                        let Some((_, bytes)) =
                            self.cold_payload(group, path, true, Some(&object)).await?
                        else {
                            self.cold_propose(
                                group,
                                cold::Action::Abort {
                                    birth: object.birth,
                                },
                            )
                            .await
                            .map_err(|s| {
                                io::Error::other(format!("cold Abort unavailable: {s}"))
                            })?;
                            return Ok(());
                        };
                        bytes
                    };
                    if bytes.len() as u64 != object.len
                        || format!("{:x}", Sha256::digest(&bytes)) != object.sha256
                    {
                        return Err(io::Error::other("pending cold bytes changed"));
                    }
                    runtime.count("put_attempts");
                    runtime.add("put_attempt_bytes", object.len);
                    blob.put(&key, bytes).await?;
                }
            },
        }
        if blob.head(&key).await? != Some(object.len) {
            return Err(io::Error::other("cold PUT not verifiable"));
        }
        match object.kind {
            Kind::Payload { .. } => {
                let bytes = blob.get_range(&key, 0, object.len).await?;
                if bytes.len() as u64 != object.len
                    || format!("{:x}", Sha256::digest(&bytes)) != object.sha256
                {
                    return Err(io::Error::other("cold PUT checksum mismatch"));
                }
            }
            Kind::Inventory { cut } => {
                fetch_inventory(
                    &blob,
                    &runtime.options.tier.key_prefix,
                    object.clone(),
                    &g.machine.dir,
                )
                .await?;
                let participants = g
                    .machine
                    .view
                    .read()
                    .await
                    .cold
                    .as_ref()
                    .unwrap()
                    .participants
                    .clone();
                let members = g.raft.metrics().borrow_watched().membership_config.clone();
                let mut blocked = false;
                for (node, floor) in participants {
                    if floor >= cut {
                        continue;
                    }
                    let done = if node == self.config.node {
                        self.cold_certificate(group, cut).await.unwrap_or(false)
                    } else if let Some(peer) = members.membership().get_node(&node) {
                        self.cold_call(group, peer, &Call::Certificate { floor: cut })
                            .await
                            .ok()
                            == Some(serde_json::json!(true))
                    } else {
                        false
                    };
                    if done {
                        self.cold_propose(group, cold::Action::Certify { node, floor: cut })
                            .await
                            .map_err(|s| {
                                io::Error::other(format!(
                                    "cold certificate publication unavailable: {s}"
                                ))
                            })?;
                    } else {
                        runtime.count("certificate_blocked");
                        blocked = true;
                    }
                }
                if blocked {
                    return Err(io::Error::other(
                        "inventory waiting for participant certificates",
                    ));
                }
            }
        }
        self.cold_propose(
            group,
            cold::Action::Publish {
                birth: object.birth,
            },
        )
        .await
        .map_err(|s| io::Error::other(format!("cold publication unavailable: {s}")))?;
        runtime.count("publications");
        if matches!(object.kind, Kind::Payload { .. }) {
            runtime.add("published_payload_bytes", object.len);
        }
        Ok(())
    }

    async fn cold_inventory(&self, group: usize) -> io::Result<()> {
        let g = &self.groups[group];
        let runtime = g.machine.cold.as_ref().unwrap();
        let mut file = spool(&g.machine.dir)?;
        let (cut, blob) = {
            let view = g.machine.view.read().await;
            let state = view.cold.as_ref().unwrap();
            if let Some(object) = state.pending.values().find(|o| o.cut().is_some()).cloned() {
                drop(view);
                return self.cold_finish(group, object, None).await;
            }
            let cut = view
                .applied
                .ok_or_else(|| io::Error::other("cold group uninitialized"))?
                .index;
            tokio::task::block_in_place(|| {
                state.capture(&runtime.options.tier.key_prefix, cut, &mut file)
            })?;
            (cut, view.store.blobstore.clone().unwrap())
        };
        let reply = self
            .cold_propose(
                group,
                cold::Action::Begin {
                    kind: Kind::Inventory { cut },
                    len: file.metadata()?.len(),
                    sha256: digest(&file)?,
                },
            )
            .await
            .map_err(|s| io::Error::other(format!("inventory Begin rejected: {s}")))?;
        let object: Object = serde_json::from_slice(&reply.body).map_err(io::Error::other)?;
        runtime.count("inventory_put_attempts");
        blob.put_file(&object.key(&runtime.options.tier.key_prefix), file)
            .await?;
        self.cold_finish(group, object, None).await
    }

    pub(super) async fn cold_worker(&self, group: usize) {
        let g = &self.groups[group];
        let runtime = g.machine.cold.as_ref().unwrap();
        let interval =
            Duration::from_millis(self.config.cold.as_ref().unwrap().inventory_interval_ms);
        let mut inventory_at = tokio::time::Instant::now() + interval;
        let mut last_stream = String::new();
        let mut last_pending = 0;
        loop {
            // Register before inspecting state; apply notifications coalesce.
            // The fallback also finds inherited work after leadership changes.
            let ready = runtime.ready.notified();
            tokio::pin!(ready);
            ready.as_mut().enable();
            if g.raft.metrics().borrow_watched().current_leader != Some(self.config.node) {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            let _work = runtime.work.lock().await;
            runtime.count("work_passes");
            let mut productive = false;
            let mut failed = false;
            let (pending, retired) = {
                let view = g.machine.view.read().await;
                let state = view.cold.as_ref().unwrap();
                let pending = state
                    .pending
                    .range((
                        std::ops::Bound::Excluded(last_pending),
                        std::ops::Bound::Unbounded,
                    ))
                    .next()
                    .or_else(|| state.pending.first_key_value())
                    .map(|(_, o)| o.clone());
                let retired = state
                    .live
                    .values()
                    .find(|o| match &o.kind {
                        Kind::Payload {
                            path, incarnation, ..
                        } => view.store.streams.get(path).is_none_or(|s| {
                            s.id != *incarnation
                                || (s.is_expired() && s.shared.read().unwrap().ref_count == 0)
                        }),
                        _ => false,
                    })
                    .map(|o| o.birth);
                (pending, retired)
            };
            // A blocked inventory/offline participant must not starve payload
            // publication or retirement. Each class gets bounded work per pass.
            if let Some(object) = pending {
                last_pending = object.birth;
                if self.cold_finish(group, object, None).await.is_err() {
                    runtime.count("resume_incomplete");
                    failed = true;
                } else {
                    productive = true;
                }
            }
            if let Some(birth) = retired {
                if self
                    .cold_propose(group, cold::Action::Retire { birth })
                    .await
                    .is_ok()
                {
                    runtime.count("retirements");
                    productive = true;
                } else {
                    runtime.count("retire_rejected");
                    failed = true;
                }
            }
            if tokio::time::Instant::now() >= inventory_at {
                inventory_at = tokio::time::Instant::now() + interval;
                if self.cold_inventory(group).await.is_err() {
                    runtime.count("inventory_incomplete");
                }
            }
            // Constant queued/in-flight work: select from authoritative state,
            // upload sequentially, and release the work lock after 16 attempts.
            // No reset-only turn at the end of the catalog, even for one stream.
            for _ in 0..16 {
                // Many partial tails can fill the budget without any full
                // segment. Force short seals before that becomes a deadlock.
                let force = self.cold_admission.as_ref().is_some_and(|budget| budget.pressure());
                let path = {
                    let view = g.machine.view.read().await;
                    let state = view.cold.as_ref().unwrap();
                    view.store.streams.iter().filter(|s| {
                        if state.pending.values().any(
                            |o| matches!(&o.kind, Kind::Payload { path, .. } if path == &s.path),
                        ) { return false; }
                        let sealed = s.tier.manifest.lock().unwrap().sealed_offset.max(s.base_offset);
                        let tail = s.tail();
                        tail.bytes > sealed && (force || tail.closed || tail.bytes - sealed >= runtime.options.tier.segment_bytes)
                    }).map(|s| s.path.clone())
                        .min_by_key(|path| (path <= &last_stream, path.clone()))
                };
                let Some(path) = path else { break; };
                last_stream = path.clone();
                runtime.count("payload_scheduled");
                if force { runtime.count("pressure_seals"); }
                match self.cold_seal(group, &path, force).await {
                    Ok(done) => productive |= done,
                    Err(_) => { runtime.count("seal_incomplete"); failed = true; }
                }
                if g.raft.metrics().borrow_watched().current_leader != Some(self.config.node) { break; }
            }
            drop(_work);
            if failed {
                // Our own Begin/apply wakeups must not bypass outage backoff.
                runtime.count("work_retry_waits");
                tokio::time::sleep(Duration::from_millis(100)).await;
            } else if productive {
                tokio::task::yield_now().await;
            } else {
                runtime.count("work_idle_waits");
                tokio::select! {
                    _ = ready => (),
                    _ = tokio::time::sleep(Duration::from_millis(100)) => (),
                }
            }
        }
    }

    pub(super) async fn cold_gc_worker(&self, group: usize) {
        let g = &self.groups[group];
        let runtime = g.machine.cold.as_ref().unwrap();
        let prefix = &runtime.options.tier.key_prefix;
        let mut round: Option<(cold::Inventory, String)> = None;
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if g.raft.metrics().borrow_watched().current_leader != Some(self.config.node) {
                continue;
            }
            let (blob, active, pin) = {
                let view = g.machine.view.read().await;
                (
                    view.store.blobstore.clone().unwrap(),
                    view.cold.as_ref().unwrap().inventory.clone(),
                    runtime.pins.read(view.applied.map_or(0, |id| id.index)),
                )
            };
            if round.is_none() {
                let Some(active) = active else {
                    continue;
                };
                match fetch_inventory(&blob, prefix, active, &g.machine.dir).await {
                    Ok(inventory) => round = Some((inventory, String::new())),
                    Err(_) => {
                        runtime.count("inventory_read_errors");
                        continue;
                    }
                }
            }
            drop(pin); // Complete local verified file, independent of remote GC.
            let (inventory, after) = round.as_mut().unwrap();
            let page = match blob.list_page(prefix, after, 128).await {
                Ok(page) => page,
                Err(_) => {
                    runtime.count("list_errors");
                    continue;
                }
            };
            let mut done = page.is_empty();
            for key in page {
                let Some((birth, _, _, _)) = parse_key(prefix, &key) else {
                    runtime.count("foreign_keys");
                    *after = key;
                    continue;
                };
                if birth > inventory.object.cut().unwrap() {
                    done = true;
                    break;
                }
                runtime.count("scanned");
                match inventory.protects(birth, &key) {
                    Ok(false) => {
                        runtime.count("delete_attempts");
                        if blob.delete(&key).await.is_err() {
                            runtime.count("delete_unknown");
                            break;
                        }
                        runtime.count("delete_completed");
                    }
                    Ok(true) => (),
                    Err(_) => {
                        runtime.count("inventory_local_errors");
                        break;
                    }
                }
                *after = key;
            }
            if done {
                runtime.count("scan_rounds");
                round = None; // Always wrap over the cumulative birth prefix.
            }
        }
    }
}
