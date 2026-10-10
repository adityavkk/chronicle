//! Consensus-owned cold references. `ColdOwnership.tla` is the protocol model;
//! the native manifest is a read projection, never an independent GC authority.
use super::*;
use sha2::{Digest, Sha256};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Mutex;

pub(super) const MAX_PENDING: usize = 64;
pub(super) const MAX_PARTICIPANTS: usize = 64;
pub(crate) const MAX_SEGMENT: u64 = 8 * 1024 * 1024;
pub(super) const MAX_INVENTORY: u64 = 256 * 1024 * 1024;
const INVENTORY_HEADER: &[u8; 8] = b"ERIC0001";
const RECORD: u64 = 40;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum Kind {
    Payload {
        path: String,
        incarnation: u64,
        start: u64,
    },
    Inventory {
        cut: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Object {
    pub birth: u64,
    pub kind: Kind,
    pub len: u64,
    pub sha256: String,
}
impl Object {
    pub fn key(&self, prefix: &str) -> String {
        let (kind, incarnation, start) = match &self.kind {
            Kind::Payload {
                incarnation, start, ..
            } => ("p", *incarnation, *start),
            Kind::Inventory { cut } => ("i", 0, *cut),
        };
        format!(
            "{prefix}/{:020}/{kind}/{incarnation:020}/{start:020}/{:020}/{}",
            self.birth, self.len, self.sha256
        )
    }
    pub fn cut(&self) -> Option<u64> {
        match self.kind {
            Kind::Inventory { cut } => Some(cut),
            _ => None,
        }
    }
}

/// Only live descriptors and admitted Begins are retained, not a tombstone for
/// every past object. The high node ID fences identity reuse after drainage.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(super) struct State {
    pub pending: BTreeMap<u64, Object>,
    pub live: BTreeMap<u64, Object>,
    pub inventory: Option<Object>,
    pub participants: BTreeMap<u64, u64>,
    pub max_node: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) enum Action {
    Begin {
        kind: Kind,
        len: u64,
        sha256: String,
    },
    Publish {
        birth: u64,
    },
    Abort {
        birth: u64,
    },
    Retire {
        birth: u64,
    },
    Register {
        node: u64,
    },
    Certify {
        node: u64,
        floor: u64,
    },
}

impl State {
    pub fn genesis(nodes: impl Iterator<Item = u64>) -> Self {
        let participants: BTreeMap<_, _> = nodes.map(|n| (n, 0)).collect();
        Self {
            max_node: participants.keys().next_back().copied().unwrap_or(0),
            participants,
            ..Self::default()
        }
    }

    pub fn floor(&self) -> u64 {
        self.inventory.as_ref().and_then(Object::cut).unwrap_or(0)
    }

    /// Called only under committed apply, after the native-WAL publication
    /// marker. Publication has no remote I/O: the worker verifies immutable PUT
    /// bytes before submitting Publish. Provider honesty/durability is an
    /// explicit assumption, not something Raft can establish by itself.
    pub async fn apply(
        &mut self,
        store: &Arc<Store>,
        prefix: &str,
        index: u64,
        action: Action,
    ) -> io::Result<Resp> {
        match action {
            Action::Begin { kind, len, sha256 } => {
                if self.pending.len() >= MAX_PENDING {
                    return Ok(response(429, "cold Begin bound reached"));
                }
                if sha256.len() != 64
                    || !sha256
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                {
                    return Ok(response(400, "invalid cold checksum"));
                }
                match &kind {
                    Kind::Payload {
                        path,
                        incarnation,
                        start,
                    } => {
                        if len == 0 || len > MAX_SEGMENT || start.checked_add(len).is_none() {
                            return Ok(response(400, "invalid cold range"));
                        }
                        let Some(st) = store.streams.get(path).map(|s| s.clone()) else {
                            return Ok(response(409, "cold source retired"));
                        };
                        if st.id != *incarnation
                            || st.tail().bytes < start + len
                            || st
                                .tier
                                .manifest
                                .lock()
                                .unwrap()
                                .sealed_offset
                                .max(st.base_offset)
                                != *start
                            || self.pending.values().any(|o| o.kind == kind)
                        {
                            return Ok(response(409, "cold range or incarnation changed"));
                        }
                    }
                    Kind::Inventory { cut } => {
                        if *cut >= index
                            || *cut <= self.floor()
                            || !(16..=MAX_INVENTORY).contains(&len)
                            || (len - 16) % RECORD != 0
                        {
                            return Ok(response(409, "invalid inventory cut or length"));
                        }
                        if self.pending.values().any(|o| o.cut().is_some()) {
                            return Ok(response(409, "inventory Begin already pending"));
                        }
                    }
                }
                let object = Object {
                    birth: index,
                    kind,
                    len,
                    sha256,
                };
                self.pending.insert(index, object.clone());
                Ok(json(&object))
            }
            Action::Abort { birth } => {
                // An old retry cannot recreate the descriptor it consumes.
                self.pending.remove(&birth);
                Ok(Resp::new(204))
            }
            Action::Publish { birth } => {
                let Some(object) = self.pending.get(&birth).cloned() else {
                    return Ok(response(409, "no matching pending cold Begin"));
                };
                match &object.kind {
                    Kind::Payload {
                        path,
                        incarnation,
                        start,
                    } => {
                        let Some(st) = store.streams.get(path).map(|s| s.clone()) else {
                            return Ok(response(409, "cold source retired"));
                        };
                        if st.id != *incarnation || st.tail().bytes < start + object.len {
                            return Ok(response(409, "cold source incarnation changed"));
                        }
                        {
                            let mut manifest = st.tier.manifest.lock().unwrap();
                            if manifest.sealed_offset.max(st.base_offset) != *start {
                                return Ok(response(409, "cold publication is not contiguous"));
                            }
                            manifest.segments.push(crate::tier::SegmentEntry {
                                logical_start: *start,
                                len: object.len,
                                placement: crate::tier::Placement::Remote(object.key(prefix)),
                                remote: true,
                            });
                            manifest.sealed_offset = start + object.len;
                        }
                        self.live.insert(birth, object);
                        self.pending.remove(&birth);
                        // This is a local materialization change under apply,
                        // never an independent offloader or second payload WAL.
                        store.maybe_compact(&st).await?;
                    }
                    Kind::Inventory { cut } => {
                        if self.participants.is_empty()
                            || self.participants.values().any(|f| f < cut)
                        {
                            return Ok(response(
                                409,
                                "storage participant has not certified inventory cut",
                            ));
                        }
                        self.inventory = Some(object);
                        self.pending.remove(&birth);
                    }
                }
                Ok(Resp::new(204))
            }
            Action::Retire { birth } => {
                if let Some(Object {
                    kind:
                        Kind::Payload {
                            path, incarnation, ..
                        },
                    ..
                }) = self.live.get(&birth)
                {
                    // Lazy native expiry may have happened on just one replica.
                    // Resolve it against the committed clock on EVERY replica;
                    // preserve soft-deleted ancestors/grants with live children.
                    store.get(path);
                    if store
                        .streams
                        .get(path)
                        .is_some_and(|s| s.id == *incarnation)
                    {
                        return Ok(response(409, "cold object still has a native owner"));
                    }
                }
                self.live.remove(&birth);
                Ok(Resp::new(204))
            }
            Action::Register { node } => {
                if let Some(floor) = self.participants.get(&node) {
                    return Ok(json(floor));
                }
                if node <= self.max_node {
                    return Ok(response(409, "storage identity cannot be reused"));
                }
                if self.participants.len() >= MAX_PARTICIPANTS {
                    return Ok(response(429, "storage participant bound reached"));
                }
                let floor = self.floor();
                self.participants.insert(node, floor);
                self.max_node = node;
                Ok(json(&floor))
            }
            Action::Certify { node, floor } => {
                let Some(previous) = self.participants.get_mut(&node) else {
                    return Ok(response(409, "unregistered storage participant"));
                };
                if floor >= index {
                    return Ok(response(400, "certificate exceeds committed prefix"));
                }
                *previous = (*previous).max(floor);
                Ok(Resp::new(204))
            }
        }
    }

    /// Exact-cut immutable inventory. Both maps are birth-ordered and disjoint;
    /// merge them without a whole-inventory allocation or a history-sized sort.
    /// Caller holds the view lock through capture; subsequent retirement cannot
    /// change these bytes. The inventory's own Begin is allocated afterwards.
    pub fn capture(&self, prefix: &str, cut: u64, file: &mut std::fs::File) -> io::Result<()> {
        let records = self.live.len() + self.pending.len() + usize::from(self.inventory.is_some());
        if records as u64 > (MAX_INVENTORY - 16) / RECORD {
            return Err(io::Error::other("inventory resource bound reached"));
        }
        let mut out = io::BufWriter::new(file);
        out.write_all(INVENTORY_HEADER)?;
        out.write_all(&cut.to_be_bytes())?;
        let mut live = self.live.values().peekable();
        let mut pending = self.pending.values().peekable();
        let mut root = self.inventory.as_ref();
        loop {
            let next = [live.peek().copied(), pending.peek().copied(), root]
                .into_iter()
                .flatten()
                .min_by_key(|o| o.birth);
            let Some(object) = next else { break };
            if object.birth > cut {
                return Err(io::Error::other("inventory object newer than cut"));
            }
            out.write_all(&object.birth.to_be_bytes())?;
            out.write_all(&Sha256::digest(object.key(prefix).as_bytes()))?;
            if root.is_some_and(|o| o.birth == object.birth) {
                root = None;
            } else if live.peek().is_some_and(|o| o.birth == object.birth) {
                live.next();
            } else {
                pending.next();
            }
        }
        out.flush()
    }
}

/// A complete, hash-verified local inventory is independent of deletion of its
/// remote source. Fixed-width sorted records permit bounded-memory membership
/// queries; an invalid/partial inventory is never treated as empty.
pub(super) struct Inventory {
    pub object: Object,
    file: std::fs::File,
}
impl Inventory {
    pub fn verify(object: Object, mut file: std::fs::File) -> io::Result<Self> {
        let len = file.metadata()?.len();
        if object.cut().is_none()
            || len != object.len
            || !(16..=MAX_INVENTORY).contains(&len)
            || (len - 16) % RECORD != 0
        {
            return Err(io::Error::other("invalid inventory size"));
        }
        file.rewind()?;
        let mut hash = Sha256::new();
        let mut header = [0; 16];
        file.read_exact(&mut header)?;
        hash.update(header);
        if &header[..8] != INVENTORY_HEADER
            || u64::from_be_bytes(header[8..].try_into().unwrap()) != object.cut().unwrap()
        {
            return Err(io::Error::other("inventory cut/version mismatch"));
        }
        let mut previous = None;
        for _ in 0..(len - 16) / RECORD {
            let mut record = [0; RECORD as usize];
            file.read_exact(&mut record)?;
            hash.update(record);
            let birth = u64::from_be_bytes(record[..8].try_into().unwrap());
            if previous.is_some_and(|p| p >= birth) || birth > object.cut().unwrap() {
                return Err(io::Error::other("inventory birth order/cut mismatch"));
            }
            previous = Some(birth);
        }
        if format!("{:x}", hash.finalize()) != object.sha256 {
            return Err(io::Error::other("inventory checksum mismatch"));
        }
        Ok(Self { object, file })
    }

    pub fn protects(&mut self, birth: u64, key: &str) -> io::Result<bool> {
        let (mut lo, mut hi) = (0, (self.object.len - 16) / RECORD);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            self.file.seek(SeekFrom::Start(16 + mid * RECORD))?;
            let mut record = [0; RECORD as usize];
            self.file.read_exact(&mut record)?;
            let found = u64::from_be_bytes(record[..8].try_into().unwrap());
            match found.cmp(&birth) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => {
                    return Ok(record[8..] == Sha256::digest(key.as_bytes())[..])
                }
            }
        }
        Ok(false)
    }
}

/// Pins are admitted under the machine view lock; a certificate takes its
/// exclusive side. A pin covers the complete lazy response, not just lookup.
#[derive(Default)]
pub(crate) struct Pins(Mutex<BTreeMap<u64, usize>>);
pub(crate) struct Pin {
    pins: Arc<Pins>,
    at: u64,
}
impl Pins {
    pub fn read(self: &Arc<Self>, at: u64) -> Arc<Pin> {
        *self.0.lock().unwrap().entry(at).or_default() += 1;
        Arc::new(Pin {
            pins: self.clone(),
            at,
        })
    }
    pub fn oldest(&self) -> Option<u64> {
        self.0.lock().unwrap().first_key_value().map(|(at, _)| *at)
    }
    pub fn blocks(&self, cut: u64) -> bool {
        self.oldest().is_some_and(|at| at < cut)
    }
}
impl Drop for Pin {
    fn drop(&mut self) {
        let mut pins = self.pins.0.lock().unwrap();
        let count = pins.get_mut(&self.at).unwrap();
        *count -= 1;
        if *count == 0 {
            pins.remove(&self.at);
        }
    }
}

/// OpenRaft owns this descriptor through the final snapshot RPC response or
/// cancellation. The receiver's durable registration protects a transfer even
/// if the sender crashes and loses this volatile pin. Incoming handles have no
/// pin; install must check the receiver's durable floor before materialization.
pub struct SnapshotFile {
    pub file: tokio::fs::File,
    pub(crate) _pin: Option<Arc<Pin>>,
}
impl tokio::io::AsyncRead for SnapshotFile {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.file).poll_read(cx, buf)
    }
}
impl tokio::io::AsyncWrite for SnapshotFile {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        std::pin::Pin::new(&mut self.file).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.file).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.file).poll_shutdown(cx)
    }
}
impl tokio::io::AsyncSeek for SnapshotFile {
    fn start_seek(mut self: std::pin::Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        std::pin::Pin::new(&mut self.file).start_seek(position)
    }
    fn poll_complete(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<u64>> {
        std::pin::Pin::new(&mut self.file).poll_complete(cx)
    }
}

pub(super) fn protect_response(resp: &mut Resp, pin: Option<Arc<Pin>>) {
    let Some(pin) = pin else { return };
    match &mut resp.body {
        Body::Channel(body) => body.cold_pin = Some(pin),
        Body::Sse(_) => {
            let Body::Sse(source) = std::mem::replace(&mut resp.body, Body::Empty) else {
                unreachable!()
            };
            resp.body = Body::Sse(Box::new(PinnedEvents { source, _pin: pin }));
        }
        // Full bytes and opened local descriptors survive remote deletion.
        _ => (),
    }
}
struct PinnedEvents {
    source: Box<dyn crate::api::EventSource>,
    _pin: Arc<Pin>,
}
impl crate::api::EventSource for PinnedEvents {
    fn next_chunk(&mut self) -> crate::blobstore::BoxFuture<'_, Option<bytes::Bytes>> {
        self.source.next_chunk()
    }
    // Native cold sources already use inline SSE, not the hot-only reactor.
}
