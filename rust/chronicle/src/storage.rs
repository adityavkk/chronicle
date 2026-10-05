//! Durable SQLite implementation of OpenRaft's storage-v2 contracts.
//!
//! Every operation is submitted to one bounded blocking actor.  This is intentional: rusqlite is
//! synchronous, and Raft requires vote and log writes to be ordered even when their futures are
//! cancelled.  A request already admitted to the actor is therefore always completed.

use std::fmt::Debug;
use std::io::{Cursor, Seek, SeekFrom};
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use crate::{Entry, LogId, Membership as StoredMembership, Snapshot, SnapshotMeta, Vote};
use futures_util::{Stream, StreamExt};
use openraft::storage::{EntryResponder, IOFlushed, RaftLogStorage, RaftStateMachine};
use openraft::{EntryPayload, LogState, RaftLogReader, RaftSnapshotBuilder};
use rusqlite::{Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, oneshot};

use crate::{TypeConfig, metrics::Histogram, model};

type Result<T> = std::io::Result<T>;
type Job = Box<dyn FnOnce(&mut Worker) + Send>;
const QUEUE_DEPTH: usize = 128;
const APPLY_BATCH_SIZE: usize = 128;
const SNAPSHOT_CHECKSUM_BYTES: usize = 32;
/// Maximum encoded snapshot size, enforced during receipt and before persistence.
pub const MAX_SNAPSHOT_BYTES: usize = 256 * 1024 * 1024;
static QUEUE: Histogram = Histogram::new();
static PERSIST: Histogram = Histogram::new();
static APPLY: Histogram = Histogram::new();

pub fn timing_metrics(text: &mut String) {
    QUEUE.render("chronicle_storage_queue_duration_seconds", text);
    PERSIST.render("chronicle_storage_persist_duration_seconds", text);
    APPLY.render("chronicle_storage_apply_duration_seconds", text);
}

#[derive(Clone)]
pub struct SqliteStore {
    tx: mpsc::Sender<Job>,
    stopped: tokio::sync::watch::Receiver<bool>,
    applied: tokio::sync::watch::Sender<()>,
    busy_us: Arc<AtomicU64>,
    instance: u64,
}

#[derive(Serialize, Deserialize)]
struct SnapshotBody {
    state: model::State,
    last_applied: Option<LogId>,
    membership: StoredMembership,
}

struct Worker {
    db: Connection,
    state: model::State,
    projection: crate::projection::Cache,
    read_generation: Arc<()>,
    #[cfg(feature = "storage-faults")]
    faults: crate::faults::Context,
    // SQLite transaction locks do not fence two independent cached Raft state machines.
    // Keep this OS lock for the entire actor lifetime, including shutdown.
    _process_lock: std::fs::File,
}

/// A committed metadata view, without cloning payload or producer results.
pub struct StreamInfo {
    generation: Arc<()>,
    pub incarnation: u64,
    pub config: model::StreamConfig,
    pub end: u64,
    pub closed: bool,
    pub deleted: bool,
    pub access_ms: u64,
    pub fork_pending: bool,
    pub soft_deleted: bool,
    pub fork_sequence: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    #[error("stream changed while opening range; retry read")]
    Changed,
    #[error("offset is not a JSON boundary")]
    Offset,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Storage(Box<std::io::Error>),
}

impl SqliteStore {
    /// Open (or create) a store. The parent directory is durably created before SQLite opens it.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_mode(path.as_ref(), true).await
    }

    /// Restart only: never recreate a missing database or silently initialize an empty one.
    pub async fn open_existing(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_mode(path.as_ref(), false).await
    }

    async fn open_mode(path: &Path, initialize: bool) -> Result<Self> {
        let path = path.to_path_buf();
        let instance = getrandom::u64().map_err(store_write)?;
        let busy_us = Arc::new(AtomicU64::new(0));
        let actor_busy = busy_us.clone();
        let (ready_tx, ready_rx) = oneshot::channel();
        let (tx, mut rx) = mpsc::channel::<Job>(QUEUE_DEPTH);
        let (stopped_tx, stopped) = tokio::sync::watch::channel(false);
        std::thread::Builder::new()
            .name("chronicle-sqlite".into())
            .spawn(move || {
                let opened = Worker::open_mode(&path, initialize);
                let mut worker = match opened {
                    Ok(w) => {
                        let _ = ready_tx.send(Ok(()));
                        w
                    }
                    Err(e) => {
                        let _ = ready_tx.send(Err(e));
                        return;
                    }
                };
                while let Some(job) = rx.blocking_recv() {
                    let start = Instant::now();
                    job(&mut worker);
                    actor_busy.fetch_add(start.elapsed().as_micros() as u64, Ordering::Relaxed);
                }
                drop(worker);
                let _ = stopped_tx.send(true);
            })
            .map_err(store_write)?;
        ready_rx.await.map_err(store_read)??;
        let (applied, _) = tokio::sync::watch::channel(());
        Ok(Self {
            tx,
            stopped,
            applied,
            busy_us,
            instance,
        })
    }

    /// Bounded private placement sample; no payload copy or telemetry dependency.
    pub async fn load(&self) -> Result<crate::balance::Load> {
        let queued = QUEUE_DEPTH - self.tx.capacity();
        let charged_bytes = self.call(|w| Ok(w.state.charged_bytes())).await?;
        Ok(crate::balance::Load {
            instance: self.instance,
            busy_us: self.busy_us.load(Ordering::Relaxed),
            queued,
            charged_bytes,
            view: None,
        })
    }

    /// Subscribe before capturing a read view to avoid missed wakeups. Signals
    /// coalesce and are only hints; consumers still require a safe read barrier.
    pub fn applied_changes(&self) -> tokio::sync::watch::Receiver<()> {
        self.applied.subscribe()
    }

    /// Release the final store handle and await durable actor shutdown.
    /// All other clones (including Raft) must already have been dropped.
    pub async fn close(self) {
        let Self {
            tx, mut stopped, ..
        } = self;
        drop(tx);
        let _ = stopped.wait_for(|done| *done).await;
    }

    async fn call<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Worker) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let (tx, rx) = oneshot::channel();
        let queued = Instant::now();
        self.tx
            .send(Box::new(move |w| {
                QUEUE.observe(queued.elapsed());
                let _ = tx.send(f(w));
            }))
            .await
            .map_err(store_write)?;
        rx.await.map_err(store_read)?
    }

    pub async fn read_state(&self) -> Result<model::State> {
        self.call(|w| Ok(w.state.clone())).await
    }

    pub async fn read_stream(&self, key: String) -> Result<Option<model::Stream>> {
        self.call(move |w| Ok(w.state.streams.get(&key).cloned()))
            .await
    }

    pub async fn fork_view(&self, key: String, sequence: Option<u64>) -> Result<crate::fork::View> {
        self.call(move |w| Ok(crate::fork::view(&w.state, &key, sequence)))
            .await
    }

    pub async fn fork_chunk(
        &self,
        id: crate::fork::Id,
        offset: u64,
    ) -> Result<std::result::Result<crate::fork::Operation, model::Error>> {
        self.call(move |w| Ok(crate::fork::chunk(&w.state, &id, offset)))
            .await
    }

    /// Local hints only; reconciliation obtains strict receipts before acting.
    pub async fn fork_work(&self, after: String) -> Result<Vec<(String, crate::fork::Work)>> {
        self.call(move |w| {
            use crate::fork::Work;
            let mut work = std::collections::BTreeMap::new();
            for (key, prepared) in w
                .state
                .fork_targets
                .range((Bound::Excluded(after.clone()), Bound::Unbounded))
                .take(16)
            {
                work.insert(key.clone(), Work::Reconcile(prepared.offer.clone()));
            }
            let streams = w
                .state
                .streams
                .range((Bound::Excluded(after), Bound::Unbounded));
            for (key, item) in streams
                .filter_map(|(key, stream)| {
                    if let Some(transaction) =
                        stream.forks.transactions.values().find(|t| !t.finalized)
                    {
                        Some((key, Work::Reconcile(transaction.offer.clone())))
                    } else if stream.deleted
                        && !stream.forks.retained()
                        && stream.forks.origin.is_some()
                    {
                        Some((key, Work::Release(key.clone())))
                    } else {
                        None
                    }
                })
                .take(16)
            {
                work.entry(key.clone()).or_insert(item);
            }
            Ok(work.into_iter().take(16).collect())
        })
        .await
    }

    /// Retain request admission even if its caller abandons this queued read.
    pub async fn read_info(
        &self,
        key: String,
        admission: impl Send + 'static,
    ) -> Result<Option<StreamInfo>> {
        self.call(move |w| {
            let _admission = admission;
            if let Some(prepared) = w.state.fork_targets.get(&key) {
                return Ok(Some(StreamInfo {
                    generation: w.read_generation.clone(),
                    incarnation: prepared.offer.request.incarnation,
                    config: prepared.offer.config.clone(),
                    end: 0,
                    closed: false,
                    deleted: false,
                    access_ms: 0,
                    fork_pending: true,
                    soft_deleted: false,
                    fork_sequence: 0,
                }));
            }
            Ok(w.state.streams.get(&key).map(|s| StreamInfo {
                generation: w.read_generation.clone(),
                incarnation: s.incarnation,
                config: s.config.clone(),
                end: s.data.len() as u64,
                closed: s.closed,
                deleted: s.deleted,
                access_ms: s.access_ms,
                fork_pending: s.forks.locked(),
                soft_deleted: s.deleted && s.forks.retained(),
                fork_sequence: s.forks.sequence,
            }))
        })
        .await
    }

    /// Open the exact captured committed prefix; a later lifecycle change cannot
    /// substitute another incarnation. Callers must bound delivery at `end`.
    /// The owned guard stays with queued or executing work after caller cancellation.
    pub async fn read_file(
        &self,
        key: String,
        view: &StreamInfo,
        start: u64,
        admission: impl Send + 'static,
    ) -> std::result::Result<std::fs::File, ReadError> {
        let (incarnation, end) = (view.incarnation, view.end);
        let generation = view.generation.clone();
        self.call(move |w| {
            let _admission = admission;
            Ok((|| {
                #[cfg(feature = "storage-faults")]
                w.faults.hit(crate::faults::BEFORE_PROJECTION_OPEN)?;
                let s = w.state.streams.get(&key).ok_or(ReadError::Changed)?;
                if s.deleted
                    || w.state.fork_targets.contains_key(&key)
                    || s.incarnation != incarnation
                    || end > s.data.len() as u64
                    || start > end
                    || !Arc::ptr_eq(&generation, &w.read_generation)
                {
                    return Err(ReadError::Changed);
                }
                if s.config.is_json()
                    && !crate::wire::json_boundary(&s.data[..end as usize], start as usize)
                {
                    return Err(ReadError::Offset);
                }
                let mut file = w.projection.open(&key, s)?;
                file.seek(SeekFrom::Start(start))?;
                Ok(file)
            })())
        })
        .await
        .map_err(|e| ReadError::Storage(Box::new(e)))?
    }
}

impl Worker {
    fn open_mode(path: &Path, initialize: bool) -> Result<Self> {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            create_dir_all_durable(parent)?;
        }
        let process_lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path.with_extension("lock"))
            .map_err(store_write)?;
        process_lock.try_lock().map_err(store_write)?;
        let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
            | if initialize {
                rusqlite::OpenFlags::SQLITE_OPEN_CREATE
            } else {
                rusqlite::OpenFlags::empty()
            };
        let db = Connection::open_with_flags(path, flags).map_err(store_write)?;
        if !initialize {
            // Check before any schema creation: a truncated-to-empty database must not rejoin
            // with forgotten votes/logs merely because its pathname still exists.
            for table in ["meta", "logs", "streams", "control"] {
                db.prepare(&format!("SELECT * FROM {table} LIMIT 0"))
                    .map_err(store_read)?;
            }
        }
        db.pragma_update(None, "journal_mode", "WAL")
            .map_err(store_write)?;
        db.pragma_update(None, "synchronous", "FULL")
            .map_err(store_write)?;
        db.pragma_update(None, "foreign_keys", "ON")
            .map_err(store_write)?;
        db.execute_batch(
            "BEGIN IMMEDIATE;
          CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v BLOB NOT NULL);
          CREATE TABLE IF NOT EXISTS logs(idx INTEGER PRIMARY KEY, entry BLOB NOT NULL);
          CREATE TABLE IF NOT EXISTS streams(k TEXT PRIMARY KEY, v BLOB NOT NULL);
          CREATE TABLE IF NOT EXISTS control(k TEXT PRIMARY KEY, v BLOB NOT NULL);
          COMMIT;",
        )
        .map_err(store_write)?;
        let mut state = model::State::default();
        {
            let mut q = db.prepare("SELECT k,v FROM streams").map_err(store_read)?;
            let rows = q
                .query_map([], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?))
                })
                .map_err(store_read)?;
            for row in rows {
                let (k, v) = row.map_err(store_read)?;
                state.streams.insert(k, decode(&v)?);
            }
        }
        state.nodes = read_meta(&db, "nodes")?.unwrap_or_default();
        state.placements = read_meta(&db, "placements")?.unwrap_or_default();
        state.fork_targets = read_meta(&db, "fork_targets")?.unwrap_or_default();
        state.leadership = read_meta(&db, "leadership")?.unwrap_or_default();
        Ok(Self {
            db,
            state,
            projection: crate::projection::Cache::new(path.with_extension("projection")),
            read_generation: Arc::new(()),
            #[cfg(feature = "storage-faults")]
            faults: crate::faults::Context::new(path),
            _process_lock: process_lock,
        })
    }

    fn state_transaction<T>(
        &mut self,
        f: impl FnOnce(&Transaction<'_>, &mut model::State) -> Result<T>,
    ) -> Result<T> {
        let tx = self.db.transaction().map_err(store_write)?;
        let mut next = self.state.clone();
        let out = f(&tx, &mut next)?;
        #[cfg(feature = "storage-faults")]
        self.faults
            .hit(crate::faults::BEFORE_STATE_COMMIT)
            .map_err(store_write)?;
        tx.commit().map_err(store_write)?;
        self.state = next;
        Ok(out)
    }

    fn db_transaction<T>(&mut self, f: impl FnOnce(&Transaction<'_>) -> Result<T>) -> Result<T> {
        let tx = self.db.transaction().map_err(store_write)?;
        let out = f(&tx)?;
        tx.commit().map_err(store_write)?;
        Ok(out)
    }
}

fn encode<T: Serialize>(v: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(v).map_err(store_write)
}
fn decode<T: serde::de::DeserializeOwned>(v: &[u8]) -> Result<T> {
    serde_json::from_slice(v).map_err(store_read)
}
fn store_read(e: impl std::error::Error + 'static) -> std::io::Error {
    std::io::Error::other(format!("storage read: {e}"))
}
fn store_write(e: impl std::error::Error + 'static) -> std::io::Error {
    std::io::Error::other(format!("storage write: {e}"))
}
fn sync_dir(path: &Path) -> Result<()> {
    std::fs::File::open(path)
        .and_then(|f| f.sync_all())
        .map_err(store_write)
}
fn create_dir_all_durable(path: &Path) -> Result<()> {
    let mut missing = Vec::new();
    let mut cursor = path;
    while !cursor.exists() {
        missing.push(cursor.to_path_buf());
        cursor = cursor.parent().ok_or_else(|| {
            store_write(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "data directory has no existing ancestor",
            ))
        })?;
    }
    std::fs::create_dir_all(path).map_err(store_write)?;
    for created in missing.iter().rev() {
        sync_dir(created)?;
        if let Some(parent) = created.parent() {
            sync_dir(parent)?;
        }
    }
    if missing.is_empty() {
        sync_dir(path)?;
    }
    Ok(())
}
fn read_meta<T: serde::de::DeserializeOwned>(db: &Connection, key: &str) -> Result<Option<T>> {
    let b: Option<Vec<u8>> = db
        .query_row("SELECT v FROM meta WHERE k=?", [key], |r| r.get(0))
        .optional()
        .map_err(store_read)?;
    b.map(|x| decode(&x)).transpose()
}
fn put_meta(tx: &Transaction<'_>, key: &str, value: &impl Serialize) -> Result<()> {
    tx.execute(
        "INSERT OR REPLACE INTO meta(k,v) VALUES(?,?)",
        (key, encode(value)?),
    )
    .map_err(store_write)?;
    Ok(())
}
fn put_meta_raw(tx: &Transaction<'_>, key: &str, value: &[u8]) -> Result<()> {
    tx.execute("INSERT OR REPLACE INTO meta(k,v) VALUES(?,?)", (key, value))
        .map_err(store_write)?;
    Ok(())
}
fn read_meta_raw(db: &Connection, key: &str) -> Result<Option<Vec<u8>>> {
    db.query_row("SELECT v FROM meta WHERE k=?", [key], |r| r.get(0))
        .optional()
        .map_err(store_read)
}
fn snapshot_bytes(body: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(SNAPSHOT_CHECKSUM_BYTES + body.len());
    bytes.extend_from_slice(&Sha256::digest(body));
    bytes.extend_from_slice(body);
    bytes
}
fn decode_snapshot(bytes: &[u8]) -> Result<SnapshotBody> {
    if bytes.len() < SNAPSHOT_CHECKSUM_BYTES || bytes.len() > MAX_SNAPSHOT_BYTES {
        return Err(store_read(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid snapshot size",
        )));
    }
    let (checksum, body) = bytes.split_at(SNAPSHOT_CHECKSUM_BYTES);
    if Sha256::digest(body).as_slice() != checksum {
        return Err(store_read(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "snapshot checksum mismatch",
        )));
    }
    decode(body)
}
fn range_sql<R: RangeBounds<u64>>(r: &R) -> (u64, u64) {
    let lo = match r.start_bound() {
        Bound::Included(x) => *x,
        Bound::Excluded(x) => x.saturating_add(1),
        Bound::Unbounded => 0,
    };
    let hi = match r.end_bound() {
        Bound::Included(x) => x.saturating_add(1),
        Bound::Excluded(x) => *x,
        Bound::Unbounded => i64::MAX as u64,
    };
    (lo, hi)
}

impl RaftLogReader<TypeConfig> for SqliteStore {
    async fn read_vote(&mut self) -> Result<Option<Vote>> {
        self.call(|w| read_meta(&w.db, "vote")).await
    }

    async fn try_get_log_entries<R: RangeBounds<u64> + Clone + Debug + openraft::OptionalSend>(
        &mut self,
        range: R,
    ) -> Result<Vec<Entry>> {
        let (lo, hi) = range_sql(&range);
        self.call(move |w| {
            let mut q =
                w.db.prepare("SELECT entry FROM logs WHERE idx>=? AND idx<? ORDER BY idx")
                    .map_err(store_read)?;
            let rows = q
                .query_map((lo, hi), |r| r.get::<_, Vec<u8>>(0))
                .map_err(store_read)?;
            rows.map(|x| decode(&x.map_err(store_read)?)).collect()
        })
        .await
    }
}

impl RaftLogStorage<TypeConfig> for SqliteStore {
    type LogReader = Self;
    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>> {
        self.call(|w| {
            let purged = read_meta(&w.db, "purged")?;
            let last: Option<Vec<u8>> =
                w.db.query_row(
                    "SELECT entry FROM logs ORDER BY idx DESC LIMIT 1",
                    [],
                    |r| r.get(0),
                )
                .optional()
                .map_err(store_read)?;
            let last_log_id = last
                .map(|b| decode::<Entry>(&b).map(|e| e.log_id))
                .transpose()?
                .or(purged);
            Ok(LogState {
                last_purged_log_id: purged,
                last_log_id,
            })
        })
        .await
    }
    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
    async fn save_vote(&mut self, vote: &Vote) -> Result<()> {
        let v = *vote;
        self.call(move |w| w.db_transaction(|t| put_meta(t, "vote", &v)))
            .await
    }
    async fn save_committed(&mut self, v: Option<LogId>) -> Result<()> {
        self.call(move |w| w.db_transaction(|t| put_meta(t, "committed", &v)))
            .await
    }
    async fn read_committed(&mut self) -> Result<Option<LogId>> {
        self.call(|w| read_meta(&w.db, "committed")).await
    }
    async fn append<I>(&mut self, entries: I, callback: IOFlushed<TypeConfig>) -> Result<()>
    where
        I: IntoIterator<Item = Entry> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let es: Vec<_> = entries.into_iter().collect();
        let (result_tx, result_rx) = oneshot::channel();
        let queued = Instant::now();
        self.tx
            .send(Box::new(move |w| {
                QUEUE.observe(queued.elapsed());
                let persisted = Instant::now();
                let result = w.db_transaction(|t| {
                    for e in es {
                        t.execute(
                            "INSERT OR REPLACE INTO logs(idx,entry) VALUES(?,?)",
                            (e.log_id.index, encode(&e)?),
                        )
                        .map_err(store_write)?;
                    }
                    Ok(())
                });
                #[cfg(feature = "storage-faults")]
                let result = result.and_then(|()| {
                    w.faults
                        .hit(crate::faults::AFTER_LOG_COMMIT)
                        .map_err(store_write)
                });
                PERSIST.observe(persisted.elapsed());
                callback.io_completed(
                    result
                        .as_ref()
                        .map(|_| ())
                        .map_err(|e| std::io::Error::other(e.to_string())),
                );
                let _ = result_tx.send(result);
            }))
            .await
            .map_err(store_write)?;
        result_rx.await.map_err(store_read)?
    }
    async fn truncate_after(&mut self, id: Option<LogId>) -> Result<()> {
        self.call(move |w| {
            w.db_transaction(|t| {
                if let Some(id) = id {
                    t.execute("DELETE FROM logs WHERE idx>?", [id.index])
                        .map_err(store_write)?;
                } else {
                    t.execute("DELETE FROM logs", []).map_err(store_write)?;
                }
                Ok(())
            })
        })
        .await
    }
    async fn purge(&mut self, id: LogId) -> Result<()> {
        self.call(move |w| {
            w.db_transaction(|t| {
                t.execute("DELETE FROM logs WHERE idx<=?", [id.index])
                    .map_err(store_write)?;
                put_meta(t, "purged", &id)
            })
        })
        .await
    }
}

impl RaftStateMachine<TypeConfig> for SqliteStore {
    type SnapshotData = Cursor<Vec<u8>>;
    type SnapshotBuilder = Self;
    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership)> {
        self.call(|w| {
            Ok((
                read_meta(&w.db, "applied")?,
                read_meta(&w.db, "membership")?.unwrap_or_default(),
            ))
        })
        .await
    }
    async fn apply<S>(&mut self, mut entries: S) -> Result<()>
    where
        S: Stream<Item = Result<EntryResponder<TypeConfig>>> + Unpin + openraft::OptionalSend,
    {
        loop {
            // A stream can be arbitrarily long; at most one bounded batch is admitted
            // at a time. On a stream error, finish its preceding entries before failing.
            let mut batch = Vec::with_capacity(APPLY_BATCH_SIZE);
            let mut error = None;
            while batch.len() < APPLY_BATCH_SIZE {
                match entries.next().await {
                    Some(Ok(entry)) => batch.push(entry),
                    Some(Err(e)) => {
                        error = Some(e);
                        break;
                    }
                    None => break,
                }
            }
            let done = batch.len() < APPLY_BATCH_SIZE;
            if !batch.is_empty() {
                self.apply_batch(batch).await?;
            }
            if let Some(e) = error {
                return Err(e);
            }
            if done {
                return Ok(());
            }
        }
    }
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<()> {
        self.install_snapshot_data(meta, snapshot).await
    }
    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot>> {
        self.current_snapshot().await
    }
}

impl SqliteStore {
    /// Apply a finite batch directly and retain application outcomes for tools and tests.
    pub async fn apply_entries(
        &mut self,
        entries: impl IntoIterator<Item = Entry>,
    ) -> Result<Vec<model::Outcome>> {
        self.apply_batch(entries.into_iter().map(|e| (e, None)).collect())
            .await
    }

    async fn apply_batch(
        &self,
        entries: Vec<EntryResponder<TypeConfig>>,
    ) -> Result<Vec<model::Outcome>> {
        let applied = self.applied.clone();
        self.call(move |w| {
            // The actor owns responders after admission, even if the waiting future is cancelled.
            let (es, responders): (Vec<_>, Vec<_>) = entries.into_iter().unzip();
            let started = Instant::now();
            let result = w.state_transaction(|t, s| {
                let mut out = Vec::with_capacity(es.len());
                for e in es {
                    match e.payload {
                        EntryPayload::Normal(c) => {
                            let key = match &c {
                                model::Command::Create { key, .. }
                                | model::Command::Append { key, .. }
                                | model::Command::Delete { key, .. }
                                | model::Command::Touch { key, .. }
                                | model::Command::Expire { key, .. } => Some(key.clone()),
                                model::Command::Fork(operation) => Some(operation.key().to_owned()),
                                _ => None,
                            };
                            let r = s.apply(&c);
                            out.push(r);
                            if matches!(c, model::Command::Fork(_)) {
                                put_meta(t, "fork_targets", &s.fork_targets)?;
                            }
                            if let Some(k) = key {
                                if let Some(v) = s.streams.get(&k) {
                                    t.execute(
                                        "INSERT OR REPLACE INTO streams(k,v) VALUES(?,?)",
                                        (&k, encode(v)?),
                                    )
                                    .map_err(store_write)?;
                                }
                            } else {
                                put_meta(t, "nodes", &s.nodes)?;
                                put_meta(t, "placements", &s.placements)?;
                                put_meta(t, "leadership", &s.leadership)?;
                            }
                        }
                        EntryPayload::Membership(m) => {
                            put_meta(t, "membership", &StoredMembership::new(Some(e.log_id), m))?;
                            out.push(model::Outcome {
                                end: 0,
                                incarnation: 0,
                                duplicate: false,
                                closed: false,
                                producer: None,
                                content_type: None,
                                error: None,
                            });
                        }
                        EntryPayload::Blank => out.push(model::Outcome {
                            end: 0,
                            incarnation: 0,
                            duplicate: false,
                            closed: false,
                            producer: None,
                            content_type: None,
                            error: None,
                        }),
                    }
                    put_meta(t, "applied", &Some(e.log_id))?;
                }
                Ok(out)
            });
            if result.is_ok() {
                applied.send_replace(());
            }
            #[cfg(feature = "storage-faults")]
            let result = result.and_then(|out| {
                w.faults
                    .hit(crate::faults::AFTER_APPLY_COMMIT)
                    .map_err(store_write)?;
                Ok(out)
            });
            APPLY.observe(started.elapsed());
            if let Ok(outcomes) = &result {
                for (responder, outcome) in responders.into_iter().zip(outcomes) {
                    if let Some(responder) = responder {
                        responder.send(outcome.clone());
                    }
                }
            }
            result
        })
        .await
    }
    async fn install_snapshot_data(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<()> {
        let bytes = snapshot.into_inner();
        if bytes.len() > MAX_SNAPSHOT_BYTES {
            return Err(store_read(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "snapshot exceeds 256 MiB",
            )));
        }
        let supplied = meta.clone();
        let applied = self.applied.clone();
        self.call(move |w| {
            let body = decode_snapshot(&bytes)?;
            if body.last_applied != supplied.last_log_id
                || body.membership != supplied.last_membership
            {
                return Err(store_read(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "snapshot metadata mismatch",
                )));
            }
            #[cfg(feature = "storage-faults")]
            w.faults
                .hit(crate::faults::BEFORE_SNAPSHOT_INSTALL)
                .map_err(store_write)?;
            let result = w.state_transaction(|t, s| {
                t.execute("DELETE FROM streams", []).map_err(store_write)?;
                for (k, v) in &body.state.streams {
                    t.execute("INSERT INTO streams(k,v) VALUES(?,?)", (k, encode(v)?))
                        .map_err(store_write)?;
                }
                put_meta(t, "nodes", &body.state.nodes)?;
                put_meta(t, "placements", &body.state.placements)?;
                put_meta(t, "fork_targets", &body.state.fork_targets)?;
                put_meta(t, "leadership", &body.state.leadership)?;
                put_meta(t, "applied", &body.last_applied)?;
                put_meta(t, "membership", &body.membership)?;
                put_meta_raw(t, "snapshot_bytes", &bytes)?;
                put_meta(t, "snapshot_meta", &supplied)?;
                *s = body.state;
                Ok(())
            });
            if result.is_ok() {
                w.projection.invalidate();
                w.read_generation = Arc::new(());
                applied.send_replace(());
            }
            #[cfg(feature = "storage-faults")]
            let result = result.and_then(|()| {
                w.faults
                    .hit(crate::faults::AFTER_SNAPSHOT_INSTALL)
                    .map_err(store_write)
            });
            result
        })
        .await
    }
    async fn current_snapshot(&mut self) -> Result<Option<Snapshot>> {
        self.call(|w| {
            let bytes = read_meta_raw(&w.db, "snapshot_bytes")?;
            let meta: Option<SnapshotMeta> = read_meta(&w.db, "snapshot_meta")?;
            match (bytes, meta) {
                (Some(b), Some(m)) => {
                    let body = decode_snapshot(&b)?;
                    if body.last_applied != m.last_log_id || body.membership != m.last_membership {
                        return Err(store_read(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "stored snapshot metadata mismatch",
                        )));
                    }
                    Ok(Some(Snapshot {
                        meta: m,
                        snapshot: Cursor::new(b),
                    }))
                }
                (None, None) => Ok(None),
                _ => Err(store_read(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "incomplete snapshot metadata",
                ))),
            }
        })
        .await
    }
}

impl RaftSnapshotBuilder<TypeConfig> for SqliteStore {
    type SnapshotData = Cursor<Vec<u8>>;
    async fn build_snapshot(&mut self) -> Result<Snapshot> {
        self.call(|w| {
            w.state_transaction(|t, s| {
                let last: Option<LogId> = read_meta(t, "applied")?;
                let membership: StoredMembership = read_meta(t, "membership")?.unwrap_or_default();
                let body = encode(&SnapshotBody {
                    state: s.clone(),
                    last_applied: last,
                    membership: membership.clone(),
                })?;
                let bytes = snapshot_bytes(&body);
                if bytes.len() > MAX_SNAPSHOT_BYTES {
                    return Err(store_write(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "snapshot exceeds 256 MiB",
                    )));
                }
                let meta = SnapshotMeta {
                    last_log_id: last,
                    last_membership: membership,
                };
                put_meta_raw(t, "snapshot_bytes", &bytes)?;
                put_meta(t, "snapshot_meta", &meta)?;
                Ok(Snapshot {
                    meta,
                    snapshot: Cursor::new(bytes),
                })
            })
        })
        .await
    }
}

impl openraft_legacy::network_v1::SnapshotReceiverFactory<TypeConfig> for SqliteStore {
    type SnapshotReceiver = Cursor<Vec<u8>>;

    async fn begin_receiving_snapshot(&mut self) -> Result<Self::SnapshotReceiver> {
        Ok(Cursor::new(Vec::new()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::type_config::TypeConfigExt;
    use openraft::vote::RaftLeaderId;

    fn blank(index: u64) -> Entry {
        Entry {
            log_id: LogId::new(
                openraft::vote::leader_id_adv::CommittedLeaderId::new(7, 3),
                index,
            ),
            payload: EntryPayload::Blank,
        }
    }

    #[tokio::test]
    async fn truncate_after_none_zero_and_middle_survive_reopen() {
        for boundary in [None, Some(0), Some(2)] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("truncate.sqlite");
            let mut store = SqliteStore::open(&path).await.unwrap();
            let vote = Vote::new_committed(7, 3);
            store.save_vote(&vote).await.unwrap();
            store
                .append((0..5).map(blank), IOFlushed::noop())
                .await
                .unwrap();
            store
                .truncate_after(boundary.map(|i| blank(i).log_id))
                .await
                .unwrap();
            store.close().await;
            let mut store = SqliteStore::open_existing(&path).await.unwrap();
            let remaining = store.try_get_log_entries(..).await.unwrap();
            let expected: Vec<_> = boundary.map_or_else(Vec::new, |i| (0..=i).collect());
            assert_eq!(
                remaining.iter().map(|e| e.log_id.index).collect::<Vec<_>>(),
                expected
            );
            assert_eq!(
                store.get_log_state().await.unwrap().last_log_id,
                boundary.map(|i| blank(i).log_id)
            );
            assert_eq!(store.read_vote().await.unwrap(), Some(vote));
            store.close().await;
        }
    }

    #[tokio::test]
    async fn truncation_retains_committed_applied_purged_and_snapshot_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("retained.sqlite");
        let mut store = SqliteStore::open(&path).await.unwrap();
        store
            .append((0..5).map(blank), IOFlushed::noop())
            .await
            .unwrap();
        let committed = blank(2).log_id;
        store.save_committed(Some(committed)).await.unwrap();
        store.apply_entries((0..=2).map(blank)).await.unwrap();
        let snapshot = store.build_snapshot().await.unwrap();
        store.purge(blank(1).log_id).await.unwrap();
        store.truncate_after(Some(committed)).await.unwrap();
        store.close().await;

        let mut store = SqliteStore::open_existing(&path).await.unwrap();
        assert_eq!(store.read_committed().await.unwrap(), Some(committed));
        assert_eq!(store.applied_state().await.unwrap().0, Some(committed));
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(blank(1).log_id));
        assert_eq!(state.last_log_id, Some(committed));
        let entries = store.try_get_log_entries(..).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].log_id, committed);
        let current = store.get_current_snapshot().await.unwrap().unwrap();
        assert_eq!(current.meta, snapshot.meta);
        assert_eq!(
            current.snapshot.into_inner(),
            snapshot.snapshot.into_inner()
        );
        store.close().await;
    }

    // Hold the actor before admission to make cancellation deterministic, without sleeps.
    async fn block_actor(
        store: &SqliteStore,
    ) -> (
        std::sync::mpsc::Sender<()>,
        tokio::task::JoinHandle<Result<()>>,
    ) {
        let (release, blocked) = std::sync::mpsc::channel();
        let (entered, ready) = oneshot::channel();
        let copy = store.clone();
        let blocker = tokio::spawn(async move {
            copy.call(move |_| {
                let _ = entered.send(());
                blocked.recv().map_err(store_read)?;
                Ok(())
            })
            .await
        });
        ready.await.unwrap();
        (release, blocker)
    }

    #[tokio::test]
    async fn cancelled_append_keeps_flush_callback_until_durable_commit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("append.sqlite");
        let mut store = SqliteStore::open(&path).await.unwrap();
        let (release, blocker) = block_actor(&store).await;
        let (tx, rx) = TypeConfig::oneshot();
        let mut completed = Box::pin(rx);
        let mut append = Box::pin(store.append([blank(0)], IOFlushed::signal(tx)));
        assert!(futures_util::poll!(&mut append).is_pending());
        drop(append);
        assert!(futures_util::poll!(&mut completed).is_pending());
        release.send(()).unwrap();
        blocker.await.unwrap().unwrap();
        completed.await.unwrap().unwrap();
        store.close().await;
        let mut store = SqliteStore::open_existing(&path).await.unwrap();
        let entries = store.try_get_log_entries(..).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].log_id, blank(0).log_id);
        assert!(matches!(entries[0].payload, EntryPayload::Blank));
        store.close().await;
    }

    #[tokio::test]
    async fn cancelled_stream_apply_finishes_admitted_bounded_batch_without_responders() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("apply.sqlite");
        let mut store = SqliteStore::open(&path).await.unwrap();
        let (release, blocker) = block_actor(&store).await;
        let polled = Arc::new(AtomicU64::new(0));
        let count = polled.clone();
        let stream = futures_util::stream::iter((0..1000).map(move |i| {
            count.fetch_add(1, Ordering::Relaxed);
            Ok((blank(i), None))
        }));
        let mut apply = Box::pin(store.apply(stream));
        assert!(futures_util::poll!(&mut apply).is_pending());
        assert_eq!(polled.load(Ordering::Relaxed), APPLY_BATCH_SIZE as u64);
        drop(apply);
        release.send(()).unwrap();
        blocker.await.unwrap().unwrap();
        assert_eq!(
            store.applied_state().await.unwrap().0,
            Some(blank(APPLY_BATCH_SIZE as u64 - 1).log_id)
        );
        store.close().await;
        let mut store = SqliteStore::open_existing(&path).await.unwrap();
        assert_eq!(
            store.applied_state().await.unwrap().0,
            Some(blank(APPLY_BATCH_SIZE as u64 - 1).log_id)
        );
        store.close().await;
    }

    #[tokio::test]
    async fn stream_error_commits_preceding_batches_and_partial_batch_but_not_following_entry() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stream-error.sqlite");
        let mut store = SqliteStore::open(&path).await.unwrap();
        store.apply(futures_util::stream::empty()).await.unwrap();
        assert_eq!(store.applied_state().await.unwrap().0, None);
        let first_error = std::io::Error::new(std::io::ErrorKind::InvalidData, "first entry");
        assert_eq!(
            store
                .apply(futures_util::stream::iter([Err(first_error)]))
                .await
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::InvalidData
        );
        assert_eq!(store.applied_state().await.unwrap().0, None);
        let last = APPLY_BATCH_SIZE as u64 + 1;
        let membership = openraft::Membership::new(
            vec![[3].into_iter().collect()],
            std::collections::BTreeMap::from([(3, openraft::BasicNode::default())]),
        )
        .unwrap();
        let mut items: Vec<_> = (0..last).map(|i| Ok((blank(i), None))).collect();
        items[last as usize - 1] = Ok((
            Entry {
                log_id: blank(last - 1).log_id,
                payload: EntryPayload::Normal(model::Command::Register {
                    id: 3,
                    node: model::Node {
                        addr: "persisted".into(),
                        zone: "zone".into(),
                        draining: false,
                    },
                }),
            },
            None,
        ));
        items.push(Ok((
            Entry {
                log_id: blank(last).log_id,
                payload: EntryPayload::Membership(membership.clone()),
            },
            None,
        )));
        items.push(Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "stream interrupted",
        )));
        items.push(Ok((blank(last + 1), None)));
        let error = store
            .apply(futures_util::stream::iter(items))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
        store.close().await;
        let mut store = SqliteStore::open_existing(&path).await.unwrap();
        let (applied, stored) = store.applied_state().await.unwrap();
        assert_eq!(applied, Some(blank(last).log_id));
        assert_eq!(stored.log_id(), &applied);
        assert_eq!(stored.membership(), &membership);
        assert_eq!(
            store.read_state().await.unwrap().nodes[&3].addr,
            "persisted"
        );
        store.close().await;
    }

    #[tokio::test]
    async fn resource_samples_charge_state_measure_work_and_reset_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("resources.sqlite");
        let mut store = SqliteStore::open(&path).await.unwrap();
        let initial = store.load().await.unwrap();
        assert_eq!(initial.charged_bytes, 0);
        let command = model::Command::Create {
            key: "resource".into(),
            expected_incarnation: Some(1),
            config: model::StreamConfig {
                content_type: "text/plain".into(),
                track_boundaries: false,
                json_framing: Some(false),
                expiry: None,
            },
            data: vec![42; 1234],
            closed: false,
            now_ms: Some(0),
        };
        let result = store
            .apply_entries([Entry {
                log_id: LogId::new(
                    openraft::vote::leader_id_adv::CommittedLeaderId::new(1, 1),
                    1,
                ),
                payload: EntryPayload::Normal(command),
            }])
            .await
            .unwrap();
        assert!(result[0].error.is_none());
        store
            .call(|_| {
                std::thread::sleep(std::time::Duration::from_millis(5));
                Ok(())
            })
            .await
            .unwrap();
        let measured = store.load().await.unwrap();
        assert_eq!(measured.charged_bytes, 1234 + 8 + 10 + 256);
        assert!(measured.busy_us >= initial.busy_us + 5000);
        assert_eq!(measured.instance, initial.instance);
        store.close().await;
        let reopened = SqliteStore::open_existing(&path).await.unwrap();
        let restored = reopened.load().await.unwrap();
        assert_eq!(restored.charged_bytes, measured.charged_bytes);
        assert_ne!(restored.instance, measured.instance);
        reopened.close().await;
    }

    fn sample_snapshot() -> (SnapshotBody, Vec<u8>) {
        let mut state = model::State::default();
        state.nodes.insert(
            7,
            model::Node {
                addr: "node-7".into(),
                zone: "test".into(),
                draining: false,
            },
        );
        let body = SnapshotBody {
            state,
            last_applied: None,
            membership: StoredMembership::default(),
        };
        let bytes = snapshot_bytes(&encode(&body).unwrap());
        (body, bytes)
    }

    #[tokio::test]
    async fn reopen_empty_store() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("db.sqlite");
        let s = SqliteStore::open(&p).await.unwrap();
        assert!(s.read_state().await.unwrap().streams.is_empty());
        s.close().await;
        let s = SqliteStore::open(&p).await.unwrap();
        assert!(s.read_state().await.unwrap().streams.is_empty());
    }
    #[tokio::test]
    async fn rejects_corrupt_database() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("db");
        std::fs::write(&p, b"not sqlite").unwrap();
        assert!(SqliteStore::open(p).await.is_err());
    }

    #[tokio::test]
    async fn restart_rejects_missing_or_truncated_store_without_initializing_it() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("group.sqlite");
        assert!(SqliteStore::open_existing(&path).await.is_err());
        assert!(!path.exists());
        std::fs::write(&path, b"").unwrap();
        assert!(SqliteStore::open_existing(&path).await.is_err());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
        SqliteStore::open(&path).await.unwrap().close().await;
        SqliteStore::open_existing(&path)
            .await
            .unwrap()
            .close()
            .await;
    }

    #[tokio::test]
    async fn overlapping_process_cannot_open_same_raft_identity() {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("group.sqlite");
        let first = SqliteStore::open(&path).await.unwrap();
        assert!(SqliteStore::open(&path).await.is_err());
        assert!(first.read_state().await.unwrap().streams.is_empty());
    }

    #[tokio::test]
    async fn cancelled_caller_does_not_release_actor_lock_or_cancel_durable_work() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("group.sqlite");
        let store = SqliteStore::open(&path).await.unwrap();
        let mut stopped = store.stopped.clone();
        let (entered, started) = oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let caller = tokio::spawn(async move {
            store
                .call(move |worker| {
                    let _ = entered.send(());
                    blocked.recv().map_err(store_write)?;
                    worker.db_transaction(|tx| put_meta(tx, "vote", &Vote::new_committed(11, 2)))
                })
                .await
        });
        started.await.unwrap();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        assert!(SqliteStore::open(&path).await.is_err());
        release.send(()).unwrap();
        stopped.wait_for(|done| *done).await.unwrap();
        let mut reopened = SqliteStore::open(&path).await.unwrap();
        assert_eq!(
            reopened.read_vote().await.unwrap(),
            Some(Vote::new_committed(11, 2))
        );
    }

    #[test]
    fn child_process_lock_holder() {
        let Ok(path) = std::env::var("CHRONICLE_TEST_LOCK_PATH") else {
            return;
        };
        let mut worker = Worker::open_mode(Path::new(&path), true).unwrap();
        worker
            .db_transaction(|tx| put_meta(tx, "vote", &Vote::new_committed(9, 3)))
            .unwrap();
        std::fs::write(format!("{path}.ready"), b"committed").unwrap();
        loop {
            if Path::new(&format!("{path}.advance")).exists() {
                worker
                    .db_transaction(|tx| put_meta(tx, "vote", &Vote::new_committed(10, 3)))
                    .unwrap();
                std::fs::write(format!("{path}.advanced"), b"committed").unwrap();
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    #[tokio::test]
    async fn process_lock_survives_overlap_and_releases_on_sigkill() {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("child.sqlite");
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "storage::tests::child_process_lock_holder",
                "--nocapture",
            ])
            .env("CHRONICLE_TEST_LOCK_PATH", &path)
            .spawn()
            .unwrap();
        let ready = path.with_extension("sqlite.ready");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !ready.exists() && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(ready.exists(), "child did not reach durable vote");
        let lock_path = path.with_extension("lock");
        #[cfg(unix)]
        let inode = {
            use std::os::unix::fs::MetadataExt;
            std::fs::metadata(&lock_path).unwrap().ino()
        };
        let before_db = std::fs::read(&path).unwrap();
        let wal_path = format!("{}.sqlite-wal", path.with_extension("").display());
        let before_wal = std::fs::read(&wal_path).unwrap();
        let result = SqliteStore::open(&path).await;
        let error = match result {
            Err(e) => e,
            Ok(_) => panic!("overlapping identity was admitted"),
        };
        assert!(
            error.to_string().contains("would block"),
            "unexpected failure: {error}"
        );
        assert_eq!(std::fs::read(&path).unwrap(), before_db);
        assert_eq!(std::fs::read(&wal_path).unwrap(), before_wal);
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(std::fs::metadata(&lock_path).unwrap().ino(), inode);
        }
        // A rejected contender must not disturb the original owner's next durable write.
        std::fs::write(format!("{}.advance", path.display()), b"go").unwrap();
        let advanced = format!("{}.advanced", path.display());
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !Path::new(&advanced).exists() && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(
            Path::new(&advanced).exists(),
            "original owner stopped writing"
        );
        let mut reopened = SqliteStore::open(&path).await.unwrap();
        assert_eq!(
            reopened.read_vote().await.unwrap(),
            Some(Vote::new_committed(10, 3))
        );
    }

    #[test]
    fn snapshot_format_is_raw_body_with_fixed_checksum_overhead() {
        let (body, bytes) = sample_snapshot();
        let encoded = encode(&body).unwrap();
        assert_eq!(bytes.len(), encoded.len() + SNAPSHOT_CHECKSUM_BYTES);
        assert_eq!(decode_snapshot(&bytes).unwrap().state.nodes.len(), 1);
    }

    #[test]
    fn corrupt_and_truncated_snapshots_are_rejected() {
        let (_, mut corrupt) = sample_snapshot();
        corrupt[SNAPSHOT_CHECKSUM_BYTES] ^= 1;
        assert!(decode_snapshot(&corrupt).is_err());
        assert!(decode_snapshot(&corrupt[..SNAPSHOT_CHECKSUM_BYTES - 1]).is_err());
    }

    #[test]
    fn failed_state_transaction_rolls_back_database_and_publication() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("db");
        let mut worker = Worker::open_mode(&p, true).unwrap();
        let result: Result<()> = worker.state_transaction(|tx, state| {
            state.nodes.insert(
                1,
                model::Node {
                    addr: "unpublished".into(),
                    zone: String::new(),
                    draining: false,
                },
            );
            put_meta(tx, "nodes", &state.nodes)?;
            Err(store_write(std::io::Error::other("injected failure")))
        });
        assert!(result.is_err());
        assert!(worker.state.nodes.is_empty());
        let persisted: Option<std::collections::BTreeMap<u64, model::Node>> =
            read_meta(&worker.db, "nodes").unwrap();
        assert!(persisted.is_none());
    }

    #[tokio::test]
    async fn nonempty_vote_and_state_survive_reopen() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("nested").join("db.sqlite");
        let mut store = SqliteStore::open(&p).await.unwrap();
        let vote = Vote::new_committed(4, 9);
        store.save_vote(&vote).await.unwrap();
        store
            .call(|worker| {
                worker.state_transaction(|tx, state| {
                    state.nodes.insert(
                        9,
                        model::Node {
                            addr: "persisted".into(),
                            zone: "z".into(),
                            draining: false,
                        },
                    );
                    put_meta(tx, "nodes", &state.nodes)
                })
            })
            .await
            .unwrap();
        store.close().await;

        let mut reopened = SqliteStore::open(&p).await.unwrap();
        assert_eq!(reopened.read_vote().await.unwrap(), Some(vote));
        assert_eq!(
            reopened.read_state().await.unwrap().nodes[&9].addr,
            "persisted"
        );
    }

    #[tokio::test]
    async fn cancelled_metadata_read_retains_admission_until_actor_finishes() {
        let d = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(d.path().join("metadata.db"))
            .await
            .unwrap();
        let (release, blocked) = std::sync::mpsc::channel();
        let (entered, ready) = oneshot::channel();
        let copy = store.clone();
        let blocker = tokio::spawn(async move {
            copy.call(move |_| {
                let _ = entered.send(());
                let _ = blocked.recv();
                Ok(())
            })
            .await
        });
        ready.await.unwrap();
        let requests = Arc::new(tokio::sync::Semaphore::new(1));
        let live = Arc::new(tokio::sync::Semaphore::new(1));
        let guards = [
            requests.clone().try_acquire_owned().unwrap(),
            live.clone().try_acquire_owned().unwrap(),
        ];
        let mut read = Box::pin(store.read_info("missing".into(), guards));
        // The actor is blocked and its empty queue accepts the metadata job.
        assert!(futures_util::poll!(&mut read).is_pending());
        drop(read);
        assert_eq!(requests.available_permits(), 0);
        assert_eq!(live.available_permits(), 0);
        release.send(()).unwrap();
        blocker.await.unwrap().unwrap();
        // FIFO completion fences the abandoned job without timing assumptions.
        store.read_state().await.unwrap();
        assert_eq!(requests.available_permits(), 1);
        assert_eq!(live.available_permits(), 1);
        store.close().await;
    }

    #[tokio::test]
    async fn install_rejects_metadata_that_does_not_match_body() {
        let d = tempfile::tempdir().unwrap();
        let mut source = SqliteStore::open(d.path().join("source.db")).await.unwrap();
        let built = source.build_snapshot().await.unwrap();
        let mut mismatched = built.meta;
        mismatched.last_log_id = Some(LogId::new(
            openraft::vote::leader_id_adv::CommittedLeaderId::new(1, 1),
            1,
        ));

        let mut destination = SqliteStore::open(d.path().join("destination.db"))
            .await
            .unwrap();
        assert!(
            destination
                .install_snapshot(&mismatched, built.snapshot)
                .await
                .is_err()
        );
        assert!(destination.read_state().await.unwrap().streams.is_empty());
    }
}
