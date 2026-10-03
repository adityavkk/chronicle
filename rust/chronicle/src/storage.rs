//! Durable SQLite implementation of OpenRaft's storage-v2 contracts.
//!
//! Every operation is submitted to one bounded blocking actor.  This is intentional: rusqlite is
//! synchronous, and Raft requires vote and log writes to be ordered even when their futures are
//! cancelled.  A request already admitted to the actor is therefore always completed.
// OpenRaft's public storage trait fixes this error representation.
#![allow(clippy::result_large_err)]

use std::fmt::Debug;
use std::io::{Cursor, Seek, SeekFrom};
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use openraft::storage::{LogFlushed, RaftLogStorage, RaftStateMachine};
use openraft::{
    AnyError, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, LogState, RaftLogReader,
    RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError, StorageIOError, StoredMembership,
    Vote,
};
use rusqlite::{Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, oneshot};

use crate::{TypeConfig, metrics::Histogram, model};

type Result<T> = std::result::Result<T, StorageError<u64>>;
type Job = Box<dyn FnOnce(&mut Worker) + Send>;
const QUEUE_DEPTH: usize = 128;
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
}

#[derive(Serialize, Deserialize)]
struct SnapshotBody {
    state: model::State,
    last_applied: Option<LogId<u64>>,
    membership: StoredMembership<u64, openraft::BasicNode>,
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
    Storage(Box<StorageError<u64>>),
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
                    job(&mut worker);
                }
                drop(worker);
                let _ = stopped_tx.send(true);
            })
            .map_err(|e| io_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
        ready_rx
            .await
            .map_err(|e| io_err(ErrorSubject::Store, ErrorVerb::Read, e))??;
        let (applied, _) = tokio::sync::watch::channel(());
        Ok(Self {
            tx,
            stopped,
            applied,
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
            .map_err(|e| io_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
        rx.await
            .map_err(|e| io_err(ErrorSubject::Store, ErrorVerb::Read, e))?
    }

    pub async fn read_state(&self) -> Result<model::State> {
        self.call(|w| Ok(w.state.clone())).await
    }

    pub async fn read_stream(&self, key: String) -> Result<Option<model::Stream>> {
        self.call(move |w| Ok(w.state.streams.get(&key).cloned()))
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
            Ok(w.state.streams.get(&key).map(|s| StreamInfo {
                generation: w.read_generation.clone(),
                incarnation: s.incarnation,
                config: s.config.clone(),
                end: s.data.len() as u64,
                closed: s.closed,
                deleted: s.deleted,
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
                    || s.incarnation != incarnation
                    || end > s.data.len() as u64
                    || start > end
                    || !Arc::ptr_eq(&generation, &w.read_generation)
                {
                    return Err(ReadError::Changed);
                }
                if s.config.content_type.starts_with("application/json")
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
fn io_err(
    subject: ErrorSubject<u64>,
    verb: ErrorVerb,
    e: impl std::error::Error + 'static,
) -> StorageError<u64> {
    StorageIOError::new(subject, verb, AnyError::new(&e)).into()
}
fn store_read(e: impl std::error::Error + 'static) -> StorageError<u64> {
    io_err(ErrorSubject::Store, ErrorVerb::Read, e)
}
fn store_write(e: impl std::error::Error + 'static) -> StorageError<u64> {
    io_err(ErrorSubject::Store, ErrorVerb::Write, e)
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
    async fn try_get_log_entries<R: RangeBounds<u64> + Clone + Debug + openraft::OptionalSend>(
        &mut self,
        range: R,
    ) -> Result<Vec<Entry<TypeConfig>>> {
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
                .map(|b| decode::<Entry<TypeConfig>>(&b).map(|e| e.log_id))
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
    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<()> {
        let v = *vote;
        self.call(move |w| w.db_transaction(|t| put_meta(t, "vote", &v)))
            .await
    }
    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>> {
        self.call(|w| read_meta(&w.db, "vote")).await
    }
    async fn save_committed(&mut self, v: Option<LogId<u64>>) -> Result<()> {
        self.call(move |w| w.db_transaction(|t| put_meta(t, "committed", &v)))
            .await
    }
    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>> {
        self.call(|w| read_meta(&w.db, "committed")).await
    }
    async fn append<I>(&mut self, entries: I, callback: LogFlushed<TypeConfig>) -> Result<()>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + openraft::OptionalSend,
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
                callback.log_io_completed(
                    result
                        .as_ref()
                        .map(|_| ())
                        .map_err(|e| std::io::Error::other(e.to_string())),
                );
                let _ = result_tx.send(result);
            }))
            .await
            .map_err(|e| io_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
        result_rx
            .await
            .map_err(|e| io_err(ErrorSubject::Store, ErrorVerb::Read, e))?
    }
    async fn truncate(&mut self, id: LogId<u64>) -> Result<()> {
        self.call(move |w| {
            w.db_transaction(|t| {
                t.execute("DELETE FROM logs WHERE idx>=?", [id.index])
                    .map_err(store_write)?;
                Ok(())
            })
        })
        .await
    }
    async fn purge(&mut self, id: LogId<u64>) -> Result<()> {
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
    type SnapshotBuilder = Self;
    async fn applied_state(
        &mut self,
    ) -> Result<(
        Option<LogId<u64>>,
        StoredMembership<u64, openraft::BasicNode>,
    )> {
        self.call(|w| {
            Ok((
                read_meta(&w.db, "applied")?,
                read_meta(&w.db, "membership")?.unwrap_or_default(),
            ))
        })
        .await
    }
    async fn apply<I>(&mut self, entries: I) -> Result<Vec<model::Outcome>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let es: Vec<_> = entries.into_iter().collect();
        let applied = self.applied.clone();
        self.call(move |w| {
            let started = Instant::now();
            let result = w.state_transaction(|t, s| {
                let mut out = Vec::with_capacity(es.len());
                for e in es {
                    match e.payload {
                        EntryPayload::Normal(c) => {
                            let key = match &c {
                                model::Command::Create { key, .. }
                                | model::Command::Append { key, .. }
                                | model::Command::Delete { key, .. } => Some(key.clone()),
                                _ => None,
                            };
                            let r = s.apply(&c);
                            out.push(r);
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
                            }
                        }
                        EntryPayload::Membership(m) => {
                            put_meta(t, "membership", &StoredMembership::new(Some(e.log_id), m))?;
                            out.push(model::Outcome {
                                end: 0,
                                incarnation: 0,
                                duplicate: false,
                                error: None,
                            });
                        }
                        EntryPayload::Blank => out.push(model::Outcome {
                            end: 0,
                            incarnation: 0,
                            duplicate: false,
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
            result
        })
        .await
    }
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
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
    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>> {
        self.call(|w| {
            let bytes = read_meta_raw(&w.db, "snapshot_bytes")?;
            let meta: Option<SnapshotMeta<u64, openraft::BasicNode>> =
                read_meta(&w.db, "snapshot_meta")?;
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
                        snapshot: Box::new(Cursor::new(b)),
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
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>> {
        self.call(|w| {
            w.state_transaction(|t, s| {
                let last: Option<LogId<u64>> = read_meta(t, "applied")?;
                let membership: StoredMembership<u64, openraft::BasicNode> =
                    read_meta(t, "membership")?.unwrap_or_default();
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
                    snapshot_id: format!("sqlite-{}", last.map_or(0, |x| x.index)),
                };
                put_meta_raw(t, "snapshot_bytes", &bytes)?;
                put_meta(t, "snapshot_meta", &meta)?;
                Ok(Snapshot {
                    meta,
                    snapshot: Box::new(Cursor::new(bytes)),
                })
            })
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        mismatched.last_log_id = Some(LogId::new(openraft::CommittedLeaderId::new(1, 1), 1));

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
