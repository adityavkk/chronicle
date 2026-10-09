use super::*;
use futures_util::{Stream, StreamExt};
use openraft::storage::{EntryResponder, RaftStateMachine};
use openraft::{RaftLogReader, RaftSnapshotBuilder};
use openraft_legacy::network_v1::SnapshotReceiverFactory;
use sha2::{Digest, Sha256};
use std::io::{Read, Seek, SeekFrom, Write};
use tokio::sync::{Mutex, RwLock};

pub struct View {
    pub store: Arc<Store>,
    pub applied: Option<LogId>,
    pub membership: StoredMembership,
    pub subscriptions: subscriptions::State,
    pub forks: forks::State,
    pub(super) receipts: receipts::State,
}

pub struct Machine {
    pub view: RwLock<View>,
    pub journal: Arc<journal::Journal>,
    pub dir: PathBuf,
    // Lock order: snapshots -> view -> journal. Apply never takes snapshots.
    snapshots: Mutex<()>,
}

impl Machine {
    pub async fn open(dir: PathBuf, journal: Arc<journal::Journal>) -> io::Result<Arc<Self>> {
        std::fs::create_dir_all(&dir)?;
        let snapshot = journal.index.lock().unwrap().snapshot.clone();
        // Old materializations are disposable, unlike WAL and snapshot files.
        // No live readers exist during boot; only then collect installed old
        // generations whose path-based native readers might still have used.
        for entry in std::fs::read_dir(&dir)? {
            let path = entry?.path();
            if path.file_name().unwrap().to_str().is_some_and(|n| n.starts_with("hot-")) {
                std::fs::remove_dir_all(path)?;
            }
        }
        let hot = dir.join("hot");
        if hot.exists() {
            std::fs::remove_dir_all(&hot)?;
        }
        let (applied, membership, time, subscriptions, forks, receipts) = if let Some(snapshot) = snapshot {
            let file = dir.join(&snapshot.file);
            if digest_file(&file)? != snapshot.sha256 {
                return Err(io::Error::other("snapshot digest mismatch"));
            }
            let (time, subscriptions, forks, receipts) = unpack(&file, &hot, &snapshot.meta)?;
            (
                snapshot.meta.last_log_id,
                snapshot.meta.last_membership,
                time,
                subscriptions,
                forks,
                receipts,
            )
        } else {
            (
                None,
                StoredMembership::default(),
                0,
                subscriptions::State::default(),
                forks::State::default(),
                receipts::State::default(),
            )
        };
        let store = Arc::new(Store::new_with_tier(
            hot,
            crate::tier::TierConfig::default(),
        )?);
        store.clock.advance(time);
        let machine = Arc::new(Self {
            view: RwLock::new(View {
                store,
                applied,
                membership,
                subscriptions,
                forks,
                receipts,
            }),
            journal,
            dir,
            snapshots: Mutex::new(()),
        });
        let committed = machine.journal.index.lock().unwrap().committed;
        if let Some(committed) = committed {
            let mut from = applied.map(|id| id.index + 1).unwrap_or(0);
            let mut reader = machine.journal.clone();
            while from <= committed.index {
                let end = (from + 64).min(committed.index + 1);
                let entries = reader
                    .try_get_log_entries(from..end)
                    .await
                    .map_err(io::Error::other)?;
                if entries.len() as u64 != end - from {
                    return Err(io::Error::other("committed recovery log gap"));
                }
                machine.apply_committed(entries).await?;
                from = end;
            }
        }
        machine.cleanup_snapshots()?;
        Ok(machine)
    }

    /// Credit recovery is about a retained suffix, not terminal receipt proof.
    /// A truncated entry frees storage admission even if its outcome is unknown.
    pub async fn unresolved(&self, ids: &[LogId]) -> bool {
        let view = self.view.read().await;
        ids.iter().any(|id| view.applied.is_none_or(|a| a.index < id.index)
            && self.journal.id_at(id.index) == Some(*id))
    }

    /// Caller is either booting privately or holds snapshots after a successful
    /// durable reference. Never run alongside another builder or installation.
    fn cleanup_snapshots(&self) -> io::Result<()> {
        let index = self.journal.index.lock().unwrap();
        let current = index.snapshot.as_ref().map(|s| s.file.as_str());
        let mut removed = false;
        for entry in std::fs::read_dir(&self.dir)? {
            let path = entry?.path();
            let name = path.file_name().unwrap().to_str();
            if name.is_some_and(|n| n.starts_with("snapshot-")) && name != current {
                std::fs::remove_file(path)?;
                removed = true;
            }
        }
        if removed { crate::store::fsync_parent_dir(&self.dir.join("snapshot"))?; }
        Ok(())
    }

    /// Materialize one ordered committed cohort. The durable publication marker
    /// precedes every native handler and all client responses to this cohort.
    pub(super) async fn apply_committed<I>(&self, entries: I) -> io::Result<Vec<Vec<Reply>>>
    where
        I: IntoIterator<Item = Entry> + Send,
        I::IntoIter: Send,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        let mut replies = Vec::new();
        let lock_probe = timing::APPLY_LOCK.start();
        let mut view = self.view.write().await;
        drop(lock_probe);
        let mut previous = view.applied;
        for entry in &entries {
            if previous.is_some_and(|id| entry.log_id.index <= id.index) {
                return Err(storage_error(io::Error::other(
                    "duplicate/out-of-order apply",
                )));
            }
            previous = Some(entry.log_id);
        }
        if let Some(last) = entries.last() {
            // This same native-WAL barrier used to block the Raft core's
            // save_committed. Keep it BEFORE any handler (including wakeups),
            // clock or membership mutation, but run it on the ordered SM worker.
            self.journal.cover_apply(last.log_id).await.map_err(storage_error)?;
        }
        let _apply_probe = timing::APPLY.start();
        for entry in entries {
            let reply = match entry.payload {
                EntryPayload::Normal(batch) => {
                    view.store.set_create_id(entry.log_id.index);
                    let mut output = Vec::with_capacity(batch.commands.len());
                    for (ordinal, command) in batch.commands.into_iter().enumerate() {
                        let receipt = command.local();
                        view.store.clock.advance(command.time);
                        let resp = if command.method == "SUB" {
                            let action = serde_json::from_slice(&command.body)
                                .map_err(|e| storage_error(io::Error::other(e)))?;
                            let now = clock::millis(view.store.clock.now());
                            view.subscriptions
                                .apply(&command.path, action, entry.log_id.index, now)
                        } else if command.method == "FORK" {
                            let action = bincode::deserialize(&command.body)
                                .map_err(|e| storage_error(io::Error::other(e)))?;
                            let store = view.store.clone();
                            view.forks
                                .apply(&store, &command.path, action, entry.log_id.index)
                                .await
                                .map_err(storage_error)?
                        } else if view.forks.pending(&command.path)
                            || command
                                .headers
                                .iter()
                                .any(|(k, p)| k == "stream-forked-from" && view.forks.pending(p))
                        {
                            response(409, "fork name reserved; materialization pending")
                        } else if command.method == "TOUCH" || command.method == "TICK" {
                            if let Some(stream) = view.store.get(&command.path) {
                                if command.method == "TOUCH"
                                    && !stream.shared.read().unwrap().soft_deleted
                                {
                                    stream.touch();
                                }
                            }
                            Resp::new(204)
                        } else {
                            crate::handlers::handle(
                                view.store.clone(),
                                Req {
                                    method: Method::parse(&command.method),
                                    path: command.path,
                                    query: None,
                                    headers: command.headers,
                                    body: command.body.into(),
                                },
                            )
                            .await
                        };
                        if resp.status >= 500 {
                            return Err(storage_error(io::Error::other(
                                "native committed apply failed",
                            )));
                        }
                        let reply = Reply::from_resp(resp);
                        if receipt {
                            view.receipts.record(receipts::Position { log_id: entry.log_id, ordinal }, reply.clone());
                        }
                        output.push(reply);
                    }
                    output
                }
                EntryPayload::Membership(membership) => {
                    view.membership = StoredMembership::new(Some(entry.log_id), membership);
                    vec![]
                }
                EntryPayload::Blank => vec![],
            };
            view.applied = Some(entry.log_id);
            replies.push(reply);
        }
        drop(view);
        self.journal.changed.notify_waiters();
        Ok(replies)
    }
}

impl RaftStateMachine<Types> for Arc<Machine> {
    type SnapshotData = tokio::fs::File;
    type SnapshotBuilder = Self;
    async fn applied_state(
        &mut self,
    ) -> io::Result<(Option<LogId>, StoredMembership)> {
        let view = self.view.read().await;
        Ok((view.applied, view.membership.clone()))
    }
    async fn apply<S>(&mut self, entries: S) -> io::Result<()>
    where S: Stream<Item = io::Result<EntryResponder<Types>>> + Unpin + Send {
        // Bound replay memory and amortize publication fsync over ready entries.
        // No responder is completed until the durable marker AND native apply.
        let mut cohorts = entries.ready_chunks(64);
        while let Some(cohort) = cohorts.next().await {
            let (entries, responders): (Vec<_>, Vec<_>) = cohort.into_iter()
                .collect::<io::Result<Vec<_>>>()?.into_iter().unzip();
            let replies = self.apply_committed(entries).await?;
            for (responder, reply) in responders.into_iter().zip(replies) {
                if let Some(responder) = responder { responder.send(reply); }
            }
        }
        Ok(())
    }
    async fn get_snapshot_builder(&mut self) -> Self {
        self.clone()
    }
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: tokio::fs::File,
    ) -> io::Result<()> {
        let _snapshot = self.snapshots.lock().await;
        snapshot.sync_all().await.map_err(storage_error)?;
        drop(snapshot);
        let mut view = self.view.write().await;
        // Retain the exclusive view through durable reference publication, but
        // hand the executor's worker to Raft/network tasks during blocking I/O.
        let (reference, restored) = tokio::task::block_in_place(|| -> io::Result<_> {
            let incoming = self.dir.join("receiving");
            let generation = self.dir.join(format!("hot-{}", nonce()));
            let (time, subscriptions, forks, receipts) = unpack(&incoming, &generation, meta)?;
            let store = Arc::new(Store::new_with_tier(generation, crate::tier::TierConfig::default())?);
            store.clock.advance(time);
            let file = format!("snapshot-{}", nonce());
            std::fs::rename(&incoming, self.dir.join(&file))?;
            crate::store::fsync_parent_dir(&self.dir.join(&file))?;
            let reference = journal::SnapshotRef {
                meta: meta.clone(),
                sha256: digest_file(&self.dir.join(&file))?,
                file,
            };
            Ok((reference, View {
                store,
                applied: meta.last_log_id,
                membership: meta.last_membership.clone(),
                subscriptions,
                forks,
                receipts,
            }))
        }).map_err(storage_error)?;
        self.journal
            .save_snapshot(reference)
            .await
            .map_err(storage_error)?;
        tokio::task::block_in_place(|| self.cleanup_snapshots()).map_err(storage_error)?;
        *view = restored;
        drop(view);
        self.journal.changed.notify_waiters();
        Ok(())
    }
    async fn get_current_snapshot(&mut self) -> io::Result<Option<Snapshot>> {
        // Open while pinned against cleanup; Linux descriptors survive unlink.
        let index = self.journal.index.lock().unwrap();
        match &index.snapshot {
            Some(snapshot) => Ok(Some(Snapshot {
                meta: snapshot.meta.clone(),
                snapshot: tokio::fs::File::from_std(
                    std::fs::File::open(self.dir.join(&snapshot.file)).map_err(storage_error)?,
                ),
            })),
            None => Ok(None),
        }
    }
}

impl SnapshotReceiverFactory<Types> for Arc<Machine> {
    type SnapshotReceiver = tokio::fs::File;
    async fn begin_receiving_snapshot(&mut self) -> io::Result<tokio::fs::File> {
        tokio::fs::OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(self.dir.join("receiving")).await.map_err(storage_error)
    }
}

impl RaftSnapshotBuilder<Types> for Arc<Machine> {
    type SnapshotData = tokio::fs::File;
    async fn build_snapshot(&mut self) -> io::Result<Snapshot> {
        // Serialize the lifecycle, not apply during payload copy/hash/fsync.
        // Open descriptors pin incarnations; exact lengths pin their prefixes.
        let _snapshot = self.snapshots.lock().await;
        let view = self.view.write().await;
        let _probe = timing::SNAPSHOT.start();
        let cut = tokio::task::block_in_place(|| SnapshotCut::capture(&view)).map_err(storage_error)?;
        drop(view);
        let meta = cut.meta.clone();
        let file = format!("snapshot-{}", nonce());
        let target = self.dir.join(&file);
        let sha256 = tokio::task::block_in_place(|| {
            cut.write(&target)?;
            digest_file(&target)
        }).map_err(storage_error)?;
        self.journal
            .save_snapshot(journal::SnapshotRef {
                meta: meta.clone(),
                file,
                sha256,
            })
            .await
            .map_err(storage_error)?;
        tokio::task::block_in_place(|| self.cleanup_snapshots()).map_err(storage_error)?;
        Ok(Snapshot {
            meta,
            snapshot: tokio::fs::File::open(target).await.map_err(storage_error)?,
        })
    }
}

fn nonce() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

fn hash_prefix(file: &mut std::fs::File, len: u64) -> io::Result<[u8; 32]> {
    file.rewind()?;
    let mut hash = Sha256::new();
    let mut remaining = len;
    let mut buffer = [0u8; 64 * 1024];
    while remaining > 0 {
        let n = remaining.min(buffer.len() as u64) as usize;
        file.read_exact(&mut buffer[..n])?;
        hash.update(&buffer[..n]);
        remaining -= n as u64;
    }
    Ok(hash.finalize().into())
}

pub fn digest_file(path: &std::path::Path) -> io::Result<String> {
    let mut file = std::fs::File::open(path)?;
    let len = file.metadata()?.len();
    Ok(hash_prefix(&mut file, len)?
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}

fn pack_entry(
    out: &mut std::fs::File,
    path: &std::path::Path,
    source: impl Read,
    len: u64,
) -> io::Result<()> {
    let name = path.file_name().unwrap().to_str()
        .ok_or_else(|| io::Error::other("invalid snapshot filename"))?.as_bytes();
    out.write_all(&(name.len() as u32).to_le_bytes())?;
    out.write_all(&len.to_le_bytes())?;
    out.write_all(name)?;
    if io::copy(&mut source.take(len), out)? != len {
        return Err(io::Error::other("short snapshot source"));
    }
    Ok(())
}

pub(super) struct SnapshotCut {
    pub meta: SnapshotMeta,
    time: u64,
    files: Vec<(PathBuf, std::fs::File, u64)>,
    metadata: Vec<(PathBuf, crate::store::Meta)>,
    subscriptions: subscriptions::State,
    forks: forks::State,
    receipts: receipts::State,
}

impl SnapshotCut {
    /// Caller holds the exclusive view. No file path or mutable state is
    /// consulted after this returns. Cold tier/compaction is disabled here.
    pub fn capture(view: &View) -> io::Result<Self> {
        let _probe = timing::SNAPSHOT_CUT.start();
        let mut streams: Vec<_> = view.store.streams.iter().map(|s| s.value().clone()).collect();
        streams.sort_by(|a, b| a.file_path.cmp(&b.file_path));
        let mut paths = vec![view.store.data_dir.join("streams/.lanes")];
        paths.extend(streams.iter().map(|s| s.file_path.clone()));
        let files = paths.into_iter().map(|path| {
            // Opening anew gives the copier its own cursor, not the appender's.
            let file = std::fs::File::open(&path)?;
            let len = file.metadata()?.len();
            Ok((path, file, len))
        }).collect::<io::Result<Vec<_>>>()?;
        Ok(Self {
            meta: SnapshotMeta { last_log_id: view.applied, last_membership: view.membership.clone() },
            time: clock::millis(view.store.clock.now()),
            files,
            metadata: streams.iter().map(|s|
                (crate::store::meta_path(&s.file_path), crate::store::Meta::capture(s))).collect(),
            subscriptions: view.subscriptions.clone(),
            forks: view.forks.clone(),
            receipts: view.receipts.clone(),
        })
    }

    pub fn write(self, path: &std::path::Path) -> io::Result<()> {
        // Spool potentially large control state instead of a second JSON-sized
        // memory copy. The lifecycle lock protects this unreferenced temporary;
        // recovery cleanup collects leftovers from interrupted copies.
        let control = path.with_extension("control");
        let mut writer = io::BufWriter::new(std::fs::File::create(&control)?);
        serde_json::to_writer(&mut writer, &(self.subscriptions, self.forks, self.receipts))
            .map_err(io::Error::other)?;
        writer.flush()?;
        drop(writer);
        let mut out = std::fs::OpenOptions::new().read(true).write(true).create_new(true).open(path)?;
        out.write_all(b"ERSP0007")?;
        let header = serde_json::to_vec(&(self.meta, self.files.len() + self.metadata.len() + 1, self.time))
            .map_err(io::Error::other)?;
        out.write_all(&(header.len() as u64).to_le_bytes())?;
        out.write_all(&header)?;
        let file = std::fs::File::open(&control)?;
        let len = file.metadata()?.len();
        pack_entry(&mut out, std::path::Path::new(".control"), file, len)?;
        std::fs::remove_file(control)?;
        for (path, file, len) in self.files {
            pack_entry(&mut out, &path, file, len)?;
        }
        for (path, meta) in self.metadata {
            let data = serde_json::to_vec(&meta).map_err(io::Error::other)?;
            pack_entry(&mut out, &path, data.as_slice(), data.len() as u64)?;
        }
        let len = out.stream_position()?;
        let digest = hash_prefix(&mut out, len)?;
        out.seek(SeekFrom::End(0))?;
        out.write_all(&digest)?;
        out.sync_all()?;
        crate::store::fsync_parent_dir(path)
    }
}

fn unpack(
    path: &std::path::Path,
    target: &std::path::Path,
    expected: &SnapshotMeta,
) -> io::Result<(u64, subscriptions::State, forks::State, receipts::State)> {
    let mut input = std::fs::File::open(path)?;
    let len = input.metadata()?.len();
    if !(48..=8 * 1024 * 1024 * 1024).contains(&len) {
        return Err(io::Error::other("snapshot size outside bound"));
    }
    let hash = hash_prefix(&mut input, len - 32)?;
    let mut footer = [0; 32];
    input.read_exact(&mut footer)?;
    if hash != footer {
        return Err(io::Error::other("snapshot checksum failed"));
    }
    input.rewind()?;
    let mut magic = [0; 8];
    input.read_exact(&mut magic)?;
    if &magic != b"ERSP0007" {
        return Err(io::Error::other("snapshot version"));
    }
    let mut length = [0; 8];
    input.read_exact(&mut length)?;
    let n = u64::from_le_bytes(length);
    if n > 1024 * 1024 {
        return Err(io::Error::other("snapshot header too large"));
    }
    let mut header = vec![0; n as usize];
    input.read_exact(&mut header)?;
    let (meta, count, time): (SnapshotMeta, usize, u64) =
        serde_json::from_slice(&header).map_err(io::Error::other)?;
    if &meta != expected {
        return Err(io::Error::other("snapshot metadata mismatch"));
    }
    let streams = target.join("streams");
    std::fs::create_dir_all(&streams)?;
    for _ in 0..count {
        let mut name_len = [0; 4];
        input.read_exact(&mut name_len)?;
        let n = u32::from_le_bytes(name_len);
        input.read_exact(&mut length)?;
        let size = u64::from_le_bytes(length);
        if n == 0 || n > 255 || size > len {
            return Err(io::Error::other("snapshot file bound"));
        }
        let mut name = vec![0; n as usize];
        input.read_exact(&mut name)?;
        let name = std::str::from_utf8(&name).map_err(io::Error::other)?;
        if name == "." || name == ".." || name.contains('/') || name.contains('\\') {
            return Err(io::Error::other("unsafe snapshot path"));
        }
        let mut out = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(streams.join(name))?;
        if io::copy(&mut (&mut input).take(size), &mut out)? != size {
            return Err(io::Error::other("short snapshot file"));
        }
    }
    if input.stream_position()? != len - 32 {
        return Err(io::Error::other("snapshot trailing bytes"));
    }
    let control = io::BufReader::new(std::fs::File::open(streams.join(".control"))?);
    let (subscriptions, forks, receipts) = serde_json::from_reader(control).map_err(io::Error::other)?;
    Ok((time, subscriptions, forks, receipts))
}
