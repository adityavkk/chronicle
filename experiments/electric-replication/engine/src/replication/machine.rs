use super::*;
use openraft::storage::RaftStateMachine;
use openraft::{RaftLogReader, RaftSnapshotBuilder, Snapshot};
use sha2::{Digest, Sha256};
use std::io::{Read, Seek, SeekFrom, Write};
use tokio::sync::RwLock;

pub struct View {
    pub store: Arc<Store>,
    pub applied: Option<LogId<u64>>,
    pub membership: StoredMembership<u64, BasicNode>,
    pub subscriptions: subscriptions::State,
    pub forks: forks::State,
}

pub struct Machine {
    pub view: RwLock<View>,
    pub journal: Arc<journal::Journal>,
    pub dir: PathBuf,
}

impl Machine {
    pub async fn open(dir: PathBuf, journal: Arc<journal::Journal>) -> io::Result<Arc<Self>> {
        std::fs::create_dir_all(&dir)?;
        let snapshot = journal.index.lock().unwrap().snapshot.clone();
        // Old materializations are disposable, unlike WAL and snapshot files.
        let hot = dir.join("hot");
        if hot.exists() {
            std::fs::remove_dir_all(&hot)?;
        }
        let (applied, membership, time, subscriptions, forks) = if let Some(snapshot) = snapshot {
            let file = dir.join(&snapshot.file);
            if digest_file(&file)? != snapshot.sha256 {
                return Err(io::Error::other("snapshot digest mismatch"));
            }
            let (time, subscriptions, forks) = unpack(&file, &hot, &snapshot.meta)?;
            (
                snapshot.meta.last_log_id,
                snapshot.meta.last_membership,
                time,
                subscriptions,
                forks,
            )
        } else {
            (
                None,
                StoredMembership::default(),
                0,
                subscriptions::State::default(),
                forks::State::default(),
            )
        };
        let store = Arc::new(Store::new_with_tier(
            hot,
            crate::tier::TierConfig::default(),
        )?);
        store.clock.advance(time);
        let mut machine = Arc::new(Self {
            view: RwLock::new(View {
                store,
                applied,
                membership,
                subscriptions,
                forks,
            }),
            journal,
            dir,
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
                machine.apply(entries).await.map_err(io::Error::other)?;
                from = end;
            }
        }
        Ok(machine)
    }
}

impl RaftStateMachine<Types> for Arc<Machine> {
    type SnapshotBuilder = Self;
    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let view = self.view.read().await;
        Ok((view.applied, view.membership.clone()))
    }
    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Reply>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry> + Send,
        I::IntoIter: Send,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        let mut replies = Vec::new();
        let mut view = self.view.write().await;
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
        for entry in entries {
            let reply = match entry.payload {
                EntryPayload::Normal(command) => {
                    view.store.set_create_id(entry.log_id.index);
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
                    Reply::from_resp(resp)
                }
                EntryPayload::Membership(membership) => {
                    view.membership = StoredMembership::new(Some(entry.log_id), membership);
                    Reply::default()
                }
                EntryPayload::Blank => Reply::default(),
            };
            view.applied = Some(entry.log_id);
            replies.push(reply);
        }
        Ok(replies)
    }
    async fn get_snapshot_builder(&mut self) -> Self {
        self.clone()
    }
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<tokio::fs::File>, StorageError<u64>> {
        Ok(Box::new(
            tokio::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(self.dir.join("receiving"))
                .await
                .map_err(storage_error)?,
        ))
    }
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<tokio::fs::File>,
    ) -> Result<(), StorageError<u64>> {
        snapshot.sync_all().await.map_err(storage_error)?;
        drop(snapshot);
        let mut view = self.view.write().await;
        let incoming = self.dir.join("receiving");
        let generation = self.dir.join(format!("hot-{}", nonce()));
        let (time, subscriptions, forks) =
            unpack(&incoming, &generation, meta).map_err(storage_error)?;
        let store = Arc::new(
            Store::new_with_tier(generation, crate::tier::TierConfig::default())
                .map_err(storage_error)?,
        );
        store.clock.advance(time);
        let file = format!("snapshot-{}", nonce());
        std::fs::rename(&incoming, self.dir.join(&file)).map_err(storage_error)?;
        crate::store::fsync_parent_dir(&self.dir.join(&file)).map_err(storage_error)?;
        self.journal
            .save_snapshot(journal::SnapshotRef {
                meta: meta.clone(),
                sha256: digest_file(&self.dir.join(&file)).map_err(storage_error)?,
                file,
            })
            .await
            .map_err(storage_error)?;
        *view = View {
            store,
            applied: meta.last_log_id,
            membership: meta.last_membership.clone(),
            subscriptions,
            forks,
        };
        Ok(())
    }
    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<Types>>, StorageError<u64>> {
        let snapshot = self.journal.index.lock().unwrap().snapshot.clone();
        match snapshot {
            Some(snapshot) => Ok(Some(Snapshot {
                meta: snapshot.meta,
                snapshot: Box::new(
                    tokio::fs::File::open(self.dir.join(snapshot.file))
                        .await
                        .map_err(storage_error)?,
                ),
            })),
            None => Ok(None),
        }
    }
}

impl RaftSnapshotBuilder<Types> for Arc<Machine> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<Types>, StorageError<u64>> {
        // Serialize with apply. Payload is copied file->file in bounded buffers,
        // not collected in an in-memory JSON snapshot. This can stall group
        // writes for disk time: measure it, do not claim zero-copy snapshots.
        let view = self.view.write().await;
        let meta = SnapshotMeta {
            last_log_id: view.applied,
            last_membership: view.membership.clone(),
            snapshot_id: format!(
                "{}-{}",
                view.applied.map(|id| id.index).unwrap_or(0),
                nonce()
            ),
        };
        let file = format!("snapshot-{}", nonce());
        let target = self.dir.join(&file);
        pack(
            &view.store,
            &view.subscriptions,
            &view.forks,
            &target,
            &meta,
        )
        .map_err(storage_error)?;
        self.journal
            .save_snapshot(journal::SnapshotRef {
                meta: meta.clone(),
                file,
                sha256: digest_file(&target).map_err(storage_error)?,
            })
            .await
            .map_err(storage_error)?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(tokio::fs::File::open(target).await.map_err(storage_error)?),
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

fn pack(
    store: &Store,
    subscriptions: &subscriptions::State,
    forks: &forks::State,
    path: &std::path::Path,
    meta: &SnapshotMeta<u64, BasicNode>,
) -> io::Result<()> {
    let control = store.data_dir.join("streams/.control");
    let mut writer = io::BufWriter::new(std::fs::File::create(&control)?);
    serde_json::to_writer(&mut writer, &(subscriptions, forks)).map_err(io::Error::other)?;
    writer.flush()?;
    drop(writer);
    let mut files = vec![store.data_dir.join("streams/.lanes"), control];
    for stream in &store.streams {
        crate::store::write_meta_sync(stream.value(), false)?;
        files.push(stream.file_path.clone());
        files.push(crate::store::meta_path(&stream.file_path));
    }
    // Snapshot only live objects, not sidecars awaiting native asynchronous GC.
    files.sort();
    let mut out = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)?;
    out.write_all(b"ERSP0004")?;
    let header = serde_json::to_vec(&(meta, files.len(), clock::millis(store.clock.now())))
        .map_err(io::Error::other)?;
    out.write_all(&(header.len() as u64).to_le_bytes())?;
    out.write_all(&header)?;
    for path in files {
        let name = path
            .file_name()
            .unwrap()
            .to_str()
            .ok_or_else(|| io::Error::other("invalid snapshot filename"))?
            .as_bytes();
        let mut file = std::fs::File::open(&path)?;
        out.write_all(&(name.len() as u32).to_le_bytes())?;
        out.write_all(&file.metadata()?.len().to_le_bytes())?;
        out.write_all(name)?;
        io::copy(&mut file, &mut out)?;
    }
    let len = out.stream_position()?;
    let digest = hash_prefix(&mut out, len)?;
    out.seek(SeekFrom::End(0))?;
    out.write_all(&digest)?;
    out.sync_all()?;
    crate::store::fsync_parent_dir(path)
}

fn unpack(
    path: &std::path::Path,
    target: &std::path::Path,
    expected: &SnapshotMeta<u64, BasicNode>,
) -> io::Result<(u64, subscriptions::State, forks::State)> {
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
    if &magic != b"ERSP0004" {
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
    let (meta, count, time): (SnapshotMeta<u64, BasicNode>, usize, u64) =
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
    let (subscriptions, forks) = serde_json::from_reader(control).map_err(io::Error::other)?;
    Ok((time, subscriptions, forks))
}
