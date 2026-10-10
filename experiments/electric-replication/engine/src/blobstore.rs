// BlobStore: the cold-tier object-storage abstraction.
//
// The hot tier stays fd-backed (the contiguous data file + sealed chunk files,
// served zero-copy via sendfile/io_uring). The BlobStore is the *remote* tier: a
// sealed segment is uploaded here, verified, and only then is its local chunk
// file unlinked. Reads of an offloaded segment go through `get_range`.
//
// Two adapters:
//   - `LocalFsBlobStore` — a chunk directory on local disk. No heavy deps; the
//     primary use is testing the full seal→offload→read-from-cold path without
//     standing up S3. Always compiled.
//   - `S3BlobStore` — a generic S3-compatible adapter over the `object_store`
//     crate, behind the `tier` Cargo feature. Works against AWS S3, Cloudflare
//     R2, Fly/Tigris, MinIO, Backblaze B2, etc. via custom endpoint + path-style
//     addressing. Only compiled with `--features tier`.
//
// The trait uses explicitly-boxed futures rather than the `async_trait` macro so
// the default build pulls in no extra proc-macro dependency.

use std::io;
use std::sync::Arc;

use bytes::Bytes;

pub type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// Object-storage backend for the cold tier. Object keys are opaque strings; the
/// manifest owns key layout. All methods run off the append hot path.
pub trait BlobStore: Send + Sync {
    /// Upload an object in full.
    fn put<'a>(&'a self, key: &'a str, body: Bytes) -> BoxFuture<'a, io::Result<()>>;
    /// Read `[start, start+len)` of an object.
    fn get_range<'a>(
        &'a self,
        key: &'a str,
        start: u64,
        len: u64,
    ) -> BoxFuture<'a, io::Result<Bytes>>;
    /// Object size, or None if it does not exist (used to verify before delete).
    fn head<'a>(&'a self, key: &'a str) -> BoxFuture<'a, io::Result<Option<u64>>>;
    /// Delete an object. Missing-object is not an error (idempotent).
    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, io::Result<()>>;

    /// Replicated inventories can exceed one payload segment. Upload from the
    /// current file cursor with bounded buffers, never read the whole file.
    #[cfg(feature = "replication")]
    fn put_file<'a>(&'a self, _key: &'a str, _file: std::fs::File) -> BoxFuture<'a, io::Result<()>> {
        Box::pin(async { Err(io::Error::new(io::ErrorKind::Unsupported, "streamed object upload unsupported")) })
    }

    /// Strictly increasing opaque keys, exclusive of `after`. At most `limit`
    /// keys; an empty page ends this round, NOT all future rounds. S3 general
    /// purpose buckets support this; unordered/directory buckets do not.
    #[cfg(feature = "replication")]
    fn list_page<'a>(&'a self, _prefix: &'a str, _after: &'a str, _limit: usize)
        -> BoxFuture<'a, io::Result<Vec<String>>>
    {
        Box::pin(async { Err(io::Error::new(io::ErrorKind::Unsupported, "ordered object listing unsupported")) })
    }
}

/// Shared handle to a BlobStore.
pub type SharedBlobStore = Arc<dyn BlobStore>;

/// Validate a `[start, start+len)` byte-range request and return it as a usize
/// `start..end`. Rejects `u64` overflow and ranges that exceed this platform's
/// `usize` — defence against a corrupt/hostile manifest minting a wrapped or
/// truncated range (which could otherwise read the wrong bytes or over-allocate).
fn checked_range(start: u64, len: u64) -> io::Result<std::ops::Range<usize>> {
    let end = start
        .checked_add(len)
        .ok_or_else(|| io::Error::other("blob range overflow"))?;
    let s = usize::try_from(start).map_err(|_| io::Error::other("blob range start too large"))?;
    let e = usize::try_from(end).map_err(|_| io::Error::other("blob range end too large"))?;
    Ok(s..e)
}

// ---------------- local filesystem adapter ----------------

/// Filesystem object-store fixture. Hex path components preserve opaque keys
/// without slash/underscore aliases, traversal or NAME_MAX-sized components.
/// This layout intentionally does not open the old, ambiguous flat test layout.
pub struct LocalFsBlobStore {
    dir: std::path::PathBuf,
}

impl LocalFsBlobStore {
    pub fn new(dir: std::path::PathBuf) -> io::Result<Self> {
        std::fs::create_dir_all(&dir)?;
        let dir = std::fs::canonicalize(dir)?;
        // The configured root may itself have newly created ancestors. Complete
        // their durability before any successful PUT can rely on this namespace.
        for ancestor in dir.ancestors() {
            std::fs::File::open(ancestor)?.sync_all()?;
        }
        Ok(LocalFsBlobStore { dir })
    }

    fn path_for(&self, key: &str) -> std::path::PathBuf {
        use std::fmt::Write;
        let mut path = self.dir.clone();
        for chunk in key.as_bytes().chunks(60) {
            let mut component = String::with_capacity(chunk.len() * 2);
            for byte in chunk {
                write!(component, "{byte:02x}").unwrap();
            }
            path.push(component);
        }
        path.join("blob")
    }

    fn sync_directories(root: &std::path::Path, from: &std::path::Path) -> io::Result<()> {
        for dir in from.ancestors() {
            std::fs::File::open(dir)?.sync_all()?;
            if dir == root {
                return Ok(());
            }
        }
        Err(io::Error::other("object path escaped its root"))
    }

    fn put_source(&self, key: &str, mut source: impl io::Read + Send + 'static) -> BoxFuture<'static, io::Result<()>> {
        let path = self.path_for(key);
        let root = self.dir.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                use std::sync::atomic::{AtomicU64, Ordering};
                static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);
                let parent = path.parent().unwrap();
                std::fs::create_dir_all(parent)?;
                // Unique across concurrent requests/processes; create_new also
                // prevents PID reuse after a crash from truncating a leftover.
                let (tmp, mut file) = loop {
                    let n = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
                    let tmp = parent.join(format!(".put-{}-{n}", std::process::id()));
                    match std::fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(&tmp)
                    {
                        Ok(file) => break (tmp, file),
                        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                        Err(e) => return Err(e),
                    }
                };
                let result = (|| {
                    io::copy(&mut source, &mut file)?;
                    file.sync_all()?;
                    std::fs::rename(&tmp, &path)?;
                    Self::sync_directories(&root, parent)
                })();
                if result.is_err() {
                    let _ = std::fs::remove_file(tmp);
                }
                result
            })
            .await
            .map_err(io::Error::other)?
        })
    }
}

impl BlobStore for LocalFsBlobStore {
    fn put<'a>(&'a self, key: &'a str, body: Bytes) -> BoxFuture<'a, io::Result<()>> {
        self.put_source(key, io::Cursor::new(body))
    }

    #[cfg(feature = "replication")]
    fn put_file<'a>(&'a self, key: &'a str, file: std::fs::File) -> BoxFuture<'a, io::Result<()>> {
        self.put_source(key, file)
    }

    #[cfg(feature = "replication")]
    fn list_page<'a>(&'a self, prefix: &'a str, after: &'a str, limit: usize) -> BoxFuture<'a, io::Result<Vec<String>>> {
        let root = self.dir.clone();
        let prefix = format!("{}/", prefix.trim_end_matches('/'));
        let after = after.to_owned();
        Box::pin(async move {
            if !(1..=1000).contains(&limit) { return Err(io::Error::other("invalid list bound")); }
            tokio::task::spawn_blocking(move || {
                // The fixture's directories are hex-encoded key chunks, not S3
                // prefixes. Walk with directory iterators, keeping only the
                // smallest `limit` qualifying keys in RAM, not the full listing.
                let mut result = std::collections::BTreeSet::new();
                let mut stack = vec![(std::fs::read_dir(&root)?, String::new())];
                while let Some((entries, encoded)) = stack.last_mut() {
                    match entries.next() {
                        None => { stack.pop(); }
                        Some(entry) => {
                            let entry = entry?;
                            let name = entry.file_name();
                            let Some(name) = name.to_str() else { continue };
                            let ty = match entry.file_type() {
                                Ok(ty) => ty,
                                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                                Err(e) => return Err(e),
                            };
                            if name == "blob" && ty.is_file() {
                                let bytes = (0..encoded.len()).step_by(2)
                                    .map(|n| u8::from_str_radix(&encoded[n..n+2], 16).map_err(io::Error::other))
                                    .collect::<io::Result<Vec<_>>>()?;
                                let key = String::from_utf8(bytes).map_err(io::Error::other)?;
                                if key.starts_with(&prefix) && key > after {
                                    result.insert(key);
                                    if result.len() > limit { result.pop_last(); }
                                }
                            } else if ty.is_dir() && name.len() % 2 == 0 && name.bytes().all(|b| b.is_ascii_hexdigit()) {
                                let next = format!("{encoded}{name}");
                                match std::fs::read_dir(entry.path()) {
                                    Ok(entries) => stack.push((entries, next)),
                                    Err(e) if e.kind() == io::ErrorKind::NotFound => (),
                                    Err(e) => return Err(e),
                                }
                            }
                        }
                    }
                }
                Ok(result.into_iter().collect())
            }).await.map_err(io::Error::other)?
        })
    }

    fn get_range<'a>(
        &'a self,
        key: &'a str,
        start: u64,
        len: u64,
    ) -> BoxFuture<'a, io::Result<Bytes>> {
        let path = self.path_for(key);
        Box::pin(async move {
            let range = checked_range(start, len)?;
            tokio::task::spawn_blocking(move || {
                use std::os::unix::fs::FileExt;
                let f = std::fs::File::open(&path)?;
                let mut buf = vec![0u8; range.len()];
                f.read_exact_at(&mut buf, start)?;
                Ok(Bytes::from(buf))
            })
            .await
            .map_err(io::Error::other)?
        })
    }

    fn head<'a>(&'a self, key: &'a str) -> BoxFuture<'a, io::Result<Option<u64>>> {
        let path = self.path_for(key);
        Box::pin(async move {
            tokio::task::spawn_blocking(move || match std::fs::metadata(&path) {
                Ok(m) => Ok(Some(m.len())),
                Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e),
            })
            .await
            .map_err(io::Error::other)?
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, io::Result<()>> {
        let path = self.path_for(key);
        let root = self.dir.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                match std::fs::remove_file(&path) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e),
                }
                // Reclaim empty key directories, not only payload bytes. A
                // sibling object or active upload stops the prune. A missing
                // object still needs its closest existing parent synced: another
                // deleter may have unlinked it but not completed durability yet.
                let mut parent = path.parent().unwrap();
                while parent != root {
                    match std::fs::remove_dir(parent) {
                        Ok(()) => {}
                        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                        Err(e) if e.kind() == io::ErrorKind::DirectoryNotEmpty => break,
                        Err(e) => return Err(e),
                    }
                    parent = parent.parent().unwrap();
                }
                Self::sync_directories(&root, parent)
            })
            .await
            .map_err(io::Error::other)?
        })
    }
}

// ---------------- S3-compatible adapter (feature `tier`) ----------------

#[cfg(feature = "tier")]
pub use s3::S3BlobStore;

#[cfg(feature = "tier")]
mod s3 {
    use super::*;
    use object_store::aws::AmazonS3Builder;
    use object_store::path::Path as ObjPath;
    use object_store::{GetOptions, GetRange, ObjectStore};

    /// A generic S3-compatible BlobStore over `object_store`. Configured with a
    /// custom endpoint + path-style addressing so it targets any S3-compatible
    /// provider (AWS, Cloudflare R2, Fly/Tigris, MinIO, Backblaze B2), not just
    /// AWS.
    pub struct S3BlobStore {
        inner: object_store::aws::AmazonS3,
    }

    impl S3BlobStore {
        pub fn new(cfg: &crate::tier::TierConfig) -> io::Result<Self> {
            let bucket = cfg
                .bucket
                .as_ref()
                .ok_or_else(|| io::Error::other("--tier-bucket is required for --tier s3"))?;
            let mut b = AmazonS3Builder::new().with_bucket_name(bucket);
            if let Some(ep) = &cfg.endpoint {
                b = b.with_endpoint(ep.clone());
            }
            if let Some(region) = &cfg.region {
                b = b.with_region(region.clone());
            }
            // Path-style addressing (vs virtual-hosted) — required by MinIO and
            // most S3-compatible providers when using a custom endpoint.
            b = b.with_virtual_hosted_style_request(!cfg.path_style);
            if cfg.allow_http {
                b = b.with_allow_http(true);
            }
            if let (Some(k), Some(s)) = (&cfg.access_key_id, &cfg.secret_access_key) {
                b = b
                    .with_access_key_id(k.clone())
                    .with_secret_access_key(s.clone());
            }
            if cfg.consensus_managed {
                // Directory buckets do not implement ordered StartAfter scans.
                if bucket.ends_with("--x-s3") { return Err(io::Error::other("cold GC requires an ordered general-purpose bucket")); }
                b = b.with_client_options(object_store::ClientOptions::new()
                    .with_allow_http(cfg.allow_http)
                    .with_connect_timeout(std::time::Duration::from_secs(2))
                    .with_timeout(std::time::Duration::from_secs(10)))
                    .with_retry(object_store::RetryConfig {
                        max_retries: 2, retry_timeout: std::time::Duration::from_secs(20),
                        ..Default::default()
                    });
            }
            let inner = b.build().map_err(io::Error::other)?;
            Ok(S3BlobStore { inner })
        }
    }

    impl BlobStore for S3BlobStore {
        fn put<'a>(&'a self, key: &'a str, body: Bytes) -> BoxFuture<'a, io::Result<()>> {
            let path = ObjPath::from(key);
            Box::pin(async move {
                self.inner
                    .put(&path, body.into())
                    .await
                    .map(|_| ())
                    .map_err(io::Error::other)
            })
        }

        #[cfg(feature = "replication")]
        fn put_file<'a>(&'a self, key: &'a str, file: std::fs::File) -> BoxFuture<'a, io::Result<()>> {
            Box::pin(async move {
                use tokio::io::AsyncReadExt;
                let mut file = tokio::fs::File::from_std(file);
                let mut upload = self.inner.put_multipart(&ObjPath::from(key)).await.map_err(io::Error::other)?;
                let result = async {
                    for _ in 0..10_000 {
                        let mut bytes = vec![0; 8 * 1024 * 1024];
                        let mut filled = 0;
                        while filled < bytes.len() {
                            let n = file.read(&mut bytes[filled..]).await?;
                            if n == 0 { break; }
                            filled += n;
                        }
                        if filled == 0 { return upload.complete().await.map(|_| ()).map_err(io::Error::other); }
                        let last = filled < bytes.len();
                        bytes.truncate(filled);
                        upload.put_part(bytes.into()).await.map_err(io::Error::other)?;
                        if last { return upload.complete().await.map(|_| ()).map_err(io::Error::other); }
                    }
                    Err(io::Error::other("multipart part bound exceeded"))
                }.await;
                // Abort is cleanup, NOT rollback of an unknown completion.
                // Process death needs the bucket's incomplete-multipart policy.
                if result.is_err() { let _ = upload.abort().await; }
                result
            })
        }

        #[cfg(feature = "replication")]
        fn list_page<'a>(&'a self, prefix: &'a str, after: &'a str, limit: usize) -> BoxFuture<'a, io::Result<Vec<String>>> {
            Box::pin(async move {
                use futures_util::TryStreamExt;
                if !(1..=1000).contains(&limit) { return Err(io::Error::other("invalid list bound")); }
                let prefix = ObjPath::from(prefix.trim_end_matches('/'));
                let offset = ObjPath::from(after);
                let mut stream = if after.is_empty() { self.inner.list(Some(&prefix)) }
                    else { self.inner.list_with_offset(Some(&prefix), &offset) };
                let mut page: Vec<String> = Vec::with_capacity(limit);
                while page.len() < limit {
                    let Some(object) = stream.try_next().await.map_err(io::Error::other)? else { break };
                    let key = object.location.to_string();
                    let previous = page.last().map_or(after, String::as_str);
                    if key.as_str() <= previous || !key.starts_with(&format!("{prefix}/")) {
                        return Err(io::Error::other("provider violated ordered prefix listing"));
                    }
                    page.push(key);
                }
                Ok(page)
            })
        }

        fn get_range<'a>(
            &'a self,
            key: &'a str,
            start: u64,
            len: u64,
        ) -> BoxFuture<'a, io::Result<Bytes>> {
            let path = ObjPath::from(key);
            Box::pin(async move {
                let range = super::checked_range(start, len)?;
                let opts = GetOptions {
                    range: Some(GetRange::Bounded(range)),
                    ..Default::default()
                };
                let res = self
                    .inner
                    .get_opts(&path, opts)
                    .await
                    .map_err(io::Error::other)?;
                res.bytes().await.map_err(io::Error::other)
            })
        }

        fn head<'a>(&'a self, key: &'a str) -> BoxFuture<'a, io::Result<Option<u64>>> {
            let path = ObjPath::from(key);
            Box::pin(async move {
                match self.inner.head(&path).await {
                    Ok(meta) => Ok(Some(meta.size as u64)),
                    Err(object_store::Error::NotFound { .. }) => Ok(None),
                    Err(e) => Err(io::Error::other(e)),
                }
            })
        }

        fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, io::Result<()>> {
            let path = ObjPath::from(key);
            Box::pin(async move {
                match self.inner.delete(&path).await {
                    Ok(()) => Ok(()),
                    Err(object_store::Error::NotFound { .. }) => Ok(()),
                    Err(e) => Err(io::Error::other(e)),
                }
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(48))]
        #[test]
        fn opaque_keys_ranges_and_reclamation_survive_reopen(
            prefix in "[a-zA-Z0-9]{1,15}",
            operations in prop::collection::vec(
                (0usize..6, 0u8..4, prop::collection::vec(any::<u8>(), 0..1024)), 1..40),
        ) {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let root = tempfile::tempdir().unwrap();
                let dir = root.path().join("objects");
                let keys = [format!("{prefix}/s_a"), format!("{prefix}_s/a"),
                    format!("{prefix}/s.bin/1"), format!("{prefix}/s.bin/2"),
                    format!("../{prefix}/./λ?"), format!("{prefix}/{}", "λ".repeat(180))];
                let mut expected = std::collections::BTreeMap::from([
                    (0usize,vec![17,3,7]), (1usize,vec![59])]);
                let mut bs = LocalFsBlobStore::new(dir.clone()).unwrap();
                for (&index,data) in &expected {
                    bs.put(&keys[index],Bytes::copy_from_slice(data)).await.unwrap();
                }
                // Different keys must not alias even when '/' and '_' move.
                assert_eq!(bs.head(&keys[0]).await.unwrap(),Some(3));
                for (index,action,data) in operations {
                    match action {
                        0 | 1 => {
                            bs.put(&keys[index],Bytes::copy_from_slice(&data)).await.unwrap();
                            expected.insert(index,data);
                        }
                        2 => { bs.delete(&keys[index]).await.unwrap(); expected.remove(&index); }
                        _ => { bs = LocalFsBlobStore::new(dir.clone()).unwrap(); }
                    }
                    for (i,key) in keys.iter().enumerate() {
                        assert_eq!(bs.head(key).await.unwrap(),expected.get(&i).map(|d|d.len() as u64));
                        if let Some(data) = expected.get(&i) {
                            let start = data.len()/3;
                            assert_eq!(bs.get_range(key,start as u64,(data.len()-start) as u64)
                                .await.unwrap().as_ref(),&data[start..]);
                            assert!(bs.get_range(key,data.len() as u64,1).await.is_err());
                        }
                    }
                }
                for key in &keys {
                    bs.delete(key).await.unwrap();
                    bs.delete(key).await.unwrap();
                }
                assert_eq!(std::fs::read_dir(&dir).unwrap().count(),0,
                    "retired keys must reclaim temporary files and empty key directories");
            });
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_immutable_puts_do_not_share_temporary_files() {
        let root = tempfile::tempdir().unwrap();
        let bs = Arc::new(LocalFsBlobStore::new(root.path().join("objects")).unwrap());
        let mut writes = tokio::task::JoinSet::new();
        for i in 0..24usize {
            let bs = bs.clone();
            writes.spawn(async move {
                let key = format!("shared/stream.bin/{:016}", i % 3);
                bs.put(&key, Bytes::from(vec![31 + (i % 3) as u8; 1027 + i % 3]))
                    .await
                    .unwrap();
            });
        }
        while let Some(result) = writes.join_next().await {
            result.unwrap();
        }
        for i in 0..3usize {
            let key = format!("shared/stream.bin/{i:016}");
            assert_eq!(bs.head(&key).await.unwrap(), Some((1027 + i) as u64));
            assert_eq!(
                bs.get_range(&key, 13, (1014 + i) as u64)
                    .await
                    .unwrap()
                    .as_ref(),
                vec![31 + i as u8; 1014 + i]
            );
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn local_storage_errors_never_acknowledge_durability() {
        // Real syscalls in a fresh process: no global environment mutation or
        // fake BlobStore. A returned PUT/DELETE error is an unknown outcome,
        // never permission for the caller to discard its last durable copy.
        if let Ok(mode) = std::env::var("DS_BLOB_FAULT_CHILD") {
            let root = std::path::PathBuf::from(std::env::var("DS_TEST_DATA_ROOT").unwrap());
            let flag = std::path::PathBuf::from(std::env::var("DS_TEST_FAULT_FILE").unwrap());
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let bs = LocalFsBlobStore::new(root.join("objects")).unwrap();
                let key = "group/17/immutable.42";
                let old = Bytes::from_static(b"old committed bytes");
                let new = Bytes::from_static(b"a different complete immutable object");
                bs.put(key, old.clone()).await.unwrap();
                std::fs::write(&flag, format!("{mode} objects\n")).unwrap();
                assert!(
                    bs.put(key, new.clone()).await.is_err(),
                    "failed storage must not acknowledge PUT"
                );
                std::fs::remove_file(&flag).unwrap();
                let bs = LocalFsBlobStore::new(root.join("objects")).unwrap();
                let len = bs.head(key).await.unwrap().unwrap();
                let actual = bs.get_range(key, 0, len).await.unwrap();
                if mode == "eio-dir-sync" {
                    assert!(
                        actual == old || actual == new,
                        "unknown rename outcome is still atomic"
                    );
                } else {
                    assert_eq!(
                        actual, old,
                        "failed staging must not damage the prior object"
                    );
                }
                std::fs::write(&flag, "eio-dir-sync objects\n").unwrap();
                assert!(
                    bs.delete(key).await.is_err(),
                    "failed directory sync must not acknowledge DELETE"
                );
                std::fs::remove_file(&flag).unwrap();
                bs.delete(key).await.unwrap();
                assert_eq!(bs.head(key).await.unwrap(), None);
            });
            return;
        }
        let temp = tempfile::tempdir().unwrap();
        let interposer = temp.path().join("storage_faults.so");
        let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("scripts/storage_faults.c");
        assert!(std::process::Command::new("cc")
            .args(["-shared", "-fPIC", "-O2", "-Wall", "-Wextra", "-Werror"])
            .arg("-o")
            .arg(&interposer)
            .arg(source)
            .args(["-ldl", "-pthread"])
            .status()
            .unwrap()
            .success());
        for mode in ["enospc-write", "eio-sync", "eio-dir-sync"] {
            let root = temp.path().join(mode);
            std::fs::create_dir(&root).unwrap();
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "blobstore::tests::local_storage_errors_never_acknowledge_durability",
                    "--nocapture",
                ])
                .env("LD_PRELOAD", &interposer)
                .env("DS_BLOB_FAULT_CHILD", mode)
                .env("DS_TEST_DATA_ROOT", &root)
                .env("DS_TEST_FAULT_FILE", root.join("fault"))
                .env("DS_TEST_FAULT_LOG", root.join("syscalls.jsonl"))
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{mode}: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            let faults = std::fs::read_to_string(root.join("syscalls.jsonl")).unwrap();
            let rows: Vec<serde_json::Value> = faults
                .lines()
                .map(|s| serde_json::from_str(s).unwrap())
                .collect();
            assert_eq!(
                rows.len(),
                2,
                "both PUT and DELETE must hit real storage errors"
            );
            assert_eq!(rows[0]["fault"], mode);
            assert_eq!(rows[1]["fault"], "eio-dir-sync");
            println!("{mode}: unknown PUT plus unknown DELETE checked; two real failing syscalls");
        }
    }

    #[tokio::test]
    async fn local_blobstore_roundtrip() {
        let dir = std::env::temp_dir().join(format!("ds-blob-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let bs = LocalFsBlobStore::new(dir.clone()).unwrap();
        let key = "stream-abc/0000000000000000";
        bs.put(key, Bytes::from_static(b"hello world payload"))
            .await
            .unwrap();
        assert_eq!(bs.head(key).await.unwrap(), Some(19));
        let got = bs.get_range(key, 6, 5).await.unwrap();
        assert_eq!(&got[..], b"world");
        bs.delete(key).await.unwrap();
        assert_eq!(bs.head(key).await.unwrap(), None);
        // Idempotent delete.
        bs.delete(key).await.unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // S3-compatible adapter integration test against a real MinIO server.
    // Ignored by default — run manually after starting MinIO:
    //
    //   docker run -d --rm -p 9000:9000 -e MINIO_ROOT_USER=minioadmin \
    //     -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data
    //   # create the bucket (mc or the console at :9000), e.g. "ds-tier-test"
    //
    //   DS_S3_ACCESS_KEY_ID=minioadmin DS_S3_SECRET_ACCESS_KEY=minioadmin \
    //   cargo test --features tier s3_minio_roundtrip -- --ignored --nocapture
    #[cfg(feature = "tier")]
    #[tokio::test]
    #[ignore]
    async fn s3_minio_roundtrip() {
        let cfg = crate::tier::TierConfig {
            kind: crate::tier::TierKind::S3,
            endpoint: Some("http://127.0.0.1:9000".into()),
            region: Some("us-east-1".into()),
            bucket: Some(std::env::var("DS_S3_BUCKET").unwrap_or_else(|_| "ds-tier-test".into())),
            path_style: true,
            allow_http: true,
            access_key_id: std::env::var("DS_S3_ACCESS_KEY_ID").ok(),
            secret_access_key: std::env::var("DS_S3_SECRET_ACCESS_KEY").ok(),
            ..Default::default()
        };
        let bs = S3BlobStore::new(&cfg).expect("build S3 blobstore");
        let key = "stream-test/0000000000000042";
        let payload = Bytes::from_static(b"the quick brown fox jumps over the lazy dog");
        bs.put(key, payload.clone()).await.expect("put");
        assert_eq!(
            bs.head(key).await.expect("head"),
            Some(payload.len() as u64)
        );
        let got = bs.get_range(key, 4, 5).await.expect("get_range");
        assert_eq!(&got[..], b"quick");
        bs.delete(key).await.expect("delete");
        assert_eq!(bs.head(key).await.expect("head after delete"), None);
    }
}
