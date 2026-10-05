//! File-controlled storage fault gates for subprocess crash tests.
//!
//! This module only exists with `storage-faults`. Set `CHRONICLE_FAULT_DIR` to an
//! explicit test directory. For store `group.sqlite`, gate `NAME` is armed by
//! creating `<dir>/<hex filename>/<NAME>.arm`. Once reached, the blocking storage
//! actor durably creates `<NAME>.reached` and waits until `<NAME>.release` exists.
//! It then creates `<NAME>.resumed`; wait for that marker before deleting release.
//! The actor never removes control files (in particular, it cannot unlink locks).
//! Alternatively, `<NAME>.error` returns an injected I/O error after writing the
//! reached marker, without waiting. It remains effective across process restart
//! until the external harness removes it. This is not a simulated power failure.

use std::ffi::OsStr;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub const AFTER_LOG_COMMIT: &str = "after-log-commit-before-log-flushed";
pub const BEFORE_STATE_COMMIT: &str = "before-state-transaction-commit";
pub const AFTER_APPLY_COMMIT: &str = "after-apply-commit-before-return";
pub const BEFORE_SNAPSHOT_INSTALL: &str = "before-snapshot-install-transaction";
pub const AFTER_SNAPSHOT_INSTALL: &str = "after-snapshot-install-transaction";
/// Pauses the storage actor before validating/materializing a captured read range.
pub const BEFORE_PROJECTION_OPEN: &str = "before-projection-open";
/// Uses the synthetic filename `http-body`; pauses the blocking file reader,
/// not the SQLite actor. The harness can then truncate only its disposable cache.
pub const BEFORE_BODY_READ: &str = "before-response-file-read";
/// Pause only after a live-read deadline, before its fresh strict read. The
/// synthetic filename is `http-live-SHARD`; only the external harness releases it.
pub const BEFORE_LIVE_RECHECK: &str = "before-live-timeout-recheck";

pub async fn before_live_recheck(
    shard: u64,
    admission: [std::sync::Arc<tokio::sync::OwnedSemaphorePermit>; 2],
) -> io::Result<()> {
    tokio::task::spawn_blocking(move || {
        // Retain both request and live admission if the async caller is cancelled.
        let _admission = admission;
        Context::new(Path::new(&format!("http-live-{shard}"))).hit(BEFORE_LIVE_RECHECK)
    })
    .await
    .map_err(io::Error::other)?
}

pub struct Context {
    directory: Option<PathBuf>,
}

impl Context {
    pub(crate) fn new(store: &Path) -> Self {
        let directory = std::env::var_os("CHRONICLE_FAULT_DIR")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .map(|root| {
                root.join(encoded_filename(
                    store.file_name().unwrap_or(OsStr::new("store")),
                ))
            });
        Self { directory }
    }

    pub(crate) fn hit(&self, name: &str) -> io::Result<()> {
        let Some(directory) = &self.directory else {
            return Ok(());
        };
        let fail = directory.join(format!("{name}.error")).is_file();
        if !fail && !directory.join(format!("{name}.arm")).is_file() {
            return Ok(());
        }
        std::fs::create_dir_all(directory)?;
        let reached = std::fs::File::create(directory.join(format!("{name}.reached")))?;
        reached.sync_all()?;
        std::fs::File::open(directory)?.sync_all()?;
        if fail {
            return Err(io::Error::other(format!(
                "injected storage fault at {name}"
            )));
        }
        while !directory.join(format!("{name}.release")).is_file() {
            std::thread::sleep(Duration::from_millis(5));
        }
        std::fs::File::create(directory.join(format!("{name}.resumed")))?;
        Ok(())
    }
}

/// Return the gate directory used by a store, so a subprocess harness can arm,
/// observe, and release gates without sharing any in-process state.
pub fn store_directory(root: &Path, store: &Path) -> PathBuf {
    root.join(encoded_filename(
        store.file_name().unwrap_or(OsStr::new("store")),
    ))
}

#[cfg(unix)]
fn encoded_filename(name: &OsStr) -> String {
    use std::os::unix::ffi::OsStrExt;
    encode_bytes(name.as_bytes())
}

#[cfg(not(unix))]
fn encoded_filename(name: &OsStr) -> String {
    encode_bytes(name.to_string_lossy().as_bytes())
}

fn encode_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0xf) as usize] as char);
    }
    encoded
}
