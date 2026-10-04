use axum::body::to_bytes;
use chronicle_raft::{
    model::{Command, StreamConfig},
    storage::{ReadError, SqliteStore},
    wire,
};
use openraft::{Entry, EntryPayload, LogId, RaftSnapshotBuilder, storage::RaftStateMachine};
use std::sync::Arc;
use tokio::sync::Semaphore;

async fn range(
    store: &SqliteStore,
    incarnation: u64,
    start: u64,
    end: u64,
) -> Result<std::fs::File, ReadError> {
    let mut view = store.read_info("s".into(), ()).await.unwrap().unwrap();
    view.incarnation = incarnation;
    view.end = end;
    store.read_file("s".into(), &view, start, ()).await
}

fn body(file: std::fs::File, length: u64, json: bool) -> axum::body::Body {
    let permit = Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap();
    wire::file_body(file, length, json, Arc::new(permit))
}

async fn apply(store: &mut SqliteStore, index: u64, command: Command) {
    let out = store
        .apply([Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(command),
        }])
        .await
        .unwrap();
    assert!(out[0].error.is_none());
}

fn create(data: &[u8], expected: Option<u64>) -> Command {
    Command::Create {
        key: "s".into(),
        expected_incarnation: expected,
        config: StreamConfig {
            content_type: "application/octet-stream".into(),
            expiry: None,
        },
        data: data.to_vec(),
        closed: false,
        now_ms: None,
    }
}

async fn bytes(file: std::fs::File, length: u64) -> Vec<u8> {
    to_bytes(body(file, length, false), 1024 * 1024)
        .await
        .unwrap()
        .to_vec()
}

#[tokio::test]
async fn bounded_ranges_have_independent_cursors_and_survive_recreation() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = SqliteStore::open(dir.path().join("store.db"))
        .await
        .unwrap();
    apply(&mut store, 1, create(b"abcd", None)).await;
    let old = range(&store, 1, 0, 4).await.unwrap();
    let tail = range(&store, 1, 2, 4).await.unwrap();
    apply(
        &mut store,
        2,
        Command::Append {
            key: "s".into(),
            incarnation: 1,
            data: b"efg".to_vec(),
            producer: None,
            close: false,
            empty_body: false,
            stream_seq: None,
            now_ms: None,
        },
    )
    .await;
    let extended = range(&store, 1, 0, 7).await.unwrap();
    assert_eq!(bytes(old, 4).await, b"abcd");
    assert_eq!(bytes(tail, 2).await, b"cd");
    apply(
        &mut store,
        3,
        Command::Delete {
            key: "s".into(),
            incarnation: 1,
            expired_at: None,
        },
    )
    .await;
    apply(&mut store, 4, create(b"NEW", Some(2))).await;
    assert!(matches!(
        range(&store, 1, 0, 7).await,
        Err(ReadError::Changed)
    ));
    let new = range(&store, 2, 0, 3).await.unwrap();
    assert_eq!(bytes(extended, 7).await, b"abcdefg");
    assert_eq!(bytes(new, 3).await, b"NEW");
    store.close().await;
}

#[tokio::test]
async fn restart_rebuilds_untrusted_cache_and_snapshot_replaces_inode() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let mut store = SqliteStore::open(&path).await.unwrap();
    apply(&mut store, 1, create(b"abc", None)).await;
    let snapshot = store.build_snapshot().await.unwrap();
    apply(
        &mut store,
        2,
        Command::Append {
            key: "s".into(),
            incarnation: 1,
            data: b"def".to_vec(),
            producer: None,
            close: false,
            empty_body: false,
            stream_seq: None,
            now_ms: None,
        },
    )
    .await;
    let old = range(&store, 1, 0, 6).await.unwrap();
    // Exercise replacement even with the same incarnation and a shorter prefix.
    store
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    let restored = range(&store, 1, 0, 3).await.unwrap();
    assert_eq!(bytes(old, 6).await, b"abcdef");
    assert_eq!(bytes(restored, 3).await, b"abc");
    store.close().await;
    let cache = path.with_extension("projection");
    for corrupt in [b"XXX".as_slice(), b"a".as_slice(), b"".as_slice()] {
        std::fs::write(cache.join("abandoned"), corrupt).unwrap();
        let store = SqliteStore::open_existing(&path).await.unwrap();
        assert_eq!(
            bytes(range(&store, 1, 0, 3).await.unwrap(), 3).await,
            b"abc"
        );
        assert!(!cache.join("abandoned").exists());
        store.close().await;
    }
}

#[tokio::test]
async fn cache_failure_does_not_undo_committed_appends_and_truncation_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let mut store = SqliteStore::open(&path).await.unwrap();
    let cache = path.with_extension("projection");
    std::fs::write(&cache, b"not a directory").unwrap();
    apply(&mut store, 1, create(b"durable", None)).await;
    assert!(matches!(
        range(&store, 1, 0, 7).await,
        Err(ReadError::Io(_))
    ));
    assert_eq!(
        store.read_info("s".into(), ()).await.unwrap().unwrap().end,
        7
    );
    std::fs::remove_file(&cache).unwrap();
    let reader = range(&store, 1, 0, 7).await.unwrap();
    let cached = std::fs::read_dir(&cache)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::write(&cached, b"dur").unwrap();
    assert!(to_bytes(body(reader, 7, false), 100).await.is_err());
    assert_eq!(
        bytes(range(&store, 1, 0, 7).await.unwrap(), 7).await,
        b"durable"
    );
    store.close().await;
}

#[test]
fn json_cursor_is_a_complete_value_boundary() {
    let data = br#""a,b",[1,2],true,123,"#;
    for offset in 0..=data.len() + 1 {
        assert_eq!(
            wire::json_boundary(data, offset),
            [0, 6, 12, 17, 21].contains(&offset),
            "offset {offset}"
        );
    }
}

#[tokio::test]
async fn json_delivery_frames_values_and_empty_ranges() {
    use std::io::Write;
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(br#""a,b",true,"#).unwrap();
    let response = body(std::fs::File::open(file.path()).unwrap(), 10, true);
    assert_eq!(
        to_bytes(response, 100).await.unwrap().as_ref(),
        br#"["a,b",true]"#
    );
    let response = body(std::fs::File::open(file.path()).unwrap(), 0, true);
    assert_eq!(to_bytes(response, 100).await.unwrap().as_ref(), b"[]");
}

#[tokio::test]
async fn snapshot_fences_unopened_metadata_even_for_identical_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = SqliteStore::open(dir.path().join("store.db"))
        .await
        .unwrap();
    apply(&mut store, 1, create(b"abc", None)).await;
    let view = store.read_info("s".into(), ()).await.unwrap().unwrap();
    let snapshot = store.build_snapshot().await.unwrap();
    store
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    assert!(matches!(
        store.read_file("s".into(), &view, 0, ()).await,
        Err(ReadError::Changed)
    ));
    store.close().await;
}

#[tokio::test]
async fn notifications_coalesce_applies_and_only_signal_successful_snapshot_install() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = SqliteStore::open(dir.path().join("notifications"))
        .await
        .unwrap();
    let mut changes = store.applied_changes();
    assert!(!changes.has_changed().unwrap());
    apply(&mut store, 1, create(b"abc", None)).await;
    apply(
        &mut store,
        2,
        Command::Append {
            key: "s".into(),
            incarnation: 1,
            data: b"XYZW".to_vec(),
            producer: None,
            close: false,
            empty_body: false,
            stream_seq: None,
            now_ms: None,
        },
    )
    .await;
    // Both applies preceded awaiting: the wake must not be lost, and the reader
    // must fetch authority rather than interpret one notification as one append.
    assert!(changes.has_changed().unwrap());
    changes.changed().await.unwrap();
    assert_eq!(
        store.read_info("s".into(), ()).await.unwrap().unwrap().end,
        7
    );
    let snapshot = store.build_snapshot().await.unwrap();
    assert!(!changes.has_changed().unwrap());
    assert!(
        store
            .install_snapshot(&snapshot.meta, Box::new(std::io::Cursor::new(vec![0])))
            .await
            .is_err()
    );
    assert!(!changes.has_changed().unwrap());
    store
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    assert!(changes.has_changed().unwrap());
    store.close().await;
}

#[test]
fn cancellation_retains_admission_until_queued_blocking_read_finishes() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let (release, blocked) = std::sync::mpsc::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            let _ = blocked.recv();
        });
        let permits = Arc::new(Semaphore::new(1));
        let permit = Arc::new(permits.clone().try_acquire_owned().unwrap());
        let live_permits = Arc::new(Semaphore::new(1));
        let live_permit = Arc::new(live_permits.clone().try_acquire_owned().unwrap());
        let file = tempfile::tempfile().unwrap();
        let mut read = Box::pin(to_bytes(
            wire::file_body(file, 1, false, [permit, live_permit]),
            100,
        ));
        assert!(futures_util::poll!(&mut read).is_pending());
        drop(read);
        assert_eq!(permits.available_permits(), 0);
        assert_eq!(live_permits.available_permits(), 0);
        release.send(()).unwrap();
        blocker.await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while permits.available_permits() == 0 || live_permits.available_permits() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    });
}
