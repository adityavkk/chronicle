use std::ffi::CString;

use chronicle_raft::{Entry, LogId, model, storage::SqliteStore};
use openraft::storage::RaftStateMachine;
use openraft::vote::{RaftLeaderId, leader_id_adv::CommittedLeaderId};
use openraft::{EntryPayload, RaftSnapshotBuilder};

unsafe extern "C" {
    fn chronicle_fault_vfs_register() -> i32;
    fn chronicle_fault_vfs_target(path: *const std::ffi::c_char) -> i32;
    fn chronicle_fault_vfs_arm(kind: i32);
    fn chronicle_fault_vfs_fired(kind: i32) -> i32;
}

const WRITE: i32 = 1;
const SYNC: i32 = 2;

fn entry(index: u64, data: &[u8]) -> Entry {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Normal(model::Command::Create {
            key: format!("stream-{index}"),
            expected_incarnation: None,
            config: model::StreamConfig {
                content_type: "application/octet-stream".into(),
                track_boundaries: false,
                json_framing: None,
                expiry: None,
            },
            data: data.to_vec(),
            closed: false,
            now_ms: None,
        }),
    }
}

fn register_and_target(path: &std::path::Path) {
    let path = CString::new(path.to_str().expect("temporary path is UTF-8")).unwrap();
    // SAFETY: the C VFS copies `path` before returning. Registration precedes opening any
    // connection, and this package runs one test, so its process-global controls cannot race.
    unsafe {
        assert_eq!(chronicle_fault_vfs_register(), libsqlite3_sys::SQLITE_OK);
        assert_eq!(
            chronicle_fault_vfs_target(path.as_ptr()),
            libsqlite3_sys::SQLITE_OK
        );
    }
}

async fn exercise(kind: i32) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(format!("fault-{kind}.sqlite"));
    register_and_target(&path);
    let mut store = SqliteStore::open(&path).await.unwrap();
    store
        .apply_entries([entry(1, b"acknowledged")])
        .await
        .unwrap();

    // SAFETY: these functions only atomically arm/read the filename-scoped one-shot.
    unsafe { chronicle_fault_vfs_arm(kind) };
    let result = store
        .apply_entries([entry(2, b"must not be published speculatively")])
        .await;
    assert!(
        result.is_err(),
        "injected SQLite I/O failure returned success"
    );
    assert_eq!(unsafe { chronicle_fault_vfs_fired(kind) }, 1);
    assert!(
        store
            .read_stream("stream-2".into())
            .await
            .unwrap()
            .is_none()
    );
    store.close().await;

    let mut reopened = SqliteStore::open_existing(&path).await.unwrap();
    assert_eq!(
        reopened
            .read_stream("stream-1".into())
            .await
            .unwrap()
            .unwrap()
            .data,
        b"acknowledged"
    );
    // A failed sync has an explicitly unknown commit outcome; either durable result is valid.
    let maybe_new = reopened.read_stream("stream-2".into()).await.unwrap();
    let (applied, _) = reopened.applied_state().await.unwrap();
    assert_eq!(
        applied.unwrap().index,
        if maybe_new.is_some() { 2 } else { 1 }
    );
    assert!(
        maybe_new.is_none() || maybe_new.unwrap().data == b"must not be published speculatively"
    );
    reopened.close().await;
    println!(
        "fault kind {kind}: fired once, no speculative publication, prior state retained, applied boundary atomic"
    );
}

#[tokio::test]
async fn sqlite_vfs_write_and_sync_failures_preserve_acknowledged_state() {
    exercise(WRITE).await;
    exercise(SYNC).await;
    snapshot_failure(WRITE).await;
    snapshot_failure(SYNC).await;
}

async fn snapshot_failure(kind: i32) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot.sqlite");
    register_and_target(&path);
    let mut store = SqliteStore::open(&path).await.unwrap();
    store.apply_entries([entry(1, b"old")]).await.unwrap();
    let old = store.build_snapshot().await.unwrap();
    store.apply_entries([entry(2, b"new")]).await.unwrap();
    // Generate the new pair before arming the filename-scoped failure, in a separate store.
    let mut source = SqliteStore::open(dir.path().join("source.sqlite"))
        .await
        .unwrap();
    source
        .apply_entries([entry(1, b"old"), entry(2, b"new")])
        .await
        .unwrap();
    let new = source.build_snapshot().await.unwrap();
    source.close().await;
    // SAFETY: process-global controls are exercised sequentially in the single test above.
    unsafe { chronicle_fault_vfs_arm(kind) };
    assert!(store.build_snapshot().await.is_err());
    assert_eq!(unsafe { chronicle_fault_vfs_fired(kind) }, 1);
    store.close().await;
    let mut reopened = SqliteStore::open_existing(&path).await.unwrap();
    assert_eq!(reopened.applied_state().await.unwrap().0.unwrap().index, 2);
    assert_eq!(
        reopened
            .read_stream("stream-2".into())
            .await
            .unwrap()
            .unwrap()
            .data,
        b"new"
    );
    let actual = reopened.get_current_snapshot().await.unwrap().unwrap();
    let expected = match actual.meta.last_log_id.unwrap().index {
        1 => old,
        2 => new,
        other => panic!("unexpected snapshot index {other}"),
    };
    assert_eq!(actual.meta, expected.meta);
    assert_eq!(actual.snapshot.into_inner(), expected.snapshot.into_inner());
    reopened.close().await;
    println!(
        "snapshot fault kind {kind}: I/O error, complete old-or-new pair, applied state retained"
    );
}
