#![cfg(all(feature = "storage-faults", unix))]

use std::path::{Path, PathBuf};
use std::process::{Child, Command as ProcessCommand};
use std::time::{Duration, Instant};

use chronicle_raft::faults::{
    AFTER_APPLY_COMMIT, AFTER_LOG_COMMIT, AFTER_SNAPSHOT_INSTALL, BEFORE_LIVE_RECHECK,
    BEFORE_PROJECTION_OPEN, BEFORE_SNAPSHOT_INSTALL, before_live_recheck, store_directory,
};
use chronicle_raft::model::{Command, StreamConfig};
use chronicle_raft::{TypeConfig, storage::SqliteStore};
use openraft::storage::{RaftLogStorageExt, RaftStateMachine};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId, RaftLogReader, RaftSnapshotBuilder};

const DEADLINE: Duration = Duration::from_secs(10);

struct FaultChild(Child);

impl Drop for FaultChild {
    fn drop(&mut self) {
        // Also runs on assertion failure/timeout. Never strand a paused actor.
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn create_entry(index: u64, key: &str, data: &[u8]) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Normal(Command::Create {
            key: key.into(),
            expected_incarnation: None,
            config: StreamConfig {
                content_type: "application/octet-stream".into(),
                expires_ms: None,
            },
            data: data.into(),
            closed: false,
        }),
    }
}

fn wait_for(path: &Path, child: &mut FaultChild) {
    let deadline = Instant::now() + DEADLINE;
    while Instant::now() < deadline {
        if path.is_file() {
            return;
        }
        if let Some(status) = child.0.try_wait().unwrap() {
            panic!("fault child exited before {}: {status}", path.display());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("timed out waiting for {}", path.display());
}

fn spawn_child(root: &Path, operation: &str) -> FaultChild {
    FaultChild(
        ProcessCommand::new(std::env::current_exe().unwrap())
            .args(["--exact", "fault_gate_child", "--nocapture"])
            .env("CHRONICLE_FAULT_CHILD", operation)
            .env("CHRONICLE_FAULT_CASE_DIR", root)
            .env("CHRONICLE_FAULT_DIR", root.join("controls"))
            .spawn()
            .unwrap(),
    )
}

fn arrange_gate(root: &Path, child: &mut FaultChild, gate: &str) -> PathBuf {
    wait_for(&root.join("setup"), child);
    let directory = store_directory(&root.join("controls"), &root.join("store.sqlite"));
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(directory.join(format!("{gate}.arm")), b"armed").unwrap();
    std::fs::write(root.join("trigger"), b"go").unwrap();
    let reached = directory.join(format!("{gate}.reached"));
    wait_for(&reached, child);
    directory
}

#[test]
fn fault_gate_child() {
    let Ok(operation) = std::env::var("CHRONICLE_FAULT_CHILD") else {
        return;
    };
    let root = PathBuf::from(std::env::var_os("CHRONICLE_FAULT_CASE_DIR").unwrap());
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let path = root.join("store.sqlite");
        let mut store = SqliteStore::open(&path).await.unwrap();
        if operation == "open-cancel" {
            store.apply([create_entry(1, "s", b"abc")]).await.unwrap();
        }
        let snapshot = if operation.starts_with("snapshot-") {
            store.apply([create_entry(1, "old", b"old")]).await.unwrap();
            let mut source = SqliteStore::open(root.join("source.sqlite")).await.unwrap();
            source
                .apply([create_entry(2, "new", b"new")])
                .await
                .unwrap();
            let snapshot = source.build_snapshot().await.unwrap();
            source.close().await;
            Some(snapshot)
        } else {
            None
        };
        std::fs::write(root.join("setup"), b"ready").unwrap();
        while !root.join("trigger").is_file() {
            std::thread::sleep(Duration::from_millis(5));
        }
        match operation.as_str() {
            "log" => store
                .blocking_append([create_entry(1, "logged", b"durable")])
                .await
                .unwrap(),
            "apply" | "release" => {
                store
                    .apply([create_entry(1, "applied", b"durable")])
                    .await
                    .unwrap();
            }
            "snapshot-before" | "snapshot-after" => {
                let snapshot = snapshot.unwrap();
                store
                    .install_snapshot(&snapshot.meta, snapshot.snapshot)
                    .await
                    .unwrap();
            }
            "open-cancel" => {
                use std::sync::Arc;
                use tokio::sync::Semaphore;
                let requests = Arc::new(Semaphore::new(4));
                let live = Arc::new(Semaphore::new(1));
                let view = store.read_info("s".into(), ()).await.unwrap().unwrap();
                let guards = [
                    Arc::new(requests.clone().try_acquire_owned().unwrap()),
                    Arc::new(live.clone().try_acquire_owned().unwrap()),
                ];
                let copy = store.clone();
                let task = tokio::spawn(async move {
                    tokio::time::timeout(
                        Duration::from_millis(100),
                        copy.read_file("s".into(), &view, 0, guards),
                    )
                    .await
                });
                let directory = store_directory(&root.join("controls"), &path);
                tokio::time::timeout(DEADLINE, async {
                    while !directory
                        .join(format!("{BEFORE_PROJECTION_OPEN}.reached"))
                        .is_file()
                    {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                })
                .await
                .unwrap();
                assert!(
                    task.await.unwrap().is_err(),
                    "gated open must hit the async deadline"
                );
                assert_eq!(requests.available_permits(), 3);
                assert_eq!(live.available_permits(), 0);
                std::fs::write(root.join("cancelled"), b"checked").unwrap();
                tokio::time::timeout(DEADLINE, async {
                    while requests.available_permits() != 4 || live.available_permits() != 1 {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                })
                .await
                .unwrap();
            }
            "live-cancel" => {
                use std::sync::Arc;
                use tokio::sync::Semaphore;
                let requests = Arc::new(Semaphore::new(4));
                let live = Arc::new(Semaphore::new(1));
                let directory = store_directory(&root.join("controls"), Path::new("http-live-1"));
                let task = tokio::spawn(before_live_recheck(
                    1,
                    [
                        Arc::new(requests.clone().try_acquire_owned().unwrap()),
                        Arc::new(live.clone().try_acquire_owned().unwrap()),
                    ],
                ));
                tokio::time::timeout(DEADLINE, async {
                    while !directory
                        .join(format!("{BEFORE_LIVE_RECHECK}.reached"))
                        .is_file()
                    {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                })
                .await
                .unwrap();
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
                assert_eq!(requests.available_permits(), 3);
                assert!(live.try_acquire().is_err());
                // All three reserved writer slots remain available while a
                // cancelled live task is still detached and blocked in the gate.
                drop(requests.clone().try_acquire_many_owned(3).unwrap());
                std::fs::write(root.join("cancelled"), b"checked").unwrap();
                tokio::time::timeout(DEADLINE, async {
                    while requests.available_permits() != 4 || live.available_permits() != 1 {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                })
                .await
                .unwrap();
            }
            _ => panic!("unknown child operation"),
        }
        store.close().await;
    });
}

#[test]
fn cancelled_live_gate_retains_writer_reservation() {
    for (operation, filename, gate) in [
        ("live-cancel", "http-live-1", BEFORE_LIVE_RECHECK),
        ("open-cancel", "store.sqlite", BEFORE_PROJECTION_OPEN),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let mut child = spawn_child(root, operation);
        wait_for(&root.join("setup"), &mut child);
        let directory = store_directory(&root.join("controls"), Path::new(filename));
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join(format!("{gate}.arm")), b"armed").unwrap();
        std::fs::write(root.join("trigger"), b"go").unwrap();
        wait_for(&root.join("cancelled"), &mut child);
        std::fs::write(directory.join(format!("{gate}.release")), b"release").unwrap();
        let deadline = Instant::now() + DEADLINE;
        loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            assert!(Instant::now() < deadline, "released child did not finish");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

#[test]
fn crashes_at_durable_boundaries_reopen_with_exact_state() {
    for (operation, gate) in [
        ("log", AFTER_LOG_COMMIT),
        ("apply", AFTER_APPLY_COMMIT),
        ("snapshot-before", BEFORE_SNAPSHOT_INSTALL),
        ("snapshot-after", AFTER_SNAPSHOT_INSTALL),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let mut child = spawn_child(temp.path(), operation);
        arrange_gate(temp.path(), &mut child, gate);
        child.0.kill().unwrap();
        assert!(!child.0.wait().unwrap().success());

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let path = temp.path().join("store.sqlite");
            let mut reopened = SqliteStore::open_existing(&path).await.unwrap();
            match operation {
                "log" => {
                    let entries = reopened.try_get_log_entries(0..2).await.unwrap();
                    assert_eq!(entries.len(), 1);
                    assert_eq!(entries[0].log_id.index, 1);
                    let EntryPayload::Normal(Command::Create { key, data, .. }) =
                        &entries[0].payload
                    else {
                        panic!("persisted log entry changed shape");
                    };
                    assert_eq!(key, "logged");
                    assert_eq!(data, b"durable");
                }
                "apply" => assert_eq!(
                    reopened
                        .read_stream("applied".into())
                        .await
                        .unwrap()
                        .unwrap()
                        .data,
                    b"durable"
                ),
                "snapshot-before" => {
                    assert_eq!(reopened.applied_state().await.unwrap().0.unwrap().index, 1);
                    assert!(reopened.get_current_snapshot().await.unwrap().is_none());
                    assert!(reopened.read_stream("new".into()).await.unwrap().is_none());
                    assert_eq!(
                        reopened
                            .read_stream("old".into())
                            .await
                            .unwrap()
                            .unwrap()
                            .data,
                        b"old"
                    );
                }
                "snapshot-after" => {
                    assert_eq!(reopened.applied_state().await.unwrap().0.unwrap().index, 2);
                    let snapshot = reopened.get_current_snapshot().await.unwrap().unwrap();
                    assert_eq!(snapshot.meta.last_log_id.unwrap().index, 2);
                    assert!(reopened.read_stream("old".into()).await.unwrap().is_none());
                    assert_eq!(
                        reopened
                            .read_stream("new".into())
                            .await
                            .unwrap()
                            .unwrap()
                            .data,
                        b"new"
                    );
                }
                _ => unreachable!(),
            }
            reopened.close().await;
        });
    }
}

#[test]
fn release_file_allows_the_paused_operation_to_finish() {
    let temp = tempfile::tempdir().unwrap();
    let mut child = spawn_child(temp.path(), "release");
    let directory = arrange_gate(temp.path(), &mut child, AFTER_APPLY_COMMIT);
    std::fs::write(
        directory.join(format!("{AFTER_APPLY_COMMIT}.release")),
        b"release",
    )
    .unwrap();
    let deadline = Instant::now() + DEADLINE;
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        assert!(Instant::now() < deadline, "released child did not finish");
        std::thread::sleep(Duration::from_millis(10));
    }
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let reopened = SqliteStore::open_existing(temp.path().join("store.sqlite"))
            .await
            .unwrap();
        assert_eq!(
            reopened
                .read_stream("applied".into())
                .await
                .unwrap()
                .unwrap()
                .data,
            b"durable"
        );
        reopened.close().await;
    });
}

#[test]
fn unwinding_test_reaps_paused_child_and_releases_store() {
    let temp = tempfile::tempdir().unwrap();
    let mut child = spawn_child(temp.path(), "apply");
    arrange_gate(temp.path(), &mut child, AFTER_APPLY_COMMIT);
    #[cfg(target_os = "linux")]
    let proc_path = PathBuf::from(format!("/proc/{}", child.0.id()));
    let result = std::panic::catch_unwind(move || {
        let _child = child;
        panic!("intentional harness failure");
    });
    assert!(result.is_err());
    #[cfg(target_os = "linux")]
    assert!(!proc_path.exists(), "child was not reaped");
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let reopened = SqliteStore::open_existing(temp.path().join("store.sqlite"))
            .await
            .unwrap();
        assert_eq!(
            reopened
                .read_stream("applied".into())
                .await
                .unwrap()
                .unwrap()
                .data,
            b"durable"
        );
        reopened.close().await;
    });
}
