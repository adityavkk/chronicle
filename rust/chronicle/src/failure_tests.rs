use super::monitor;
use chronicle_raft::{
    Raft,
    faults::{
        AFTER_APPLY_COMMIT, AFTER_LOG_COMMIT, BEFORE_BODY_READ, BEFORE_STATE_COMMIT,
        store_directory,
    },
    model::{Command, StreamConfig},
    network::Network,
    storage::SqliteStore,
    wire,
};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};

struct Child(std::process::Child);
impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn fatal_child() {
    let Ok(root) = std::env::var("CHRONICLE_FATAL_TEST") else {
        return;
    };
    let root = PathBuf::from(root);
    let failed: usize = std::env::var("CHRONICLE_FATAL_GROUP")
        .unwrap()
        .parse()
        .unwrap();
    let gate = std::env::var("CHRONICLE_FATAL_GATE").unwrap();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let mut groups = Vec::new();
        for group in 0..2 {
            let store = SqliteStore::open(root.join(format!("group-{group}.sqlite")))
                .await
                .unwrap();
            let raft = Raft::new(
                1,
                Arc::new(openraft::Config::default().validate().unwrap()),
                Network {
                    client: reqwest::Client::new(),
                    cluster: "fatal-test".into(),
                    group,
                },
                store.clone(),
                store,
            )
            .await
            .unwrap();
            tokio::spawn(monitor(1, group, raft.metrics()));
            raft.initialize(BTreeMap::from([(1, openraft::BasicNode::new("unused"))]))
                .await
                .unwrap();
            raft.wait(Some(Duration::from_secs(3)))
                .state(openraft::ServerState::Leader, "bootstrap")
                .await
                .unwrap();
            raft.client_write(Command::Create {
                key: "retained".into(),
                expected_incarnation: None,
                config: StreamConfig {
                    content_type: "application/octet-stream".into(),
                    track_boundaries: false,
                    json_framing: None,
                    expiry: None,
                },
                data: b"acknowledged".to_vec(),
                closed: false,
                now_ms: None,
            })
            .await
            .unwrap();
            groups.push(raft);
        }
        // Actual portable file-body task, not a fabricated metrics failure. Its
        // unopened release gate would prevent normal Tokio runtime destruction.
        std::fs::write(root.join("body"), b"x").unwrap();
        let body = file_gate(&root, "http-body");
        std::fs::write(body.join(format!("{BEFORE_BODY_READ}.arm")), b"1").unwrap();
        let file = std::fs::File::open(root.join("body")).unwrap();
        tokio::spawn(async move {
            axum::body::to_bytes(wire::file_body(file, 1, false, ()), 1)
                .await
                .unwrap();
        });
        while !body.join(format!("{BEFORE_BODY_READ}.reached")).exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let fault = file_gate(&root, &format!("group-{failed}.sqlite"));
        std::fs::write(fault.join(format!("{gate}.error")), b"1").unwrap();
        // Exercise log and application failures through the real core. The
        // outcome is unknown even when a transaction committed before the error.
        let result = groups[failed]
            .client_write(Command::Append {
                key: "retained".into(),
                incarnation: 1,
                data: b"?".to_vec(),
                producer: None,
                close: false,
                empty_body: false,
                stream_seq: None,
                now_ms: None,
            })
            .await;
        assert!(result.is_err(), "failed persistence returned success");
        std::future::pending::<()>().await;
    });
}

fn file_gate(root: &std::path::Path, name: &str) -> PathBuf {
    let directory = store_directory(&root.join("faults"), std::path::Path::new(name));
    std::fs::create_dir_all(&directory).unwrap();
    directory
}

#[test]
fn storage_fatal_exits_with_blocked_body_and_retains_acknowledged_state() {
    for (failed, gate) in [
        (0, AFTER_LOG_COMMIT),
        (1, AFTER_LOG_COMMIT),
        (0, BEFORE_STATE_COMMIT),
        (1, BEFORE_STATE_COMMIT),
        (0, AFTER_APPLY_COMMIT),
        (1, AFTER_APPLY_COMMIT),
    ] {
        let root = tempfile::tempdir().unwrap();
        let mut child = Child(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "failure::subprocess::fatal_child", "--nocapture"])
                .env("CHRONICLE_FATAL_TEST", root.path())
                .env("CHRONICLE_FATAL_GROUP", failed.to_string())
                .env("CHRONICLE_FATAL_GATE", gate)
                .env("CHRONICLE_FAULT_DIR", root.path().join("faults"))
                .spawn()
                .unwrap(),
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        let status = loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                break status;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "fatal did not terminate process"
            );
            std::thread::sleep(Duration::from_millis(10));
        };
        assert_eq!(status.code(), Some(1));
        let body = file_gate(root.path(), "http-body");
        assert!(body.join(format!("{BEFORE_BODY_READ}.reached")).exists());
        assert!(!body.join(format!("{BEFORE_BODY_READ}.release")).exists());
        let fault = file_gate(root.path(), &format!("group-{failed}.sqlite"));
        assert!(fault.join(format!("{gate}.reached")).exists());
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            for group in 0..2 {
                let store =
                    SqliteStore::open_existing(root.path().join(format!("group-{group}.sqlite")))
                        .await
                        .unwrap();
                let stream = store.read_stream("retained".into()).await.unwrap().unwrap();
                assert!(stream.data.starts_with(b"acknowledged"));
                store.close().await;
            }
        });
    }
}
