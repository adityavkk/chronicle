use super::*;
use crate::{App, Group, identity, telemetry::Telemetry};
use axum::{
    Router,
    extract::DefaultBodyLimit,
    middleware,
    routing::{any, get, post},
};
use chronicle_raft::{Raft, model::Node, network::Network, storage::SqliteStore};
use openraft::{BasicNode, Config, ServerState};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_leader_chunked_fork_and_cascade_use_real_raft_quorums() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let directory = tempfile::tempdir().unwrap();
        let client = reqwest::Client::new();
        let mut listeners = Vec::new();
        let mut nodes = BTreeMap::new();
        for id in 1..=3 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            nodes.insert(
                id,
                Node {
                    addr: listener.local_addr().unwrap().to_string(),
                    zone: id.to_string(),
                    draining: false,
                },
            );
            listeners.push((id, listener));
        }
        let (logs, _guard) = tracing_appender::non_blocking(std::io::sink());
        let mut apps = BTreeMap::new();
        let mut servers = tokio::task::JoinSet::new();
        for (id, listener) in listeners {
            let mut groups = BTreeMap::new();
            for group in 0..=model::SHARDS {
                let store =
                    SqliteStore::open(directory.path().join(format!("{id}-{group}.sqlite")))
                        .await
                        .unwrap();
                let raft = Raft::new(
                    id,
                    Arc::new(
                        Config {
                            heartbeat_interval: 200,
                            election_timeout_min: 800,
                            election_timeout_max: 1600,
                            max_payload_entries: 1,
                            ..Config::default()
                        }
                        .validate()
                        .unwrap(),
                    ),
                    Network {
                        client: client.clone(),
                        cluster: "fork-test".into(),
                        group,
                    },
                    store.clone(),
                    store.clone(),
                )
                .await
                .unwrap();
                groups.insert(
                    group,
                    Group {
                        raft,
                        store,
                        movement: Mutex::new(()),
                    },
                );
            }
            let app = Arc::new(App {
                id,
                stream_tenant: Some("test".into()),
                identity: identity::Identity {
                    node: id,
                    cluster: "fork-test".into(),
                    genesis: true,
                },
                nodes: nodes.clone(),
                groups,
                client: client.clone(),
                admission: Arc::new(Semaphore::new(32)),
                live_admission: Arc::new(Semaphore::new(8)),
                telemetry: Arc::new(Telemetry::new(id, logs.error_counter(), String::new())),
            });
            let router = Router::new()
                .route("/raft/{group}/append", post(crate::append_rpc))
                .route("/raft/{group}/vote", post(crate::vote_rpc))
                .route("/raft/{group}/snapshot", post(crate::snapshot_rpc))
                .route("/admin/status", get(crate::status))
                .route(
                    "/admin/fork",
                    post(rpc).layer(middleware::from_fn_with_state(
                        Arc::new(Semaphore::new(16)),
                        crate::admit_stream,
                    )),
                )
                .route(
                    "/v1/stream/{*path}",
                    any(crate::stream)
                        .layer(DefaultBodyLimit::max(1024 * 1024))
                        .layer(middleware::from_fn_with_state(
                            app.admission.clone(),
                            crate::admit_stream,
                        )),
                )
                .layer(DefaultBodyLimit::max(8 * 1024 * 1024))
                .with_state(app.clone());
            servers.spawn(async move { axum::serve(listener, router).await.unwrap() });
            apps.insert(id, app);
        }
        for group in 0..=model::SHARDS {
            // Bootstrap different leaders deliberately, then grow to three voters.
            let leader = group % 3 + 1;
            let raft = &apps[&leader].groups[&group].raft;
            raft.initialize(BTreeMap::from([(
                leader,
                BasicNode::new(nodes[&leader].addr.clone()),
            )]))
            .await
            .unwrap();
            raft.wait(Some(Duration::from_secs(5)))
                .state(ServerState::Leader, "bootstrap")
                .await
                .unwrap();
            for id in 1..=3 {
                if id != leader {
                    raft.add_learner(id, BasicNode::new(nodes[&id].addr.clone()), true)
                        .await
                        .unwrap();
                }
            }
            raft.change_membership(std::collections::BTreeSet::from([1, 2, 3]), false)
                .await
                .unwrap();
        }
        for group in 0..=model::SHARDS {
            let raft = &apps[&(group % 3 + 1)].groups[&group].raft;
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if raft.ensure_linearizable().await.is_ok() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
        }
        let source = "source";
        let source_group = model::shard("4:testsource");
        let target = (0..100)
            .map(|n| format!("target-{n}"))
            .find(|key| model::shard(&format!("4:test{key}")) % 3 != source_group % 3)
            .unwrap();
        let base = format!("http://{}/v1/stream", nodes[&2].addr);
        let bytes: Vec<u8> = (0..600_013).map(|n| (n % 251) as u8).collect();
        let result = client
            .put(format!("{base}/{source}"))
            .header("content-type", "application/octet-stream")
            .body(bytes.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(
            result.status(),
            StatusCode::CREATED,
            "{}",
            result.text().await.unwrap()
        );
        let fork_request = || {
            client
                .put(format!("{base}/{target}"))
                .header("stream-forked-from", format!("/v1/stream/{source}"))
                .body(b"tail".to_vec())
        };
        let result = fork_request().send().await.unwrap();
        assert_eq!(
            result.status(),
            StatusCode::CREATED,
            "{}",
            result.text().await.unwrap()
        );
        assert_eq!(
            fork_request().send().await.unwrap().status(),
            StatusCode::OK
        );
        let mut expected = bytes;
        expected.extend_from_slice(b"tail");
        let result = client
            .get(format!("{base}/{target}?offset=-1"))
            .send()
            .await
            .unwrap();
        assert_eq!(result.status(), StatusCode::OK);
        assert_eq!(result.bytes().await.unwrap().as_ref(), expected);
        // Deterministically hold A's absent-target observation while B publishes.
        assert_eq!(
            client
                .put(format!("{base}/race-source"))
                .header("content-type", "text/plain")
                .body("race")
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::CREATED
        );
        let (arrived, resume) =
            crate::fork_http::gates::next("4:testrace-fork".into(), "after_target_read");
        let request = client
            .put(format!("{base}/race-fork"))
            .header("stream-forked-from", "/v1/stream/race-source");
        let first = tokio::spawn(async move { request.send().await.unwrap() });
        arrived.await.unwrap();
        assert_eq!(
            client
                .put(format!("{base}/race-fork"))
                .header("stream-forked-from", "/v1/stream/race-source")
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::CREATED
        );
        resume.send(()).unwrap();
        let duplicate = first.await.unwrap();
        assert_eq!(
            duplicate.status(),
            StatusCode::OK,
            "{}",
            duplicate.text().await.unwrap()
        );
        let settled = read(&apps[&1], "4:testrace-source", Some(2))
            .await
            .unwrap()
            .transaction
            .unwrap();
        assert!(settled.finalized && matches!(settled.decision, Decision::Aborted(_)));
        let owner = read(&apps[&1], "4:testrace-fork", None)
            .await
            .unwrap()
            .stream
            .unwrap()
            .origin
            .unwrap();
        assert_eq!(owner.id.sequence, 1);

        // Pause after Begin, erase an aborted source incarnation, and reuse its
        // sequence for a different target. A must not acknowledge B's outcome.
        assert_eq!(
            client
                .put(format!("{base}/generation-source"))
                .header("content-type", "text/plain")
                .body("old")
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::CREATED
        );
        let (arrived, resume) =
            crate::fork_http::gates::next("4:testold-target".into(), "after_begin");
        let request = client
            .put(format!("{base}/old-target"))
            .header("stream-forked-from", "/v1/stream/generation-source");
        let old = tokio::spawn(async move { request.send().await.unwrap() });
        arrived.await.unwrap();
        let retired_offer = read(&apps[&1], "4:testgeneration-source", Some(1))
            .await
            .unwrap()
            .transaction
            .unwrap()
            .offer;
        accepted(
            apply(
                &apps[&1],
                Operation::Decide {
                    id: Id {
                        source: "4:testgeneration-source".into(),
                        incarnation: 1,
                        sequence: 1,
                    },
                    commit: false,
                    now_ms: now_ms(),
                    rejection: Some(Error::ConfigConflict),
                },
            )
            .await
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            client
                .delete(format!("{base}/generation-source"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            client
                .put(format!("{base}/generation-source"))
                .header("content-type", "text/plain")
                .body("new")
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::CREATED
        );
        assert_eq!(
            client
                .put(format!("{base}/new-target"))
                .header("stream-forked-from", "/v1/stream/generation-source")
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::CREATED
        );
        resume.send(()).unwrap();
        assert_eq!(old.await.unwrap().status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            client
                .get(format!("{base}/old-target"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            client
                .get(format!("{base}/new-target"))
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap(),
            "new"
        );
        accepted(
            apply(&apps[&1], Operation::Prepare(retired_offer))
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            client
                .head(format!("{base}/old-target"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            client
                .delete(format!("{base}/{source}"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            client
                .head(format!("{base}/{source}"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::GONE
        );
        assert_eq!(
            client
                .delete(format!("{base}/{target}"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::NO_CONTENT
        );
        // Run the real periodic reconciler, not Kubernetes drain or direct model mutation.
        let mut reconcilers = tokio::task::JoinSet::new();
        for app in apps.values() {
            reconcilers.spawn(run(app.clone()));
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if client
                    .head(format!("{base}/{source}"))
                    .send()
                    .await
                    .unwrap()
                    .status()
                    == StatusCode::NOT_FOUND
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if client
                    .head(format!("{base}/old-target"))
                    .send()
                    .await
                    .unwrap()
                    .status()
                    == StatusCode::NOT_FOUND
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
        // Foreground and background coordinators may stage/finalize the same ID.
        assert_eq!(
            client
                .put(format!("{base}/live-source"))
                .header("content-type", "application/octet-stream")
                .body(vec![b'v'; 600_013])
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::CREATED
        );
        let result = client
            .put(format!("{base}/live-fork"))
            .header("stream-forked-from", "/v1/stream/live-source")
            .send()
            .await
            .unwrap();
        assert_eq!(
            result.status(),
            StatusCode::CREATED,
            "{}",
            result.text().await.unwrap()
        );
        assert_eq!(
            client
                .get(format!("{base}/live-fork"))
                .send()
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            vec![b'v'; 600_013]
        );
        reconcilers.abort_all();
        while reconcilers.join_next().await.is_some() {}
        let mut stores = Vec::new();
        for app in apps.values() {
            for group in app.groups.values() {
                group.raft.shutdown().await.unwrap();
                stores.push(group.store.clone());
            }
        }
        servers.abort_all();
        while servers.join_next().await.is_some() {}
        drop(apps);
        for store in stores {
            store.close().await;
        }
    })
    .await
    .unwrap();
}
