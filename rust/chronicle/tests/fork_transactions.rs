use chronicle_raft::{
    Entry, LogId,
    expiry::Expiry,
    fork::{self, Decision, Id, Operation, Request},
    model::{Command, Error, State, StreamConfig},
    storage::SqliteStore,
};
use openraft::{EntryPayload, RaftSnapshotBuilder, storage::RaftStateMachine, vote::RaftLeaderId};

fn create(key: &str, incarnation: u64) -> Command {
    Command::Create {
        key: key.into(),
        expected_incarnation: Some(incarnation),
        config: StreamConfig {
            content_type: "text/plain".into(),
            track_boundaries: true,
            json_framing: Some(false),
            expiry: None,
        },
        data: b"abcdef".to_vec(),
        closed: false,
        now_ms: Some(100),
    }
}

fn id() -> Id {
    Id {
        source: "s".into(),
        incarnation: 1,
        sequence: 1,
    }
}

fn begin() -> Operation {
    Operation::Begin {
        id: id(),
        request: Request {
            target: "t".into(),
            incarnation: 1,
            anchor: Some(0),
            sub: 4,
            content_type: None,
            expiry: None,
            closed: false,
        },
        body: b"XYZ".to_vec(),
        now_ms: 100,
    }
}

fn run(state: &mut State, op: Operation) {
    assert_eq!(state.apply(&Command::Fork(Box::new(op))).error, None);
}

fn decide(commit: bool, now_ms: u64) -> Operation {
    Operation::Decide {
        id: id(),
        commit,
        now_ms,
        rejection: Some(Error::ConfigConflict),
    }
}

fn finish(decision: Decision) -> Operation {
    Operation::Finish {
        target: "t".into(),
        id: id(),
        decision,
    }
}

fn delete(key: &str) -> Command {
    Command::Delete {
        key: key.into(),
        incarnation: 1,
        expired_at: None,
    }
}

#[test]
fn preparation_fences_both_identities_and_rejects_incomplete_or_mismatched_chunks() {
    let mut source = State::default();
    source.apply(&create("s", 1));
    run(&mut source, begin());
    let offer = source.streams["s"].forks.transactions[&1].offer.clone();
    let mut target = State::default();
    run(&mut target, Operation::Prepare(offer.clone()));
    assert_eq!(source.apply(&delete("s")).error, Some(Error::PendingFork));
    assert_eq!(
        target.apply(&create("t", 1)).error,
        Some(Error::PendingFork)
    );
    let mut competitor = offer;
    competitor.id.sequence = 2;
    assert_eq!(
        target
            .apply(&Command::Fork(Box::new(Operation::Prepare(competitor))))
            .error,
        Some(Error::PendingFork)
    );
    assert_eq!(
        target
            .apply(&Command::Fork(Box::new(finish(Decision::Committed {
                created_ms: 101
            }))))
            .error,
        Some(Error::PendingFork)
    );
    let chunk = fork::chunk(&source, &id(), 0).unwrap();
    run(&mut target, chunk.clone());
    run(&mut target, chunk.clone());
    let mut mismatch = chunk;
    if let Operation::Stage { data, .. } = &mut mismatch {
        data[0] = b'!';
    }
    assert_eq!(
        target.apply(&Command::Fork(Box::new(mismatch))).error,
        Some(Error::InvalidFork)
    );
    assert_eq!(target.fork_targets["t"].data, b"abcdXYZ");
    assert_eq!(target.fork_targets["t"].append_ends, [4, 7]);
    run(&mut source, decide(true, 101));
    run(&mut source, decide(false, 102));
    let decision = source.streams["s"].forks.transactions[&1].decision.clone();
    assert_eq!(decision, Decision::Committed { created_ms: 101 });
    run(&mut target, finish(decision));
    assert_eq!(target.streams["t"].data, b"abcdXYZ");
    assert!(target.streams["t"].producers.is_empty());
    assert!(target.fork_targets.is_empty());
}

#[test]
fn expiry_abort_is_irreversible_and_releases_target_without_visibility() {
    let mut source = State::default();
    let mut command = create("s", 1);
    if let Command::Create { config, .. } = &mut command {
        config.expiry = Some(Expiry::Ttl(1));
    }
    source.apply(&command);
    run(&mut source, begin());
    let mut target = State::default();
    run(
        &mut target,
        Operation::Prepare(source.streams["s"].forks.transactions[&1].offer.clone()),
    );
    run(&mut source, decide(true, 1101));
    run(&mut source, decide(true, 100));
    let decision = source.streams["s"].forks.transactions[&1].decision.clone();
    assert_eq!(decision, Decision::Aborted(Error::Missing));
    run(&mut target, finish(decision));
    assert!(target.fork_targets.is_empty());
    assert!(!target.streams.contains_key("t"));
    assert!(!source.streams["s"].forks.locked());
}

#[test]
fn soft_delete_retains_prefix_until_child_release_and_fences_recreation() {
    let mut state = State::default();
    state.apply(&create("s", 1));
    run(&mut state, begin());
    let offer = state.streams["s"].forks.transactions[&1].offer.clone();
    run(&mut state, Operation::Prepare(offer));
    let chunk = fork::chunk(&state, &id(), 0).unwrap();
    run(&mut state, chunk);
    run(&mut state, decide(true, 101));
    assert!(state.apply(&delete("s")).error.is_none());
    assert_eq!(state.streams["s"].data, b"abcdef");
    assert_eq!(
        state.apply(&create("s", 2)).error,
        Some(Error::ConfigConflict)
    );
    run(&mut state, finish(Decision::Committed { created_ms: 101 }));
    run(&mut state, Operation::Finalized(id()));
    assert!(state.apply(&delete("t")).error.is_none());
    assert!(state.streams["t"].data.is_empty());
    assert_eq!(state.apply(&create("t", 2)).error, Some(Error::PendingFork));
    run(&mut state, Operation::Release(id()));
    run(
        &mut state,
        Operation::Released {
            target: "t".into(),
            incarnation: 1,
            id: id(),
        },
    );
    assert!(state.streams["s"].data.is_empty());
    assert!(state.apply(&create("s", 2)).error.is_none());
    assert!(state.apply(&create("t", 2)).error.is_none());
    assert_eq!(
        state.apply(&Command::Fork(Box::new(begin()))).error,
        Some(Error::StaleIncarnation)
    );
    run(&mut state, Operation::Release(id())); // Delayed old release cannot alter recreation.
    assert_eq!(state.streams["s"].data, b"abcdef");
}

#[tokio::test]
async fn every_phase_survives_sqlite_snapshot_install_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let mut current_path = dir.path().join("initial");
    let mut store = SqliteStore::open(&current_path).await.unwrap();
    let mut state = State::default();
    state.apply(&create("s", 1));
    run(&mut state, begin());
    let offer = state.streams["s"].forks.transactions[&1].offer.clone();
    let chunk = fork::chunk(&state, &id(), 0).unwrap();
    let mut commands = vec![create("s", 1)];
    commands.extend(
        [
            begin(),
            Operation::Prepare(offer),
            chunk,
            decide(true, 101),
            finish(Decision::Committed { created_ms: 101 }),
            Operation::Finalized(id()),
        ]
        .into_iter()
        .map(|op| Command::Fork(Box::new(op))),
    );
    for (index, command) in commands.into_iter().enumerate() {
        let entry: Entry = Entry {
            log_id: LogId::new(
                openraft::vote::leader_id_adv::CommittedLeaderId::new(1, 1),
                index as u64 + 1,
            ),
            payload: EntryPayload::Normal(command),
        };
        assert!(
            store.apply_entries([entry]).await.unwrap()[0]
                .error
                .is_none()
        );
        let snapshot = store.build_snapshot().await.unwrap();
        let bytes = snapshot.snapshot.get_ref().clone();
        // Reopen the applying DB separately: installing a cached-state snapshot
        // alone would conceal omitted per-command SQLite writes.
        store.close().await;
        store = SqliteStore::open_existing(&current_path).await.unwrap();
        assert_eq!(
            store.build_snapshot().await.unwrap().snapshot.get_ref(),
            &bytes
        );
        let path = dir.path().join(format!("phase-{index}"));
        let mut installed = SqliteStore::open(&path).await.unwrap();
        installed
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .unwrap();
        installed.close().await;
        let mut reopened = SqliteStore::open_existing(&path).await.unwrap();
        let rebuilt = reopened.build_snapshot().await.unwrap();
        assert_eq!(rebuilt.snapshot.get_ref(), &bytes);
        store.close().await;
        store = reopened;
        current_path = path;
    }
    let stream = store.read_stream("t".into()).await.unwrap().unwrap();
    assert_eq!(stream.data, b"abcdXYZ");
    assert_eq!(stream.append_ends, [4, 7]);
    store.close().await;
}

#[test]
fn legacy_header_case_override_preserves_effective_framing() {
    for (header, override_header, data, body, expected, json) in [
        (
            "Application/Json",
            "application/json",
            &b"binary"[..],
            &b"tail"[..],
            &b"binarytail"[..],
            false,
        ),
        (
            "application/json",
            "Application/Json",
            &b"1,"[..],
            &b"[2,3]"[..],
            &b"1,2,3,"[..],
            true,
        ),
    ] {
        let mut state = State::default();
        let mut command = create("s", 1);
        if let Command::Create {
            config,
            data: initial,
            ..
        } = &mut command
        {
            config.content_type = header.into();
            config.json_framing = None;
            config.track_boundaries = false;
            *initial = data.to_vec();
        }
        assert!(state.apply(&command).error.is_none());
        let mut operation = begin();
        if let Operation::Begin {
            request,
            body: initial,
            ..
        } = &mut operation
        {
            request.anchor = None;
            request.sub = 0;
            request.content_type = Some(override_header.into());
            *initial = body.to_vec();
        }
        run(&mut state, operation);
        let offer = state.streams["s"].forks.transactions[&1].offer.clone();
        assert_eq!(offer.config.json_framing, Some(json));
        run(&mut state, Operation::Prepare(offer));
        let chunk = fork::chunk(&state, &id(), 0).unwrap();
        run(&mut state, chunk);
        run(&mut state, decide(true, 101));
        run(&mut state, finish(Decision::Committed { created_ms: 101 }));
        assert_eq!(state.streams["t"].data, expected);
    }
}

#[test]
fn delayed_prepare_after_abort_cleanup_has_a_retained_or_retired_decision() {
    let mut source = State::default();
    source.apply(&create("s", 1));
    run(&mut source, begin());
    let offer = source.streams["s"].forks.transactions[&1].offer.clone();
    let mut target = State::default();
    run(&mut target, Operation::Prepare(offer.clone()));
    run(&mut source, decide(false, 101));
    let abort = Decision::Aborted(Error::ConfigConflict);
    run(&mut target, finish(abort.clone()));
    run(&mut source, Operation::Finalized(id()));
    run(&mut target, Operation::Prepare(offer.clone()));
    assert_eq!(source.streams["s"].forks.transactions[&1].decision, abort);
    run(&mut target, finish(abort));
    source.apply(&delete("s"));
    assert!(source.apply(&create("s", 2)).error.is_none());
    run(&mut target, Operation::Prepare(offer));
    // The coordinator must obtain this newer incarnation through a strict read;
    // a timeout or missing state does not justify discarding a reservation.
    assert!(source.streams["s"].incarnation > id().incarnation);
    run(
        &mut target,
        finish(Decision::Aborted(Error::StaleIncarnation)),
    );
    assert!(target.fork_targets.is_empty());
    assert!(!target.streams.contains_key("t"));
}
