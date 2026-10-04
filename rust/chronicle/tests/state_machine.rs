use chronicle_raft::{TypeConfig, model::*, storage::SqliteStore};
use openraft::storage::RaftStateMachine;
use openraft::{
    BasicNode, CommittedLeaderId, Entry, EntryPayload, LogId, Membership, RaftSnapshotBuilder,
};

#[test]
fn admission_never_reuses_an_identity_or_address() {
    let mut state = State::default();
    let node = Node {
        addr: "node-4:8080".into(),
        zone: "a".into(),
        draining: false,
    };
    let admit = Command::Admit {
        id: 4,
        node: node.clone(),
    };
    assert!(state.apply(&admit).error.is_none());
    assert_eq!(state.apply(&admit).error, Some(Error::InvalidPlacement));
    assert_eq!(
        state
            .apply(&Command::Admit {
                id: 5,
                node: node.clone()
            })
            .error,
        Some(Error::InvalidPlacement)
    );
    let mut changed = node.clone();
    changed.addr = "other:8080".into();
    assert_eq!(
        state
            .apply(&Command::Register {
                id: 4,
                node: changed
            })
            .error,
        Some(Error::InvalidPlacement)
    );
    let mut drained = node;
    drained.draining = true;
    assert!(
        state
            .apply(&Command::Register {
                id: 4,
                node: drained
            })
            .error
            .is_none()
    );
    assert_eq!(state.nodes.len(), 1);
    assert!(state.nodes[&4].draining);
}

#[tokio::test]
async fn retirement_history_survives_snapshot_and_distinguishes_repromotion_from_untouched() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("control");
    let mut store = SqliteStore::open(&path).await.unwrap();
    for id in 1..=4 {
        store
            .apply(vec![entry(
                id,
                Command::Register {
                    id,
                    node: Node {
                        addr: format!("node-{id}"),
                        zone: "a".into(),
                        draining: false,
                    },
                },
            )])
            .await
            .unwrap();
    }
    // Retire 4, promote it again, then demote it. An unrelated completed
    // placement must preserve its last demotion boundary rather than recopy it.
    for (generation, voters, boundary) in [
        (1, [1, 2, 3], 17),
        (2, [1, 2, 4], 23),
        (3, [1, 2, 3], 31),
        (4, [1, 2, 3], 39),
    ] {
        let result = store
            .apply(vec![entry(
                3 + generation * 2,
                Command::Place {
                    shard: 0,
                    expected_generation: generation - 1,
                    voters: voters.into_iter().collect(),
                    now_ms: generation,
                    eligible_only: true,
                    repair_pending: false,
                },
            )])
            .await
            .unwrap();
        assert!(result[0].error.is_none());
        let mut pending = store.read_state().await.unwrap();
        if generation == 2 || generation == 3 {
            assert_eq!(
                pending
                    .apply(&Command::Placed {
                        shard: 0,
                        generation: generation - 1,
                        membership: Some(LogId::new(CommittedLeaderId::new(3, 1), 999)),
                    })
                    .error,
                Some(Error::InvalidPlacement)
            );
            assert_eq!(
                pending.placements[&0].replicas.as_ref().unwrap()[&4],
                ReplicaHistory::MayVote
            );
        }
        store
            .apply(vec![entry(
                4 + generation * 2,
                Command::Placed {
                    shard: 0,
                    generation,
                    membership: Some(LogId::new(CommittedLeaderId::new(3, 1), boundary)),
                },
            )])
            .await
            .unwrap();
    }
    let snapshot = store.build_snapshot().await.unwrap();
    let other_path = directory.path().join("restored");
    let mut restored = SqliteStore::open(&other_path).await.unwrap();
    restored
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    restored.close().await;
    let mut restored = SqliteStore::open_existing(&other_path).await.unwrap();
    let state = restored.read_state().await.unwrap();
    let placement = &state.placements[&0];
    assert!(placement.retirement_known());
    let replicas = placement.replicas.as_ref().unwrap();
    assert_eq!(
        replicas[&4],
        ReplicaHistory::NonvoterAfter(LogId::new(CommittedLeaderId::new(3, 1), 31))
    );
    assert!(!replicas.contains_key(&5));
    assert_eq!(
        restored
            .apply(vec![entry(
                13,
                Command::Placed {
                    shard: 0,
                    generation: 3,
                    membership: Some(LogId::new(CommittedLeaderId::new(3, 1), 999)),
                }
            )])
            .await
            .unwrap()[0]
            .error,
        Some(Error::InvalidPlacement)
    );
    let retry = restored
        .apply(vec![entry(
            14,
            Command::Placed {
                shard: 0,
                generation: 4,
                membership: Some(LogId::new(CommittedLeaderId::new(3, 1), 999)),
            },
        )])
        .await
        .unwrap();
    assert!(retry[0].error.is_none());
    assert_eq!(
        restored.read_state().await.unwrap().placements[&0].replicas,
        placement.replicas
    );
    store.close().await;
    restored.close().await;
}

#[test]
fn legacy_placement_never_claims_unknown_identities_are_untouched() {
    let mut state = State::default();
    for id in 1..=4 {
        state.apply(&Command::Register {
            id,
            node: Node {
                addr: format!("node-{id}"),
                zone: "a".into(),
                draining: false,
            },
        });
    }
    state.placements.insert(
        0,
        serde_json::from_str(r#"{"generation":1,"voters":[1,2,3],"complete":true,"changed_ms":0}"#)
            .unwrap(),
    );
    assert!(!state.placements[&0].retirement_known());
    let boundary = LogId::new(CommittedLeaderId::new(3, 1), 17);
    state.apply(&Command::Placed {
        shard: 0,
        generation: 1,
        membership: Some(boundary),
    });
    assert!(state.placements[&0].retirement_known());
    assert_eq!(
        state.placements[&0].replicas.as_ref().unwrap()[&4],
        ReplicaHistory::NonvoterAfter(boundary)
    );
}

#[test]
fn draining_fences_cached_placement_without_changing_legacy_replay() {
    let mut state = State::default();
    for id in 1..=4 {
        state.apply(&Command::Register {
            id,
            node: Node {
                addr: format!("node-{id}"),
                zone: "a".into(),
                draining: false,
            },
        });
    }
    let cached = Command::Place {
        shard: 0,
        expected_generation: 0,
        voters: [1, 2, 4].into_iter().collect(),
        now_ms: 0,
        eligible_only: true,
        repair_pending: false,
    };
    let mut drained = state.nodes[&4].clone();
    drained.draining = true;
    state.apply(&Command::Register {
        id: 4,
        node: drained,
    });
    let before = serde_json::to_value(&state).unwrap();
    assert_eq!(state.apply(&cached).error, Some(Error::InvalidPlacement));
    assert_eq!(serde_json::to_value(&state).unwrap(), before);
    let mut legacy = serde_json::to_value(&cached).unwrap();
    legacy["Place"]
        .as_object_mut()
        .unwrap()
        .remove("eligible_only");
    let legacy = serde_json::from_value(legacy).unwrap();
    assert!(state.apply(&legacy).error.is_none()); // Previously committed legacy command.
}

#[tokio::test]
async fn pending_replacement_preserves_history_and_fences_stale_completion_after_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = SqliteStore::open(directory.path().join("control"))
        .await
        .unwrap();
    for id in 1..=4 {
        store
            .apply(vec![entry(
                id,
                Command::Register {
                    id,
                    node: Node {
                        addr: format!("node-{id}"),
                        zone: "a".into(),
                        draining: false,
                    },
                },
            )])
            .await
            .unwrap();
    }
    let original = Command::Place {
        shard: 1,
        expected_generation: 0,
        voters: [1, 2, 4].into(),
        now_ms: 0,
        eligible_only: true,
        repair_pending: false,
    };
    assert!(
        store.apply(vec![entry(5, original)]).await.unwrap()[0]
            .error
            .is_none()
    );
    let replacement = Command::Place {
        shard: 1,
        expected_generation: 1,
        voters: [1, 2, 3].into(),
        now_ms: 15_000,
        eligible_only: true,
        repair_pending: true,
    };
    let state = store.read_state().await.unwrap();
    let before = serde_json::to_value(&state).unwrap();
    // Legacy replay must still reject pending replacement. A second shard and
    // an obsolete generation must also reject without changing placement state.
    for (shard, generation, legacy) in [(1, 1, true), (2, 0, false), (1, 0, false)] {
        let mut value = serde_json::to_value(&replacement).unwrap();
        value["Place"]["shard"] = shard.into();
        value["Place"]["expected_generation"] = generation.into();
        if legacy {
            value["Place"]
                .as_object_mut()
                .unwrap()
                .remove("repair_pending");
        }
        let mut candidate = state.clone();
        assert_eq!(
            candidate
                .apply(&serde_json::from_value(value).unwrap())
                .error,
            Some(Error::InvalidPlacement)
        );
        assert_eq!(serde_json::to_value(candidate).unwrap(), before);
    }
    assert!(
        store.apply(vec![entry(6, replacement)]).await.unwrap()[0]
            .error
            .is_none()
    );
    let snapshot = store.build_snapshot().await.unwrap();
    let path = directory.path().join("restored");
    let mut restored = SqliteStore::open(&path).await.unwrap();
    restored
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    restored.close().await;
    let mut restored = SqliteStore::open_existing(&path).await.unwrap();
    let state = restored.read_state().await.unwrap();
    let p = &state.placements[&1];
    assert_eq!(p.generation, 2);
    assert_eq!(p.voters, [1, 2, 3].into());
    assert!(!p.complete);
    assert_eq!(p.replicas.as_ref().unwrap()[&4], ReplicaHistory::MayVote);
    let boundary = LogId::new(CommittedLeaderId::new(3, 2), 99);
    for (index, generation, expected) in [(7, 1, Some(Error::InvalidPlacement)), (8, 2, None)] {
        assert_eq!(
            restored
                .apply(vec![entry(
                    index,
                    Command::Placed {
                        shard: 1,
                        generation,
                        membership: Some(boundary),
                    }
                )])
                .await
                .unwrap()[0]
                .error,
            expected
        );
        assert_eq!(
            restored.read_state().await.unwrap().placements[&1].complete,
            generation == 2
        );
    }
    let state = restored.read_state().await.unwrap();
    assert_eq!(
        state.placements[&1].replicas.as_ref().unwrap()[&4],
        ReplicaHistory::NonvoterAfter(boundary)
    );
    store.close().await;
    restored.close().await;
}

fn create() -> Command {
    Command::Create {
        key: "s".into(),
        expected_incarnation: None,
        config: StreamConfig {
            content_type: "application/octet-stream".into(),
            json_framing: None,
            expiry: None,
        },
        data: vec![],
        closed: false,
        now_ms: None,
    }
}
fn append(id: &str, seq: u64, data: &[u8]) -> Command {
    Command::Append {
        key: "s".into(),
        incarnation: 1,
        data: data.to_vec(),
        producer: Some(Producer {
            id: id.into(),
            epoch: 0,
            seq,
        }),
        close: false,
        empty_body: data.is_empty(),
        stream_seq: None,
        now_ms: None,
    }
}
fn entry(index: u64, command: Command) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Normal(command),
    }
}

fn ordered(mut command: Command, token: &str) -> Command {
    let Command::Append { stream_seq, .. } = &mut command else {
        panic!("expected append fixture");
    };
    *stream_seq = Some(token.into());
    command
}

#[tokio::test]
async fn stream_order_survives_snapshot_reopen_without_changing_retry_or_epoch_fences() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = SqliteStore::open(directory.path().join("source"))
        .await
        .unwrap();
    let results = store
        .apply([
            entry(1, create()),
            entry(2, ordered(append("p", 0, b"ab"), "10")),
            entry(3, ordered(append("p", 1, b"c"), "2")),
        ])
        .await
        .unwrap();
    assert!(results.iter().all(|r| r.error.is_none()));
    let snapshot = store.build_snapshot().await.unwrap();
    let path = directory.path().join("restored");
    let mut restored = SqliteStore::open(&path).await.unwrap();
    restored
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    restored.close().await;
    let mut restored = SqliteStore::open_existing(&path).await.unwrap();
    let retry = restored
        .apply([entry(4, ordered(append("p", 0, b"ignored"), "zz"))])
        .await
        .unwrap();
    assert!(retry[0].duplicate && retry[0].error.is_none());
    assert_eq!(retry[0].end, 2);
    let mut new_epoch = ordered(append("p", 0, b"rejected"), "10");
    let Command::Append { producer, .. } = &mut new_epoch else {
        unreachable!()
    };
    producer.as_mut().unwrap().epoch = 1;
    let rejected = restored.apply([entry(5, new_epoch)]).await.unwrap();
    assert_eq!(rejected[0].error, Some(Error::StreamSequenceConflict));
    assert_eq!(rejected[0].end, 3);
    let next = restored
        .apply([entry(6, ordered(append("p", 2, b"d"), "3"))])
        .await
        .unwrap();
    assert!(next[0].error.is_none()); // Rejection neither fenced epoch zero nor consumed sequence two.
    let mut close = ordered(append("p", 3, b""), "4");
    let Command::Append { close: flag, .. } = &mut close else {
        unreachable!()
    };
    *flag = true;
    assert!(restored.apply([entry(7, close)]).await.unwrap()[0].closed);
    let s = restored.read_state().await.unwrap();
    assert_eq!(s.streams["s"].last_seq.as_deref(), Some("4"));
    assert_eq!(s.streams["s"].data, b"abcd");
    store.close().await;
    restored.close().await;
}

#[test]
fn stream_order_empty_absent_recreation_and_legacy_replay_are_distinct() {
    let mut state = State::default();
    state.apply(&create());
    assert!(
        state
            .apply(&ordered(append("p", 0, b"a"), ""))
            .error
            .is_none()
    );
    assert!(state.apply(&append("p", 1, b"b")).error.is_none());
    assert_eq!(state.streams["s"].last_seq.as_deref(), Some(""));
    assert_eq!(
        state.apply(&ordered(append("p", 2, b"wrong"), "")).error,
        Some(Error::StreamSequenceConflict)
    );
    assert!(
        state
            .apply(&ordered(append("p", 2, b"c"), "10"))
            .error
            .is_none()
    );
    assert!(state.apply(&create()).duplicate);
    assert_eq!(state.streams["s"].last_seq.as_deref(), Some("10"));
    let mut legacy = serde_json::to_value(append("p", 3, b"d")).unwrap();
    legacy["Append"]
        .as_object_mut()
        .unwrap()
        .remove("stream_seq");
    assert!(
        state
            .apply(&serde_json::from_value(legacy).unwrap())
            .error
            .is_none()
    );
    assert_eq!(state.streams["s"].last_seq.as_deref(), Some("10"));
    let mut legacy_state = serde_json::to_value(&state).unwrap();
    legacy_state["streams"]["s"]
        .as_object_mut()
        .unwrap()
        .remove("last_seq");
    let legacy_state: State = serde_json::from_value(legacy_state).unwrap();
    assert!(legacy_state.streams["s"].last_seq.is_none());
    state.apply(&Command::Delete {
        key: "s".into(),
        incarnation: 1,
        expired_at: None,
    });
    assert!(state.streams["s"].last_seq.is_none());
    let mut recreate = create();
    let Command::Create {
        expected_incarnation,
        ..
    } = &mut recreate
    else {
        unreachable!()
    };
    *expected_incarnation = Some(2);
    assert!(state.apply(&recreate).error.is_none());
    let mut fresh = ordered(append("p", 0, b"new"), "");
    assert_eq!(state.apply(&fresh).error, Some(Error::StaleIncarnation));
    let Command::Append { incarnation, .. } = &mut fresh else {
        unreachable!()
    };
    *incarnation = 2;
    assert!(state.apply(&fresh).error.is_none());
}

#[test]
fn stream_order_metadata_is_bounded_and_replacement_charges_only_growth() {
    let mut state = State::default();
    state.apply(&create());
    // Independent accounting: key(1), content type(24), stream metadata(256), byte(1).
    let length = MAX_SHARD_BYTES - 282;
    let mut command = ordered(append("p", 0, b"x"), &"a".repeat(length));
    let Command::Append { producer, .. } = &mut command else {
        unreachable!()
    };
    *producer = None;
    assert!(state.apply(&command).error.is_none());
    let Command::Append {
        data, empty_body, ..
    } = &mut command
    else {
        unreachable!()
    };
    data.clear();
    *empty_body = false; // Legacy zero-byte command remains replayable.
    let command = ordered(command, &"b".repeat(length));
    assert!(state.apply(&command).error.is_none());
    let command = ordered(command, &"c".repeat(length + 1));
    assert_eq!(state.apply(&command).error, Some(Error::Capacity));
    assert_eq!(
        state.streams["s"].last_seq.as_deref(),
        Some("b".repeat(length).as_str())
    );
}

#[test]
fn old_sequence_returns_its_original_frontier_after_interleaving() {
    let mut s = State::default();
    s.apply(&create());
    assert_eq!(s.apply(&append("p", 0, b"abc")).end, 3);
    assert_eq!(s.apply(&append("other", 0, b"12")).end, 5);
    assert_eq!(s.apply(&append("p", 1, b"defg")).end, 9);
    let retry = s.apply(&append(
        "p",
        0,
        b"different bytes are ignored for duplicate identity",
    ));
    assert_eq!(retry.end, 3);
    assert!(retry.duplicate);
    assert_eq!(s.streams["s"].data, b"abc12defg");
}

#[test]
fn content_type_and_metadata_are_bounded_by_capacity() {
    let mut state = State::default();
    let mut oversized = create();
    let Command::Create { config, .. } = &mut oversized else {
        unreachable!()
    };
    config.content_type = "x".repeat(MAX_CONTENT_TYPE_BYTES + 1);
    assert_eq!(state.apply(&oversized).error, Some(Error::Capacity));

    let content_type = "x".repeat(MAX_CONTENT_TYPE_BYTES);
    let mut rejected = false;
    for index in 0..10_000 {
        let result = state.apply(&Command::Create {
            key: format!("metadata-heavy-{index:05}"),
            expected_incarnation: None,
            config: StreamConfig {
                content_type: content_type.clone(),
                json_framing: None,
                expiry: None,
            },
            data: Vec::new(),
            closed: false,
            now_ms: None,
        });
        if result.error == Some(Error::Capacity) {
            rejected = true;
            break;
        }
    }
    assert!(rejected);
    assert!(state.streams.len() < 10_000);
}

#[test]
fn media_identity_preserves_config_and_distinguishes_json_prefixes() {
    for (a, b, expected) in [
        ("APPLICATION/JSON; charset=utf-8", "application/json", true),
        ("text/plain;charset=ascii", "TEXT/PLAIN;charset=utf-8", true),
        ("application/jsonp", "application/json", false),
        ("text/K", "text/k", false),
        ("", "APPLICATION/OCTET-STREAM", true),
        ("text/plain ", "text/plain", false),
    ] {
        assert_eq!(content_type_matches(a, b), expected, "{a} vs {b}");
        assert_eq!(content_type_matches(b, a), expected);
    }
    let mut state = State::default();
    let mut command = create();
    let Command::Create { config, .. } = &mut command else {
        unreachable!()
    };
    config.content_type = "APPLICATION/JSON; charset=utf-8".into();
    config.json_framing = Some(true);
    assert!(state.apply(&command).error.is_none());
    let Command::Create { config, .. } = &mut command else {
        unreachable!()
    };
    config.content_type = "application/json".into();
    let reply = state.apply(&command);
    assert!(reply.duplicate);
    assert_eq!(
        reply.content_type.as_deref(),
        Some("APPLICATION/JSON; charset=utf-8")
    );
    assert_eq!(
        state.streams["s"].config.content_type,
        "APPLICATION/JSON; charset=utf-8"
    );
    let Command::Create { config, .. } = &mut command else {
        unreachable!()
    };
    config.content_type = "application/jsonp".into();
    assert_eq!(state.apply(&command).error, Some(Error::ConfigConflict));
    let Command::Create { config, .. } = &mut command else {
        unreachable!()
    };
    config.content_type = "APPLICATION/JSON; charset=utf-8".into();
    config.json_framing = None; // Same media type, different legacy wire interpretation.
    assert_eq!(state.apply(&command).error, Some(Error::ConfigConflict));
}

#[test]
fn stale_lifecycle_operations_cannot_mutate_recreated_stream() {
    let mut s = State::default();
    s.apply(&create());
    let old = append("p", 0, b"old");
    s.apply(&old);
    s.apply(&Command::Delete {
        key: "s".into(),
        incarnation: 1,
        expired_at: None,
    });
    assert_eq!(s.apply(&create()).error, Some(Error::StaleIncarnation));
    let Command::Create {
        key,
        config,
        data,
        closed,
        ..
    } = create()
    else {
        unreachable!()
    };
    s.apply(&Command::Create {
        key,
        config,
        data,
        closed,
        expected_incarnation: Some(2),
        now_ms: None,
    });
    assert_eq!(s.apply(&old).error, Some(Error::StaleIncarnation));
    assert_eq!(
        s.apply(&Command::Delete {
            key: "s".into(),
            incarnation: 1,
            expired_at: None
        })
        .error,
        Some(Error::StaleIncarnation)
    );
    assert!(!s.streams["s"].deleted);
    assert!(s.streams["s"].data.is_empty());
}

#[test]
fn gaps_are_rejected_without_consuming_producer_identity() {
    let mut s = State::default();
    s.apply(&create());
    assert_eq!(
        s.apply(&append("p", 1, b"later")).error,
        Some(Error::SequenceGap)
    );
    s.apply(&append("p", 0, b"first"));
    let gap = s.apply(&append("p", 2, b"third"));
    assert_eq!(gap.error, Some(Error::SequenceGap));
    assert_eq!(gap.producer, Some(ProducerPosition { epoch: 0, seq: 0 }));
    assert_eq!(s.apply(&append("p", 1, b"later")).end, 10);
    assert_eq!(s.apply(&append("p", 2, b"third")).end, 15);
    assert_eq!(gap.producer, Some(ProducerPosition { epoch: 0, seq: 0 }));
}

#[tokio::test]
async fn closure_replies_follow_apply_and_survive_snapshot_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = SqliteStore::open(directory.path().join("source"))
        .await
        .unwrap();
    let first = store
        .apply([entry(1, create()), entry(2, append("p", 0, b"abc"))])
        .await
        .unwrap();
    assert!(!first[1].closed);
    assert_eq!(
        first[1].producer,
        Some(ProducerPosition { epoch: 0, seq: 0 })
    );
    let mut changed_retry = append("p", 0, b"not applied");
    if let Command::Append { close, .. } = &mut changed_retry {
        *close = true;
    }
    let duplicate = store.apply([entry(3, changed_retry)]).await.unwrap();
    assert!(duplicate[0].duplicate && !duplicate[0].closed);
    assert_eq!(duplicate[0].end, 3);
    let mut closing = append("p", 1, b"12");
    if let Command::Append { close, .. } = &mut closing {
        *close = true;
    }
    let closed = store.apply([entry(4, closing)]).await.unwrap();
    assert!(closed[0].closed && closed[0].error.is_none());
    assert!(!first[1].closed); // A later close cannot rewrite an earlier reply.
    let snapshot = store.build_snapshot().await.unwrap();
    let path = directory.path().join("installed");
    let mut installed = SqliteStore::open(&path).await.unwrap();
    installed
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    installed.close().await;
    let mut installed = SqliteStore::open_existing(&path).await.unwrap();
    let replay = installed
        .apply([entry(5, append("p", 0, b"ignored"))])
        .await
        .unwrap();
    assert!(replay[0].closed && replay[0].duplicate);
    assert_eq!(replay[0].end, 3); // Original reply frontier, not closed tail 5.
    assert_eq!(
        replay[0].producer,
        Some(ProducerPosition { epoch: 0, seq: 1 })
    );
    let rejected = installed
        .apply([entry(6, append("p", 2, b"rejected"))])
        .await
        .unwrap();
    assert_eq!(rejected[0].error, Some(Error::Closed));
    assert!(rejected[0].closed);
    assert_eq!(rejected[0].end, 5);
    let repeated = Command::Append {
        key: "s".into(),
        incarnation: 1,
        data: Vec::new(),
        producer: None,
        close: true,
        empty_body: true,
        stream_seq: None,
        now_ms: None,
    };
    let repeated = installed.apply([entry(7, repeated)]).await.unwrap();
    assert!(repeated[0].closed && repeated[0].duplicate && repeated[0].error.is_none());
    assert_eq!(repeated[0].end, 5);
    assert_eq!(
        installed
            .read_stream("s".into())
            .await
            .unwrap()
            .unwrap()
            .data,
        b"abc12"
    );
    let empty_retry = installed
        .apply([entry(8, append("p", 1, b""))])
        .await
        .unwrap();
    assert!(empty_retry[0].closed && empty_retry[0].duplicate && empty_retry[0].error.is_none());
    assert_eq!(empty_retry[0].end, 5);
    installed.close().await;
    store.close().await;
}

#[test]
fn empty_body_validation_preserves_retention_and_legacy_zero_byte_commands() {
    let mut state = State::default();
    state.apply(&create());
    let empty = append("p", 0, b"");
    assert_eq!(state.apply(&empty).error, Some(Error::EmptyBody));
    assert!(state.streams["s"].producers.is_empty());
    assert!(state.apply(&append("p", 0, b"abc")).error.is_none());
    let retry = state.apply(&empty);
    assert!(retry.duplicate && retry.error.is_none() && !retry.closed);
    assert_eq!(retry.end, 3);
    let mut legacy = serde_json::to_value(append("p", 1, b"")).unwrap();
    legacy["Append"]
        .as_object_mut()
        .unwrap()
        .remove("empty_body");
    let legacy = serde_json::from_value(legacy).unwrap();
    assert!(state.apply(&legacy).error.is_none());
    assert_eq!(state.streams["s"].producers["p"].seq, 1);
    assert_eq!(state.streams["s"].data, b"abc");
}

#[test]
fn create_matches_current_closure_without_mutating_it() {
    for closed in [false, true] {
        let mut state = State::default();
        let mut command = create();
        if let Command::Create { closed: flag, .. } = &mut command {
            *flag = closed;
        }
        let created = state.apply(&command);
        assert_eq!(created.closed, closed);
        let duplicate = state.apply(&command);
        assert!(duplicate.duplicate && duplicate.error.is_none());
        assert_eq!(duplicate.closed, closed);
        if let Command::Create { closed: flag, .. } = &mut command {
            *flag = !closed;
        }
        let before = serde_json::to_value(&state).unwrap();
        assert_eq!(state.apply(&command).error, Some(Error::ConfigConflict));
        assert_eq!(serde_json::to_value(&state).unwrap(), before);
    }
}

#[tokio::test]
async fn snapshot_reopen_preserves_successful_retry_and_membership_boundary() {
    let d = tempfile::tempdir().unwrap();
    let mut a = SqliteStore::open(d.path().join("a")).await.unwrap();
    let membership = Membership::new(
        vec![[1, 2, 3].into_iter().collect()],
        [
            (1, BasicNode::new("node-1")),
            (2, BasicNode::new("node-2")),
            (3, BasicNode::new("node-3")),
        ]
        .into_iter()
        .collect::<std::collections::BTreeMap<_, _>>(),
    );
    a.apply(vec![
        entry(1, create()),
        entry(2, append("p", 0, b"abc")),
        entry(3, append("p", 1, b"12")),
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), 4),
            payload: EntryPayload::Membership(membership.clone()),
        },
    ])
    .await
    .unwrap();
    let snapshot = a.build_snapshot().await.unwrap();
    let mut b = SqliteStore::open(d.path().join("b")).await.unwrap();
    b.install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    b.close().await;
    let mut b = SqliteStore::open(d.path().join("b")).await.unwrap();
    let (applied, installed_membership) = b.applied_state().await.unwrap();
    assert_eq!(applied.unwrap().index, 4);
    assert_eq!(installed_membership.membership(), &membership);
    let results = b
        .apply(vec![entry(5, append("p", 0, b"not applied"))])
        .await
        .unwrap();
    assert_eq!(results[0].end, 3);
    assert!(results[0].duplicate);
    assert_eq!(
        results[0].producer,
        Some(ProducerPosition { epoch: 0, seq: 1 })
    );
    assert_eq!(
        b.read_stream("s".into()).await.unwrap().unwrap().data,
        b"abc12"
    );
    let mut fenced = append("p", 0, b"not applied");
    let Command::Append { producer, .. } = &mut fenced else {
        unreachable!()
    };
    producer.as_mut().unwrap().epoch = u64::MAX;
    let upgraded = b.apply(vec![entry(6, fenced)]).await.unwrap();
    let position = Some(ProducerPosition {
        epoch: u64::MAX,
        seq: 0,
    });
    assert!(upgraded[0].error.is_none());
    assert_eq!(upgraded[0].producer, position);
    let rejected = b
        .apply(vec![entry(7, append("p", 1, b"fenced"))])
        .await
        .unwrap();
    assert_eq!(rejected[0].error, Some(Error::EpochFenced));
    assert_eq!(rejected[0].producer, position);
    // An epoch change cannot rewrite the position captured for the old retry.
    assert_eq!(
        results[0].producer,
        Some(ProducerPosition { epoch: 0, seq: 1 })
    );
}

proptest::proptest! {
    #[test]
    fn stream_order_matches_rank_model(
        schedule in proptest::collection::vec(0usize..8, 1..100),
    ) {
        let tokens = ["", "09", "1", "10", "2", "A", "a"];
        let mut state = State::default();
        state.apply(&create());
        let mut rank = None;
        let mut seq = 0;
        for candidate in schedule {
            let token = tokens.get(candidate);
            let command = append("p", seq, b"x");
            let command = match token {
                Some(t) => ordered(command, t),
                None => command,
            };
            let accepted = token.is_none() || rank.is_none_or(|previous| candidate > previous);
            let outcome = state.apply(&command);
            if accepted {
                seq += 1;
                if token.is_some() { rank = Some(candidate); }
                proptest::prop_assert!(outcome.error.is_none());
            } else {
                proptest::prop_assert_eq!(outcome.error, Some(Error::StreamSequenceConflict));
            }
            proptest::prop_assert_eq!(state.streams["s"].data.len(), seq as usize);
            proptest::prop_assert_eq!(state.streams["s"].last_seq.as_deref(), rank.map(|r| tokens[r]));
        }
    }

    #[test]
    fn committed_duplicate_schedules_preserve_exact_bytes(
        lengths in proptest::collection::vec(1usize..50, 1..30),
        retries in proptest::collection::vec(0usize..100, 0..80),
    ) {
        let mut state = State::default();
        state.apply(&create());
        let mut expected = Vec::new();
        let mut offsets = Vec::new();
        for (seq, len) in lengths.iter().enumerate() {
            let bytes = vec![(seq + 1) as u8; *len];
            expected.extend_from_slice(&bytes);
            offsets.push(expected.len() as u64);
            let result = state.apply(&append("p", seq as u64, &bytes));
            proptest::prop_assert_eq!(result.end, expected.len() as u64);
        }
        for index in retries {
            let seq = index % lengths.len();
            let result = state.apply(&append("p", seq as u64, b"retry"));
            proptest::prop_assert!(result.duplicate);
            proptest::prop_assert_eq!(result.end, offsets[seq]);
            proptest::prop_assert_eq!(result.producer, Some(ProducerPosition {
                epoch: 0,
                seq: lengths.len() as u64 - 1,
            }));
        }
        proptest::prop_assert_eq!(&state.streams["s"].data, &expected);
    }
}
