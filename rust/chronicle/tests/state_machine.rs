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
            expires_ms: None,
        },
        data: vec![],
        closed: false,
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
    }
}
fn entry(index: u64, command: Command) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Normal(command),
    }
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
                expires_ms: None,
            },
            data: Vec::new(),
            closed: false,
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
    assert_eq!(s.apply(&append("p", 1, b"later")).end, 10);
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
        b.read_stream("s".into()).await.unwrap().unwrap().data,
        b"abc12"
    );
    let mut fenced = append("p", 0, b"not applied");
    let Command::Append { producer, .. } = &mut fenced else {
        unreachable!()
    };
    producer.as_mut().unwrap().epoch = u64::MAX;
    assert!(
        b.apply(vec![entry(6, fenced)]).await.unwrap()[0]
            .error
            .is_none()
    );
    assert_eq!(
        b.apply(vec![entry(7, append("p", 1, b"fenced"))])
            .await
            .unwrap()[0]
            .error,
        Some(Error::EpochFenced)
    );
}

proptest::proptest! {
    #[test]
    fn committed_duplicate_schedules_preserve_exact_bytes(lengths in proptest::collection::vec(1usize..50,1..30), retries in proptest::collection::vec(0usize..100,0..80)) {
        let mut s=State::default();s.apply(&create());
        let mut expected=Vec::new();let mut offsets=Vec::new();
        for (seq,len) in lengths.iter().enumerate() {
            let bytes=vec![(seq+1) as u8;*len];expected.extend_from_slice(&bytes);offsets.push(expected.len() as u64);
            proptest::prop_assert_eq!(s.apply(&append("p",seq as u64,&bytes)).end,expected.len() as u64);
        }
        for index in retries {let seq=index%lengths.len();let result=s.apply(&append("p",seq as u64,b"retry"));proptest::prop_assert!(result.duplicate);proptest::prop_assert_eq!(result.end,offsets[seq]);}
        proptest::prop_assert_eq!(&s.streams["s"].data,&expected);
    }
}
