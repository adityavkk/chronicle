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
