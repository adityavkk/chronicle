use chronicle_raft::{TypeConfig, expiry::Expiry, model::*, storage::SqliteStore};
use openraft::{
    CommittedLeaderId, Entry, EntryPayload, LogId, RaftSnapshotBuilder, storage::RaftStateMachine,
};

fn create(policy: Expiry, now: u64) -> Command {
    Command::Create {
        key: "s".into(),
        expected_incarnation: None,
        config: StreamConfig {
            content_type: "text/plain".into(),
            json_framing: None,
            expiry: Some(policy),
        },
        data: b"abc".to_vec(),
        closed: false,
        now_ms: Some(now),
    }
}

fn touch(incarnation: u64, now_ms: u64) -> Command {
    Command::Touch {
        key: "s".into(),
        incarnation,
        now_ms,
    }
}

fn expire(incarnation: u64, access_ms: u64, now_ms: u64) -> Command {
    Command::Expire {
        key: "s".into(),
        incarnation,
        access_ms,
        now_ms,
    }
}

fn entry(index: u64, command: Command) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Normal(command),
    }
}

#[test]
fn canonical_policy_parsing_and_overflow_boundaries() {
    for bad in [
        "",
        "00",
        "+1",
        "01",
        "-1",
        "1.0",
        "1e3",
        " 1",
        "1 ",
        "9223372036854775808",
    ] {
        assert!(Expiry::parse(Some(bad), None).is_err(), "{bad}");
    }
    assert_eq!(
        Expiry::parse(Some("0"), None).unwrap(),
        Some(Expiry::Ttl(0))
    );
    let max = Expiry::parse(Some("9223372036854775807"), None)
        .unwrap()
        .unwrap();
    assert!(!max.expired(u64::MAX, u64::MAX));
    assert!(!Expiry::Ttl(2).expired(100, 2100));
    assert!(Expiry::Ttl(2).expired(100, 2101));
    assert!(Expiry::parse(Some("1"), Some("2026-01-01T00:00:00Z")).is_err());
    for bad in [
        "",
        "2026-02-30T00:00:00Z",
        "2026-01-01",
        "not-a-date",
        "2026-01-01X00:00:00Z",
        "2026-01-01 00:00:00Z",
        "2026-01-01T00:00:00+24:00",
        "2026-01-01T00:00:00+00:60",
    ] {
        assert!(Expiry::parse(None, Some(bad)).is_err());
    }
    let absolute = Expiry::parse(None, Some("1970-01-01T01:00:02.000001+01:00"))
        .unwrap()
        .unwrap();
    assert_eq!(
        absolute,
        Expiry::At {
            seconds: 2,
            nanos: 1000
        }
    );
    assert!(!absolute.expired(9000, 2000));
    assert!(absolute.expired(9000, 2001));
    assert_eq!(
        absolute.absolute_header().as_deref(),
        Some("1970-01-01T00:00:02.000001Z")
    );
}

#[test]
fn expiry_fences_renewal_and_recreation_without_sliding_absolute_deadline() {
    let mut state = State::default();
    assert!(state.apply(&create(Expiry::Ttl(2), 100)).error.is_none());
    assert!(state.apply(&touch(1, 1500)).error.is_none());
    assert!(state.apply(&touch(1, 900)).error.is_none()); // Clock rollback cannot shorten TTL.
    assert_eq!(state.streams["s"].access_ms, 1500);
    assert_eq!(
        state.apply(&expire(1, 100, 2101)).error,
        Some(Error::ConfigConflict)
    );
    assert!(state.apply(&create(Expiry::Ttl(2), 2000)).duplicate);
    assert_eq!(state.streams["s"].access_ms, 1500); // Idempotent PUT compares policy.
    assert_eq!(state.apply(&touch(1, 3501)).error, Some(Error::Missing));
    assert!(state.apply(&expire(1, 1500, 3501)).error.is_none());
    let mut next = create(
        Expiry::At {
            seconds: 8,
            nanos: 0,
        },
        4000,
    );
    let Command::Create {
        expected_incarnation,
        ..
    } = &mut next
    else {
        unreachable!()
    };
    *expected_incarnation = Some(2);
    assert!(state.apply(&next).error.is_none());
    assert_eq!(
        state.apply(&expire(1, 1500, 9000)).error,
        Some(Error::StaleIncarnation)
    );
    assert!(state.apply(&touch(2, 7000)).error.is_none());
    assert_eq!(state.streams["s"].access_ms, 4000);
    assert_eq!(state.apply(&touch(2, 8001)).error, Some(Error::Missing));
}

#[tokio::test]
async fn renewal_and_expiry_survive_snapshot_install_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let mut source = SqliteStore::open(dir.path().join("source")).await.unwrap();
    source
        .apply([
            entry(1, create(Expiry::Ttl(2), 100)),
            entry(2, touch(1, 1500)),
        ])
        .await
        .unwrap();
    let snapshot = source.build_snapshot().await.unwrap();
    let path = dir.path().join("target");
    let mut target = SqliteStore::open(&path).await.unwrap();
    target
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    target.close().await;
    let mut target = SqliteStore::open_existing(&path).await.unwrap();
    assert_eq!(
        target
            .read_info("s".into(), ())
            .await
            .unwrap()
            .unwrap()
            .access_ms,
        1500
    );
    let outcomes = target
        .apply([entry(3, expire(1, 100, 2101)), entry(4, touch(1, 3000))])
        .await
        .unwrap();
    assert_eq!(outcomes[0].error, Some(Error::ConfigConflict));
    assert!(outcomes[1].error.is_none());
    target.close().await;
    let mut target = SqliteStore::open_existing(&path).await.unwrap();
    assert_eq!(
        target
            .read_info("s".into(), ())
            .await
            .unwrap()
            .unwrap()
            .access_ms,
        3000
    );
    assert!(
        target
            .apply([entry(5, expire(1, 3000, 5001))])
            .await
            .unwrap()[0]
            .error
            .is_none()
    );
    target.close().await;
    let target = SqliteStore::open_existing(&path).await.unwrap();
    let stream = target.read_stream("s".into()).await.unwrap().unwrap();
    assert!(stream.deleted);
    assert!(stream.data.is_empty());
    target.close().await;
    source.close().await;
}

#[test]
fn legacy_deadline_and_commands_preserve_replay_semantics() {
    let create: Command = serde_json::from_str(r#"{"Create":{"key":"s","expected_incarnation":null,"config":{"content_type":"text/plain","expires_ms":1234},"data":[97],"closed":false}}"#).unwrap();
    let append: Command = serde_json::from_str(
        r#"{"Append":{"key":"s","incarnation":1,"data":[98],"producer":null,"close":false}}"#,
    )
    .unwrap();
    let mut state = State::default();
    state.apply(&create);
    assert!(state.apply(&append).error.is_none());
    assert_eq!(state.streams["s"].data, b"ab");
    assert_eq!(
        state.streams["s"].config.expiry,
        Some(Expiry::At {
            seconds: 1,
            nanos: 234_000_000
        })
    );
    assert!(
        state
            .apply(&Command::Delete {
                key: "s".into(),
                incarnation: 1,
                expired_at: Some(1234)
            })
            .error
            .is_none()
    );
}

#[tokio::test]
async fn append_renewal_precedes_validation_but_not_incarnation_or_expiry_fences() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("append");
    let mut store = SqliteStore::open(&path).await.unwrap();
    store
        .apply([entry(1, create(Expiry::Ttl(2), 100))])
        .await
        .unwrap();
    let append = |incarnation, now_ms, seq| Command::Append {
        key: "s".into(),
        incarnation,
        now_ms: Some(now_ms),
        data: b"xy".to_vec(),
        producer: Some(Producer {
            id: "p".into(),
            epoch: 0,
            seq,
        }),
        close: false,
        empty_body: false,
        stream_seq: None,
    };
    let out = store
        .apply([
            entry(2, append(1, 1000, 0)),
            entry(3, append(1, 1500, 0)),
            entry(4, append(1, 2000, 2)), // Gap rejection renews, but does not consume seq2.
        ])
        .await
        .unwrap();
    assert_eq!(out[0].end, 5);
    assert!(out[1].duplicate);
    assert_eq!(out[2].error, Some(Error::SequenceGap));
    store.close().await;
    let mut store = SqliteStore::open_existing(&path).await.unwrap();
    assert_eq!(
        store
            .read_stream("s".into())
            .await
            .unwrap()
            .unwrap()
            .access_ms,
        2000
    );
    let out = store
        .apply([
            entry(5, append(2, 3500, 1)),
            entry(6, append(1, 1500, 1)),
            entry(7, append(1, 4001, 2)),
        ])
        .await
        .unwrap();
    assert_eq!(out[0].error, Some(Error::StaleIncarnation));
    assert_eq!(out[1].end, 7);
    assert_eq!(out[2].error, Some(Error::Missing));
    assert_eq!(
        store
            .read_stream("s".into())
            .await
            .unwrap()
            .unwrap()
            .access_ms,
        2000
    );
    store.close().await;
}

#[tokio::test]
async fn legacy_snapshot_installs_without_inventing_sliding_policy() {
    use sha2::{Digest, Sha256};
    use std::io::Cursor;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy");
    let body = br#"{"state":{"streams":{"s":{"incarnation":1,"config":{"content_type":"text/plain","expires_ms":1234},"data":[97],"closed":false,"deleted":false,"producers":{}}},"nodes":{},"placements":{}},"last_applied":null,"membership":{"log_id":null,"membership":{"configs":[],"nodes":{}}}}"#;
    let mut bytes = Sha256::digest(body).to_vec();
    bytes.extend_from_slice(body);
    let mut store = SqliteStore::open(&path).await.unwrap();
    store
        .install_snapshot(
            &openraft::SnapshotMeta::default(),
            Box::new(Cursor::new(bytes)),
        )
        .await
        .unwrap();
    store.close().await;
    let mut store = SqliteStore::open_existing(&path).await.unwrap();
    assert!(
        store.apply([entry(1, touch(1, 1200))]).await.unwrap()[0]
            .error
            .is_none()
    );
    assert_eq!(
        store
            .read_stream("s".into())
            .await
            .unwrap()
            .unwrap()
            .access_ms,
        0
    );
    assert_eq!(
        store.apply([entry(2, touch(1, 1235))]).await.unwrap()[0].error,
        Some(Error::Missing)
    );
    store.close().await;
    for value in ["null", "18446744073709551615"] {
        let config: StreamConfig = serde_json::from_str(&format!(
            r#"{{"content_type":"text/plain","expires_ms":{value}}}"#
        ))
        .unwrap();
        assert_eq!(
            config.expiry.and_then(Expiry::fixed_millis),
            value.parse().ok()
        );
    }
}
