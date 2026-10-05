use chronicle_raft::{
    Entry, LogId,
    fork::{BoundaryError, boundary},
    model::{Command, Error, Producer, State, StreamConfig},
    storage::SqliteStore,
};
use openraft::{EntryPayload, RaftSnapshotBuilder, storage::RaftStateMachine, vote::RaftLeaderId};

fn create(json: bool) -> Command {
    Command::Create {
        key: "s".into(),
        expected_incarnation: Some(1),
        config: StreamConfig {
            content_type: if json {
                "application/json"
            } else {
                "text/plain"
            }
            .into(),
            json_framing: Some(json),
            track_boundaries: true,
            expiry: None,
        },
        data: if json {
            br#""a,b",[1,2],"#.to_vec()
        } else {
            b"abc".to_vec()
        },
        closed: false,
        now_ms: None,
    }
}

fn append(seq: u64, data: &[u8], close: bool) -> Command {
    Command::Append {
        key: "s".into(),
        incarnation: 1,
        data: data.to_vec(),
        producer: Some(Producer {
            id: "p".into(),
            epoch: 0,
            seq,
        }),
        close,
        empty_body: data.is_empty(),
        stream_seq: None,
        now_ms: None,
    }
}

fn entry(index: u64, command: Command) -> Entry {
    Entry {
        log_id: LogId::new(
            openraft::vote::leader_id_adv::CommittedLeaderId::new(1, 1),
            index,
        ),
        payload: EntryPayload::Normal(command),
    }
}

#[tokio::test]
async fn boundaries_survive_duplicate_gap_close_snapshot_install_and_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let mut source = SqliteStore::open(directory.path().join("source"))
        .await
        .unwrap();
    let results = source
        .apply_entries([
            entry(1, create(false)),
            entry(2, append(0, b"defgh", false)),
            entry(3, append(0, b"different retry bytes", false)),
            entry(4, append(2, b"gap", false)),
            entry(5, append(1, b"", true)),
        ])
        .await
        .unwrap();
    assert!(results[2].duplicate);
    assert_eq!(results[3].error, Some(Error::SequenceGap));
    assert!(results[4].closed);
    let snapshot = source.build_snapshot().await.unwrap();
    let path = directory.path().join("target");
    let mut target = SqliteStore::open(&path).await.unwrap();
    target
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    target.close().await;
    let target = SqliteStore::open_existing(&path).await.unwrap();
    let stream = target.read_stream("s".into()).await.unwrap().unwrap();
    assert_eq!(stream.data, b"abcdefgh");
    assert_eq!(stream.append_ends, [3, 8]);
    assert_eq!(boundary(&stream, Some(0), 3), Ok(3));
    assert_eq!(boundary(&stream, Some(0), 4), Err(BoundaryError::Invalid));
    assert_eq!(boundary(&stream, Some(3), 4), Ok(7));
    assert_eq!(boundary(&stream, Some(3), 6), Err(BoundaryError::Invalid));
    assert_eq!(boundary(&stream, Some(2), 0), Err(BoundaryError::Invalid));
    assert_eq!(
        boundary(&stream, Some(3), u64::MAX),
        Err(BoundaryError::Invalid)
    );
    assert_eq!(boundary(&stream, None, 0), Ok(8));
    target.close().await;
    source.close().await;
}

#[test]
fn json_counts_values_not_commas_or_bytes_and_crosses_batches() {
    let mut state = State::default();
    assert!(state.apply(&create(true)).error.is_none());
    assert!(state.apply(&append(0, b"false,", false)).error.is_none());
    let stream = &state.streams["s"];
    assert_eq!(boundary(stream, Some(0), 1), Ok(6));
    assert_eq!(boundary(stream, Some(6), 1), Ok(12));
    assert_eq!(boundary(stream, Some(0), 3), Ok(18));
    assert_eq!(boundary(stream, Some(0), 4), Err(BoundaryError::Invalid));
    assert_eq!(boundary(stream, Some(3), 1), Err(BoundaryError::Invalid));
}

#[test]
fn legacy_replay_does_not_invent_boundaries_and_delete_clears_them() {
    let mut state = State::default();
    let mut encoded = serde_json::to_value(create(false)).unwrap();
    encoded["Create"]["config"]
        .as_object_mut()
        .unwrap()
        .remove("track_boundaries");
    let legacy = serde_json::from_value(encoded).unwrap();
    assert!(state.apply(&legacy).error.is_none());
    assert!(state.apply(&append(0, b"defgh", false)).error.is_none());
    let stream = &state.streams["s"];
    assert!(!stream.config.track_boundaries);
    assert!(stream.append_ends.is_empty());
    assert_eq!(boundary(stream, Some(0), 1), Err(BoundaryError::Legacy));
    assert_eq!(boundary(stream, None, 0), Ok(8));
    let mut tracked = State::default();
    tracked.apply(&create(false));
    assert!(
        tracked
            .apply(&Command::Delete {
                key: "s".into(),
                incarnation: 1,
                expired_at: None,
            })
            .error
            .is_none()
    );
    assert!(tracked.streams["s"].append_ends.is_empty());
}

#[test]
fn boundary_metadata_can_exhaust_capacity_before_payload_does() {
    use chronicle_raft::model::{MAX_SHARD_BYTES, MAX_STREAM_BYTES};
    let mut state = State::default();
    // Two one-byte keys, text/plain headers, fixed 256-byte stream charge,
    // and one 8-byte boundary each. Leave eight bytes: a byte append needs nine.
    let second_bytes = MAX_SHARD_BYTES - MAX_STREAM_BYTES - 2 * (1 + 10 + 256 + 8) - 8;
    for (key, length) in [("a", MAX_STREAM_BYTES), ("b", second_bytes)] {
        let mut command = create(false);
        if let Command::Create {
            key: target, data, ..
        } = &mut command
        {
            *target = key.into();
            *data = vec![b'x'; length];
        }
        assert!(state.apply(&command).error.is_none());
    }
    let result = state.apply(&Command::Append {
        key: "b".into(),
        incarnation: 1,
        data: vec![b'y'],
        producer: None,
        close: false,
        empty_body: false,
        stream_seq: None,
        now_ms: None,
    });
    assert_eq!(result.error, Some(Error::Capacity));
    assert_eq!(state.streams["b"].data.len(), second_bytes);
    assert_eq!(state.streams["b"].append_ends, [second_bytes as u64]);
}
