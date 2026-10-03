use chronicle_raft::model::{Command, Error, Producer, State, StreamConfig};

fn create(expected_incarnation: Option<u64>) -> Command {
    Command::Create {
        key: "s".into(),
        expected_incarnation,
        config: StreamConfig {
            content_type: "application/octet-stream".into(),
            expires_ms: None,
        },
        data: Vec::new(),
        closed: false,
    }
}

#[test]
fn replays_formal_retention_scenario() {
    let mut state = State::default();
    assert!(state.apply(&create(None)).error.is_none());
    let mut snapshot = None;

    for line in include_str!("../formal/bridge/retention_trace.tsv").lines() {
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        let fields: Vec<_> = line.split('\t').collect();
        let outcome = match fields[0] {
            "append" => state.apply(&Command::Append {
                key: "s".into(),
                incarnation: fields[1].parse().unwrap(),
                data: fields[4].as_bytes().to_vec(),
                producer: Some(Producer {
                    id: "p".into(),
                    epoch: fields[2].parse().unwrap(),
                    seq: fields[3].parse().unwrap(),
                }),
                close: false,
            }),
            "snapshot" => {
                snapshot = Some(serde_json::to_vec(&state).unwrap());
                continue;
            }
            "recover" => {
                state = serde_json::from_slice(snapshot.as_ref().unwrap()).unwrap();
                continue;
            }
            "delete" => state.apply(&Command::Delete {
                key: "s".into(),
                incarnation: fields[1].parse().unwrap(),
                expired_at: None,
            }),
            "recreate" => state.apply(&create(Some(fields[1].parse().unwrap()))),
            operation => panic!("unknown formal operation: {operation}"),
        };

        let expected_error = match fields[5] {
            "-" => None,
            "SequenceGap" => Some(Error::SequenceGap),
            "EpochFenced" => Some(Error::EpochFenced),
            "StaleIncarnation" => Some(Error::StaleIncarnation),
            error => panic!("unknown formal error: {error}"),
        };
        assert_eq!(outcome.error, expected_error, "{line}");
        assert_eq!(outcome.end, fields[6].parse::<u64>().unwrap(), "{line}");
        assert_eq!(
            outcome.duplicate,
            fields[7].parse::<bool>().unwrap(),
            "{line}"
        );
    }

    assert_eq!(state.streams["s"].data, b"e");
    assert_eq!(state.streams["s"].producers["p"].results.len(), 1);
}
