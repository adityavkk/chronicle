use chronicle_raft::{
    Entry, LogId, Vote,
    leadership::{EXPIRES_MS, Observation, Operation, Phase, Proposal},
    model::{Command, Error, Node, SHARDS, State},
    storage::SqliteStore,
};
use openraft::{
    EntryPayload,
    storage::{RaftSnapshotBuilder, RaftStateMachine},
};

fn setup() -> Vec<Command> {
    let mut commands: Vec<_> = (1..=4)
        .map(|id| Command::Register {
            id,
            node: Node {
                addr: format!("n{id}"),
                zone: id.to_string(),
                draining: false,
            },
        })
        .collect();
    for shard in 0..=SHARDS {
        commands.extend([
            Command::Place {
                shard,
                expected_generation: 0,
                voters: [1, 2, 3].into(),
                now_ms: 0,
                eligible_only: true,
                repair_pending: false,
            },
            Command::Placed {
                shard,
                generation: 1,
                membership: None,
            },
        ]);
    }
    commands
}

fn state() -> State {
    let mut state = State::default();
    for command in setup() {
        assert_eq!(state.apply(&command).error, None);
    }
    state
}

fn proposal() -> Proposal {
    let vote = Vote::new_committed(4, 1);
    Proposal {
        shard: 2,
        generation: 1,
        source_vote: vote,
        membership: LogId::new(vote.leader_id, 15),
        target: 2,
        created_ms: 60_000,
    }
}

fn plan(expected_id: u64, proposal: Proposal) -> Command {
    Command::Leadership(Operation::Plan {
        expected_id,
        proposal,
    })
}

fn claim(id: u64, now_ms: u64) -> Command {
    Command::Leadership(Operation::Claim {
        id,
        executor: 1,
        now_ms,
    })
}

fn close(id: u64, now_ms: u64) -> Command {
    Command::Leadership(Operation::Close {
        id,
        now_ms,
        observed: None,
    })
}

#[test]
fn claim_response_is_one_shot_and_new_ids_cannot_rearm_consumed_vote() {
    let mut state = state();
    let p = proposal();
    assert_eq!(state.apply(&plan(0, p.clone())).error, None);
    assert_eq!(
        state.apply(&plan(0, p.clone())).error,
        Some(Error::InvalidPlacement)
    );
    assert_eq!(state.apply(&claim(1, 60_010)).error, None);
    let saved = state.leadership.clone();
    for time in [60_010, 60_011, 120_010] {
        assert_eq!(
            state.apply(&claim(1, time)).error,
            Some(Error::InvalidPlacement)
        );
        assert_eq!(state.leadership, saved);
    }
    assert_eq!(state.apply(&close(1, 90_000)).error, None);
    assert_eq!(
        state.leadership.attempt.as_ref().unwrap().phase,
        Phase::ClosedUnknown
    );
    let mut next = p.clone();
    next.created_ms = 120_010;
    assert_eq!(
        state.apply(&plan(1, next.clone())).error,
        Some(Error::InvalidPlacement)
    );
    next.source_vote = Vote::new_committed(3, 3);
    assert_eq!(
        state.apply(&plan(1, next.clone())).error,
        Some(Error::InvalidPlacement)
    );
    // Full committed identity, not just term: a higher node in the same term
    // ranks above the old source. This is admission, not evidence of a real vote.
    next.source_vote = Vote::new_committed(4, 2);
    next.target = 3;
    assert_eq!(state.apply(&plan(1, next)).error, None);
    assert_eq!(state.placements[&2].generation, 1);
}

#[test]
fn cooldown_is_global_and_expiry_does_not_erase_it() {
    let mut state = state();
    assert_eq!(state.apply(&plan(0, proposal())).error, None);
    assert_eq!(state.apply(&claim(1, 60_010)).error, None);
    assert_eq!(state.apply(&close(1, 90_000)).error, None);
    let mut next = proposal();
    next.shard = 3;
    for time in [0, 60_000, 120_009] {
        next.created_ms = time;
        assert_eq!(
            state.apply(&plan(1, next.clone())).error,
            Some(Error::InvalidPlacement)
        );
    }
    next.created_ms = 120_010;
    assert_eq!(state.apply(&plan(1, next)).error, None);
    assert_eq!(
        state.apply(&close(1, 200_000)).error,
        Some(Error::InvalidPlacement)
    );
    assert_eq!(
        state.apply(&claim(2, 120_010 + EXPIRES_MS)).error,
        Some(Error::InvalidPlacement)
    );
    assert_eq!(state.apply(&close(2, 120_010 + EXPIRES_MS)).error, None);
    assert_eq!(
        state.leadership.attempt.as_ref().unwrap().phase,
        Phase::ClosedUnused
    );
    assert_eq!(state.leadership.last_claim_ms, Some(60_010));
}

#[test]
fn repair_preempts_claim_while_elective_replica_balance_waits() {
    let mut state = state();
    assert_eq!(state.apply(&plan(0, proposal())).error, None);
    assert!(!chronicle_raft::balance::admissible(
        &state,
        1,
        &[2, 3, 4].into(),
        60_000
    ));
    assert_eq!(
        state
            .apply(&Command::Place {
                shard: 1,
                expected_generation: 1,
                voters: [2, 3, 4].into(),
                now_ms: 60_001,
                eligible_only: true,
                repair_pending: false
            })
            .error,
        None
    );
    assert_eq!(
        state.apply(&claim(1, 60_002)).error,
        Some(Error::InvalidPlacement)
    );
    assert_eq!(state.apply(&close(1, 60_002)).error, None);
    assert_eq!(
        state.leadership.attempt.as_ref().unwrap().phase,
        Phase::ClosedUnused
    );
    assert!(state.leadership.consumed.is_empty());
}

#[test]
fn completion_requires_claim_successor_target_and_matching_membership() {
    let mut state = state();
    let p = proposal();
    assert_eq!(state.apply(&plan(0, p.clone())).error, None);
    let observe = |vote, membership| {
        Command::Leadership(Operation::Close {
            id: 1,
            now_ms: 60_020,
            observed: Some(Observation { vote, membership }),
        })
    };
    let successor = Vote::new_committed(5, 2);
    assert_eq!(
        state.apply(&observe(successor, p.membership)).error,
        Some(Error::InvalidPlacement)
    );
    assert_eq!(state.apply(&claim(1, 60_010)).error, None);
    for vote in [p.source_vote, Vote::new_committed(5, 3), Vote::new(5, 2)] {
        assert_eq!(
            state.apply(&observe(vote, p.membership)).error,
            Some(Error::InvalidPlacement)
        );
    }
    assert_eq!(
        state
            .apply(&observe(successor, LogId::new(p.source_vote.leader_id, 16)))
            .error,
        Some(Error::InvalidPlacement)
    );
    assert_eq!(state.apply(&observe(successor, p.membership)).error, None);
    assert_eq!(
        state.leadership.attempt.as_ref().unwrap().phase,
        Phase::ObservedTarget
    );
    assert_eq!(
        state.apply(&claim(1, 120_000)).error,
        Some(Error::InvalidPlacement)
    );
}

async fn persist(
    store: &mut SqliteStore,
    index: u64,
    command: Command,
) -> chronicle_raft::model::Outcome {
    store
        .apply_entries([Entry {
            log_id: LogId::new(Vote::new_committed(4, 1).leader_id, index),
            payload: EntryPayload::Normal(command),
        }])
        .await
        .unwrap()
        .remove(0)
}

#[tokio::test]
async fn committed_claim_survives_restart_and_snapshot_install_without_a_new_permit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.sqlite");
    let mut store = SqliteStore::open(&path).await.unwrap();
    let mut commands = setup();
    commands.extend([plan(0, proposal()), claim(1, 60_010)]);
    let mut index = 1;
    for command in commands {
        assert_eq!(persist(&mut store, index, command).await.error, None);
        index += 1;
    }
    let saved = store.read_state().await.unwrap().leadership;
    let snapshot = store
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    store.close().await;
    let mut store = SqliteStore::open_existing(&path).await.unwrap();
    assert_eq!(store.read_state().await.unwrap().leadership, saved);
    assert_eq!(
        persist(&mut store, index, claim(1, 120_010)).await.error,
        Some(Error::InvalidPlacement)
    );
    store.close().await;
    let target = dir.path().join("target.sqlite");
    let mut store = SqliteStore::open(&target).await.unwrap();
    store
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    store.close().await;
    let mut store = SqliteStore::open_existing(&target).await.unwrap();
    assert_eq!(store.read_state().await.unwrap().leadership, saved);
    assert_eq!(
        persist(&mut store, index, claim(1, 120_010)).await.error,
        Some(Error::InvalidPlacement)
    );
    assert_eq!(
        persist(&mut store, index + 1, close(1, 120_010))
            .await
            .error,
        None
    );
    let mut next = proposal();
    next.created_ms = 120_010;
    assert_eq!(
        persist(&mut store, index + 2, plan(1, next)).await.error,
        Some(Error::InvalidPlacement)
    );
    store.close().await;
}
