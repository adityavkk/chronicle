use super::*;
use crate::model::{Error, Placement};

fn fixture() -> (State, Loads) {
    let mut state = State::default();
    for id in 1..=5 {
        state.nodes.insert(
            id,
            Node {
                addr: format!("n{id}"),
                zone: id.to_string(),
                draining: false,
            },
        );
    }
    for g in 0..=SHARDS {
        state.placements.insert(
            g,
            Placement {
                generation: 1,
                complete: true,
                voters: [1, 2, 3].into(),
                ..Default::default()
            },
        );
    }
    let loads = (1..=5)
        .map(|id| (id, (0..=SHARDS).map(|g| (g, Load::default())).collect()))
        .collect();
    (state, loads)
}

fn weights() -> BTreeMap<u64, u64> {
    [(0, 16), (1, 144), (2, 16), (3, 16), (4, 16)].into()
}

#[test]
fn unknown_domains_are_not_evidence_of_diversity() {
    let (mut state, loads) = fixture();
    state.nodes.get_mut(&4).unwrap().zone = "unknown".into();
    state.nodes.get_mut(&5).unwrap().zone.clear();
    assert!(choose(&state, &loads, &weights(), 60_000).is_none());
    state.nodes.get_mut(&4).unwrap().zone = "1".into();
    let Some(Command::Balance { voters, .. }) = choose(&state, &loads, &weights(), 60_000) else {
        panic!()
    };
    assert_eq!(voters, [2, 3, 4].into());
}

#[test]
fn measured_weight_changes_selection_and_queue_pressure_blocks_destination() {
    let (state, mut loads) = fixture();
    assert!(matches!(
        choose(&state, &loads, &weights(), 60_000),
        Some(Command::Balance { shard: 1, .. })
    ));
    let uniform = (0..=SHARDS).map(|g| (g, 16)).collect();
    assert!(matches!(
        choose(&state, &loads, &uniform, 60_000),
        Some(Command::Balance { shard: 0, .. })
    ));
    for id in [4, 5] {
        loads.get_mut(&id).unwrap().get_mut(&2).unwrap().queued = 32;
    }
    assert!(choose(&state, &loads, &weights(), 60_000).is_none());
    loads.get_mut(&5).unwrap().get_mut(&2).unwrap().queued = 31;
    let Some(Command::Balance { voters, .. }) = choose(&state, &loads, &weights(), 60_000) else {
        panic!()
    };
    assert!(voters.contains(&5) && !voters.contains(&4));
}

#[test]
fn apply_enforces_global_cooldown_generation_zones_and_single_swap() {
    let (mut state, loads) = fixture();
    state.nodes.get_mut(&4).unwrap().zone = "1".into();
    assert!(!admissible(&state, 1, &[1, 2, 4].into(), 60_000));
    assert!(admissible(&state, 1, &[2, 3, 4].into(), 60_000));
    assert!(!admissible(&state, 1, &[3, 4, 5].into(), 60_000));
    let first = choose(&state, &loads, &weights(), 60_000).unwrap();
    let Command::Balance { shard, .. } = first.clone() else {
        panic!()
    };
    assert!(state.apply(&first).error.is_none());
    assert_eq!(state.apply(&first).error, Some(Error::InvalidPlacement));
    state.placements.get_mut(&shard).unwrap().complete = true;
    assert!(choose(&state, &loads, &weights(), 119_999).is_none());
    let next = choose(&state, &loads, &weights(), 120_000).unwrap();
    let mut replay = first;
    if let Command::Balance { now_ms, .. } = &mut replay {
        *now_ms = 120_000;
    }
    assert_eq!(state.apply(&replay).error, Some(Error::InvalidPlacement));
    assert!(state.apply(&next).error.is_none());
}

#[test]
fn fixed_weights_descend_to_a_stable_local_minimum() {
    let (mut state, loads) = fixture();
    let weight = weights();
    let potential = |s: &State| -> u64 {
        s.nodes
            .keys()
            .map(|id| {
                s.placements
                    .iter()
                    .filter(|(_, p)| p.voters.contains(id))
                    .map(|(g, _)| weight[g])
                    .sum::<u64>()
                    .pow(2)
            })
            .sum()
    };
    let mut previous = potential(&state);
    let mut count = 0;
    while let Some(command) = choose(&state, &loads, &weight, (count + 1) * 60_000) {
        assert!(state.apply(&command).error.is_none());
        for p in state.placements.values_mut() {
            p.complete = true;
        }
        let next = potential(&state);
        assert!(next < previous);
        previous = next;
        count += 1;
        assert!(count < 30, "fixed demand must not oscillate");
    }
    // Two heavy-replica moves: [208,208,208,0,0] -> [64,64,208,144,144].
    // Subsequent light moves improve the affected pair by only 2.4%, below 10%.
    assert_eq!(count, 2);
    assert_eq!(previous, 92_928);
    assert!(choose(&state, &loads, &weight, 9_000_000).is_none());
}

#[test]
fn observations_require_continuity_and_detect_actor_restart() {
    for interruption in 0..9 {
        let (mut state, mut loads) = fixture();
        let mut window = Window::default();
        let start = Instant::now();
        for seconds in [0, 5, 10, 15, 20, 25] {
            loads.get_mut(&1).unwrap().get_mut(&0).unwrap().busy_us = seconds * 100;
            assert!(
                window
                    .observe(
                        &state,
                        loads.clone(),
                        1,
                        start + Duration::from_secs(seconds),
                        60_000
                    )
                    .is_none()
            );
        }
        match interruption {
            0 => {
                loads.remove(&5);
            }
            1 => {
                loads.get_mut(&1).unwrap().get_mut(&0).unwrap().instance = 1;
            }
            2 => {
                window.reset();
            }
            3 => {
                assert!(
                    window
                        .observe(&state, loads, 1, start + Duration::from_secs(36), 60_000)
                        .is_none()
                );
                continue;
            }
            5 => {
                loads.get_mut(&1).unwrap().get_mut(&0).unwrap().busy_us = 0;
            }
            6 => {
                state.nodes.get_mut(&5).unwrap().zone = "changed".into();
            }
            7 => {
                state.placements.get_mut(&1).unwrap().generation += 1;
            }
            _ => (),
        }
        let term = if interruption == 8 { 2 } else { 1 };
        let command = window.observe(&state, loads, term, start + Duration::from_secs(30), 60_000);
        assert_eq!(command.is_some(), interruption == 4);
    }
}

#[test]
fn actual_service_and_byte_samples_inform_selection() {
    for signal in 0..3 {
        let (state, mut loads) = fixture();
        let start = Instant::now();
        let mut window = Window::default();
        let mut command = None;
        for seconds in (0..=30).step_by(5) {
            for groups in loads.values_mut() {
                if signal == 1 {
                    groups.get_mut(&2).unwrap().charged_bytes = 4 * 1024 * 1024;
                }
                if signal == 2 {
                    groups.get_mut(&3).unwrap().busy_us = seconds * 1_000_000;
                }
            }
            command = window.observe(
                &state,
                loads.clone(),
                1,
                start + Duration::from_secs(seconds),
                60_000,
            );
        }
        let Some(Command::Balance { shard, .. }) = command else {
            panic!("missing move")
        };
        assert_eq!(shard, [0, 2, 3][signal]);
    }
}

#[test]
fn competing_commands_cannot_bypass_global_budget_or_generation() {
    let (mut state, _) = fixture();
    let command = |shard, generation, time| Command::Balance {
        shard,
        expected_generation: generation,
        voters: [2, 3, 4].into(),
        now_ms: time,
    };
    // Wrong generation, but all structural and cooldown checks pass.
    assert!(admissible(&state, 0, &[2, 3, 4].into(), 60_000));
    assert_eq!(
        state.apply(&command(0, 0, 60_000)).error,
        Some(Error::InvalidPlacement)
    );
    assert!(state.apply(&command(0, 1, 60_000)).error.is_none());
    state.placements.get_mut(&0).unwrap().complete = true;
    // A different shard still has generation 1 and a changed_ms of zero.
    // Its otherwise valid command must consult the GLOBAL move timestamp.
    assert_eq!(
        state.apply(&command(1, 1, 60_000)).error,
        Some(Error::InvalidPlacement)
    );
    assert_eq!(
        state.apply(&command(1, 1, 119_999)).error,
        Some(Error::InvalidPlacement)
    );
    assert!(state.apply(&command(1, 1, 120_000)).error.is_none());
}
