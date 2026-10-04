//! Advisory resource placement. Only replicated intent admission grants authority.
use crate::model::{Command, Node, SHARDS, State};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

pub const COOLDOWN_MS: u64 = 60_000;
const WINDOW: Duration = Duration::from_secs(30);
const MAX_GAP: Duration = Duration::from_secs(10);

#[cfg(test)]
#[path = "balance_tests.rs"]
mod tests;

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct Load {
    /// Random per actor open; not an ownership fence or a persisted identity.
    pub instance: u64,
    pub busy_us: u64,
    pub queued: usize,
    pub charged_bytes: usize,
}

pub type Loads = BTreeMap<u64, BTreeMap<u64, Load>>;

fn zones(state: &State, voters: &BTreeSet<u64>) -> usize {
    voters
        .iter()
        .filter_map(|id| state.nodes.get(id))
        .filter_map(Node::failure_domain)
        .collect::<BTreeSet<_>>()
        .len()
}

pub fn admissible(state: &State, shard: u64, voters: &BTreeSet<u64>, now: u64) -> bool {
    let Some(old) = state.placements.get(&shard) else {
        return false;
    };
    state.placements.len() == (SHARDS + 1) as usize
        && state.placements.values().all(|p| p.complete)
        && state
            .placements
            .values()
            .all(|p| now.saturating_sub(p.changed_ms) >= COOLDOWN_MS)
        && voters.len() == 3
        && old.voters.difference(voters).count() == 1
        && voters
            .iter()
            .all(|id| state.nodes.get(id).is_some_and(|n| !n.draining))
        && zones(state, voters) >= zones(state, &old.voters)
}

#[derive(Default)]
pub struct Window {
    sample: Option<Sample>,
}

struct Sample {
    term: u64,
    nodes: BTreeMap<u64, Node>,
    generations: Vec<(u64, u64)>,
    start: Instant,
    last: Instant,
    initial: Loads,
    previous: Loads,
}

impl Window {
    pub fn reset(&mut self) {
        self.sample = None;
    }

    pub fn observe(
        &mut self,
        state: &State,
        loads: Loads,
        term: u64,
        now: Instant,
        wall_ms: u64,
    ) -> Option<Command> {
        let complete = state.placements.len() == (SHARDS + 1) as usize
            && state.placements.values().all(|p| p.complete)
            && state
                .nodes
                .iter()
                .filter(|(_, n)| !n.draining)
                .all(|(id, _)| {
                    loads
                        .get(id)
                        .is_some_and(|groups| (0..=SHARDS).all(|g| groups.contains_key(&g)))
                });
        if !complete {
            self.reset();
            return None;
        }
        let generations: Vec<_> = state
            .placements
            .iter()
            .map(|(id, p)| (*id, p.generation))
            .collect();
        let continuous = self.sample.as_ref().is_some_and(|s| {
            s.term == term
                && s.nodes == state.nodes
                && s.generations == generations
                && now.duration_since(s.last) <= MAX_GAP
                && s.previous.iter().all(|(id, groups)| {
                    groups.iter().all(|(g, old)| {
                        loads.get(id).and_then(|v| v.get(g)).is_some_and(|new| {
                            old.instance == new.instance && old.busy_us <= new.busy_us
                        })
                    })
                })
        });
        if !continuous {
            self.sample = Some(Sample {
                term,
                nodes: state.nodes.clone(),
                generations,
                start: now,
                last: now,
                initial: loads.clone(),
                previous: loads,
            });
            return None;
        }
        let sample = self.sample.as_mut()?;
        sample.last = now;
        sample.previous = loads;
        let elapsed = now.duration_since(sample.start);
        if elapsed < WINDOW {
            return None;
        }
        let weights = (0..=SHARDS)
            .map(|g| {
                let weight = state.placements[&g]
                    .voters
                    .iter()
                    .filter_map(|id| {
                        let old = sample.initial.get(id)?.get(&g)?;
                        let new = sample.previous.get(id)?.get(&g)?;
                        let work = u128::from(new.busy_us.checked_sub(old.busy_us)?) * 64
                            / elapsed.as_micros();
                        Some(
                            16 + (new.charged_bytes.min(crate::model::MAX_SHARD_BYTES)
                                / (256 * 1024)) as u64
                                + work.min(64) as u64,
                        )
                    })
                    .max()
                    .unwrap_or(16);
                (g, weight)
            })
            .collect();
        let command = choose(state, &sample.previous, &weights, wall_ms);
        if let Some(intent) = &command {
            tracing::info!(control_term = term, observation_ms = %elapsed.as_millis(),
                group_weights = ?weights, intent = ?intent, "resource placement observed");
        }
        // Non-overlapping windows adapt to workload changes instead of averaging
        // an arbitrarily long actor lifetime. A missed proposal is retried later.
        self.reset();
        command
    }
}

fn choose(state: &State, loads: &Loads, weights: &BTreeMap<u64, u64>, now: u64) -> Option<Command> {
    let mut assigned: BTreeMap<u64, u64> = state
        .nodes
        .iter()
        .filter(|(_, n)| !n.draining)
        .map(|(id, _)| (*id, 0))
        .collect();
    for (g, p) in &state.placements {
        for id in &p.voters {
            *assigned.get_mut(id)? += weights[g];
        }
    }
    let mut best = None;
    let mut best_gain = 0;
    for (g, p) in &state.placements {
        let weight = u128::from(weights[g]);
        for source in &p.voters {
            for (destination, load) in &assigned {
                if p.voters.contains(destination)
                    || loads
                        .get(destination)
                        .is_none_or(|groups| groups.values().any(|l| l.queued >= 32))
                {
                    continue;
                }
                let mut voters = p.voters.clone();
                voters.remove(source);
                voters.insert(*destination);
                if !admissible(state, *g, &voters, now) {
                    continue;
                }
                let from = u128::from(assigned[source]);
                let to = u128::from(*load);
                let before = from * from + to * to;
                let after = (from - weight).pow(2) + (to + weight).pow(2);
                if after * 10 <= before * 9 && before - after > best_gain {
                    best_gain = before - after;
                    best = Some(Command::Balance {
                        shard: *g,
                        expected_generation: p.generation,
                        voters,
                        now_ms: now,
                    });
                }
            }
        }
    }
    best
}
