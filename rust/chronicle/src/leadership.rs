//! Durable one-shot authorization, not a second leadership authority.
use crate::{
    LogId, Vote,
    balance::COOLDOWN_MS,
    model::{Error, Outcome, SHARDS, State},
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const EXPIRES_MS: u64 = 30_000;

/// Advisory snapshot of matching applied/effective uniform membership. The
/// source rechecks it under a read barrier before consuming an attempt.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct View {
    pub vote: Vote,
    pub membership: LogId,
    pub voters: [u64; 3],
    pub leader: bool,
    pub applied: Option<LogId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Proposal {
    pub shard: u64,
    pub generation: u64,
    pub source_vote: Vote,
    pub membership: LogId,
    pub target: u64,
    pub created_ms: u64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Phase {
    Planned,
    Claimed,
    ObservedTarget,
    ClosedUnused,
    ClosedUnknown,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Attempt {
    pub id: u64,
    pub proposal: Proposal,
    pub phase: Phase,
    pub claimed_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Ledger {
    pub last_id: u64,
    pub attempt: Option<Attempt>,
    /// Only committed votes are admitted; their full (term, node) order matters.
    pub consumed: BTreeMap<u64, Vote>,
    pub last_claim_ms: Option<u64>,
}

/// Evidence from a strict read on the target itself, never a forwarded read.
/// The caller owns the probe; control apply cannot verify remote quorum contact.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Observation {
    pub vote: Vote,
    pub membership: LogId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Operation {
    Plan {
        expected_id: u64,
        proposal: Proposal,
    },
    Claim {
        id: u64,
        executor: u64,
        now_ms: u64,
    },
    Close {
        id: u64,
        now_ms: u64,
        observed: Option<Observation>,
    },
}

impl Ledger {
    pub fn pending(&self) -> bool {
        self.attempt
            .as_ref()
            .is_some_and(|a| matches!(a.phase, Phase::Planned | Phase::Claimed))
    }

    pub fn cooled(&self, now: u64) -> bool {
        self.last_claim_ms
            .is_none_or(|last| now.checked_sub(last).is_some_and(|d| d >= COOLDOWN_MS))
    }

    fn unused_vote(&self, p: &Proposal) -> bool {
        p.source_vote.committed
            && self
                .consumed
                .get(&p.shard)
                .is_none_or(|old| p.source_vote > *old)
    }
}

pub fn eligible(state: &State, p: &Proposal) -> bool {
    p.shard <= SHARDS
        && state.placements.len() == (SHARDS + 1) as usize
        && state.placements.values().all(|v| v.complete)
        && p.source_vote.committed
        && p.source_vote.leader_id.node_id != p.target
        && state.placements.get(&p.shard).is_some_and(|v| {
            v.generation == p.generation
                && v.voters.contains(&p.source_vote.leader_id.node_id)
                && v.voters.contains(&p.target)
                && v.voters
                    .iter()
                    .all(|id| state.nodes.get(id).is_some_and(|n| !n.draining))
        })
}

fn cooled(state: &State, now: u64) -> bool {
    state.leadership.cooled(now)
        && state.placements.values().all(|p| {
            now.checked_sub(p.changed_ms)
                .is_some_and(|d| d >= COOLDOWN_MS)
        })
}

pub fn admissible(state: &State, p: &Proposal) -> bool {
    !state.leadership.pending()
        && eligible(state, p)
        && cooled(state, p.created_ms)
        && state.leadership.unused_vote(p)
}

pub(crate) fn apply(state: &mut State, operation: &Operation) -> Outcome {
    match operation {
        Operation::Plan {
            expected_id,
            proposal,
        } => {
            if state.leadership.last_id != *expected_id || !admissible(state, proposal) {
                return Outcome::err(Error::InvalidPlacement);
            }
            let Some(id) = expected_id.checked_add(1) else {
                return Outcome::err(Error::Capacity);
            };
            state.leadership.last_id = id;
            state.leadership.attempt = Some(Attempt {
                id,
                proposal: proposal.clone(),
                phase: Phase::Planned,
                claimed_ms: None,
            });
        }
        Operation::Claim {
            id,
            executor,
            now_ms,
        } => {
            let Some(attempt) = state.leadership.attempt.as_ref() else {
                return Outcome::err(Error::InvalidPlacement);
            };
            let p = &attempt.proposal;
            if attempt.id != *id
                || attempt.phase != Phase::Planned
                || p.source_vote.leader_id.node_id != *executor
                || !eligible(state, p)
                || !cooled(state, *now_ms)
                || !state.leadership.unused_vote(p)
                || now_ms
                    .checked_sub(p.created_ms)
                    .is_none_or(|age| age >= EXPIRES_MS)
            {
                return Outcome::err(Error::InvalidPlacement);
            }
            state.leadership.consumed.insert(p.shard, p.source_vote);
            state.leadership.last_claim_ms = Some(*now_ms);
            if let Some(attempt) = &mut state.leadership.attempt {
                attempt.phase = Phase::Claimed;
                attempt.claimed_ms = Some(*now_ms);
            }
            // Never return this success on a repeated claim. Only this original
            // committed response grants the source executor one submission.
        }
        Operation::Close {
            id,
            now_ms,
            observed,
        } => {
            let Some(attempt) = state.leadership.attempt.as_ref() else {
                return Outcome::err(Error::InvalidPlacement);
            };
            if attempt.id != *id || !state.leadership.pending() {
                return Outcome::err(Error::InvalidPlacement);
            }
            let p = &attempt.proposal;
            let phase = if let Some(observed) = observed {
                if attempt.phase != Phase::Claimed
                    || !eligible(state, p)
                    || !observed.vote.committed
                    || observed.vote <= p.source_vote
                    || observed.vote.leader_id.node_id != p.target
                    || observed.membership != p.membership
                {
                    return Outcome::err(Error::InvalidPlacement);
                }
                Phase::ObservedTarget
            } else {
                // A known-invalid placement preempts optimization immediately;
                // otherwise expiry closes admission, not a queued Raft trigger.
                if eligible(state, p)
                    && now_ms
                        .checked_sub(p.created_ms)
                        .is_none_or(|age| age < EXPIRES_MS)
                {
                    return Outcome::err(Error::InvalidPlacement);
                }
                if attempt.phase == Phase::Claimed {
                    Phase::ClosedUnknown
                } else {
                    Phase::ClosedUnused
                }
            };
            if let Some(attempt) = &mut state.leadership.attempt {
                attempt.phase = phase;
            }
        }
    }
    Outcome::ok(0, 0, false)
}
