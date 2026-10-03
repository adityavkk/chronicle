//! Deterministic apply: no clock, network, filesystem, or speculative producer state.
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// Fixed at cluster creation; changing this remaps existing identities and is forbidden.
pub const SHARDS: u64 = 4;
pub const MAX_STREAM_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_SHARD_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_CONTENT_TYPE_BYTES: usize = 4096;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Producer {
    pub id: String,
    pub epoch: u64,
    pub seq: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProducerState {
    pub epoch: u64,
    pub seq: u64,
    pub end: u64,
    pub results: BTreeMap<u64, u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamConfig {
    pub content_type: String,
    pub expires_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Stream {
    pub incarnation: u64,
    pub config: StreamConfig,
    pub data: Vec<u8>,
    pub closed: bool,
    pub deleted: bool,
    pub producers: BTreeMap<String, ProducerState>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Node {
    pub addr: String,
    pub zone: String,
    pub draining: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Placement {
    pub generation: u64,
    pub voters: BTreeSet<u64>,
    pub complete: bool,
    pub changed_ms: u64,
    /// None means legacy/unknown history. A missing identity in known history
    /// has never been assigned; it must not receive a full shard just to retire.
    #[serde(default)]
    pub replicas: Option<BTreeMap<u64, ReplicaHistory>>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReplicaHistory {
    MayVote,
    NonvoterAfter(openraft::LogId<u64>),
}

impl Placement {
    pub fn retirement_known(&self) -> bool {
        self.complete
            && self.replicas.as_ref().is_some_and(|replicas| {
                replicas.iter().all(|(id, history)| {
                    self.voters.contains(id) || matches!(history, ReplicaHistory::NonvoterAfter(_))
                })
            })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Command {
    Create {
        key: String,
        expected_incarnation: Option<u64>,
        config: StreamConfig,
        data: Vec<u8>,
        closed: bool,
    },
    Append {
        key: String,
        incarnation: u64,
        data: Vec<u8>,
        producer: Option<Producer>,
        close: bool,
        /// Original HTTP-body emptiness; default false preserves legacy log replay.
        #[serde(default)]
        empty_body: bool,
    },
    Delete {
        key: String,
        incarnation: u64,
        expired_at: Option<u64>,
    },
    Register {
        id: u64,
        node: Node,
    },
    Admit {
        id: u64,
        node: Node,
    },
    Place {
        shard: u64,
        expected_generation: u64,
        voters: BTreeSet<u64>,
        now_ms: u64,
        /// New controllers set this; false preserves legacy committed-log replay.
        #[serde(default)]
        eligible_only: bool,
        /// Replace this shard's pending intent, never another concurrent move.
        #[serde(default)]
        repair_pending: bool,
    },
    Placed {
        shard: u64,
        generation: u64,
        #[serde(default)]
        membership: Option<openraft::LogId<u64>>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Error {
    Missing,
    Closed,
    StaleIncarnation,
    ConfigConflict,
    EpochFenced,
    SequenceGap,
    EmptyBody,
    Capacity,
    InvalidPlacement,
}

/// Highest accepted producer position at this command's apply boundary.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProducerPosition {
    pub epoch: u64,
    pub seq: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Outcome {
    pub end: u64,
    pub incarnation: u64,
    pub duplicate: bool,
    /// Captured during apply, never reconstructed from request intent or a later read.
    #[serde(default)]
    pub closed: bool,
    #[serde(default)]
    pub producer: Option<ProducerPosition>,
    pub error: Option<Error>,
}

impl Outcome {
    fn ok(end: u64, incarnation: u64, duplicate: bool) -> Self {
        Self {
            end,
            incarnation,
            duplicate,
            closed: false,
            producer: None,
            error: None,
        }
    }
    fn stream(stream: &Stream, duplicate: bool) -> Self {
        Self {
            closed: stream.closed,
            ..Self::ok(stream.data.len() as u64, stream.incarnation, duplicate)
        }
    }
    fn with_producer(mut self, epoch: u64, seq: u64) -> Self {
        self.producer = Some(ProducerPosition { epoch, seq });
        self
    }
    fn err(error: Error) -> Self {
        Self {
            end: 0,
            incarnation: 0,
            duplicate: false,
            closed: false,
            producer: None,
            error: Some(error),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct State {
    pub streams: BTreeMap<String, Stream>,
    pub nodes: BTreeMap<u64, Node>,
    pub placements: BTreeMap<u64, Placement>,
}

impl State {
    pub fn apply(&mut self, command: &Command) -> Outcome {
        match command {
            Command::Create {
                key,
                expected_incarnation,
                config,
                data,
                closed,
            } => {
                let previous = self.streams.get(key);
                let requested = expected_incarnation.unwrap_or(1);
                if let Some(s) = previous.filter(|s| !s.deleted) {
                    if s.incarnation != requested {
                        return Outcome::err(Error::StaleIncarnation);
                    }
                    return if s.config == *config && s.closed == *closed {
                        Outcome::stream(s, true)
                    } else {
                        Outcome::err(Error::ConfigConflict)
                    };
                }
                let Some(incarnation) = previous.map_or(Some(1), |s| s.incarnation.checked_add(1))
                else {
                    return Outcome::err(Error::Capacity);
                };
                if requested != incarnation {
                    return Outcome::err(Error::StaleIncarnation);
                }
                let metadata = key
                    .len()
                    .saturating_add(config.content_type.len())
                    .saturating_add(256);
                if config.content_type.len() > MAX_CONTENT_TYPE_BYTES
                    || !self.fits(data.len(), metadata)
                    || data.len() > MAX_STREAM_BYTES
                    || self.streams.len() >= 100_000
                {
                    return Outcome::err(Error::Capacity);
                }
                self.streams.insert(
                    key.clone(),
                    Stream {
                        incarnation,
                        config: config.clone(),
                        data: data.clone(),
                        closed: *closed,
                        deleted: false,
                        producers: BTreeMap::new(),
                    },
                );
                Outcome {
                    closed: *closed,
                    ..Outcome::ok(data.len() as u64, incarnation, false)
                }
            }
            Command::Append {
                key,
                incarnation,
                data,
                producer,
                close,
                empty_body,
            } => {
                let producer_metadata = producer.as_ref().map_or(0, |p| {
                    self.streams
                        .get(key)
                        .and_then(|s| s.producers.get(&p.id))
                        .map_or_else(|| p.id.len().saturating_add(128 + 64), |_| 64)
                });
                let fits = self.fits(data.len(), producer_metadata);
                let Some(s) = self.streams.get_mut(key).filter(|s| !s.deleted) else {
                    return Outcome::err(Error::Missing);
                };
                if s.incarnation != *incarnation {
                    return Outcome::err(Error::StaleIncarnation);
                }
                if let Some(p) = producer {
                    if let Some(old) = s.producers.get(&p.id) {
                        if p.epoch < old.epoch {
                            return Outcome::err(Error::EpochFenced)
                                .with_producer(old.epoch, old.seq);
                        }
                        if p.epoch == old.epoch && p.seq <= old.seq {
                            return match old.results.get(&p.seq) {
                                Some(end) => Outcome {
                                    end: *end,
                                    ..Outcome::stream(s, true).with_producer(old.epoch, old.seq)
                                },
                                None => Outcome::err(Error::SequenceGap)
                                    .with_producer(old.epoch, old.seq),
                            };
                        }
                        if (p.epoch == old.epoch && old.seq.checked_add(1) != Some(p.seq))
                            || (p.epoch > old.epoch && p.seq != 0)
                        {
                            return Outcome::err(Error::SequenceGap)
                                .with_producer(old.epoch, old.seq);
                        }
                    } else if p.seq != 0 {
                        return Outcome::err(Error::SequenceGap);
                    }
                    if !s.producers.contains_key(&p.id) && s.producers.len() >= 10_000 {
                        return Outcome::err(Error::Capacity);
                    }
                    if s.producers.values().map(|p| p.results.len()).sum::<usize>() >= 100_000 {
                        return Outcome::err(Error::Capacity);
                    }
                }
                if *empty_body && !close {
                    return Outcome::err(Error::EmptyBody);
                }
                if s.closed {
                    if producer.is_none() && *close && data.is_empty() {
                        return Outcome::stream(s, true);
                    }
                    return Outcome {
                        error: Some(Error::Closed),
                        ..Outcome::stream(s, false)
                    };
                }
                if !fits || s.data.len().saturating_add(data.len()) > MAX_STREAM_BYTES {
                    return Outcome::err(Error::Capacity);
                }
                s.data.extend_from_slice(data);
                s.closed = *close;
                let end = s.data.len() as u64;
                if let Some(p) = producer {
                    let mut results = s
                        .producers
                        .remove(&p.id)
                        .filter(|old| old.epoch == p.epoch)
                        .map_or_else(BTreeMap::new, |old| old.results);
                    results.insert(p.seq, end);
                    s.producers.insert(
                        p.id.clone(),
                        ProducerState {
                            epoch: p.epoch,
                            seq: p.seq,
                            end,
                            results,
                        },
                    );
                }
                let outcome = Outcome::stream(s, false);
                match producer {
                    Some(p) => outcome.with_producer(p.epoch, p.seq),
                    None => outcome,
                }
            }
            Command::Delete {
                key,
                incarnation,
                expired_at,
            } => {
                let Some(s) = self.streams.get_mut(key).filter(|s| !s.deleted) else {
                    return Outcome::err(Error::Missing);
                };
                if s.incarnation != *incarnation {
                    return Outcome::err(Error::StaleIncarnation);
                }
                if expired_at.is_some() && s.config.expires_ms != *expired_at {
                    return Outcome::err(Error::ConfigConflict);
                }
                s.deleted = true;
                s.data.clear();
                s.producers.clear();
                Outcome::ok(0, s.incarnation, false)
            }
            Command::Admit { id, node } => {
                if self.nodes.contains_key(id) {
                    return Outcome::err(Error::InvalidPlacement);
                }
                self.apply(&Command::Register {
                    id: *id,
                    node: node.clone(),
                })
            }
            Command::Register { id, node } => {
                if self.nodes.get(id).is_some_and(|old| old.addr != node.addr)
                    || self
                        .nodes
                        .iter()
                        .any(|(other, old)| other != id && old.addr == node.addr)
                    || (!self.nodes.contains_key(id) && self.nodes.len() >= 128)
                {
                    return Outcome::err(Error::InvalidPlacement);
                }
                self.nodes.insert(*id, node.clone());
                Outcome::ok(0, 0, false)
            }
            Command::Place {
                shard,
                expected_generation,
                voters,
                now_ms,
                eligible_only,
                repair_pending,
            } => {
                if *shard > SHARDS
                    || self
                        .placements
                        .iter()
                        .any(|(other, p)| !p.complete && (!repair_pending || other != shard))
                {
                    return Outcome::err(Error::InvalidPlacement);
                }
                if *eligible_only
                    && voters
                        .iter()
                        .any(|id| self.nodes.get(id).is_none_or(|n| n.draining))
                {
                    return Outcome::err(Error::InvalidPlacement);
                }
                let p = self.placements.entry(*shard).or_default();
                if p.generation != *expected_generation
                    || voters.len() != 3
                    || !voters.iter().all(|n| self.nodes.contains_key(n))
                {
                    return Outcome::err(Error::InvalidPlacement);
                }
                let Some(generation) = p.generation.checked_add(1) else {
                    return Outcome::err(Error::Capacity);
                };
                // Bootstrap may have used any registered seed. For legacy
                // placements the registry conservatively bounds lost history.
                let mut replicas = p.replicas.clone().unwrap_or_else(|| {
                    self.nodes
                        .keys()
                        .map(|id| (*id, ReplicaHistory::MayVote))
                        .collect()
                });
                for id in voters {
                    replicas.insert(*id, ReplicaHistory::MayVote);
                }
                *p = Placement {
                    generation,
                    voters: voters.clone(),
                    complete: false,
                    changed_ms: *now_ms,
                    replicas: Some(replicas),
                };
                Outcome::ok(0, 0, false)
            }
            Command::Placed {
                shard,
                generation,
                membership,
            } => {
                let Some(p) = self
                    .placements
                    .get_mut(shard)
                    .filter(|p| p.generation == *generation)
                else {
                    return Outcome::err(Error::InvalidPlacement);
                };
                p.complete = true;
                if let Some(boundary) = membership {
                    let replicas = p.replicas.get_or_insert_with(|| {
                        self.nodes
                            .keys()
                            .map(|id| (*id, ReplicaHistory::MayVote))
                            .collect()
                    });
                    for (id, history) in replicas {
                        if !p.voters.contains(id) && *history == ReplicaHistory::MayVote {
                            *history = ReplicaHistory::NonvoterAfter(*boundary);
                        }
                    }
                }
                Outcome::ok(0, 0, false)
            }
        }
    }

    fn fits(&self, extra_data: usize, extra_metadata: usize) -> bool {
        self.streams
            .iter()
            .map(|(key, s)| {
                s.data.len()
                    + key.len()
                    + s.config.content_type.len()
                    + 256
                    + s.producers
                        .iter()
                        .map(|(id, p)| id.len() + 128 + p.results.len() * 64)
                        .sum::<usize>()
            })
            .sum::<usize>()
            .saturating_add(extra_data)
            .saturating_add(extra_metadata)
            <= MAX_SHARD_BYTES
    }
}

/// Stable, length-delimited identity hash. Node joins never change this mapping.
pub fn shard(key: &str) -> u64 {
    use sha2::{Digest, Sha256};
    let hash = Sha256::digest(key.as_bytes());
    u64::from_be_bytes(hash[..8].try_into().unwrap_or_default()) % SHARDS + 1
}
