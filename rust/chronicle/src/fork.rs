//! Fork range resolution over committed Electric-format bytes.
use crate::{
    expiry::Expiry,
    model::{Error, StreamConfig},
};
use crate::{model::Stream, wire};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const CHUNK_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Id {
    pub source: String,
    pub incarnation: u64,
    pub sequence: u64,
}

/// Request configuration, not its initial body: repeated PUT ignores new bytes.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Request {
    pub target: String,
    pub incarnation: u64,
    pub anchor: Option<u64>,
    pub sub: u64,
    pub content_type: Option<String>,
    pub expiry: Option<Expiry>,
    pub closed: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Offer {
    pub id: Id,
    pub request: Request,
    pub config: StreamConfig,
    pub boundary: u64,
    pub total: u64,
    pub ends: usize,
}

impl Offer {
    pub(crate) fn charge(&self) -> usize {
        1024 + self.id.source.len()
            + self.request.target.len()
            + self.config.content_type.len()
            + self.request.content_type.as_ref().map_or(0, String::len)
    }

    pub(crate) fn reservation_charge(&self) -> usize {
        self.charge()
            + 256
            + self.request.target.len()
            + self.config.content_type.len()
            + self.ends * 8
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Decision {
    Preparing,
    Committed { created_ms: u64 },
    Aborted(Error),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Transaction {
    pub offer: Offer,
    pub initial: Vec<u8>,
    pub decision: Decision,
    pub finalized: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Lifecycle {
    pub sequence: u64,
    pub transactions: BTreeMap<u64, Transaction>,
    pub origin: Option<Offer>,
}

impl Lifecycle {
    pub fn locked(&self) -> bool {
        self.transactions
            .values()
            .any(|t| t.decision == Decision::Preparing)
    }

    pub fn retained(&self) -> bool {
        self.transactions
            .values()
            .any(|t| matches!(t.decision, Decision::Committed { .. }))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Prepared {
    pub offer: Offer,
    pub data: Vec<u8>,
    pub append_ends: Vec<u64>,
}

impl Prepared {
    pub fn ready(&self) -> bool {
        self.data.len() as u64 == self.offer.total && self.append_ends.len() == self.offer.ends
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Operation {
    Begin {
        id: Id,
        request: Request,
        body: Vec<u8>,
        now_ms: u64,
    },
    Prepare(Offer),
    Stage {
        target: String,
        id: Id,
        offset: u64,
        data: Vec<u8>,
        ends: Vec<u64>,
    },
    Decide {
        id: Id,
        commit: bool,
        now_ms: u64,
        rejection: Option<Error>,
    },
    Finish {
        target: String,
        id: Id,
        decision: Decision,
    },
    Finalized(Id),
    Release(Id),
    Released {
        target: String,
        incarnation: u64,
        id: Id,
    },
}

impl Operation {
    /// Every operation changes one stream plus, optionally, its target reservation.
    pub fn key(&self) -> &str {
        match self {
            Self::Begin { id, .. }
            | Self::Decide { id, .. }
            | Self::Finalized(id)
            | Self::Release(id) => &id.source,
            Self::Prepare(offer) => &offer.request.target,
            Self::Stage { target, .. }
            | Self::Finish { target, .. }
            | Self::Released { target, .. } => target,
        }
    }
}

#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub enum BoundaryError {
    #[error("invalid fork offset or sub-offset")]
    Invalid,
    #[error("legacy stream has no retained append boundaries")]
    Legacy,
}

/// Resolve an exclusive copied prefix; binary sub-offsets cannot cross appends.
pub fn boundary(stream: &Stream, anchor: Option<u64>, sub: u64) -> Result<usize, BoundaryError> {
    let start = usize::try_from(anchor.unwrap_or(stream.data.len() as u64))
        .map_err(|_| BoundaryError::Invalid)?;
    if start > stream.data.len() {
        return Err(BoundaryError::Invalid);
    }
    if !stream.config.is_json() {
        if !stream.config.track_boundaries {
            return if sub == 0 && (start == 0 || start == stream.data.len()) {
                Ok(start)
            } else {
                Err(BoundaryError::Legacy)
            };
        }
        if start != 0 && stream.append_ends.binary_search(&(start as u64)).is_err() {
            return Err(BoundaryError::Invalid);
        }
        if sub == 0 {
            return Ok(start);
        }
        let next = stream
            .append_ends
            .iter()
            .copied()
            .find(|&end| end > start as u64)
            .ok_or(BoundaryError::Invalid)?;
        return (start as u64)
            .checked_add(sub)
            .filter(|&end| end <= next)
            .map(|end| end as usize)
            .ok_or(BoundaryError::Invalid);
    }
    if !wire::json_boundary(&stream.data, start) || sub > stream.data.len() as u64 {
        return Err(BoundaryError::Invalid);
    }
    let mut end = start;
    for _ in 0..sub {
        let mut value = serde_json::Deserializer::from_slice(&stream.data[end..])
            .into_iter::<serde::de::IgnoredAny>();
        value
            .next()
            .ok_or(BoundaryError::Invalid)?
            .map_err(|_| BoundaryError::Invalid)?;
        end += value.byte_offset();
        if stream.data.get(end) != Some(&b',') {
            return Err(BoundaryError::Invalid);
        }
        end += 1;
    }
    Ok(end)
}

fn begin(
    state: &mut crate::model::State,
    id: &Id,
    request: &Request,
    body: &[u8],
    now_ms: u64,
) -> Result<crate::model::Outcome, Error> {
    use crate::model::{MAX_CONTENT_TYPE_BYTES, MAX_STREAM_BYTES, Outcome, content_type_matches};
    if id.source == request.target || request.target.len() > 2048 || body.len() > 1024 * 1024 {
        return Err(Error::InvalidFork);
    }
    if state.fork_targets.contains_key(&id.source) {
        return Err(Error::PendingFork);
    }
    let source = state.streams.get(&id.source).ok_or(Error::Missing)?;
    if source.incarnation != id.incarnation {
        return Err(Error::StaleIncarnation);
    }
    if let Some(existing) = source.forks.transactions.get(&id.sequence) {
        return if &existing.offer.request == request {
            Ok(Outcome::ok(existing.offer.total, id.incarnation, true))
        } else {
            Err(Error::ConfigConflict)
        };
    }
    if source.deleted {
        return Err(if source.forks.retained() {
            Error::Gone
        } else {
            Error::Missing
        });
    }
    if source.forks.locked() {
        return Err(Error::PendingFork);
    }
    if source.forks.sequence.checked_add(1) != Some(id.sequence) {
        return Err(Error::StaleIncarnation);
    }
    if source
        .config
        .expiry
        .is_some_and(|p| p.expired(source.access_ms, now_ms))
    {
        return Err(Error::Missing);
    }
    let end = boundary(source, request.anchor, request.sub).map_err(|error| match error {
        BoundaryError::Invalid => Error::InvalidFork,
        BoundaryError::Legacy => Error::LegacyFork,
    })?;
    let mut config = source.config.clone();
    // A case-insensitive header override must not reinterpret legacy bytes.
    config.json_framing = Some(source.config.is_json());
    if let Some(content_type) = &request.content_type {
        if !content_type_matches(content_type, &config.content_type) {
            return Err(Error::ConfigConflict);
        }
        if content_type.len() > MAX_CONTENT_TYPE_BYTES {
            return Err(Error::Capacity);
        }
        config.content_type.clone_from(content_type);
    }
    config.expiry = request.expiry.or(config.expiry);
    let initial = if body.is_empty() {
        Vec::new()
    } else {
        wire::encode_wire(&bytes::Bytes::copy_from_slice(body), config.is_json(), true)
            .map_err(|_| Error::InvalidFork)?
            .to_vec()
    };
    let total = end
        .checked_add(initial.len())
        .filter(|&n| n <= MAX_STREAM_BYTES)
        .ok_or(Error::Capacity)?;
    let ends = if config.track_boundaries {
        source.append_ends.partition_point(|&n| n <= end as u64)
            + usize::from(end != 0 && source.append_ends.binary_search(&(end as u64)).is_err())
            + usize::from(!initial.is_empty())
    } else {
        0
    };
    let offer = Offer {
        id: id.clone(),
        request: request.clone(),
        config,
        boundary: end as u64,
        total: total as u64,
        ends,
    };
    if !state.fits(initial.len(), offer.charge()) {
        return Err(Error::Capacity);
    }
    let source = state.streams.get_mut(&id.source).ok_or(Error::Missing)?;
    source.forks.sequence = id.sequence;
    source.forks.transactions.insert(
        id.sequence,
        Transaction {
            offer,
            initial,
            decision: Decision::Preparing,
            finalized: false,
        },
    );
    Ok(Outcome::ok(total as u64, id.incarnation, false))
}

fn prepare(state: &mut crate::model::State, offer: &Offer) -> Result<crate::model::Outcome, Error> {
    use crate::model::{MAX_STREAM_BYTES, Outcome};
    if offer.total > MAX_STREAM_BYTES as u64
        || offer.ends > offer.total as usize
        || offer.boundary > offer.total
        || offer.id.source == offer.request.target
    {
        return Err(Error::InvalidFork);
    }
    let key = &offer.request.target;
    if let Some(previous) = state.fork_targets.get(key) {
        return if previous.offer == *offer {
            Ok(Outcome::ok(
                previous.data.len() as u64,
                offer.request.incarnation,
                true,
            ))
        } else {
            Err(Error::PendingFork)
        };
    }
    if let Some(existing) = state.streams.get(key) {
        if existing.incarnation == offer.request.incarnation
            && existing
                .forks
                .origin
                .as_ref()
                .is_some_and(|origin| origin.id == offer.id)
        {
            return Ok(Outcome::ok(offer.total, existing.incarnation, true));
        }
        if !existing.deleted || existing.forks.retained() {
            return Err(Error::ConfigConflict);
        }
        if existing.forks.locked() || existing.forks.origin.is_some() {
            return Err(Error::PendingFork);
        }
        if existing.incarnation.checked_add(1) != Some(offer.request.incarnation) {
            return Err(Error::StaleIncarnation);
        }
    } else if offer.request.incarnation != 1 {
        return Err(Error::StaleIncarnation);
    }
    if state.fork_targets.len() >= 128
        || state.streams.len() + state.fork_targets.len() >= 100_000
        || !state.fits(offer.total as usize, offer.reservation_charge())
    {
        return Err(Error::Capacity);
    }
    state.fork_targets.insert(
        key.clone(),
        Prepared {
            offer: offer.clone(),
            data: Vec::new(),
            append_ends: Vec::new(),
        },
    );
    Ok(Outcome::ok(0, offer.request.incarnation, false))
}

fn stage(
    state: &mut crate::model::State,
    target: &str,
    id: &Id,
    offset: u64,
    data: &[u8],
    ends: &[u64],
) -> Result<crate::model::Outcome, Error> {
    let prepared = state.fork_targets.get_mut(target).ok_or(Error::Missing)?;
    let end = offset
        .checked_add(data.len() as u64)
        .ok_or(Error::InvalidFork)?;
    if &prepared.offer.id != id
        || data.is_empty()
        || data.len() > CHUNK_BYTES
        || ends.len() > data.len()
        || end > prepared.offer.total
        || ends.windows(2).any(|pair| pair[0] >= pair[1])
        || ends.first().is_some_and(|&n| n <= offset)
        || ends.last().is_some_and(|&n| n > end)
    {
        return Err(Error::InvalidFork);
    }
    if offset < prepared.data.len() as u64 {
        let begin_end = prepared.append_ends.partition_point(|&n| n <= offset);
        let finish_end = prepared.append_ends.partition_point(|&n| n <= end);
        if prepared.data.get(offset as usize..end as usize) != Some(data)
            || &prepared.append_ends[begin_end..finish_end] != ends
        {
            return Err(Error::InvalidFork);
        }
    } else {
        if offset != prepared.data.len() as u64
            || prepared.append_ends.len() + ends.len() > prepared.offer.ends
        {
            return Err(Error::InvalidFork);
        }
        prepared.data.extend_from_slice(data);
        prepared.append_ends.extend_from_slice(ends);
    }
    Ok(crate::model::Outcome::ok(
        prepared.data.len() as u64,
        prepared.offer.request.incarnation,
        false,
    ))
}

/// Read a bounded portion of the frozen source prefix plus the initial fork body.
pub fn chunk(state: &crate::model::State, id: &Id, offset: u64) -> Result<Operation, Error> {
    let source = state.streams.get(&id.source).ok_or(Error::Missing)?;
    if source.incarnation != id.incarnation {
        return Err(Error::StaleIncarnation);
    }
    let transaction = source
        .forks
        .transactions
        .get(&id.sequence)
        .ok_or(Error::Missing)?;
    let offer = &transaction.offer;
    if transaction.finalized
        || matches!(transaction.decision, Decision::Aborted(_))
        || offset >= offer.total
    {
        return Err(Error::InvalidFork);
    }
    let end = offset.saturating_add(CHUNK_BYTES as u64).min(offer.total);
    let mut data = Vec::with_capacity((end - offset) as usize);
    if offset < offer.boundary {
        data.extend_from_slice(
            source
                .data
                .get(offset as usize..end.min(offer.boundary) as usize)
                .ok_or(Error::InvalidFork)?,
        );
    }
    if end > offer.boundary {
        data.extend_from_slice(
            transaction
                .initial
                .get(
                    offset.saturating_sub(offer.boundary) as usize..(end - offer.boundary) as usize,
                )
                .ok_or(Error::InvalidFork)?,
        );
    }
    let mut ends = Vec::new();
    if offer.config.track_boundaries {
        let start_index = source.append_ends.partition_point(|&n| n <= offset);
        let end_index = source
            .append_ends
            .partition_point(|&n| n <= end.min(offer.boundary));
        if start_index < end_index {
            ends.extend_from_slice(&source.append_ends[start_index..end_index]);
        }
        if offset < offer.boundary && offer.boundary <= end && ends.last() != Some(&offer.boundary)
        {
            ends.push(offer.boundary);
        }
        if offer.total > offer.boundary && end == offer.total {
            ends.push(offer.total);
        }
    }
    Ok(Operation::Stage {
        target: offer.request.target.clone(),
        id: id.clone(),
        offset,
        data,
        ends,
    })
}

fn finish(
    state: &mut crate::model::State,
    target: &str,
    id: &Id,
    decision: &Decision,
) -> Result<crate::model::Outcome, Error> {
    use crate::model::{Outcome, Stream};
    if *decision == Decision::Preparing {
        return Err(Error::PendingFork);
    }
    let Some(prepared) = state.fork_targets.get(target) else {
        return match state.streams.get(target) {
            Some(stream) if stream.forks.origin.as_ref().is_some_and(|o| &o.id == id) => {
                Ok(Outcome {
                    content_type: Some(stream.config.content_type.clone()),
                    ..Outcome::stream(stream, true)
                })
            }
            _ if matches!(decision, Decision::Aborted(_)) => Ok(Outcome::ok(0, 0, true)),
            _ => Err(Error::Missing),
        };
    };
    if &prepared.offer.id != id {
        return if matches!(decision, Decision::Aborted(_)) {
            Ok(Outcome::ok(0, 0, true))
        } else {
            Err(Error::ConfigConflict)
        };
    }
    match decision {
        Decision::Preparing => Err(Error::PendingFork),
        Decision::Aborted(_) => {
            state.fork_targets.remove(target);
            Ok(Outcome::ok(0, 0, false))
        }
        Decision::Committed { created_ms } => {
            if !prepared.ready() {
                return Err(Error::PendingFork);
            }
            let prepared = state.fork_targets.remove(target).ok_or(Error::Missing)?;
            let stream = Stream {
                incarnation: prepared.offer.request.incarnation,
                config: prepared.offer.config.clone(),
                data: prepared.data,
                append_ends: prepared.append_ends,
                closed: prepared.offer.request.closed,
                deleted: false,
                producers: BTreeMap::new(),
                last_seq: None,
                access_ms: *created_ms,
                forks: Lifecycle {
                    origin: Some(prepared.offer),
                    ..Lifecycle::default()
                },
            };
            let result = Outcome {
                content_type: Some(stream.config.content_type.clone()),
                ..Outcome::stream(&stream, false)
            };
            state.streams.insert(target.into(), stream);
            Ok(result)
        }
    }
}

pub(crate) fn apply(
    state: &mut crate::model::State,
    operation: &Operation,
) -> Result<crate::model::Outcome, Error> {
    use crate::model::Outcome;
    match operation {
        Operation::Begin {
            id,
            request,
            body,
            now_ms,
        } => begin(state, id, request, body, *now_ms),
        Operation::Prepare(offer) => prepare(state, offer),
        Operation::Stage {
            target,
            id,
            offset,
            data,
            ends,
        } => stage(state, target, id, *offset, data, ends),
        Operation::Finish {
            target,
            id,
            decision,
        } => finish(state, target, id, decision),
        Operation::Decide {
            id,
            commit,
            now_ms,
            rejection,
        } => {
            let source = state.streams.get_mut(&id.source).ok_or(Error::Missing)?;
            if source.incarnation != id.incarnation {
                return Err(Error::StaleIncarnation);
            }
            let transaction = source
                .forks
                .transactions
                .get_mut(&id.sequence)
                .ok_or(Error::Missing)?;
            if transaction.decision == Decision::Preparing {
                transaction.decision = if *commit {
                    if source
                        .config
                        .expiry
                        .is_some_and(|p| p.expired(source.access_ms, *now_ms))
                    {
                        Decision::Aborted(Error::Missing)
                    } else {
                        Decision::Committed {
                            created_ms: *now_ms,
                        }
                    }
                } else {
                    Decision::Aborted(rejection.clone().unwrap_or(Error::ConfigConflict))
                };
            }
            Ok(Outcome::ok(transaction.offer.total, id.incarnation, false))
        }
        Operation::Finalized(id) => {
            let source = state.streams.get_mut(&id.source).ok_or(Error::Missing)?;
            if source.incarnation != id.incarnation {
                return Err(Error::StaleIncarnation);
            }
            let transaction = source
                .forks
                .transactions
                .get_mut(&id.sequence)
                .ok_or(Error::Missing)?;
            if transaction.decision == Decision::Preparing {
                return Err(Error::PendingFork);
            }
            transaction.finalized = true;
            transaction.initial = Vec::new();
            Ok(Outcome::ok(transaction.offer.total, id.incarnation, false))
        }
        Operation::Release(id) => {
            let source = state.streams.get_mut(&id.source).ok_or(Error::Missing)?;
            if source.incarnation > id.incarnation {
                return Ok(Outcome::ok(0, id.incarnation, true));
            }
            if source.incarnation != id.incarnation || id.sequence > source.forks.sequence {
                return Err(Error::StaleIncarnation);
            }
            if source
                .forks
                .transactions
                .get(&id.sequence)
                .is_some_and(|t| !matches!(t.decision, Decision::Committed { .. }))
            {
                return Err(Error::InvalidFork);
            }
            source.forks.transactions.remove(&id.sequence);
            source.reclaim();
            Ok(Outcome::ok(0, id.incarnation, false))
        }
        Operation::Released {
            target,
            incarnation,
            id,
        } => {
            let stream = state.streams.get_mut(target).ok_or(Error::Missing)?;
            if stream.incarnation != *incarnation {
                return Err(Error::StaleIncarnation);
            }
            if !stream.deleted || stream.forks.retained() {
                return Err(Error::InvalidFork);
            }
            if stream
                .forks
                .origin
                .as_ref()
                .is_some_and(|origin| &origin.id == id)
            {
                stream.forks.origin = None;
            }
            Ok(Outcome::ok(0, *incarnation, false))
        }
    }
}
