//! Fork range resolution over committed Electric-format bytes.
use crate::{model::Stream, wire};

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
