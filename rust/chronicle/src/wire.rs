//! Adapted from Electric durable-streams 0.1.5, Apache-2.0.
//! Source/provenance and license: ../vendor/electric/. Extracted store.rs offset
//! grammar and handlers.rs encode_wire. Public visibility, descriptive offset
//! errors and equivalent branch simplifications differ from upstream.
use bytes::{BufMut, Bytes, BytesMut};
use serde_json::value::RawValue;

pub fn format_offset(bytes: u64) -> String {
    format!("{:016}_{:016}", 0, bytes)
}

/// Electric store.rs cursor intervals and clock-derived jitter. Unlike upstream,
/// take the clock explicitly and reject client-controlled integer overflow.
pub fn compute_cursor(client: Option<u64>, now: std::time::Duration) -> Result<u64, &'static str> {
    let interval = now.as_secs().saturating_sub(1_728_432_000) / 20;
    match client {
        Some(cursor) if cursor >= interval => cursor
            .checked_add(1 + u64::from(now.subsec_nanos() % 180))
            .ok_or("cursor overflow"),
        _ => Ok(interval),
    }
}

pub enum ParsedOffset {
    Start,
    Now,
    At(u64),
}

pub fn parse_offset(raw: Option<&str>) -> Result<ParsedOffset, &'static str> {
    match raw {
        None | Some("-1") => Ok(ParsedOffset::Start),
        Some("now") => Ok(ParsedOffset::Now),
        Some(s) => {
            let (a, b) = s.split_once('_').ok_or("invalid offset")?;
            if a.len() != 16
                || b.len() != 16
                || !a.bytes().all(|c| c.is_ascii_digit())
                || !b.bytes().all(|c| c.is_ascii_digit())
            {
                return Err("invalid offset");
            }
            Ok(ParsedOffset::At(b.parse().map_err(|_| "invalid offset")?))
        }
    }
}

/// JSON values are stored verbatim with commas; arrays flatten exactly one level.
pub fn encode_wire(
    body: &Bytes,
    is_json: bool,
    allow_empty_array: bool,
) -> Result<Bytes, &'static str> {
    if !is_json {
        return Ok(body.clone());
    }
    let text = std::str::from_utf8(body).map_err(|_| "invalid UTF-8 in JSON body")?;
    if text.trim_start().starts_with('[') {
        let elems: Vec<&RawValue> = serde_json::from_str(text).map_err(|_| "invalid JSON body")?;
        if elems.is_empty() && !allow_empty_array {
            return Err("empty JSON array append");
        }
        let mut out = BytesMut::with_capacity(body.len());
        for e in &elems {
            out.put_slice(e.get().as_bytes());
            out.put_u8(b',');
        }
        Ok(out.freeze())
    } else {
        let v: &RawValue = serde_json::from_str(text).map_err(|_| "invalid JSON body")?;
        let mut out = BytesMut::with_capacity(v.get().len() + 1);
        out.put_slice(v.get().as_bytes());
        out.put_u8(b',');
        Ok(out.freeze())
    }
}

/// A comma inside a string or nested array is not a cursor boundary.
pub fn json_boundary(data: &[u8], offset: usize) -> bool {
    use std::io::Read;
    if offset == 0 {
        return true;
    }
    let Some(prefix) = data.get(..offset).and_then(|p| p.strip_suffix(b",")) else {
        return false;
    };
    // Parse without allocating another payload or a Vec of individual values.
    let framed = b"[".as_slice().chain(prefix).chain(b"]".as_slice());
    serde_json::from_reader::<_, serde::de::IgnoredAny>(framed).is_ok()
}

/// Portable bounded range delivery, following Electric engine_raw.rs's fallback.
/// Unexpected EOF is an error, not successful completion of a short response.
pub fn file_body(
    file: std::fs::File,
    length: u64,
    json: bool,
    admission: impl Clone + Send + 'static,
) -> axum::body::Body {
    use futures_util::{StreamExt, stream};
    use std::io::Read;
    let reader = stream::try_unfold(
        (file, length, admission),
        |(mut file, remaining, admission)| async move {
            if remaining == 0 {
                return Ok::<_, std::io::Error>(None);
            }
            // Cancellation cannot release admission while this blocking read still
            // owns an FD/buffer. Tokio File's internal task does not retain our guard.
            let retained = admission.clone();
            let (file, bytes) = tokio::task::spawn_blocking(move || {
                let _admission = retained;
                #[cfg(feature = "storage-faults")]
                crate::faults::Context::new(std::path::Path::new("http-body"))
                    .hit(crate::faults::BEFORE_BODY_READ)?;
                let mut bytes = vec![0; remaining.min(256 * 1024) as usize];
                file.read_exact(&mut bytes)?;
                Ok::<_, std::io::Error>((file, bytes))
            })
            .await
            .map_err(std::io::Error::other)??;
            let left = remaining - bytes.len() as u64;
            Ok(Some((Bytes::from(bytes), (file, left, admission))))
        },
    );
    if json {
        axum::body::Body::from_stream(
            stream::once(async { Ok(Bytes::from_static(b"[")) })
                .chain(reader)
                .chain(stream::once(async { Ok(Bytes::from_static(b"]")) })),
        )
    } else {
        axum::body::Body::from_stream(reader)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn cursor_crosses_interval_and_rejects_overflow() {
        let now = Duration::new(1_728_432_000 + 39, 179);
        assert_eq!(compute_cursor(None, now), Ok(1));
        assert_eq!(compute_cursor(Some(0), now), Ok(1));
        assert_eq!(compute_cursor(Some(1), now), Ok(181));
        assert_eq!(compute_cursor(None, now + Duration::from_secs(1)), Ok(2));
        assert_eq!(compute_cursor(Some(u64::MAX - 180), now), Ok(u64::MAX));
        assert!(compute_cursor(Some(u64::MAX - 179), now).is_err());
    }
}
