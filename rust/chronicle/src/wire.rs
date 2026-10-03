//! Adapted from Electric durable-streams 0.1.5, Apache-2.0.
//! Source/provenance and license: ../vendor/electric/. Extracted store.rs offset
//! grammar and handlers.rs encode_wire. Public visibility, descriptive offset
//! errors and equivalent branch simplifications differ from upstream.
use bytes::{BufMut, Bytes, BytesMut};
use serde_json::value::RawValue;

pub fn format_offset(bytes: u64) -> String {
    format!("{:016}_{:016}", 0, bytes)
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
