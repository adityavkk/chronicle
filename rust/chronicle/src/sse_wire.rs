// Derived from ElectricSQL durable-streams-server's `src/handlers.rs` SSE
// framing, licensed under Apache-2.0. The pinned source is vendored at
// `vendor/electric/src/handlers.rs`.
// Modified for bounded chunk encoding and to preserve payload-leading spaces:
// SSE parsers discard one optional separator space after `data:`.

//! Bounded, streaming Server-Sent Event framing for stream reads.

use axum::body::{Body, BodyDataStream};
use base64::Engine as _;
use bytes::{Bytes, BytesMut};
use futures_util::{StreamExt as _, stream};

/// The representation used for an SSE data event's payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Encoding {
    /// JSON bytes, already formatted as the array required on the wire.
    Json,
    /// Arbitrary text decoded with [`String::from_utf8_lossy`].
    Text,
    /// Standard padded Base64.
    Base64,
}

struct State {
    source: BodyDataStream,
    encoding: Encoding,
    pending: Vec<u8>,
    started: bool,
    line_start: bool,
    completed: bool,
}

/// Frames a body as one complete SSE `data` event without buffering the body.
///
/// JSON input must already include its surrounding array. Text and JSON use
/// lossy UTF-8 decoding and split CR and LF into separate SSE `data:` fields.
/// A source error is returned before the event's terminating blank line.
pub fn data_body(body: Body, encoding: Encoding) -> Body {
    let state = State {
        source: body.into_data_stream(),
        encoding,
        pending: Vec::with_capacity(3),
        started: false,
        line_start: true,
        completed: false,
    };

    Body::from_stream(stream::unfold(state, |mut state| async move {
        if state.completed {
            return None;
        }
        if !state.started {
            state.started = true;
            return Some((
                Ok::<_, axum::Error>(Bytes::from_static(b"event: data\ndata:")),
                state,
            ));
        }

        match state.source.next().await {
            Some(Ok(chunk)) => {
                let framed = match state.encoding {
                    Encoding::Json | Encoding::Text => {
                        encode_text(&mut state.pending, &mut state.line_start, &chunk, false)
                    }
                    Encoding::Base64 => encode_base64(&mut state.pending, &chunk, false),
                };
                Some((Ok(framed), state))
            }
            Some(Err(error)) => {
                state.completed = true;
                Some((Err(error), state))
            }
            None => {
                let tail = match state.encoding {
                    Encoding::Json | Encoding::Text => {
                        encode_text(&mut state.pending, &mut state.line_start, &[], true)
                    }
                    Encoding::Base64 => encode_base64(&mut state.pending, &[], true),
                };
                let mut framed = BytesMut::with_capacity(tail.len() + 2);
                framed.extend_from_slice(&tail);
                framed.extend_from_slice(b"\n\n");
                state.completed = true;
                Some((Ok(framed.freeze()), state))
            }
        }
    }))
}

fn encode_text(pending: &mut Vec<u8>, line_start: &mut bool, chunk: &[u8], finish: bool) -> Bytes {
    let mut input = Vec::with_capacity(pending.len() + chunk.len());
    input.append(pending);
    input.extend_from_slice(chunk);

    let split = if finish {
        input.len()
    } else {
        input.len() - incomplete_utf8_suffix(&input)
    };
    pending.extend_from_slice(&input[split..]);
    let text = String::from_utf8_lossy(&input[..split]);
    let mut output = BytesMut::with_capacity(text.len());
    for byte in text.bytes() {
        match byte {
            b'\r' | b'\n' => {
                output.extend_from_slice(b"\ndata:");
                *line_start = true;
            }
            _ => {
                if *line_start && byte == b' ' {
                    output.extend_from_slice(b" ");
                }
                output.extend_from_slice(&[byte]);
                *line_start = false;
            }
        }
    }
    output.freeze()
}

fn incomplete_utf8_suffix(input: &[u8]) -> usize {
    for len in (1..=input.len().min(3)).rev() {
        let suffix = &input[input.len() - len..];
        if matches!(std::str::from_utf8(suffix), Err(error) if error.valid_up_to() == 0 && error.error_len().is_none())
        {
            return len;
        }
    }
    0
}

fn encode_base64(pending: &mut Vec<u8>, chunk: &[u8], finish: bool) -> Bytes {
    let mut input = Vec::with_capacity(pending.len() + chunk.len());
    input.append(pending);
    input.extend_from_slice(chunk);
    let split = if finish {
        input.len()
    } else {
        input.len() / 3 * 3
    };
    pending.extend_from_slice(&input[split..]);
    Bytes::from(base64::engine::general_purpose::STANDARD.encode(&input[..split]))
}

/// Builds an Electric-compatible SSE control event.
pub fn control(next: u64, cursor: u64, closed: bool) -> Bytes {
    let mut output = format!(
        "event: control\ndata:{{\"streamNextOffset\":\"{:016}_{next:016}\"",
        0
    );
    if !closed {
        output.push_str(&format!(",\"streamCursor\":\"{cursor}\""));
    }
    output.push_str(",\"upToDate\":true");
    if closed {
        output.push_str(",\"streamClosed\":true");
    }
    output.push_str("}\n\n");
    Bytes::from(output)
}
