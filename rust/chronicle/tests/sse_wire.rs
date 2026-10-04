use std::io;

use axum::body::Body;
use base64::Engine as _;
use bytes::Bytes;
use chronicle_raft::sse_wire::{Encoding, control, data_body};
use futures_util::{StreamExt as _, stream};
use proptest::prelude::*;

async fn collect(body: Body) -> Result<Vec<u8>, axum::Error> {
    let mut output = Vec::new();
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        output.extend_from_slice(&chunk?);
    }
    Ok(output)
}

fn chunked_body(input: &[u8], cuts: &[usize]) -> Body {
    let mut chunks = Vec::new();
    let mut position = 0;
    for &size in cuts {
        if position == input.len() {
            break;
        }
        let end = (position + size.max(1)).min(input.len());
        chunks.push(Ok::<_, io::Error>(Bytes::copy_from_slice(
            &input[position..end],
        )));
        position = end;
    }
    if position < input.len() {
        chunks.push(Ok(Bytes::copy_from_slice(&input[position..])));
    }
    Body::from_stream(stream::iter(chunks))
}

fn eager(input: &[u8], encoding: Encoding) -> Vec<u8> {
    let payload = match encoding {
        Encoding::Json | Encoding::Text => String::from_utf8_lossy(input).into_owned(),
        Encoding::Base64 => base64::engine::general_purpose::STANDARD.encode(input),
    };
    let fields = payload
        .split(['\r', '\n'])
        .map(|line| {
            format!(
                "data:{}{line}",
                if line.starts_with(' ') { " " } else { "" }
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!("event: data\n{fields}\n\n").into_bytes()
}

#[tokio::test]
async fn sse_parsing_preserves_payload_spaces() {
    for (input, expected) in [
        (" first\n second\r  third", " first\n second\n  third"),
        ("   ", "   "),
        ("\r\n ", "\n\n "),
        (" ♥", " ♥"),
    ] {
        let bytes = collect(data_body(
            chunked_body(input.as_bytes(), &[1, 1, 1, 1]),
            Encoding::Text,
        ))
        .await
        .unwrap();
        // SSE removes one optional field-value separator, not payload whitespace.
        let decoded = std::str::from_utf8(&bytes)
            .unwrap()
            .lines()
            .filter_map(|line| line.strip_prefix("data:"))
            .map(|line| line.strip_prefix(' ').unwrap_or(line))
            .collect::<Vec<_>>()
            .join("\n");
        assert_eq!(decoded, expected);
    }
}

#[tokio::test]
async fn text_preserves_lossy_utf8_at_every_split_point() {
    let input = b"a\xf0\x9f\x92\xa9b\xe2\x82c\xffd\xe2\x82";
    for split in 0..=input.len() {
        let actual = collect(data_body(
            chunked_body(input, &[split, input.len() - split]),
            Encoding::Text,
        ))
        .await
        .unwrap();
        assert_eq!(actual, eager(input, Encoding::Text), "split {split}");
    }
}

#[tokio::test]
async fn text_splits_cr_and_lf_to_prevent_field_injection() {
    let input = b"safe\r\nevent: forged\ndata: forged";
    let actual = collect(data_body(chunked_body(input, &[5, 1, 2]), Encoding::Text))
        .await
        .unwrap();
    assert_eq!(actual, eager(input, Encoding::Text));
}

#[tokio::test]
async fn json_is_not_rewrapped_or_trimmed() {
    let input = br#"[{"value":1},{"value":2}]"#;
    let actual = collect(data_body(chunked_body(input, &[1, 4, 2]), Encoding::Json))
        .await
        .unwrap();
    assert_eq!(actual, eager(input, Encoding::Json));
}

#[tokio::test]
async fn base64_has_padding_only_at_the_end_for_every_split_point() {
    let input = b"base64 tail bytes";
    for split in 0..=input.len() {
        let actual = collect(data_body(
            chunked_body(input, &[split, input.len() - split]),
            Encoding::Base64,
        ))
        .await
        .unwrap();
        assert_eq!(actual, eager(input, Encoding::Base64), "split {split}");
    }
}

#[tokio::test]
async fn source_error_does_not_emit_event_terminator() {
    let source = stream::iter([
        Ok::<_, io::Error>(Bytes::from_static(b"accepted")),
        Err(io::Error::other("source failed")),
    ]);
    let mut framed = data_body(Body::from_stream(source), Encoding::Text).into_data_stream();
    let mut received = Vec::new();
    let mut saw_error = false;
    while let Some(item) = framed.next().await {
        match item {
            Ok(chunk) => received.extend_from_slice(&chunk),
            Err(_) => saw_error = true,
        }
    }
    assert_eq!(
        (received, saw_error),
        (b"event: data\ndata:accepted".to_vec(), true)
    );
}

#[test]
fn control_formats_open_and_closed_streams() {
    assert_eq!(
        control(42, 9, false),
        Bytes::from_static(b"event: control\ndata:{\"streamNextOffset\":\"0000000000000000_0000000000000042\",\"streamCursor\":\"9\",\"upToDate\":true}\n\n")
    );
    assert_eq!(
        control(42, 9, true),
        Bytes::from_static(b"event: control\ndata:{\"streamNextOffset\":\"0000000000000000_0000000000000042\",\"upToDate\":true,\"streamClosed\":true}\n\n")
    );
}

proptest! {
    #[test]
    fn streaming_matches_independent_eager_framing(
        input in proptest::collection::vec(any::<u8>(), 0..2048),
        chunks in proptest::collection::vec(1usize..300, 0..40),
        encoding_index in 0usize..3,
    ) {
        let encoding = [Encoding::Json, Encoding::Text, Encoding::Base64][encoding_index];
        let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let actual = runtime.block_on(collect(data_body(chunked_body(&input, &chunks), encoding))).unwrap();
        prop_assert_eq!(actual, eager(&input, encoding));
    }
}
