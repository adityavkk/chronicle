# Chronicle notes on the vendored protocol

This document holds Chronicle's own annotations on the vendored Durable
Streams spec, kept **separate** from [PROTOCOL.md](./PROTOCOL.md) so that file
stays a pristine, byte-identical mirror of upstream (see
[README.md](./README.md)). Vendoring pristine means we can diff against a new
upstream commit and see exactly what changed, with zero noise from our own
edits — pulling a local addition back out of a merge conflict is exactly the
workflow this file exists to avoid.

Each note below cites the PROTOCOL.md section it annotates. Nothing here
changes the protocol; it records implementation-relevant precision Chronicle
found worth writing down while hardening the Redis/Go implementation.

## Section 5.2, Append to Stream — `Stream-Seq` header (the `INV-DIFF-03` note)

Annotates the `Stream-Seq` request header in
[PROTOCOL.md §5.2](./PROTOCOL.md#52-append-to-stream), specifically the
conditional-append / regression-check text around the `Stream-Seq` bullet.

**Lex-safe client precondition (INV-DIFF-03).** Because the comparison is byte-wise — not numeric — clients **MUST** choose `Stream-Seq` values that are lexicographically monotonic. A naive **unpadded decimal counter is unsafe**: `"10"` sorts *before* `"9"` byte-wise, so the valid advance `"9"` → `"10"` is wrongly rejected with `409 Conflict` at every digit-width boundary (the same class of footgun as a non-fixed-width offset encoding). Clients **SHOULD** use a representation that keeps byte-wise order equal to the intended order, such as **fixed-width zero-padded decimals** (`"0000000010" > "0000000009"`), monotonic timestamps/ULIDs, or any other lexicographically-monotone scheme. The server compares exactly the bytes it is given and applies no numeric interpretation, so this is a client-side obligation, not a server normalization.

### Provenance

This note was lifted verbatim out of PROTOCOL.md (issue #80) to restore the
vendored file to pristine. It originated as the documentation half of the
**LB-2** finding — see
[docs/specs/formal-verification/FINDINGS.md](../specs/formal-verification/FINDINGS.md)
— which named `Stream-Seq`'s bytewise regression check "the same digit-width
hazard as **LB-1**" (`Offset.String()`'s `%016d` minimum-width footgun,
tracked in [ADR-0003](../adr/0003-offset-string-width-migration-lb1.md)) and
recommended stating the lex-safe-`Stream-Seq` precondition explicitly. The
invariant itself is cataloged as `INV-DIFF-03` in
[docs/specs/formal-verification/INVARIANTS.md](../specs/formal-verification/INVARIANTS.md)
and enforced identically by `store/redis/scripts/append.lua` and
`store/memory_store.go`.
