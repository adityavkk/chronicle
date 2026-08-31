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

## Section 5.2.1, Idempotent Producers — epoch establishment on a write-fenced stream

Annotates the client-declared-epoch design in
[PROTOCOL.md §5.2.1](./PROTOCOL.md#521-idempotent-producers).

On a stream created with `Write-Fence: true`, the fenced write class gates
epoch establishment by the §7.3 claim generation: `Producer-Epoch` **must
equal** the generation of the presented write token, so a fenced writer cannot
self-declare an epoch the control plane did not grant it, and a producer id an
accepted fenced write has bound cannot advance its epoch as an open write —
see [WRITE-FENCING.md §5–§6](./WRITE-FENCING.md#5-write-classes). **This
changes nothing in the base protocol**: on streams that never opt in — and for
unbound producer ids on the open class of streams that do — the §5.2.1 state
machine (client-declared epochs, auto-claim, sequence rules, all four
producer response headers) is unchanged, byte for byte.

## Section 7.3, Generation Fencing and Leases — the append-side fence

Annotates the fencing rules of
[PROTOCOL.md §7.3](./PROTOCOL.md#73-generation-fencing-and-leases).

§7.3 fences the *control plane*: callbacks, acks, and releases are judged
against the current `(generation, wake_id)`. Chronicle's write-fencing
extension extends the same fence to the *data plane* on streams that opt in:
the claim mints a write token, appends under it are checked against the live
claim marker atomically with the write, and `done`/release/supersession seal
the generation per authority — see
[WRITE-FENCING.md §4 and §7](./WRITE-FENCING.md#4-the-write-token). **This
changes nothing in the base protocol**: §7.3's rejection rules, lease
semantics, and `409 FENCED` control-plane responses are untouched, and a
subscription over streams that never opt in behaves exactly as before.
