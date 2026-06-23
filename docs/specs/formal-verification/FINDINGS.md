# Findings & latent-bug register

*The verification effort's job is not only to prove what holds, but to surface what is currently invisible. This register collects the concrete, code-located findings the research pass turned up: confirmed latent bugs in the shipped code, scope gaps the adversarial critic raised, and research-hygiene notes to re-check before any of this lands in a published design doc. Each confirmed bug below was read in source during the pass; the line references are real. Read this alongside the [stack & roadmap](./DESIGN.md), the Lean plan in [./research/02-proof-assistants-lean.md](./research/02-proof-assistants-lean.md), and the PBT plan in the [property-based-testing worktree](../../../property-based-testing).*

## Confirmed latent bugs

These are not hypotheticals. Each was located in the current `main` source during research. Severity is rated on **reachability today** (can it bite a running deployment now?) versus **latent reachability** (does some unshipped or at-scale condition arm it?). The recommended action distinguishes *surface* (write a failing test/property that pins the behavior) from *fix* (change code) from *document* (record the safe domain and move on) — because for the offset-width bug in particular the right first move is explicitly **not** to change the wire format.

### LB-1 — `Offset.String()` uses `%016d` (minimum width), so lexicographic order diverges from numeric order at a field >= 10^16

**What it is.** [`store/offset.go:25`](https://github.com/adityavkk/chronicle/blob/main/store/offset.go) formats an offset as:

```go
func (o Offset) String() string {
	return fmt.Sprintf("%016d_%016d", o.ReadSeq, o.ByteOffset)
}
```

`%016d` is a **minimum** field width, not a maximum. It zero-pads to 16 digits but does not cap. The maximum `uint64` is `18446744073709551615` — **20 digits**. So as soon as either field reaches 10^16, the rendered string grows past 16 digits and the zero-padding stops doing its one job: keeping all strings the same length so that `strcmp` agrees with numeric `<`. Concretely:

| numeric value | `String()` rendering | length |
|---|---|---|
| 9 999 999 999 999 999 (10^16 - 1) | `9999999999999999` | 16 |
| 10 000 000 000 000 000 (10^16) | `10000000000000000` | 17 |

Bytewise, `"10000000000000000"` sorts **before** `"9999999999999999"` (because `'1' < '9'` at position 0). So the larger numeric offset sorts *earlier* as a string.

**Where it bites.** Redis stores stream frames as ZSET members keyed by the offset string, and `read.lua` does the range scan with `ZRANGEBYLEX` over those string members ([`store/redis/scripts/read.lua:27`](https://github.com/adityavkk/chronicle/blob/main/store/redis/scripts/read.lua)). The read path's correctness rests entirely on **lexicographic member order equalling numeric frame order**. Past a 10^16-byte `ByteOffset` (~10 PB on one stream) the ZSET ordering inverts at the boundary and reads return frames out of order — a silent data-plane corruption, not a crash. `Offset.Compare` ([`store/offset.go:128`](https://github.com/adityavkk/chronicle/blob/main/store/offset.go)) is correct (it compares the numeric fields), so the Go core and the Redis lex order *disagree* exactly in this region; the MemoryStore oracle would never reproduce it because it never uses the string order for reads.

**Why it is currently invisible.** Every existing test uses tiny values. The differential harness ([`store/redis/differential_test.go`](https://github.com/adityavkk/chronicle/blob/main/store/redis/differential_test.go)) covers a small producer table, not 10^16-scale offsets. No property exercises the `Compare` vs `strcmp(String())` agreement across the full `uint64` domain.

**Severity / reachability.** Latent, high-impact, low-reachability-today. A single stream must accumulate >= 10 PB of appended bytes before `ByteOffset` arms it; `ReadSeq` is always 0 today (see LB-3), so the `ReadSeq` field can't arm it yet. It is a *scale* bug, invisible until a very large deployment, and silent (wrong order, not an error) when it triggers.

**Recommended action — SURFACE + PROVE + DOCUMENT; do NOT change the persisted/wire format yet.** The decision is deliberate: the offset string is the persisted ZSET member key and the wire `Stream-Next-Offset` value, so widening `%016d` -> `%020d` is a **format migration** (old members stop sorting against new ones), not a one-line fix, and must not be done casually inside a verification pass.

1. Write a **rapid** property `Compare(a,b)` sign == `strcmp(a.String(), b.String())` sign, run it **unguarded** over the full `uint64` domain, and let it produce the >= 10^16 counterexample as a checked-in regression fixture (property-based-testing worktree).
2. Write the **Lean** theorem with the explicit `< 10^16` hypothesis (`./research/02-proof-assistants-lean.md`), which both proves the *safe domain* and documents the unsafe one in a machine-checked form.
3. File the format migration as a tracked, separate decision (width bump to 20, or an enforced runtime invariant that `ByteOffset < 10^16`). Do not bundle it into the verification work.

### LB-2 — `Stream-Seq` regression check: caller-supplied opaque strings compared bytewise — same digit-width class as LB-1, and already shipped to clients

**What it is.** `Stream-Seq` is an application-layer ordering token the caller supplies as an opaque string. The regression check compares it bytewise with `<=`, in both backends:

- Redis: [`store/redis/scripts/append.lua:85`](https://github.com/adityavkk/chronicle/blob/main/store/redis/scripts/append.lua) — `if stream_seq ~= '' and m.lastSeq ~= nil and m.lastSeq ~= '' and stream_seq <= m.lastSeq then` (Lua string `<=`, C-locale memcmp).
- MemoryStore: [`store/memory_store.go:585`](https://github.com/adityavkk/chronicle/blob/main/store/memory_store.go) — `if stream.metadata.LastSeq != "" && opts.Seq <= stream.metadata.LastSeq {` (Go string `<=`, bytewise).

This is the **same digit-width hazard as LB-1**, one layer up: a naive client that sends unpadded decimal sequence numbers gets wrong ordering at the 9->10 transition. `"10" <= "2"` is **true** bytewise, so a client that appends `Seq:"2"` then `Seq:"10"` has its numerically-newer write **rejected** as a regression.

**The twist that makes this subtler than LB-1: this behavior is already encoded as the intended contract.** [`store/redis/integration_test.go:287`](https://github.com/adityavkk/chronicle/blob/main/store/redis/integration_test.go) (`TestIntegrationStreamSeqLexRegression`) asserts *exactly* this — `"10"` after `"2"` returns `ErrSequenceConflict` ("REJECTED even though numerically larger"), while the zero-padded sequence `"09"` then `"10"` is accepted. So the two backends **agree** with each other and with a committed test; this is not a Go-vs-Lua divergence. It is a **client-facing footgun** that the protocol documents implicitly through a test rather than explicitly through the contract.

**Why it is currently invisible *as a hazard*.** The bytewise compare is correct and tested; what is missing is any surfaced statement of the *precondition the client must meet* (fixed-width or otherwise lex-safe sequence values). A client author reading the header docs would not learn that `9 -> 10` breaks.

**Severity / reachability.** Reachable **today** by any client using unpadded decimal `Stream-Seq`, with no scale precondition — but it manifests as a rejected append (`ErrSequenceConflict`), a loud failure, not silent corruption. Severity: medium, correctness-of-contract.

**Recommended action — DOCUMENT the client precondition + SURFACE via differential.** No code change (the backends are consistent and the behavior is by-design). (1) State the lex-safe-`Stream-Seq` precondition explicitly in the protocol/header docs and connect it to LB-1 as one shared "string order must match intended order" failure class. (2) Add the **triple-mirror** differential property (Go `<=` vs live Lua `<=`) over generated sequence-string pairs including the `9/10`, `09/10`, and leading-zero cases, so any future drift between the two `<=` implementations is caught (property-based-testing worktree).

### LB-3 — `ReadSeq` divergence: MemoryStore compares only `ByteOffset` while Redis compares the full offset string (incl. `ReadSeq`)

**What it is.** The two backends use **different keys** for the read-from-offset comparison, and agree today only because `ReadSeq` is always 0.

- MemoryStore reads by comparing the **`ByteOffset` field alone**: [`store/memory_store.go:672`](https://github.com/adityavkk/chronicle/blob/main/store/memory_store.go) in `readOwnMessages` — `if msg.Offset.ByteOffset > offset.ByteOffset {`.
- Redis reads by lex-ranging over the **full offset string**, which encodes `ReadSeq` as the high-order 16 digits: `ZRANGEBYLEX KEYS[2] ARGV[2] '+'` where the bound is `"(<offset>\xff"` ([`store/redis/scripts/read.lua:6,27`](https://github.com/adityavkk/chronicle/blob/main/store/redis/scripts/read.lua)).

`ReadSeq` is documented as "For future log rotation support" ([`store/offset.go:15`](https://github.com/adityavkk/chronicle/blob/main/store/offset.go)) and is **never set non-zero today** because log rotation is unimplemented. So `ByteOffset`-only and full-string comparison produce identical results — *for now*. The moment `ReadSeq` becomes non-zero (when rotation lands), two messages with the same `ByteOffset` but different `ReadSeq` would be ordered correctly by Redis and **conflated** by MemoryStore, and the differential harness would start failing — or worse, the oracle (MemoryStore) would be the *wrong* one.

**Why it is currently invisible.** No test sets `ReadSeq != 0` (there is no code path that can). The differential and conformance suites only ever see `ReadSeq == 0`, so the divergence is dormant. The claim "they agree" is an **untested assumption about current data**, not a verified invariant.

**Severity / reachability.** Zero reachability today; **guaranteed** to arm the day log rotation ships. Severity: high *as a latent trap for a future feature*, because it will surface as a confusing oracle disagreement in a worktree built to trust MemoryStore as the oracle.

**Recommended action — SURFACE now with a regression property.** Add a property that constructs offsets with non-zero `ReadSeq` and asserts MemoryStore's read-comparison matches the full-offset ordering (or, more honestly, **document that MemoryStore's `readOwnMessages` must be changed to compare the full `Offset` via `Compare` before rotation ships**). Pin it now while the contract is simple, so the rotation feature inherits a red test instead of a silent oracle bug.

### LB-4 — `model_shard.go` still carries a `SCAFFOLD` header — confirm live wiring to `claim_shard.lua` before building on it

**What it is.** [`jepsen/checker/model_shard.go`](https://github.com/adityavkk/chronicle/blob/main/jepsen/checker/model_shard.go) is the Porcupine pure-core model for the owner-epoch slot-ownership exclusivity test (T3). Its header still reads, verbatim:

> `SCAFFOLD. The mechanism it models — claim_shard.lua / check_owner.lua and the ds:{ownership}:slot:<h> record — does NOT exist on today's code; it lands in #14. So this model is the EXECUTABLE SPEC for that slice ... it compiles, it is unit-tested against [model_shard_test.go] ...`

But `claim_shard.lua`, `check_owner.lua`, and the owner-epoch fence **have since shipped** — `webhook/scripts/claim_shard.lua` and `webhook/scripts/check_owner.lua` exist, and `expire_lease.lua` already calls `owner_fenced(KEYS[4], ARGV[3], ARGV[4])` ([`webhook/scripts/expire_lease.lua`](https://github.com/adityavkk/chronicle/blob/main/webhook/scripts/expire_lease.lua)). The model's header is now **stale**: it claims to be an unbacked spec for code that no longer is unbacked.

**Why it matters.** The model is unit-tested against itself (`model_shard_test.go`), which proves the *model* is internally consistent but proves **nothing about the shipped Lua**. The driver that would bind it to a live cluster is `scenario_shard.go` (present in `jepsen/checker/`). If that driver is not actually exercising the now-shipped `claim_shard.lua` / `check_owner.lua` against a real Redis, then `INV-OWNER-01/02` (owner-epoch single-holder) are **specified but never validated against real Lua** — the GREEN you see is the model agreeing with itself.

**Severity / reachability.** Process/assurance gap, not a runtime bug. But it is **load-bearing for Phase 0**: the entire owner-epoch verification story is built on top of this model.

**Recommended action — CONFIRM wiring in Phase 0 before anything else builds on it.** (1) Verify `scenario_shard.go` drives the live `claim_shard.lua`/`check_owner.lua` and the `ds:{ownership}:slot:<h>` record, not a stub. (2) If wired, **delete the SCAFFOLD header** and re-baseline the test against real Lua. (3) If not wired, wire it — this is the listed Phase 0 deliverable. Until then, treat any `model_shard` GREEN as model-internal, not a verification of the shipped owner-epoch fence.

### LB-5 — the `3 * sweepInterval` T4 re-emit threshold is an unvalidated magic number

**What it is.** The recently-landed T4 recovery sweep (commit `a43c5fb`) re-emits a pull-wake stranded *after* a durable emit. The staleness gate is a bare multiplier:

```go
if sub.Config.Type == DispatchPullWake && sub.Phase == PhaseWaking && sub.WakeEventSentNs != 0 &&
	now.UnixNano()-sub.WakeEventSentNs > (3*m.sweepInterval).Nanoseconds() {
	m.writeWakeEvent(sub, "", sub.Generation, sub.WakeID)
```

([`webhook/manager.go:1196-1198`](https://github.com/adityavkk/chronicle/blob/main/webhook/manager.go); `sweepInterval` defaults to 30s, so the threshold is 90s.) The surrounding comment is candid that this path strands forever otherwise: a pull-wake arms **no lease**, so `LeaseExpired` never fires, `DecideDue` is `DueSkip` for a non-idle phase, and `reconcileLeases` skips it (`lease_until_ns == 0`, never `LeaseStranded`). So the only thing that recovers it is this re-emit, gated on this one number.

**Why it is currently invisible.** There is a failpoint test for the stranding scenario, but **no proof or test** that `3x` is (a) long enough to guarantee a genuinely-stranded wake is eventually re-emitted, nor (b) short-enough-bounded that it cannot re-emit to a **still-live but slow consumer** that simply hasn't claimed yet — which would be a double-delivery. The `(gen, wake)` fence makes a duplicate *claim-safe* (at most one live holder), so a double-emit degrades to at-least-once, not to a safety violation; but the *liveness* claim ("3x guarantees eventual re-emit") and the *no-spurious-re-emit-to-a-live-consumer* claim are both asserted, not validated.

**Severity / reachability.** Reachable on any pull-wake that crashes post-emit; the safety net (the fence) holds, so the risk is **liveness/efficiency** (a slow consumer gets a redundant wake, or a stranded wake waits up to 3 ticks) rather than correctness. Severity: medium, on a freshly-shipped path.

**Recommended action — MODEL explicitly in TLA+.** In the `SubscriptionFence` spec, model the crash at the arm->emit instruction boundary and the post-emit stranding, and check the `3x` threshold against a slow-but-live consumer (Phase 2 liveness deliverable in [./DESIGN.md](./DESIGN.md)). The temporal property is `[](stranded-waking) ~> re-emit` under weak fairness of the sweep, plus a safety check that re-emit does not produce two simultaneously-live holders (it should not — the fence is the backstop). Until modelled, document the `3x` as a tuning heuristic, not a guarantee.

## Scope gaps surfaced by the adversarial critic

The critic's central observation: **every proposed artifact stops at the `store.Store` interface or the webhook control plane.** The HTTP read surface, JSON semantics, fork-graph termination, and the data-plane reader's own liveness are the largest untouched correctness areas — and they are exactly the surfaces a user hits directly. Ranked by exposure:

| Gap | Where (real source) | Why it is unguarded | Suggested artifact |
|---|---|---|---|
| **SSE / long-poll EOF semantics** | [`handler_sse.go`](https://github.com/adityavkk/chronicle/blob/main/handler_sse.go) (~190 lines: `sentInitialControl` flag, the 100ms wait-then-reread loop, base64 framing); [`handler.go`](https://github.com/adityavkk/chronicle/blob/main/handler.go) long-poll | An explicit in-code comment marks a hand-found **data-loss race**: emitting the closing control in the wrong place "would silently drop the final append for a live reader." That is a regression-prone, author-patched bug with **zero** model/property/differential coverage. | Small TLA+ or rapid metamorphic over reader-timing schedules: final append delivered as data **before** the single `streamClosed` control event, then EOF. |
| **JSON-mode flatten + fork-sub-offset** | [`store/json.go`](https://github.com/adityavkk/chronicle/blob/main/store/json.go) `ProcessJSONAppend` (flatten top-level array one level, empty-array allowed on create but not append); `resolveForkSubOffset` in [`store/redis/store.go:469`](https://github.com/adityavkk/chronicle/blob/main/store/redis/store.go) | Two independent implementations of the dual-meaning sub-offset (bytes for binary forks, **flattened-message count** for JSON forks) with **no committed differential**. Textbook drift point. | rapid differential (Go memory vs live Redis) over scalars, nested/empty arrays, objects, whitespace, `allowEmpty in {true,false}`. |
| **Fork-chain READ recursion & DELETE-cascade — no termination/acyclicity proof** | `readForkedStream` ([`store/memory_store.go:728`](https://github.com/adityavkk/chronicle/blob/main/store/memory_store.go)), `deleteWithCascade` (`:365`); mutually-recursive Redis `readInherited`/`readForkChain` ([`store/redis/store.go`](https://github.com/adityavkk/chronicle/blob/main/store/redis/store.go)) | **No visible cycle detection or depth bound** in either backend. A fork-lineage cycle or a pathologically deep chain is a stack-overflow / infinite-loop **DoS**. The prompt named "soft-delete cascade termination"; refcount arithmetic was modelled but the **graph's well-foundedness** was not. | Lean: well-founded recursion on a depth measure; Alloy: no cycle in `forkedFrom` up to a bound; **plus a runtime depth guard**. |
| **HTTP status/header contract (404 vs 410-Gone)** | 410 path at four sites in [`handler.go`](https://github.com/adityavkk/chronicle/blob/main/handler.go); header derivation duplicated across `handleRead`/`handleHead`/long-poll | Soft-deleted -> 410, hard-deleted/absent -> 404, and every response's `Stream-Next-Offset` / `Stream-Closed` / `Stream-Up-To-Date`: a regression flipping 410<->404 or dropping a header is invisible to **every** store-level and webhook-level artifact. This is the user-facing contract. | rapid over `httptest.Server`, or extend the existing TS conformance suite. |
| **TTL/expiry reader-vs-reaper race** | `append.lua` touches TTL **before** the closed check; MemoryStore flips `SoftDeleted` only in `Read` while Redis flips everywhere | Expiry is a **non-monotone retraction** (CALM); an in-flight SSE/long-poll reader holding an offset into a now-reaped stream is unmodelled. The proposed frozen-clock differential is not built and does not cover the sliding-TTL touch-on-rejected-append. | TLA+ reader-vs-reaper interleaving, or an integration race test. |
| **Data-plane reader liveness** | `WaitForMessages` (`handler_sse.go`, `handler.go` long-poll) over [`store/redis/notify.go`](https://github.com/adityavkk/chronicle/blob/main/store/redis/notify.go) pub/sub + 1s defensive poll | **All** liveness work targets the `__ds` webhook control plane (INV-WAKE-01, INV-JEP-L1..L5). The HTTP reader's own wake dependency is never modelled, yet a lost pub/sub notification is a directly user-visible hang (degrading at best to the 1s poll latency). | TLA+ liveness under weak fairness, or a notification-loss integration race test. |

### Missing domains (whole categories no agent touched)

- **HTTP protocol conformance / black-box wire testing.** The context states ~330 conformance black-box tests with fast-check fuzzing exist in a TS layer, yet no proposal extends or formalizes the HTTP surface (status codes, header round-trip, content negotiation, base64 SSE, OPTIONS/CORS, HEAD). Largest untouched surface; the one users actually hit.
- **Streaming / server-push correctness theory.** SSE/long-poll have well-studied failure modes (lost-final-event, duplicate-on-reconnect, slow-consumer backpressure, half-open connections, CDN buffering). No streaming-protocol framing was brought despite `handler_sse.go` being a core "Durable Streams" deliverable.
- **Security / adversarial-input properties.** Code comments cite injection vectors (offset parsing, SSE line-terminator splitting, hash-tag escaping); [`webhook/ssrf.go`](https://github.com/adityavkk/chronicle/blob/main/webhook/ssrf.go) exists. No systematic fuzzing of path/header/JSON for injection, SSRF, CRLF-in-SSE, or Lua-injection via crafted `Stream-Seq`/content-type strings was proposed.
- **Cryptographic correctness of webhook signing.** [`webhook/crypto.go`](https://github.com/adityavkk/chronicle/blob/main/webhook/crypto.go) (HMAC/signature for delivery) and its test were not read by any agent — replay protection, signature canonicalization, and timing-safe comparison are verifiable, easily-regressed properties entirely absent.
- **Full metamorphic equivalence of the two backends under JSON / forks / TTL.** The MemoryStore-vs-Redis equivalence harness is the right idea but its author scoped it to non-JSON, single-threaded, clock-injected ops; the **hardest** divergence points (JSON flatten, fork-chain read, lazy-expiry transition) are explicitly deferred. They are where LB-3 and the JSON gap above live.
- **Resource-exhaustion / DoS bounds as a correctness property.** Confirmed unbounded fork-chain recursion, unbounded `Stream-Seq` length, unbounded JSON nesting, the Lua 5s busy-reply threshold for a large fork/GC cascade, and no retention/byte-cap found. A single hostile request can blow memory or block the single-threaded Redis server. "Every operation is bounded in time and space" was skipped as a provable property.

## Research-hygiene notes

These are claims and citations the research pass self-flagged as not primary-sourced or not yet resolved. They must be re-checked before any of them is stated as fact in a published design doc. None of them changes the engineering recommendation; they are about not over-claiming the *evidence* behind it.

### Citations to verify resolve / re-source before citing

| Citation | Concern |
|---|---|
| `lean4.dev/tactics/automation/decide` | Not the canonical Lean 4 docs host. Official docs live at `lean-lang.org/doc` and mathlib at `leanprover-community.github.io`. This path looks plausibly hallucinated — re-source `decide`/`native_decide` from `lean-lang.org` or the *Theorem Proving in Lean 4* book. |
| `an unconfirmed 2026 arXiv preprint` (claimed "OmniLink", 2026) | arXiv IDs encode YYMM; `2601` = Jan 2026, temporally possible (today is 2026-06-22) but the agent never confirmed it resolves and self-flagged it as small-model-summarized and "unverified." High risk of a transposed/fabricated ID — do **not** rely on the technique. |
| `blog.acolyer.org/2014/11/24/use-of-formal-methods-at-amazon-web-services/` | The morning-paper post on the AWS formal-methods paper is dated 2015 (the CACM paper is 2015), not 2014-11-24. The date looks conflated; re-check the "fearless optimization" quote attribution against the correctly-dated post. |
| `proofsandintuitions.net/2026/02/09/distributed-verification-veil/` | A single Feb-2026 personal-blog URL carrying the entire weight of the "Lean is production-ready for distributed-protocol verification" argument. Cite the underlying **Veil paper/repo**, not the blog, and confirm the URL exists. |
| `welltyped.systems/blog/verified-conformance-testing-for-dummies` (+ the `welltyped-systems/verified-ledger` repo) | The Lean-oracle differential pattern is the single highest-leverage core recommendation, and its entire evidentiary basis is one blog + one repo from "Welltyped Systems" — easily confused with the unrelated Haskell consultancy **Well-Typed LLP** (`well-typed.com`). Confirm this is a distinct, real, reputable source and that the repo actually does Lean->C as described before betting the proof-to-code-gap strategy on it. |

### Unverified quantitative claims (do not quote the numbers without the primary text)

- **AUTOSAR/Volvo** "~200 problems, 100+ standard ambiguities, ~1M LOC, 3000+ pages": the IEEE paper and Hughes chapter PDF were fetched but **not text-extractable**; figures come from secondary summaries.
- **AWS DynamoDB** "939 lines / 3 bugs / 35-step counterexample": canonical CACM/Amazon PDFs did not render as text this run; widely-cited and plausible, but treat as well-attributed-but-not-primary.
- **IronFleet** "7.7:1 overall, ~3.7 person-years, 5:1 safety / 8:1 liveness, IronRSL 85 / IronKV 34 lines": partly from the morning-paper summary, not the SOSP PDF; confirm the ratio decomposition against the paper before stating it as fact. (The *qualitative* warning — full refinement of a concurrent layer in a proof assistant is enormously expensive — stands regardless.)
- **Riak eventual-consistency bugs**: only the *existence* of the talks was confirmed (via a curated index), not the specific bugs. The technique is fine to mention; the specific-bug claim is unverified.
- **Lean->C->cgo as a low-overhead oracle at fuzz-loop call volumes**: the prior art (Welltyped) did Lean->C->**Rust**; the Go/cgo variant "needs a spike to confirm performance and memory behavior." The recommendation leans on a path nobody in the cited work has run for Go. **Budget the spike before committing to the third-oracle plan.**

### Claims asserted in code/comments but not yet validated by any test

These overlap with the latent-bug register above but are listed here because they are specifically *self-described as unproven* and should each get a guarding artifact:

- **`3*sweepInterval` sufficiency + no-double-deliver** (LB-5) — asserted on a path shipped in `a43c5fb`; no proof.
- **"Owner-epoch fence is optimization-only, never a correctness dependency"** (INV-OWNER-01) — asserted in comments and prose, but there is **no test** that forces all `owner_fenced`/`check_owner` checks to pass and shows the `(gen, wake)` fence alone still upholds single-holder. The layering claim is unproven; the Phase 2 "fence-on vs owner-fenced-always-pass" composed-spec check is what would discharge it.
- **"MemoryStore and Redis agree because `ReadSeq` is always 0"** (LB-3) — an untested assumption about current data, not a verified invariant.
