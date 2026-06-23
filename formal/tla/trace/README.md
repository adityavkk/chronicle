# Trace validation: binding the running engine to SubscriptionFence (#39)

This is the trace-validation bridge from the research recipe in
[`../../../docs/specs/formal-verification/research/01-tla-and-trace-validation.md`](../../../docs/specs/formal-verification/research/01-tla-and-trace-validation.md).
It checks that the subscription engine running against a real Redis does only
the things the TLA+ model [`../SubscriptionFence.tla`](../SubscriptionFence.tla)
(issue #37) allows. The formal statement is `S ∩ T ≠ ∅`: the set of behaviors
the spec permits has at least one in common with the behavior the real code
just ran. If it does not, the code and the design disagree, and that is a bug in
one of them.

This is the same method Microsoft used on the CCF consensus code, where four of
the six bugs they found came from trace validation and from nothing else.

## The three pieces

1. **A trace seam in the engine.** [`../../../webhook/trace.go`](../../../webhook/trace.go)
   is a `Store` decorator, `TracingStore`, behind the `subtrace` build tag. It
   wraps the real `RedisStore`, so every fence operation still runs the shipped
   Lua script against Redis. Around each of the six fence linearization points —
   `arm_wake`, `claim`, `ack`, `release`, `expire_lease`, `record_wake_sent` — it
   reads the durable subscription state just before and just after the Lua
   commit and appends one JSON line: `{sub, op, preState, args, luaStatus,
   postState}`. The build tag means a normal `go build` never compiles this
   file, so production and the existing tests are unchanged. There is one trace
   line per Lua commit, which is exactly one spec action.

2. **A model wrapper.** [`../Trace.tla`](../Trace.tla) extends `SubscriptionFence`
   and adds one `IsEvent` predicate per action. At step `i` of a run, only the
   spec action that matches trace line `i` may fire; TLC fills in the parts the
   trace does not record (which offset an ack carried, how far the clock moved,
   which worker holds a token) by trying every possibility. A granting line
   (`ARMED`, `CLAIMED`, `OK`, `EXPIRED`) drives the matching action and then
   requires the resulting state to equal the recorded `postState`. A
   non-granting line (`BUSY`, `FENCED`, `STALE`) is recorded as a no-op that must
   leave the durable state unchanged, which is the direct check of the
   stale-inert property (INV-FENCE-03). The one action the spec splits in two —
   arm a pull-wake, then write and stamp its wake event — appears as two trace
   lines (`arm` then `stamp`) and is composed back together in `IsStamp`.

3. **A converter and a runner.** [`tracegen/`](tracegen) turns one
   subscription's trace into a generated `TraceData` module that `Trace.tla`
   reads. [`produce/`](produce) is the driver that runs real fence races against
   Redis to make the trace. [`validate.sh`](validate.sh) ties it together.

## Running it

You need a Redis 8 on `localhost:6379` (or pass another address). The runner
reuses a Redis that is already up, namespaces every key it writes under a unique
run id so it never collides with other data, and deletes every subscription it
created on exit.

```
make trace-validate                       # from formal/tla/
# or with an explicit address:
JAR=/tmp/tla2tools.jar bash trace/validate.sh localhost:6379
```

It prints one line per scenario. The scenarios the driver produces are: a clean
pull-wake lifecycle; a contended claim where the second worker gets `BUSY`; an
expired-lease takeover where the deposed worker's late ack is `FENCED` (the
central single-holder race); a heartbeat then done; a voluntary release; and a
server-side expire then re-arm.

## How to read the verdict

TLC runs in DFS mode and uses the standard witness-by-invariant trick. The
config checks the invariant `NotAccepted`, which says "the run has not yet
reached the end of the trace". So:

- **`Invariant NotAccepted is violated` means the trace VALIDATED.** TLC found a
  spec behavior that reaches the end of the trace; the error trace it prints is
  that accepting run. This is the success case.
- **`Invariant TraceInv is violated` means a safety property failed on the real
  run** — single-holder, stale-inert, generation-monotone, or the type
  invariant. This is a HIGH finding.
- **`No error has been found` means the trace did NOT validate.** No spec
  behavior could follow the whole trace, so the real code did something the
  design forbids. This is a HIGH finding. To find the first bad line, add an
  invariant like `ti <= K` to the config and bisect `K`; the largest `K` that
  still reports a violation is the last line the spec could follow.

## The check is not vacuous

A trace-validation setup that accepts everything proves nothing (research/01
calls this out as the main pitfall). The negative control is in the report and
is reproducible: take the validated takeover trace and relabel the deposed
worker's `FENCED` ack as `OK`. That run claims a worker whose token is two
generations stale acked successfully — which the fence makes impossible. TLC
then reports `No error has been found`: the tampered trace does not validate. So
a real fence violation in the code would be caught here, not hidden.

## What this does and does not prove

It proves that the specific executions the driver produced are legal behaviors
of the bounded model, and it re-checks the model's safety properties on those
real executions. It does not prove anything about executions the driver did not
run, and it does not check liveness. The exhaustive check over all interleavings
of a small instance is the separate `make tlc` run on `SubscriptionFence`
itself; the size-independent guarantee is the deferred Apalache inductive-
invariant step (research/01 Phase 3). Trace validation is the bridge that keeps
that model honest about the code as the code changes.
