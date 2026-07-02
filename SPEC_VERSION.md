# Spec version

Chronicle's certification provenance, pinned by commit SHA and conformance
suite version rather than a floating branch (recommended in
`docs/research/06-ecosystem.md` §1, following the precedent set by the
independent Rust server).

- **Protocol spec:** vendored `docs/spec/PROTOCOL.md` copied from
  [durable-streams/durable-streams](https://github.com/durable-streams/durable-streams)
  at commit `82f9963ae0b489566352393be9b4796c788c99c2`.
- **Conformance suite:** `@durable-streams/server-conformance-tests@0.3.5`,
  pinned in `test/conformance/package.json`.
- **Certified result:** 332/332 at 0.3.5.

The suite runs against chronicle + a live Redis in CI's `conformance` job
(`.github/workflows/ci.yml`); see `docs/spec/IMPLEMENTATION_TESTING.md` for
how to run it locally.
