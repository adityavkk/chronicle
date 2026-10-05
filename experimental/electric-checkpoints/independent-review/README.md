# Independent export-guard follow-up

This is a narrow review of the two public export fixes in the final cumulative
v5 patches, not another broad race review or an independent performance/E2E run.

- **SDK: four real-client tests pass.** Export without `onCommittedBatch`
  rejects, including bootstrap-only use. Enabled capture exports canonical rows
  and their exact source cut while an optimistic transaction persists; hydration
  and independent replay agree. Default legacy visibility remains unchanged.
- **EntityStreamDB: three tests pass; two unrelated tests are skipped.** The
  unchanged synchronous `onBatch` reproduction now rejects instead of mixing
  prior rows/cut with advanced indexes. `onBeforeBatch` export also rejects;
  export inside `onCommittedSnapshot` matches the complete committed image.
- Runtime/state typechecks and client/state/server builds pass. The wider client
  typecheck was not rerun by this follow-up; the implementation bundle retains
  its separately verified 113 baseline diagnostics.

No remaining defect was found in these requested guard scenarios. Neither prior
public API defect was demonstrated in normal bounded `processWake`, which
already enables committed capture.

The source copies match the applied cumulative patches at the pins in the parent
README. `SOURCE-SHA256SUMS` preserves the reviewer's original workspace paths;
its SHA-256 is
`e3f84361277a5663193968c7cecda202b023e2d0fb96a3a7acf8f0c3db2ae0bb`.
All 13 original entries were independently verified on transfer. `SHA256SUMS`
maps those same unchanged files to this layout, referring to the two patches in
the parent directory. `HANDOFF-v5.md` is the original pre-campaign review input,
not the final campaign delivery report.

Run `sha256sum -c SHA256SUMS` from this directory. To rerun the tests after
applying and building the parent bundle, place `review-v5-export.test.ts` in
Durable's `packages/state/test/`; place the other two test files in Electric's
`packages/agents-runtime/test/`. Repeat the focused selection with:

```sh
# From the patched Durable root:
pnpm exec vitest run --project state packages/state/test/review-v5-export.test.ts
# From the patched Electric root:
pnpm --dir packages/agents-runtime exec vitest run \
  test/review-v5-onbatch-export.test.ts test/entity-stream-db-snapshot.test.ts \
  -t 'exports a whole committed image|rejects in-batch export|rejects export without'
```

The logs retain result counts and exported rows/cuts. No Chronicle/Postgres
E2Es, fork campaign, or performance measurements were executed by this follow-up.
