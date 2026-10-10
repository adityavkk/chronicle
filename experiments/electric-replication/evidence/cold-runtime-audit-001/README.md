# Local cold-runtime checkpoint audit

This is a local-commit audit, not a push, merge, security certification or
production qualification. `inventory.json` records the audited unpublished
range (two formal/foundation commits), plus the runtime's working files before
the final documentation clarification and addition of this audit evidence.
The final staged closure is checked separately in `staged-scope.json`.

- `claim-fixtures.json`: the fail-fast `audit_claim_fixtures.py HEAD --all-runs`
  invocation ran with HEAD at `b231c8ffec60fbc605f61f513bfdf8c8731b4c2b`.
  All 255 Authorization fixtures have the actual two-part HMAC schema: 238
  issued tokens verify against retired local WAL keys; 17 intentional tamper
  cases return 401. Sixty-eight distinct local keys have no tested encoding
  matches in 14,179 historical blobs, including 2,519 gzip and 29 zstd blobs.
  All 51 recorded PIDs are absent and no orb services are running.
- `gitleaks.json.gz` retains the redacted stock Gitleaks 8.30.1 **exit-1** report,
  not a clean scanner exit. All 85,852 findings were reviewed: 21 actual source
  SHA-256 values and 85,831 object-key SHA-256 values. The latter are recomputed
  from observed immutable object names/descriptors, and the complete inventory
  bytes are reconstructed and hashed. Zero unexplained findings; no rule or
  path exclusions. `findings-review.json` records the report/reviewer hashes.
- `audit-export.py.txt` and `audit-review.py.txt` retain the scratch programs as
  executed. Copy them to `.tmp/cold-runtime-audit/{export,classify}.py` to inspect
  their original paths; the exporter refuses to overwrite an existing export.
  `map.json.gz` ties decoded text hashes back to original files. The initial
  reviewer failures are preserved: scanner columns do not always address the
  complete source line, and not every hash is reported. Final review verifies
  **all** hash fields on each finding's reported line, not a general hash-shaped
  allowlist or an assumption that any 64-character string is harmless.
- `lint.txt` retains the initial missing-Go-PATH failure. With the existing
  Go 1.26.2 toolchain added to PATH, `make lint` returns `0 issues.` in
  `lint-with-toolchain.txt`. No source was changed to silence lint.

Raw lab databases/WALs, provider credentials/diagnostics, compiled binaries,
private infrastructure and profiling memory captures are not staged. MinIO's
fresh per-run credentials were passed through private files and retired by
fixture shutdown; they are not node configuration or consensus metadata.
Licenses/provenance and public links are checked. These finite scanners and
fixture-specific signature checks do not prove absence under arbitrary secret
encodings or replace a production security review.
