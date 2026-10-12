# Experimental cold checkpoint: full publication-range audit

This audit covers all three unpublished ancestors after remote branch
`de52d72bff77fd4bab736516a03644a77755c2ca`, through local
`a75aa98c59c27aa4cf8f7df3ade56986665d55e5`, plus the completed uncommitted
scheduler/admission and benchmark work. The final index/tree closure is checked
before the one-off local checkpoint. No production acceptance, main merge,
deployment or cloud spending follows from this audit.

* `inventory.json`: 673 historical blob versions plus 6,911 working files,
  4,877,072,390 decoded bytes, including compressed evidence and numeric HDRs.
  All 22,302,773 inventory key-hash fields are independently verified against
  public object names and complete inventory checksums before scanner projection.
  Original histories are unchanged. Largest stored file: 7,013,295 bytes.
* `claim-fixtures.json`: a fresh fail-fast issuer/consumer audit verifies all
  255 two-part JSON/HMAC authorization fixtures against 68 retired local keys.
  It scans 14,583 historical blobs, including 2,549 gzip and 29 zstd blobs;
  the publication exporter independently scans the new working evidence too.
  No tested key encoding matches; all 51 recorded test PIDs are absent.
  These are not JWTs or external credentials. Private retired WALs remain local.
* Stock Gitleaks 8.30.1 returns **exit 1**, preserved in both reports. The
  decoded scan has 89 findings: 88 independently recomputed source hashes and
  one public object-name hash checked against actual DELETE observations. The
  history scan has 85,970 findings: 21 source hashes and 85,949 inventory-key
  hashes independently checked on the exact reported lines. Zero unexplained
  findings; no scanner rule/path exclusions. Redacted reports are retained.
* Human author AND committer identities are canonical on each ancestor. The
  initial overly broad validator rejected an existing **human** coauthor credit
  in the cold-runtime checkpoint; that failure is retained. The corrected check
  preserves that credit while rejecting unreviewed coauthors or agent-session
  trailers. No published history is rewritten and no AI credit is introduced.
* Source, dependency and binary identities match the qualified candidate:
  150 replicated / 116 native-tier Rust passes, four new formal configurations
  and seven negatives, full existing formal runner with 83 TLC negatives and
  its Lean control, hot/cold-enabled unchanged subscriptions-enabled 332/332.
  Those are retained final-source qualifications, not newly rerun Rust suites.
  Fresh `make lint` reports `0 issues.` and 36 Python checks pass.

Cold-enabled conformance remains compatibility evidence (141 publications,
zero HTTP cold bodies); separate reclaimed-payload MinIO histories verify
actual remote reads. The 114/132 raw benchmark ledger and all failures are
preserved; 107/107 selected matched checks are not a performance certificate.
Many-stream backlog, RSS growth and tail-latency regressions remain open.

Licenses/provenance, full staged scope, source identities, public links and
whitespace are checked separately. The full staged whitespace check failed:
550 warnings in 13 files: ten original perf transcripts, two build transcripts
and the retained mutation patch.
These generated artifacts retain their original bytes; authored source/docs
must pass the separate whitespace check. This is a reviewed formatting
exception, not a passing full-tree whitespace check. Vendor/source manifests
and dependency locks did not change across this publication range. Current
workflows run on main pushes or pull requests, not this experimental
feature-branch push; remote CI state must still be inspected after publication.

The first closure run stopped before scanning/publishing because its expected
warning total was mis-added as 650 rather than the actual 550; the failure is
retained. The second run compares the full raw check output byte-for-byte and
records the actual exit code, in addition to verifying all staged file bytes.

SSH signing remains configured but its key path is absent and no agent is
available. The user's exception permits one command-scoped unsigned local
checkpoint with canonical human author/committer; it does not change defaults
or grant a standing signing bypass. The remote SHA is verified after an
ordinary push with tags disabled. Post-commit/push observations are reported
in the thread rather than inventing future successful delivery in this file.

No raw databases/WALs, provider credentials, executables or process-memory
captures are included. Finite scans do not prove absence under arbitrary secret
encodings. Local MinIO does not establish independent-host/provider/power-loss
durability. See the main experiment ledger for remaining acceptance gates.
