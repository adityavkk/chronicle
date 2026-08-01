# Retained benchmark report metadata

This directory keeps the reports and top-level provenance records cited by the
configuration research. It does not include the general raw campaign corpus.

Each dated directory retains the report, manifest, image record, validation,
teardown proof, and original evidence checksum index when those files exist.
The checksum index records the omitted raw files and their digests, but
`verify-seal` requires the full external archive and does not pass against this
small documentation extract.

The per-client fanout measurements use a separate minimal raw evidence set under
`docs/benchmarks/sse-fanout/20260729-local-kubernetes/`.
