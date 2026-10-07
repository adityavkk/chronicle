# Invalid performance qualification: disk exhaustion

The first `raft3-local` cell hit ENOSPC while writing `samples.jsonl`. Its
lossless gzip archive retains a truncated final JSON row; `analyzer-failure.txt`
records the unchanged analyzer rejecting it with exit status 1. Do not discard
the row or treat this matrix as a valid performance comparison.

The raw driver records nine passing cells and three failing async cells. Two
async cells fail the zero-backpressure assertion; the first fails resource
collection. All accepted-byte drain observations remain in their original files.
Neither successful draining nor the other cells' assertions repair the missing
resource evidence or establish an environment-independent performance result.

`disposable-data-cleanup.json` lists retired, disposable lab data removed after
the run. All histories, configurations, logs and failed samples were retained,
as were the subscription labs needed to revalidate the publication audit. The
experiment's disposable debug build cache was also cleaned. A subsequent harness
guard requires 8 GiB free before starting each cell; this is local measurement
headroom, not a server disk requirement or a guarantee against future ENOSPC.

The fresh retry is `async-writes-006`; it does not overwrite this failed run.
