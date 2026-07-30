# ADR 0004: Bounded read pages and snapshot catch-up

- Status: Accepted
- Date: 2026-07-29
- Issue: GPA/chronicle#5

## Context

Before this decision, the Redis read script called `ZRANGEBYLEX` from the
requested offset to the end of the stream. Redis had to build the complete
suffix before Chronicle could apply the HTTP `limit` parameter. Chronicle then
built another complete response body before it wrote to the client.

Redis runs a Lua script atomically on one event loop. An unbounded suffix can
therefore consume both server memory and time while unrelated writes wait.
Handler-only pagination does not reduce that work.

Chronicle has two storage implementations. `MemoryStore` is also the behavioral
oracle for Redis differential tests. Other packages can implement `store.Store`,
so changing the existing interface would break source compatibility.

Fork reads add one constraint. A fork response can contain an inherited prefix
from one or more source streams before it contains the fork's own suffix.
Chronicle must page each source range without touching the source stream's
sliding TTL.

## Decision

We will add `store.PageReader` as an optional capability beside `store.Store`.
Both maintained backends will implement it. Existing `store.Store`
implementations will continue to compile. The HTTP handler will use bounded
pages when the backend implements `PageReader`. Its compatibility path will
stream the messages returned by `Store.Read` without building another body.

`ReadPage` will accept a context, a start offset, a byte target, a frame cap, and
an optional snapshot. The first call will capture the root stream's:

- content type;
- tail offset;
- closed state;
- incarnation token.

The result will contain those snapshot fields, the messages in the page, the
next offset, whether the page reached the snapshot tail, and page statistics.
The statistics will include bytes fetched from storage, bytes returned,
discarded bytes, and Redis script time.

Every later call for the same response will carry the first call's snapshot.
The backend will reject a different incarnation. It will never read a frame
whose end offset is above the captured tail.

Every page will renew the root stream's sliding TTL while the response is
active. Reading an inherited range will not renew the source stream's TTL.

Redis will fetch the first candidate alone. If it fits, Lua will derive a
lexicographic end-offset bound from the remaining byte budget and bulk-fetch
only frames whose cumulative end offsets fit that budget. It will not issue a
lookahead query. The common one-frame refresh therefore uses one
`ZRANGEBYLEX`; a multi-frame segment uses at most one additional range call.
The complete page will also stop at 1,024 frames.

The 1 MiB target therefore bounds the returned whole-frame prefix. For a normal
aligned segment, every fetched candidate is returned, so the same target bounds
candidate bytes except when the first indivisible frame is larger than 1 MiB.
At a fork boundary, one non-fitting first candidate can be inspected before the
page stops. The 1,024-frame cap independently bounds small-frame work. One
frame can still exceed 1 MiB because Chronicle cannot split a stored frame and
append bodies have no size ceiling.

Fork paging will build an ordered list of source ranges. Each range has an
exclusive lower offset and an inclusive upper offset. Chronicle will read the
oldest inherited range first, then each child range, then the root stream's own
range. Every Redis range lookup will use the same byte and frame bounds. Source
lookups will not refresh source TTLs or hide a source that is retained only for
a fork.

The HTTP handler will capture the snapshot before it sends response headers. It
can therefore set `Stream-Next-Offset`, `Stream-Up-To-Date`, and
`Stream-Closed` for that snapshot. It will then write binary bytes, JSON array
records, or envelope records directly to `http.ResponseWriter` as pages arrive.
It will check request cancellation before storage work, between pages, and
after writes and flushes.

SSE catch-up will use the same page operation and snapshot. It will emit one
data event and one control event per page. After it sends the control event for
the snapshot tail, it will resume the existing wait and live read loop with the
exact offset already sent.

The operator policy is one returned payload target for all catch-up pages. The
default is 1 MiB. The server will expose it as
`CHRONICLE_READ_PAGE_BYTES` and `-read-page-bytes`. The frame cap is an
internal safety bound because it does not represent an operator policy.

## Consequences

Redis will no longer materialize an unbounded suffix. The first range call
materializes one frame. The bulk range is limited by both the remaining
end-offset byte budget and the remaining part of the 1,024-frame cap. A final
lookahead call is not made. Candidate bytes can exceed 1 MiB only through the
first indivisible oversized frame, or through one non-fitting first candidate
when a page crosses a fork segment boundary. Hundreds of oversized frames
cannot be materialized speculatively. A response can still contain the complete
suffix that existed at snapshot capture, but Chronicle will retain only one
returned storage page at a time.

The 1 MiB default reduces the returned raw page payload held for 512 readers
from the old 16 MiB per reader shape of 8 GiB to about 512 MiB before copies,
except when a single stored frame exceeds 1 MiB. An earlier 512-reader branch
run sampled 29,436 MiB of candidate payload, returned 8,127 MiB, and discarded
21,309 MiB. That gap exposed the weakness of fetching 1,024 candidates before
applying the byte target and led to the first-candidate and end-offset bound
above. Those figures describe the rejected candidate-fetch strategy, not the
final one. The page size comparison
and metric method are recorded in
[`issue-5-bounded-catchup.md`](../benchmarks/issue-5-bounded-catchup.md).

Small pages add Redis round trips and JSON punctuation writes. The frame cap can
also stop a page below its byte target when records are small. Large pages use
more process memory and hold the Redis event loop longer.

The optional capability preserves source compatibility, but an external backend
that implements only `store.Store` cannot provide the Redis-side bound. The
handler will still avoid the second body buffer for that backend. Operators who
need bounded storage work must use a maintained backend or implement
`store.PageReader`.

An interrupted socket can contain a prefix of a frame because TCP writes cannot
be rolled back. Chronicle will never start a later frame or storage page after
it observes cancellation. SSE clients can resume from the last completed
control offset. Non-live HTTP clients must retry an interrupted response from
their last confirmed offset.

## Alternatives considered

Adding `ReadPage` directly to `store.Store` would make the capability mandatory,
but it would break every external implementation at compile time. We rejected
that change because the same result is available through an optional interface.

Using only `ZRANGEBYLEX LIMIT` with a frame count would bound the number of
records but could materialize many large records in one page. We retained the
frame cap and added a returned payload target to the range selection inside
Lua.

Reading the complete suffix and slicing it in Go would leave Redis work and
memory unbounded. It would not meet the issue.

Moving payloads to chunked strings would allow exact byte range reads for large
binary appends. That is a larger storage migration. Immutable segments and a
new data layout belong to issue #6.
