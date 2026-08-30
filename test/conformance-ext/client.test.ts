// The pinned-client control for the terminal gap pair (design §H.3, §J.3,
// U§5.18 #148): drive the exact producer Electric ships —
// @durable-streams/client@0.2.6 IdempotentProducer with autoClaim: true —
// against a fenced stream across a takeover, and assert it stands down on the
// FIRST rejected batch. The 409 FENCED reply's Producer-Expected-Seq ==
// Producer-Received-Seq == <request seq> makes the producer throw
// SequenceGapError to onError — the runtime's WRITE_FAILED — instead of
// looping on autoClaim retries or waiting on unresolved seqs.
//
// Fault sensitivity: under `-tags fence_fault_nopair` the server omits the
// pair and the producer re-sends without bound instead of standing down; this
// test MUST then fail — either at the 5 s WRITE_FAILED deadline or at the
// SequenceGapError assertion when the retry loop only ends by exhausting the
// token's own TTL (a credential FetchError, not a clean stop).
import { DurableStream, IdempotentProducer, SequenceGapError } from "@durable-streams/client"
import { expect, test } from "vitest"
import { claim, createStream, producer as fencedHeaders, post, pullWakeSub, sleep, streamUrl, subId, tail, uniq } from "./client.ts"

test("pinned IdempotentProducer observes WRITE_FAILED within one batch after a takeover", async () => {
  const path = uniq("client")
  await createStream(path, { "Write-Fence": "true" })
  const sub = subId("s")
  const crA = await pullWakeSub(sub, path, 1000)

  let writeFailed!: (e: Error) => void
  const failed = new Promise<Error>((resolve) => {
    writeFailed = resolve
  })
  const stream = new DurableStream({
    url: streamUrl(path),
    contentType: "application/json",
    headers: { "Write-Token": crA.write_token! },
  })
  const prod = new IdempotentProducer(stream, `entity-${path}`, {
    epoch: crA.generation,
    autoClaim: true,
    lingerMs: 1,
    onError: (e) => writeFailed(e),
  })

  // Positive control: the holder's first batch (seq 0) lands fenced.
  prod.append(JSON.stringify({ n: 1 }))
  await prod.flush()
  const fencedTail = await tail(path)

  // Takeover: the 1 s lease lapses and worker-B claims the next generation.
  await sleep(1200)
  const crB = await claim(sub, "worker-B")
  expect(crB.generation).toBeGreaterThan(crA.generation)

  // The deposed producer's next batch (seq 1) must terminate in WRITE_FAILED
  // within one batch — a SequenceGapError from the terminal pair, not an
  // autoClaim epoch bump, an unbounded re-send, or a timeout.
  const started = Date.now()
  prod.append(JSON.stringify({ n: 2 }))
  await prod.flush().catch(() => {})
  const err = await Promise.race([
    failed,
    sleep(5000).then(() => {
      throw new Error("WRITE_FAILED not observed within 5s: the producer did not stand down")
    }),
  ])
  expect(Date.now() - started).toBeLessThan(5000)
  expect(err).toBeInstanceOf(SequenceGapError)

  // Standing down means nothing landed: the deposed batch never reached the
  // stream, under any epoch the autoClaim path might have tried.
  expect(await tail(path)).toBe(fencedTail)

  // The successor's write still lands: the fence deposed one writer, not the stream.
  expect((await post(path, fencedHeaders(crB.write_token!, `entity-${path}`, crB.generation, 0))).status).toBe(200)
}, 20_000)
