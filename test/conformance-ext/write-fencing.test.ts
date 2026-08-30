// Black-box conformance for the Write Fencing extension (docs/spec/WRITE-FENCING.md,
// design §G.1 / §H.3): WF-01…WF-28 pin each MUST of the extension text against a
// live enforce-mode chronicle; NC-01…NC-04 are the negative controls that pin what
// the extension must NOT change about the base protocol. One test() per id.
//
// Fault sensitivity (design §H.3): under `-tags fence_fault_nobind` WF-16 must
// fail (the producer binding is never written); under `-tags fence_fault_noseal`
// WF-19 must fail (done tombstones but records no seal, so HEAD shows none).
import http from "node:http"
import type { AddressInfo } from "node:net"
import { expect, test } from "vitest"
import {
  BASE,
  INSECURE_BASE,
  ack,
  claim,
  createStream,
  createSub,
  deleteSub,
  done,
  head,
  post,
  producer,
  pullWakeSub,
  sleep,
  subId,
  svc,
  tail,
  uniq,
} from "./client.ts"

// -- §3 Creating a fenced stream -------------------------------------------

test("WF-01 Write-Fence is part of the idempotent-create comparison", async () => {
  const path = uniq("wf01")
  expect((await createStream(path, { "Write-Fence": "true" })).status).toBe(201)
  expect((await createStream(path, { "Write-Fence": "true" })).status).toBe(200)
  // Re-PUT without the header disagrees with the stored config.
  expect((await createStream(path)).status).toBe(409)
  // And the other order: a plain stream refuses a fenced re-PUT.
  const plain = uniq("wf01-plain")
  expect((await createStream(plain)).status).toBe(201)
  expect((await createStream(plain, { "Write-Fence": "true" })).status).toBe(409)
})

test("WF-02 Write-Fence is echoed on PUT 201/200 and HEAD", async () => {
  const path = uniq("wf02")
  const created = await createStream(path, { "Write-Fence": "true" })
  expect([created.status, created.headers.get("Write-Fence")]).toEqual([201, "true"])
  const matched = await createStream(path, { "Write-Fence": "true" })
  expect([matched.status, matched.headers.get("Write-Fence")]).toEqual([200, "true"])
  expect((await head(path)).headers.get("Write-Fence")).toBe("true")
  const plain = uniq("wf02-plain")
  await createStream(plain)
  expect((await head(plain)).headers.get("Write-Fence")).toBeNull()
})

test("WF-03 a fork does not inherit the fence", async () => {
  const parent = uniq("wf03")
  await createStream(parent, { "Write-Fence": "true" })
  const child = uniq("wf03-fork")
  const forked = await createStream(child, { "Stream-Forked-From": parent })
  expect(forked.status).toBe(201)
  expect(forked.headers.get("Write-Fence")).toBeNull()
  expect((await head(child)).headers.get("Write-Fence")).toBeNull()
})

test("WF-04 a server honoring the opt-in creates the stream fenced (501 reserved for stores without the capability)", async () => {
  // The 501 arm cannot be reached black-box on a fence-capable server; it is
  // pinned server-side (TestHandleCreateWriteFence). Here: the capability is
  // present, so the opt-in MUST be honored, never silently dropped.
  const path = uniq("wf04")
  const res = await createStream(path, { "Write-Fence": "true" })
  expect(res.status).toBe(201)
  expect(res.headers.get("Write-Fence")).toBe("true")
})

// -- §4 The write token -----------------------------------------------------

test("WF-05 the claim's write token writes the fenced class", async () => {
  const path = uniq("wf05")
  await createStream(path, { "Write-Fence": "true" })
  const cr = await pullWakeSub(subId("s"), path)
  const ok = await post(path, producer(cr.write_token!, "entity-wf05", cr.generation, 0))
  expect(ok.status).toBe(200)
  expect(ok.headers.get("Producer-Epoch")).toBe(String(cr.generation))
  expect(ok.headers.get("Stream-Next-Offset")).toBeTruthy()
  // The idempotent retry of the accepted tuple is a duplicate, not a rewrite.
  const dup = await post(path, producer(cr.write_token!, "entity-wf05", cr.generation, 0))
  expect(dup.status).toBe(204)
})

test("WF-06 a write token is scoped to exact paths: wrong path is 403", async () => {
  const linked = uniq("wf06-a")
  const other = uniq("wf06-b")
  await createStream(linked, { "Write-Fence": "true" })
  await createStream(other, { "Write-Fence": "true" })
  const cr = await pullWakeSub(subId("s"), linked)
  const before = await tail(other)
  const res = await post(other, producer(cr.write_token!, "entity-wf06", cr.generation, 0))
  expect(res.status).toBe(403)
  expect(res.json().error.code).toBe("FORBIDDEN")
  expect(await tail(other)).toBe(before)
})

test("WF-07 an expired write token is 401", async () => {
  const path = uniq("wf07")
  await createStream(path, { "Write-Fence": "true" })
  const cr = await pullWakeSub(subId("s"), path, 1000) // token TTL = lease + 5s
  // exp has unix-second granularity and expiry is strict (now > exp), so
  // clear the 6s TTL by a full 2s to be phase-independent.
  await sleep(8000)
  const before = await tail(path)
  const res = await post(path, producer(cr.write_token!, "entity-wf07", cr.generation, 0))
  expect(res.status).toBe(401)
  expect(res.json().error.code).toBe("UNAUTHENTICATED")
  expect(await tail(path)).toBe(before)
}, 20_000)

test("WF-08 the token works the instant the claim returns (fence installed before mint)", async () => {
  const path = uniq("wf08")
  await createStream(path, { "Write-Fence": "true" })
  const cr = await pullWakeSub(subId("s"), path)
  // No retry loop: the very first append under the fresh token must land,
  // which is only possible if the stream-side marker preceded the mint.
  const res = await post(path, producer(cr.write_token!, "entity-wf08", cr.generation, 0))
  expect(res.status).toBe(200)
})

// -- §5 Write classes -------------------------------------------------------

test("WF-09 asserting the fenced class without a token is 401 on every stream", async () => {
  const fenced = uniq("wf09")
  const plain = uniq("wf09-plain")
  await createStream(fenced, { "Write-Fence": "true" })
  await createStream(plain)
  for (const path of [fenced, plain]) {
    const res = await post(path, {
      ...svc,
      "Write-Fence": "true",
      "Producer-Id": "entity-wf09",
      "Producer-Epoch": "1",
      "Producer-Seq": "0",
    })
    expect(res.status).toBe(401)
    expect(res.json().error.code).toBe("UNAUTHENTICATED")
  }
})

test("WF-10 a fenced write without the producer triple is 400", async () => {
  const path = uniq("wf10")
  await createStream(path, { "Write-Fence": "true" })
  const cr = await pullWakeSub(subId("s"), path)
  const before = await tail(path)
  const res = await post(path, { "Write-Token": cr.write_token! })
  expect(res.status).toBe(400)
  expect(res.text).toContain("fenced write requires Producer-Id, Producer-Epoch, and Producer-Seq")
  expect(await tail(path)).toBe(before)
})

test("WF-11 Producer-Epoch must equal the claim generation", async () => {
  const path = uniq("wf11")
  await createStream(path, { "Write-Fence": "true" })
  const cr = await pullWakeSub(subId("s"), path)
  // Positive control: epoch == generation lands.
  expect((await post(path, producer(cr.write_token!, "entity-wf11", cr.generation, 0))).status).toBe(200)
  const before = await tail(path)
  const res = await post(path, producer(cr.write_token!, "entity-wf11", cr.generation + 1, 0))
  expect(res.status).toBe(409)
  expect(res.json().error.reason).toBe("epoch")
  expect(await tail(path)).toBe(before)
})

test("WF-12 a deposed claim's token is rejected atomically with the append", async () => {
  const path = uniq("wf12")
  await createStream(path, { "Write-Fence": "true" })
  const sub = subId("s")
  const crA = await pullWakeSub(sub, path, 1000)
  expect((await post(path, producer(crA.write_token!, "entity-wf12", crA.generation, 0))).status).toBe(200)
  await sleep(1200) // the 1s lease lapses; a successor takes over
  const crB = await claim(sub, "worker-B")
  expect(crB.generation).toBeGreaterThan(crA.generation)
  const before = await tail(path)
  const res = await post(path, producer(crA.write_token!, "entity-wf12", crA.generation, 1))
  expect(res.status).toBe(409)
  expect(res.json().error.code).toBe("FENCED")
  expect(await tail(path)).toBe(before)
}, 20_000)

test("WF-13 a lapsed lease fences even with no successor", async () => {
  const path = uniq("wf13")
  await createStream(path, { "Write-Fence": "true" })
  const cr = await pullWakeSub(subId("s"), path, 1000)
  expect((await post(path, producer(cr.write_token!, "entity-wf13", cr.generation, 0))).status).toBe(200)
  await sleep(1400) // past the lease, well inside the token's own TTL
  const before = await tail(path)
  const res = await post(path, producer(cr.write_token!, "entity-wf13", cr.generation, 1))
  expect(res.status).toBe(409)
  expect(res.json().error.code).toBe("FENCED")
  expect(await tail(path)).toBe(before)
}, 20_000)

test("WF-14 an open write to a fenced stream requires an authenticated principal", async () => {
  const path = uniq("wf14")
  await createStream(path, { "Write-Fence": "true" })
  // Positive control: the service principal's open write lands.
  expect((await post(path, { ...svc })).status).toBe(204)
  const before = await tail(path)
  const res = await post(path, {})
  expect(res.status).toBe(401)
  // The anonymous denial is the base phase-1 401 (today's plain envelope,
  // pinned by TestHandleAppendOpenClassOnFencedStream): the principal rule is
  // implementation-defined per WRITE-FENCING.md §5, so only the status and
  // the unchanged tail are extension MUSTs here.
  expect(res.json().error.code).toBe("UNAUTHENTICATED")
  expect(await tail(path)).toBe(before)
})

test("WF-15 an entity-identity (wake) token cannot write a fenced stream", async () => {
  const path = uniq("wf15")
  await createStream(path, { "Write-Fence": "true" })
  const cr = await pullWakeSub(subId("s"), path)
  expect(cr.wake_token, "single-link claim must mint a wake_token").toBeTruthy()
  const before = await tail(path)
  const res = await post(path, { Authorization: `Bearer ${cr.wake_token}` })
  expect(res.status).toBe(403)
  expect(res.json().error.reason).toBe("wake_token")
  expect(await tail(path)).toBe(before)
})

// -- §6 Bound producers -----------------------------------------------------

test("WF-16 an open write naming a bound producer id is 409 bound", async () => {
  const path = uniq("wf16")
  await createStream(path, { "Write-Fence": "true" })
  const cr = await pullWakeSub(subId("s"), path)
  // The fenced write binds its producer id in the stream slot.
  expect((await post(path, producer(cr.write_token!, "entity-wf16", cr.generation, 0))).status).toBe(200)
  const before = await tail(path)
  // The zombie shape: the same producer id, a bumped epoch, no token — under
  // a fully authorized service principal.
  const res = await post(path, {
    ...svc,
    "Producer-Id": "entity-wf16",
    "Producer-Epoch": String(cr.generation + 1),
    "Producer-Seq": "0",
  })
  expect(res.status).toBe(409)
  expect(res.json().error.reason).toBe("bound")
  expect(res.json().error.generation).toBe(cr.generation)
  expect(await tail(path)).toBe(before)
})

test("WF-17 an open duplicate of an accepted fenced tuple is 409 bound, not a dedupe", async () => {
  const path = uniq("wf17")
  await createStream(path, { "Write-Fence": "true" })
  const cr = await pullWakeSub(subId("s"), path)
  expect((await post(path, producer(cr.write_token!, "entity-wf17", cr.generation, 0))).status).toBe(200)
  const before = await tail(path)
  const res = await post(path, {
    ...svc,
    "Producer-Id": "entity-wf17",
    "Producer-Epoch": String(cr.generation),
    "Producer-Seq": "0",
  })
  expect(res.status).toBe(409)
  expect(res.json().error.reason).toBe("bound")
  expect(await tail(path)).toBe(before)
})

test("WF-18 an unbound producer keeps the base §5.2.1 state machine on the open class", async () => {
  const path = uniq("wf18")
  await createStream(path, { "Write-Fence": "true" })
  const cr = await pullWakeSub(subId("s"), path)
  expect((await post(path, producer(cr.write_token!, "entity-wf18", cr.generation, 0))).status).toBe(200)
  // A different producer id establishes its epoch and auto-claims a bump.
  const first = await post(path, { ...svc, "Producer-Id": "wake-reg-7", "Producer-Epoch": "1", "Producer-Seq": "0" })
  expect(first.status).toBe(200)
  expect(first.headers.get("Producer-Epoch")).toBe("1")
  const bumped = await post(path, { ...svc, "Producer-Id": "wake-reg-7", "Producer-Epoch": "2", "Producer-Seq": "0" })
  expect(bumped.status).toBe(200)
  expect(bumped.headers.get("Producer-Epoch")).toBe("2")
})

// -- §7 Seal ----------------------------------------------------------------

test("WF-19 done closes the generation: the seal is recorded and the old token refused", async () => {
  const path = uniq("wf19")
  await createStream(path, { "Write-Fence": "true" })
  const sub = subId("s")
  const cr = await pullWakeSub(sub, path)
  expect((await post(path, producer(cr.write_token!, "entity-wf19", cr.generation, 0))).status).toBe(200)
  const sealedTail = await tail(path)
  expect((await done(sub, cr, path)).status).toBe(200)
  // The durable seal record — not merely the claim's end — is what outlives
  // marker retention (fence_fault_noseal must make this line fail).
  expect((await head(path)).headers.get("Write-Fence-Sealed-Generation")).toBe(String(cr.generation))
  const res = await post(path, producer(cr.write_token!, "entity-wf19", cr.generation, 1))
  expect(res.status).toBe(409)
  expect(res.json().error.code).toBe("FENCED")
  expect(await tail(path)).toBe(sealedTail)
})

test("WF-20 HEAD exposes the sealed generation and offset", async () => {
  const path = uniq("wf20")
  await createStream(path, { "Write-Fence": "true" })
  const sub = subId("s")
  const cr = await pullWakeSub(sub, path)
  const ok = await post(path, producer(cr.write_token!, "entity-wf20", cr.generation, 0))
  expect(ok.status).toBe(200)
  const fencedOffset = ok.headers.get("Stream-Next-Offset")
  expect((await done(sub, cr, path)).status).toBe(200)
  const h = await head(path)
  expect(h.headers.get("Write-Fence-Sealed-Generation")).toBe(String(cr.generation))
  expect(h.headers.get("Write-Fence-Sealed-Offset")).toBe(fencedOffset)
})

test("WF-21 a successor's claim seals the predecessor at its last fenced offset", async () => {
  const path = uniq("wf21")
  await createStream(path, { "Write-Fence": "true" })
  const sub = subId("s")
  const crA = await pullWakeSub(sub, path, 1000)
  const fenced = await post(path, producer(crA.write_token!, "entity-wf21", crA.generation, 0))
  expect(fenced.status).toBe(200)
  const fencedOffset = fenced.headers.get("Stream-Next-Offset")
  // A later open-class write moves the tail past the fenced offset.
  expect((await post(path, { ...svc })).status).toBe(204)
  expect(await tail(path)).not.toBe(fencedOffset)
  await sleep(1200)
  const crB = await claim(sub, "worker-B")
  expect(crB.generation).toBeGreaterThan(crA.generation)
  const h = await head(path)
  expect(h.headers.get("Write-Fence-Sealed-Generation")).toBe(String(crA.generation))
  // R3.3: the seal records the fenced class's last offset, never the open tail.
  expect(h.headers.get("Write-Fence-Sealed-Offset")).toBe(fencedOffset)
  expect((await post(path, producer(crB.write_token!, "entity-wf21", crB.generation, 0))).status).toBe(200)
}, 20_000)

test("WF-22 a redelivered done is idempotent in effect: the seal is unchanged and the subscription reclaimable", async () => {
  const path = uniq("wf22")
  await createStream(path, { "Write-Fence": "true" })
  const sub = subId("s")
  const cr = await pullWakeSub(sub, path)
  expect((await post(path, producer(cr.write_token!, "entity-wf22", cr.generation, 0))).status).toBe(200)
  const doneBody = {
    wake_id: cr.wake_id,
    generation: cr.generation,
    acks: [{ stream: path, offset: await tail(path) }],
    done: true,
  }
  expect((await ack(sub, cr.token, doneBody)).status).toBe(200)
  const sealed = (await head(path)).headers.get("Write-Fence-Sealed-Generation")
  // The base §7 stale-wake fence answers the wire redelivery (the wake is
  // gone); what MUST hold is that nothing re-seals or unseals, and the
  // subscription stays claimable. (The mid-crash redelivery that answers 200
  // is pinned server-side: TestDoneSealCrashWindowFailsClosed.)
  const redelivered = await ack(sub, cr.token, doneBody)
  expect(redelivered.status).toBe(409)
  expect((await head(path)).headers.get("Write-Fence-Sealed-Generation")).toBe(sealed)
  const next = await claim(sub, "worker-A")
  expect(next.generation).toBeGreaterThan(cr.generation)
  // The next generation is a fresh epoch: its first write re-establishes at seq 0.
  expect((await post(path, producer(next.write_token!, "entity-wf22", next.generation, 0))).status).toBe(200)
})

test("WF-23 the seal is per authority: a recreated subscription starts unsealed", async () => {
  const path = uniq("wf23")
  await createStream(path, { "Write-Fence": "true" })
  const sub = subId("s")
  const cr1 = await pullWakeSub(sub, path)
  expect((await post(path, producer(cr1.write_token!, "entity-wf23", cr1.generation, 0))).status).toBe(200)
  expect((await done(sub, cr1, path)).status).toBe(200)
  expect((await deleteSub(sub)).status).toBe(204)
  // The recreated subscription is a new authority with an empty seal
  // namespace: its claim is granted and its holder writes at the new
  // generation. (A fresh producer id — the old id's higher stored epoch is
  // the documented K.9 stale-epoch limitation, pinned server-side.)
  const cr2 = await pullWakeSub(sub, path)
  const res = await post(path, producer(cr2.write_token!, "entity-wf23-v2", cr2.generation, 0))
  expect(res.status).toBe(200)
})

// -- §8 Rejection disclosure ------------------------------------------------

test("WF-24 the 409 envelope names the reason, generation, and holder", async () => {
  const path = uniq("wf24")
  await createStream(path, { "Write-Fence": "true" })
  const cr = await pullWakeSub(subId("s"), path)
  const res = await post(path, producer(cr.write_token!, "entity-wf24", cr.generation + 1, 0))
  expect(res.status).toBe(409)
  const detail = res.json().error
  expect(detail.code).toBe("FENCED")
  expect(detail.reason).toBe("epoch")
  expect(detail.generation).toBe(cr.generation)
  expect(detail.current_holder).toBe("worker-A")
})

test("WF-25 a producer-headed 409 carries the epoch echo and the terminal gap pair", async () => {
  const path = uniq("wf25")
  await createStream(path, { "Write-Fence": "true" })
  const cr = await pullWakeSub(subId("s"), path)
  const res = await post(path, producer(cr.write_token!, "entity-wf25", cr.generation + 1, 3))
  expect(res.status).toBe(409)
  expect(res.headers.get("Producer-Epoch")).toBe(String(cr.generation))
  // Expected == Received == the request's seq: base-impossible, and the
  // pinned Electric producer's clean stop (fence_fault_nopair removes it).
  expect(res.headers.get("Producer-Expected-Seq")).toBe("3")
  expect(res.headers.get("Producer-Received-Seq")).toBe("3")
  expect(res.headers.get("Stream-Next-Offset")).toBeNull()
})

test("WF-26 the pre-credential 401 discloses nothing", async () => {
  const path = uniq("wf26")
  await createStream(path, { "Write-Fence": "true" })
  const res = await post(path, {
    ...svc,
    "Write-Fence": "true",
    "Producer-Id": "entity-wf26",
    "Producer-Epoch": "8",
    "Producer-Seq": "0",
  })
  expect(res.status).toBe(401)
  expect(res.headers.get("WWW-Authenticate")).toContain("Bearer")
  expect(res.headers.get("Producer-Epoch")).toBeNull()
  expect(res.headers.get("Producer-Expected-Seq")).toBeNull()
  expect(res.headers.get("Producer-Received-Seq")).toBeNull()
  const detail = res.json().error
  expect(detail.generation).toBeUndefined()
  expect(detail.current_holder).toBeUndefined()
})

// -- §9 Subscription delivery additions ------------------------------------

test("WF-27 webhook parity end to end: the notification's write_token writes, done seals", async () => {
  const path = uniq("wf27")
  await createStream(path, { "Write-Fence": "true" })
  const sub = subId("s")

  let resolveNotif!: (n: any) => void
  const notifP = new Promise<any>((resolve) => {
    resolveNotif = resolve
  })
  const receiver = http.createServer((req, res) => {
    let body = ""
    req.on("data", (c) => (body += c))
    req.on("end", () => {
      res.writeHead(200, { "Content-Type": "application/json" })
      res.end("{}")
      resolveNotif(JSON.parse(body))
    })
  })
  await new Promise<void>((resolve) => receiver.listen(0, "127.0.0.1", resolve))
  try {
    const port = (receiver.address() as AddressInfo).port
    const created = await createSub(sub, {
      type: "webhook",
      streams: [path],
      webhook: { url: `http://127.0.0.1:${port}/wake` },
      lease_ttl_ms: 30_000,
    })
    expect(created.status).toBe(201)
    // Pending work wakes the subscription and delivers the notification.
    expect((await post(path, { ...svc })).status).toBe(204)
    const n = await notifP
    expect(n.write_token, "WakeNotification must carry write_token").toBeTruthy()
    expect((await post(path, producer(n.write_token, "entity-wf27", n.generation, 0))).status).toBe(200)
    const sealedTail = await tail(path)
    const doneRes = await ack(sub, n.callback_token, {
      wake_id: n.wake_id,
      generation: n.generation,
      acks: [{ stream: path, offset: sealedTail }],
      done: true,
    })
    expect(doneRes.status).toBe(200)
    expect((await head(path)).headers.get("Write-Fence-Sealed-Generation")).toBe(String(n.generation))
    const late = await post(path, producer(n.write_token, "entity-wf27", n.generation, 1))
    expect(late.status).toBe(409)
    expect(await tail(path)).toBe(sealedTail)
  } finally {
    receiver.close()
  }
})

test("WF-28 pull-wake parity end to end: heartbeat refresh, done seals", async () => {
  const path = uniq("wf28")
  await createStream(path, { "Write-Fence": "true" })
  const sub = subId("s")
  const cr = await pullWakeSub(sub, path)
  expect((await post(path, producer(cr.write_token!, "entity-wf28", cr.generation, 0))).status).toBe(200)
  const hb = await ack(sub, cr.token, { wake_id: cr.wake_id, generation: cr.generation })
  expect(hb.status).toBe(200)
  const refreshed = hb.json().write_token
  expect(refreshed, "a non-done ack must re-mint the write_token").toBeTruthy()
  expect(refreshed).not.toBe(cr.write_token)
  expect((await post(path, producer(refreshed, "entity-wf28", cr.generation, 1))).status).toBe(200)
  const sealedTail = await tail(path)
  expect((await done(sub, cr, path)).status).toBe(200)
  expect((await head(path)).headers.get("Write-Fence-Sealed-Generation")).toBe(String(cr.generation))
  for (const token of [cr.write_token!, refreshed]) {
    const res = await post(path, producer(token, "entity-wf28", cr.generation, 2))
    expect(res.status).toBe(409)
  }
  expect(await tail(path)).toBe(sealedTail)
})

// -- Negative controls (base behavior the extension must not change) --------

test("NC-01 an unfenced stream keeps anonymous §5.2.1 auto-claim (epoch 5 then 6)", async () => {
  // Anonymous writes need today's open posture, so this control drives the
  // second, insecure-mode server the harness starts.
  const path = uniq("nc01")
  expect((await createStream(path, {}, INSECURE_BASE)).status).toBe(201)
  const five = await post(path, { "Producer-Id": "p-nc01", "Producer-Epoch": "5", "Producer-Seq": "0" }, `{"n":1}`, INSECURE_BASE)
  expect(five.status).toBe(200)
  expect(five.headers.get("Producer-Epoch")).toBe("5")
  const six = await post(path, { "Producer-Id": "p-nc01", "Producer-Epoch": "6", "Producer-Seq": "0" }, `{"n":2}`, INSECURE_BASE)
  expect(six.status).toBe(200)
  expect(six.headers.get("Producer-Epoch")).toBe("6")
})

test("NC-02 Write-Fence: true on an unfenced stream without a token is 401", async () => {
  const path = uniq("nc02")
  await createStream(path)
  const res = await post(path, { "Write-Fence": "true" })
  expect(res.status).toBe(401)
  expect(res.json().error.code).toBe("UNAUTHENTICATED")
})

test("NC-03 the done-ack body keeps the base {ok,next_wake} shape on a fenced-stream subscription", async () => {
  const path = uniq("nc03")
  await createStream(path, { "Write-Fence": "true" })
  const sub = subId("s")
  const cr = await pullWakeSub(sub, path)
  expect((await post(path, producer(cr.write_token!, "entity-nc03", cr.generation, 0))).status).toBe(200)
  const res = await done(sub, cr, path)
  expect(res.status).toBe(200)
  expect(res.text.trim()).toBe(`{"ok":true,"next_wake":false}`)
})

test("NC-04 insecure mode keeps today's posture on unfenced streams: an invalid bearer appends", async () => {
  const path = uniq("nc04")
  expect((await createStream(path, {}, INSECURE_BASE)).status).toBe(201)
  const res = await post(path, { Authorization: "Bearer not-a-real-credential" }, `{"n":1}`, INSECURE_BASE)
  expect(res.status).toBe(204)
})
