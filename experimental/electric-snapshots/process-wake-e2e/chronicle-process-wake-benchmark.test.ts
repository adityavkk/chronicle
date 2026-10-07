import { createHash, randomUUID } from 'node:crypto'
import { readFile } from 'node:fs/promises'
import { createConnection } from 'node:net'
import { afterAll, describe, expect, it } from 'vitest'
import { DurableStream } from '@durable-streams/client'
import { z } from 'zod'
import { startChronicleGate } from './chronicle-e2e-gate'
import { runtimeTest } from './runtime-dsl'
import type {
  EntityDefinition,
  StateCollectionProxy,
  WakeMetrics,
} from '../src/index'

const chronicleRoot = process.env.CHRONICLE_STREAM_ROOT
const chronicleRedisUrl = process.env.CHRONICLE_REDIS_URL
const tokenDir = process.env.CHRONICLE_TOKEN_DIR
const scenario = process.env.CHRONICLE_BENCH_SCENARIO as
  | `update-heavy`
  | `append-only`
  | `large-inbox`
  | undefined
const mode = process.env.CHRONICLE_BENCH_MODE as
  | `replay`
  | `snapshot-full-raw-replay`
  | undefined
const measuredSamples = Number(process.env.CHRONICLE_BENCH_SAMPLES ?? `1`)
const warmupSamples = Number(process.env.CHRONICLE_BENCH_WARMUPS ?? `1`)
const sampleIndexBase = Number(process.env.CHRONICLE_BENCH_INDEX_BASE ?? `0`)
const phase = process.env.CHRONICLE_BENCH_PHASE ?? `standalone`
const phaseOrder = process.env.CHRONICLE_BENCH_PHASE_ORDER ?? mode ?? `unknown`
const configured = Boolean(
  chronicleRoot && chronicleRedisUrl && tokenDir && scenario && mode
)
const suite = configured ? describe : describe.skip

const readToken = async (name: string): Promise<string> =>
  configured
    ? (await readFile(`${tokenDir}/.${name}-token`, `utf8`)).trim()
    : ``
const runtimeToken = await readToken(`writer`)
const publisherToken = await readToken(`publisher`)
const root = configured ? new URL(chronicleRoot!) : null
const gate = root ? await startChronicleGate(root) : null
const redisUrl = configured ? new URL(chronicleRedisUrl!) : null
if (
  redisUrl &&
  ![`127.0.0.1`, `localhost`, `::1`].includes(redisUrl.hostname)
) {
  throw new Error(`Refusing non-loopback Chronicle Redis URL`)
}

type Scenario = NonNullable<typeof scenario>

interface InputPayload {
  seq: number
  measure?: boolean
  data?: string
}

const counterSchema = z.object({
  key: z.string(),
  count: z.number().int(),
  lastRawCount: z.number().int(),
  lastRawSum: z.number().int(),
  lastRawIds: z.array(z.number().int()),
})
const itemSchema = z.object({ key: z.string(), seq: z.number().int() })
type Counter = z.infer<typeof counterSchema>
type Item = z.infer<typeof itemSchema>
interface HydratedObservation {
  main: Counter | null
  counters: Array<{ key: string; count: number }>
  items: Array<{ key: string; seq: number }>
}

const metricWaiters = new Map<string, Array<(metrics: WakeMetrics) => void>>()
const wakeCounts = new Map<string, number>()
const hydratedObservations = new Map<string, HydratedObservation>()
let nextWebhookHold:
  | {
      arrived: Promise<void>
      markArrived: () => void
      released: Promise<void>
      release: () => void
    }
  | undefined

function waitForWakeMetrics(
  entityUrl: string,
  timeoutMs: number
): Promise<WakeMetrics> {
  return new Promise((resolve, reject) => {
    const waiters = metricWaiters.get(entityUrl) ?? []
    const timer = setTimeout(() => {
      const index = waiters.indexOf(onMetrics)
      if (index >= 0) waiters.splice(index, 1)
      reject(new Error(`Timed out waiting for wake metrics for ${entityUrl}`))
    }, timeoutMs)
    const onMetrics = (metrics: WakeMetrics) => {
      clearTimeout(timer)
      resolve(metrics)
    }
    waiters.push(onMetrics)
    metricWaiters.set(entityUrl, waiters)
  })
}

function fnv1a32(value: string): number {
  let hash = 0x811c9dc5
  for (const byte of Buffer.from(value)) {
    hash ^= byte
    hash = Math.imul(hash, 0x01000193) >>> 0
  }
  return hash
}

async function readLeaseState(subscriptionId: string): Promise<{
  phase: string | null
  holder: string | null
  leaseUntilNs: string | null
}> {
  if (!redisUrl) throw new Error(`CHRONICLE_REDIS_URL is required`)
  if (redisUrl.username || redisUrl.password) {
    throw new Error(`Fixture Redis URL must not contain credentials`)
  }
  const slot = fnv1a32(subscriptionId) % 256
  const key = `ds:{__ds:${slot}}:sub:${subscriptionId}`
  const parts = [`HMGET`, key, `phase`, `holder`, `lease_until_ns`]
  const command = parts
    .map((part) => `$${Buffer.byteLength(part)}\r\n${part}\r\n`)
    .join(``)
  const payload = `*${parts.length}\r\n${command}`
  return new Promise((resolve, reject) => {
    const socket = createConnection({
      host: redisUrl.hostname,
      port: Number(redisUrl.port || `6379`),
    })
    let response = ``
    socket.setTimeout(5_000)
    socket.on(`connect`, () => socket.write(payload))
    socket.on(`data`, (chunk) => {
      response += chunk.toString(`utf8`)
      const values = parseRedisBulkArray(response)
      if (!values) return
      socket.end()
      resolve({
        phase: values[0] ?? null,
        holder: values[1] ?? null,
        leaseUntilNs: values[2] ?? null,
      })
    })
    socket.on(`timeout`, () =>
      socket.destroy(new Error(`Redis read timed out`))
    )
    socket.on(`error`, reject)
  })
}

function parseRedisBulkArray(response: string): Array<string | null> | null {
  const lines = response.split(`\r\n`)
  if (!lines[0]?.startsWith(`*`)) return null
  const count = Number(lines[0].slice(1))
  const values: Array<string | null> = []
  let index = 1
  for (let item = 0; item < count; item++) {
    const lengthLine = lines[index++]
    if (lengthLine === undefined) return null
    const length = Number(lengthLine.slice(1))
    if (length === -1) {
      values.push(null)
      continue
    }
    const value = lines[index++]
    if (value === undefined || Buffer.byteLength(value) !== length) return null
    values.push(value)
  }
  return values
}

function holdNextWebhook() {
  let markArrived!: () => void
  let release!: () => void
  const hold = {
    arrived: new Promise<void>((resolve) => {
      markArrived = resolve
    }),
    released: new Promise<void>((resolve) => {
      release = resolve
    }),
    markArrived,
    release,
  }
  nextWebhookHold = hold
  return hold
}

function inboxPayloads(events: Array<{ type: string; value?: unknown }>) {
  return events
    .filter((event) => event.type === `inbox`)
    .map((event) => (event.value as { payload?: unknown })?.payload)
    .filter(
      (payload): payload is InputPayload =>
        Boolean(payload) &&
        typeof payload === `object` &&
        Number.isInteger((payload as InputPayload).seq)
    )
}

function latestCounter(
  events: Array<{ type: string; key?: string; value?: unknown }>
): Counter | undefined {
  return events
    .filter((event) => event.type === `state:counters` && event.key === `main`)
    .at(-1)?.value as Counter | undefined
}

function sampleSize(selected: Scenario): number {
  switch (selected) {
    case `update-heavy`:
      return Number(process.env.CHRONICLE_BENCH_UPDATE_EVENTS ?? `500`)
    case `append-only`:
      return Number(process.env.CHRONICLE_BENCH_APPEND_EVENTS ?? `300`)
    case `large-inbox`:
      return Number(process.env.CHRONICLE_BENCH_INBOX_EVENTS ?? `100`)
  }
}

async function appendWorkloadHistory(
  stream: DurableStream,
  selected: Exclude<Scenario, `large-inbox`>,
  count: number
): Promise<void> {
  const events: Array<Record<string, unknown>> = []
  if (selected === `update-heavy`) {
    const keyCount = Math.min(16, count)
    for (let seq = 0; seq < count; seq++) {
      const key = `work-${seq % keyCount}`
      events.push({
        type: `state:counters`,
        key,
        value: {
          key,
          count: seq,
          lastRawCount: 0,
          lastRawSum: 0,
          lastRawIds: [],
        },
        headers: { operation: seq < keyCount ? `insert` : `update` },
      })
    }
  } else {
    for (let seq = 0; seq < count; seq++) {
      events.push({
        type: `state:items`,
        key: `history-item-${seq}`,
        value: { key: `history-item-${seq}`, seq },
        headers: { operation: `insert` },
      })
    }
  }
  await stream.append(events.map((event) => JSON.stringify(event)).join(`,`))
}

function normalizedWorkload(selected: Scenario, count: number) {
  const inputs =
    selected === `large-inbox`
      ? Array.from({ length: count + 1 }, (_, seq) => ({
          seq,
          measure: seq === count,
          dataBytes: 8192,
        }))
      : [
          { seq: 0, measure: false, dataBytes: 0 },
          { seq: count, measure: true, dataBytes: 0 },
        ]
  const stateEvents =
    selected === `update-heavy`
      ? Array.from({ length: count }, (_, seq) => ({
          operation: seq < Math.min(16, count) ? `insert` : `update`,
          key: `work-${seq % Math.min(16, count)}`,
          seq,
        }))
      : selected === `append-only`
        ? Array.from({ length: count }, (_, seq) => ({
            operation: `insert`,
            key: `history-item-${seq}`,
            seq,
          }))
        : []
  return { selected, inputs, stateEvents }
}

function sourceHash(selected: Scenario, count: number): string {
  const normalized = normalizedWorkload(selected, count)
  return createHash(`sha256`).update(JSON.stringify(normalized)).digest(`hex`)
}

function hasExpectedHydratedState(
  selected: Scenario,
  count: number,
  observation: HydratedObservation | undefined
): boolean {
  if (!observation) return false
  if (
    observation.main?.count !== 1 ||
    observation.main.lastRawCount !== 1 ||
    observation.main.lastRawSum !== 0 ||
    JSON.stringify(observation.main.lastRawIds) !== JSON.stringify([0])
  ) {
    return false
  }
  if (selected === `large-inbox`) {
    return observation.counters.length === 0 && observation.items.length === 0
  }
  const items = new Map(observation.items.map((item) => [item.key, item.seq]))
  const counters = new Map(
    observation.counters.map((counter) => [counter.key, counter.count])
  )
  if (selected === `append-only`) {
    const expected = new Map([
      ...Array.from(
        { length: count },
        (_, seq) => [`history-item-${seq}`, seq] as const
      ),
      [`item-0`, 0] as const,
    ])
    return (
      items.size === expected.size &&
      [...expected].every(([key, seq]) => items.get(key) === seq) &&
      counters.size === 0
    )
  }
  const keyCount = Math.min(16, count)
  return (
    counters.size === keyCount &&
    items.size === 0 &&
    Array.from({ length: keyCount }, (_, index) => index).every((index) => {
      const expectedSeq = count - 1 - ((count - 1 - index) % keyCount)
      return counters.get(`work-${index}`) === expectedSeq
    })
  )
}

suite(`real processWake cold activation benchmark`, () => {
  const snapshots = mode === `snapshot-full-raw-replay`
  const runtime = runtimeTest({
    durableStreamsUrl: (
      gate?.streamRoot ?? `http://127.0.0.1:1/v1/stream/`
    ).replace(/\/$/, ``),
    durableStreamsBearer: runtimeToken,
    resetBackend: false,
    onWakeMetrics(metrics) {
      wakeCounts.set(
        metrics.entityUrl,
        (wakeCounts.get(metrics.entityUrl) ?? 0) + 1
      )
      metricWaiters.get(metrics.entityUrl)?.shift()?.(metrics)
    },
    async beforeWebhook() {
      const hold = nextWebhookHold
      if (!hold) return
      nextWebhookHold = undefined
      hold.markArrived()
      await hold.released
    },
    ...(snapshots
      ? {
          projectionSnapshots: {
            projectionVersion: `process-wake-benchmark-v1`,
            rawInputRecovery: `full-replay` as const,
            publisherHeaders: () => ({
              authorization: `Bearer ${publisherToken}`,
            }),
            minReplayEvents: 0,
            minReplayMs: 0,
            minPublishIntervalMs: 0,
            missBackoffMs: 0,
          },
        }
      : {}),
  })

  const definition: EntityDefinition = {
    state: {
      counters: { schema: counterSchema, primaryKey: `key` },
      items: { schema: itemSchema, primaryKey: `key` },
    },
    async handler(ctx) {
      const payloads = inboxPayloads(ctx.events)
      const counters = (
        ctx.state as { counters: StateCollectionProxy<Counter> }
      ).counters
      const items = (ctx.state as { items: StateCollectionProxy<Item> }).items
      const current = counters.get(`main`)
      if (payloads.some((payload) => payload.measure)) {
        hydratedObservations.set(ctx.entityUrl, {
          main: current
            ? {
                key: current.key,
                count: current.count,
                lastRawCount: current.lastRawCount,
                lastRawSum: current.lastRawSum,
                lastRawIds: [...current.lastRawIds],
              }
            : null,
          counters: counters.toArray
            .filter((row) => row.key !== `main`)
            .map((row) => ({ key: row.key, count: row.count })),
          items: items.toArray.map((row) => ({ key: row.key, seq: row.seq })),
        })
      }
      const maxSeq = payloads.reduce(
        (maximum, payload) => Math.max(maximum, payload.seq),
        current ? current.count - 1 : -1
      )
      const rawSum = payloads.reduce((sum, payload) => sum + payload.seq, 0)
      const rawIds = payloads.map((payload) => payload.seq)

      if (current) {
        counters.update(`main`, (draft) => {
          draft.count = Math.max(draft.count, maxSeq + 1)
          draft.lastRawCount = payloads.length
          draft.lastRawSum = rawSum
          draft.lastRawIds = rawIds
        })
      } else {
        counters.insert({
          key: `main`,
          count: maxSeq + 1,
          lastRawCount: payloads.length,
          lastRawSum: rawSum,
          lastRawIds: rawIds,
        })
      }

      if ((ctx.args as { scenario?: Scenario }).scenario === `append-only`) {
        for (const payload of payloads) {
          const key = `item-${payload.seq}`
          if (!items.get(key)) items.insert({ key, seq: payload.seq })
        }
      }

      if (payloads.some((payload) => payload.measure)) {
        ctx.replyText(`measured:${maxSeq + 1}`)
      }
    },
  }
  runtime.define(`e2e`, definition)

  const disposables: Array<{ streamPath: string; subscriptionId: string }> = []

  afterAll(async () => {
    try {
      await runtime.cleanup()
      const headers = { authorization: `Bearer ${runtimeToken}` }
      for (const disposable of disposables) {
        await fetch(
          new URL(
            `__ds/subscriptions/${encodeURIComponent(disposable.subscriptionId)}`,
            root!
          ),
          { method: `DELETE`, headers }
        )
        await fetch(new URL(disposable.streamPath, root!), {
          method: `DELETE`,
          headers,
        })
      }
    } finally {
      await gate?.stop()
    }
  }, 60_000)

  it(
    `records fixed-history paired-mode samples`,
    async () => {
      const count = sampleSize(scenario!)
      const expectedRawIds =
        scenario === `large-inbox`
          ? Array.from({ length: count + 1 }, (_, seq) => seq)
          : [0, count]
      const expectedRawSum = expectedRawIds.reduce((sum, seq) => sum + seq, 0)
      const totalSamples = warmupSamples + measuredSamples

      for (let sampleIndex = 0; sampleIndex < totalSamples; sampleIndex++) {
        const warmup = sampleIndex < warmupSamples
        const logicalSampleIndex = warmup
          ? sampleIndex - warmupSamples
          : sampleIndexBase + sampleIndex - warmupSamples
        const id = `bench-${scenario}-${randomUUID()}`
        const entityUrl = `/e2e/${id}`
        const firstPayload: InputPayload = {
          seq: 0,
          ...(scenario === `large-inbox` ? { data: `x`.repeat(8192) } : {}),
        }
        const initialWake = waitForWakeMetrics(entityUrl, 120_000)
        const entity = await runtime.spawn(
          `e2e`,
          id,
          { scenario },
          { initialMessage: firstPayload }
        )
        const initialMetrics = await initialWake
        expect(initialMetrics.succeeded).toBe(true)
        expect(initialMetrics.doneAckCompleted).toBe(true)
        await entity.waitForSettled(120_000)

        const streamPath = `e2e/${id}/main`
        const stream = new DurableStream({
          url: new URL(streamPath, root!).toString(),
          headers: { authorization: `Bearer ${runtimeToken}` },
          contentType: `application/json`,
        })
        if (scenario !== `large-inbox`) {
          const historyWake = waitForWakeMetrics(entityUrl, 120_000)
          await appendWorkloadHistory(stream, scenario!, count)
          const historyMetrics = await historyWake
          expect(historyMetrics.succeeded).toBe(true)
          expect(historyMetrics.doneAckCompleted).toBe(true)
          await entity.waitForSettled(120_000)
        }
        const subscriptionId = `webhook:e2e:${createHash(`sha256`)
          .update(entity.entityUrl)
          .digest(`hex`)
          .slice(0, 16)}`
        disposables.push({ streamPath, subscriptionId })

        let image = null
        if (snapshots) {
          const expectedImageHead = await stream.head()
          for (
            let attempt = 0;
            image?.offset !==
              (expectedImageHead.exists ? expectedImageHead.offset : null) &&
            attempt < 500;
            attempt++
          ) {
            image = await stream.loadSnapshot(`process-wake-benchmark-v1`, {
              expectedContentType: `application/vnd.electric.entity-image+json`,
            })
            if (
              image?.offset !==
              (expectedImageHead.exists ? expectedImageHead.offset : null)
            ) {
              await new Promise((resolve) => setTimeout(resolve, 10))
            }
          }
          expect(image).not.toBeNull()
          expect(image?.offset).toBe(
            expectedImageHead.exists ? expectedImageHead.offset : null
          )
        }

        let pendingDelivery: ReturnType<typeof holdNextWebhook> | undefined
        if (scenario === `large-inbox`) {
          pendingDelivery = holdNextWebhook()
          await entity.send({ seq: 1, data: `x`.repeat(8192) })
          await pendingDelivery.arrived
          for (let seq = 2; seq <= count; seq++) {
            await entity.send({
              seq,
              measure: seq === count,
              data: `x`.repeat(8192),
            })
          }
        }

        gate!.reset()
        const capture = gate!.startCapture()
        const wakeCountBefore = wakeCounts.get(entityUrl) ?? 0
        const measuredWake = waitForWakeMetrics(
          entityUrl,
          Number(process.env.CHRONICLE_BENCH_ACK_TIMEOUT_MS ?? `120000`)
        )
        const activationStartedAtUnixMs = Date.now()
        if (pendingDelivery) {
          pendingDelivery.release()
        } else {
          await entity.send({ seq: count, measure: true })
        }
        const measured = await measuredWake
        const timedRequests = capture.stop()
        const leaseState = await readLeaseState(subscriptionId)

        let publication = null
        if (snapshots) {
          try {
            publication = await gate!.waitFor(
              (request) =>
                request.method === `PUT` &&
                request.snapshot === `process-wake-benchmark-v1`,
              2_000
            )
          } catch {
            publication = null
          }
        }

        // Everything below this point is semantic validation, excluded from
        // activation timing and the frozen request capture above.
        const inputHead = await stream.head()
        expect(inputHead.exists).toBe(true)
        if (!inputHead.exists || !inputHead.offset) {
          throw new Error(`Measured input stream has no head offset`)
        }
        const inputOffset = inputHead.offset
        const finalHead = await stream.head()

        const history = await entity.history()
        const finalCounter = latestCounter(history.events)
        const outputDeltas = history.events
          .filter((event) => event.type === `text_delta`)
          .map((event) => (event.value as { delta?: unknown })?.delta)
        const finalAck = await readAck(
          root!,
          runtimeToken,
          subscriptionId,
          streamPath
        )
        const assertions = {
          state:
            finalCounter?.count === count + 1 &&
            JSON.stringify(finalCounter.lastRawIds) ===
              JSON.stringify(expectedRawIds),
          hydratedState: hasExpectedHydratedState(
            scenario!,
            count,
            hydratedObservations.get(entityUrl)
          ),
          rawInputs:
            finalCounter?.lastRawCount === expectedRawIds.length &&
            finalCounter.lastRawSum === expectedRawSum &&
            JSON.stringify(finalCounter.lastRawIds) ===
              JSON.stringify(expectedRawIds),
          persistedOutput: outputDeltas.includes(`measured:${count + 1}`),
          exactPersistedOutputs:
            JSON.stringify(outputDeltas) ===
            JSON.stringify([`measured:${count + 1}`]),
          activationCount:
            (wakeCounts.get(entityUrl) ?? 0) - wakeCountBefore === 1,
          ack: finalAck === (finalHead.exists ? finalHead.offset : null),
          release:
            measured.doneAckCompleted === true &&
            measured.succeeded === true &&
            leaseState.phase === `idle` &&
            leaseState.holder === `0` &&
            leaseState.leaseUntilNs === `0`,
          signal: null,
          cancellation: null,
        }
        expect(assertions.state, JSON.stringify(finalCounter)).toBe(true)
        expect(assertions.rawInputs, JSON.stringify(finalCounter)).toBe(true)
        expect(assertions.hydratedState).toBe(true)
        expect(assertions.persistedOutput).toBe(true)
        expect(assertions.exactPersistedOutputs).toBe(true)
        expect(assertions.activationCount).toBe(true)
        expect(assertions.ack).toBe(true)
        expect(assertions.release).toBe(true)

        const traffic = summarizeTraffic(timedRequests, image?.offset ?? null)
        const measuredLookup = timedRequests.find(
          (request) =>
            request.method === `GET` &&
            request.snapshot === `process-wake-benchmark-v1`
        )
        process.stdout.write(
          `${JSON.stringify({
            kind: `chronicle-process-wake-benchmark-sample`,
            schemaVersion: 1,
            scenario,
            mode,
            phase,
            phaseOrder,
            sampleIndex: logicalSampleIndex,
            warmup,
            status: `passed`,
            entityUrl,
            sourceHash: sourceHash(scenario!, count),
            sourceHashScope: `normalized deterministic semantic input and state-update events; excludes server-added envelope fields, offsets, and timestamps and is not an exact wire-history hash`,
            sourceEventCount:
              normalizedWorkload(scenario!, count).inputs.length +
              normalizedWorkload(scenario!, count).stateEvents.length,
            activation: {
              scope: `after claimHeaders resolution at wake-received logging through successful done:true callback completion; includes snapshot lookup, recovery, handler writes, and ack; excludes trigger append and semantic validation`,
              startedAtUnixMs: activationStartedAtUnixMs,
              totalMs: measured.totalMs,
              claimMs: measured.claimMs,
              preloadMs: measured.preloadMs,
              handlerMs: measured.handlerMs,
            },
            snapshot: {
              lookup: measuredLookup
                ? measuredLookup.status === 200
                  ? `hit`
                  : `miss-or-error`
                : `not-requested`,
              imageBytes: measuredLookup?.responseBytes ?? null,
              cut: measuredLookup?.streamSnapshotOffset ?? null,
              publication: publication
                ? {
                    status: publication.status,
                    durationMs: publication.durationMs,
                    requestBytes: publication.requestBytes,
                  }
                : null,
            },
            traffic,
            resources: {
              scope: `combined runtime handler and embedded agents-server Node process`,
              cpuUserMicros: measured.cpuUserMicros,
              cpuSystemMicros: measured.cpuSystemMicros,
              heapBeforeBytes: measured.heapBeforeBytes,
              heapAfterBytes: measured.heapAfterBytes,
              peakHeapBytes: measured.peakHeapBytes,
              rssBeforeBytes: measured.rssBeforeBytes,
              rssAfterBytes: measured.rssAfterBytes,
              peakRssBytes: measured.peakRssBytes,
              samplingIntervalMs: measured.samplingIntervalMs,
            },
            offsets: {
              inputHead: inputOffset,
              finalHead: finalHead.exists ? finalHead.offset : null,
              acked: finalAck,
            },
            leaseState,
            assertions,
            validationTimed: false,
          })}\n`
        )
      }
    },
    30 * 60_000
  )
})

async function readAck(
  streamRoot: URL,
  token: string,
  subscriptionId: string,
  streamPath: string
): Promise<string | null> {
  const response = await fetch(
    new URL(
      `__ds/subscriptions/${encodeURIComponent(subscriptionId)}`,
      streamRoot
    ),
    { headers: { authorization: `Bearer ${token}` } }
  )
  if (!response.ok) return null
  const subscription = (await response.json()) as {
    streams?: Array<{ path?: string; acked_offset?: string }>
  }
  return (
    subscription.streams?.find((entry) => entry.path === streamPath)
      ?.acked_offset ?? null
  )
}

function summarizeTraffic(
  requests: Array<{
    method: string
    search: string
    snapshot: string | null
    requestBytes: number
    responseBytes: number
  }>,
  imageCut: string | null
) {
  const categories = {
    snapshotLookup: { requests: 0, requestBytes: 0, responseBytes: 0 },
    snapshotPublication: {
      requests: 0,
      requestBytes: 0,
      responseBytes: 0,
    },
    fullReplay: { requests: 0, requestBytes: 0, responseBytes: 0 },
    suffix: { requests: 0, requestBytes: 0, responseBytes: 0 },
    other: { requests: 0, requestBytes: 0, responseBytes: 0 },
  }
  for (const request of requests) {
    const query = new URLSearchParams(request.search)
    const category =
      request.snapshot && request.method === `GET`
        ? categories.snapshotLookup
        : request.snapshot
          ? categories.snapshotPublication
          : request.method === `GET` && query.get(`offset`) === `-1`
            ? categories.fullReplay
            : request.method === `GET` &&
                imageCut &&
                query.get(`offset`) === imageCut
              ? categories.suffix
              : categories.other
    category.requests += 1
    category.requestBytes += request.requestBytes
    category.responseBytes += request.responseBytes
  }
  return {
    scope: `single-counted requests crossing the runtime/agents-server Chronicle gate during the timed interval; direct validation reads are excluded`,
    total: {
      requests: requests.length,
      requestBytes: requests.reduce(
        (sum, request) => sum + request.requestBytes,
        0
      ),
      responseBytes: requests.reduce(
        (sum, request) => sum + request.responseBytes,
        0
      ),
    },
    ...categories,
  }
}
