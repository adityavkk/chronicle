import { readFile } from 'node:fs/promises'
import { createHash, randomUUID } from 'node:crypto'
import { afterAll, describe, expect, it } from 'vitest'
import { DurableStream } from '@durable-streams/client'
import { z } from 'zod'
import { startChronicleGate } from './chronicle-e2e-gate'
import { runtimeTest } from './runtime-dsl'
import type { EntityDefinition, StateCollectionProxy } from '../src/index'

const chronicleRoot = process.env.CHRONICLE_STREAM_ROOT
const callbackGateRoot = process.env.CHRONICLE_CALLBACK_GATE_URL
const tokenDir = process.env.CHRONICLE_TOKEN_DIR
const enabled = Boolean(chronicleRoot && callbackGateRoot && tokenDir)
const snapshotEnabled = process.env.CHRONICLE_E2E_SNAPSHOTS !== `off`
const readToken = async (name: string): Promise<string> =>
  enabled ? (await readFile(`${tokenDir}/.${name}-token`, `utf8`)).trim() : ``
const runtimeToken = await readToken(`writer`)
const publisherToken = await readToken(`publisher`)
const root = enabled ? new URL(chronicleRoot!) : null
const callbackGate = enabled ? new URL(callbackGateRoot!) : null
if (
  callbackGate &&
  ![`127.0.0.1`, `localhost`, `::1`].includes(callbackGate.hostname)
) {
  throw new Error(`Refusing non-loopback Chronicle callback gate`)
}
const gate = root ? await startChronicleGate(root) : null
const cancellationGates = new Map<
  string,
  {
    started: Promise<void>
    markStarted: () => void
    observed: Promise<void>
    markObserved: () => void
  }
>()
const cancellationHandled = new Set<string>()
const redeliveryAttempts = new Map<string, number>()
const externalEffectAttempts = new Map<string, number>()

const suite = enabled ? describe : describe.skip

suite(`processWake against Chronicle`, () => {
  const runtimeOptions = {
    durableStreamsUrl: (
      gate?.streamRoot ?? `http://127.0.0.1:1/v1/stream/`
    ).replace(/\/$/, ``),
    durableStreamsBearer: runtimeToken,
    resetBackend: false,
  }
  const runtime = runtimeTest({
    ...runtimeOptions,
    ...(snapshotEnabled
      ? {
          projectionSnapshots: {
            projectionVersion: `process-wake-e2e-v1`,
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

  const stateSchema = z.object({
    key: z.string(),
    count: z.number().int(),
    eventTypes: z.array(z.string()),
    payloads: z.array(z.unknown()),
  })
  type ActivationState = z.infer<typeof stateSchema>

  const definition: EntityDefinition = {
    state: {
      activation: { schema: stateSchema, primaryKey: `key` },
    },
    async handler(ctx, wake) {
      const shouldCrash = ctx.events.some(
        (event) =>
          event.type === `inbox` &&
          (event.value as { payload?: { control?: unknown } })?.payload
            ?.control === `crash`
      )
      if (shouldCrash) {
        const attempt = (redeliveryAttempts.get(ctx.entityUrl) ?? 0) + 1
        redeliveryAttempts.set(ctx.entityUrl, attempt)
        externalEffectAttempts.set(
          ctx.entityUrl,
          (externalEffectAttempts.get(ctx.entityUrl) ?? 0) + 1
        )
        if (attempt === 2) ctx.replyText(`redelivered`)
        return
      }

      const shouldBlock = ctx.events.some(
        (event) =>
          event.type === `inbox` &&
          (event.value as { payload?: { control?: unknown } })?.payload
            ?.control === `block`
      )
      const cancellationGate = cancellationGates.get(ctx.entityUrl)
      if (shouldBlock && cancellationHandled.has(ctx.entityUrl)) return
      if (shouldBlock && cancellationGate) {
        cancellationGate.markStarted()
        if (!ctx.signal.aborted) {
          await new Promise<void>((resolve) =>
            ctx.signal.addEventListener(`abort`, () => resolve(), {
              once: true,
            })
          )
        }
        cancellationHandled.add(ctx.entityUrl)
        cancellationGate.markObserved()
        return
      }

      const activation = (
        ctx.state as { activation: StateCollectionProxy<ActivationState> }
      ).activation
      const existing = activation.get(`result`)
      const eventTypes = ctx.events.map((event) => event.type)
      const payloads = ctx.events
        .filter((event) => event.type === `inbox`)
        .map((event) => (event.value as { payload?: unknown })?.payload)
      if (existing) {
        activation.update(`result`, (draft) => {
          draft.count += 1
          draft.eventTypes.push(...eventTypes)
          draft.payloads.push(...payloads)
        })
      } else {
        activation.insert({
          key: `result`,
          count: 1,
          eventTypes,
          payloads,
        })
      }
      ctx.replyText(`handled:${wake.type}`)
    },
  }
  runtime.define(`e2e`, definition)

  const disposables: Array<{
    entityUrl: string
    streamPath: string
    subscriptionId: string
  }> = []

  afterAll(async () => {
    try {
      if (root) {
        const headers = { authorization: `Bearer ${runtimeToken}` }
        for (const disposable of disposables) {
          const subscriptionDelete = await fetch(
            new URL(
              `__ds/subscriptions/${encodeURIComponent(disposable.subscriptionId)}`,
              root
            ),
            { method: `DELETE`, headers }
          )
          expect([200, 204, 404]).toContain(subscriptionDelete.status)
        }
        await runtime.cleanup()
        for (const disposable of disposables) {
          const streamDelete = await fetch(
            new URL(disposable.streamPath, root),
            {
              method: `DELETE`,
              headers,
            }
          )
          expect([200, 204, 404]).toContain(streamDelete.status)
        }
      }
    } finally {
      await runtime.cleanup()
      await gate?.stop()
    }
  }, 30_000)

  it(`recovers a second real webhook activation from an image and acks it`, async () => {
    const id = `activation-${randomUUID()}`
    const entity = await runtime.spawn(`e2e`, id, {}, { initialMessage: `one` })
    await entity.waitForOperation(`state:activation`, `insert`, {
      timeoutMs: 30_000,
    })
    const firstHistory = await entity.waitForSettled(30_000)

    const resultEvent = firstHistory.find(
      `state:activation`,
      (event) => event.key === `result`
    )
    expect(resultEvent?.value).toMatchObject({
      count: 1,
      payloads: [`one`],
    })
    expect(
      firstHistory.find(`text_delta`, (event) =>
        Object.is((event.value as { delta?: unknown })?.delta, `handled:inbox`)
      )
    ).toBeDefined()

    const streamPath = `e2e/${id}/main`
    const stream = new DurableStream({
      url: new URL(streamPath, root!).toString(),
      headers: { authorization: `Bearer ${runtimeToken}` },
      contentType: `application/json`,
    })
    const head = await stream.head()
    expect(head.exists).toBe(true)

    const subscriptionId = `webhook:e2e:${createHash(`sha256`)
      .update(entity.entityUrl)
      .digest(`hex`)
      .slice(0, 16)}`
    disposables.push({
      entityUrl: entity.entityUrl,
      streamPath,
      subscriptionId,
    })
    const subscriptionResponse = await fetch(
      new URL(
        `__ds/subscriptions/${encodeURIComponent(subscriptionId)}`,
        root!
      ),
      { headers: { authorization: `Bearer ${runtimeToken}` } }
    )
    expect(subscriptionResponse.status).toBe(200)
    const subscription = (await subscriptionResponse.json()) as {
      streams?: Array<{ path?: string; acked_offset?: string }>
    }
    const subscribed = subscription.streams?.find(
      (entry) => entry.path === streamPath
    )
    expect(subscribed?.acked_offset).toBe(head.exists ? head.offset : undefined)

    let snapshot = null
    if (snapshotEnabled) {
      snapshot = await stream.loadSnapshot(`process-wake-e2e-v1`, {
        expectedContentType: `application/vnd.electric.entity-image+json`,
      })
      for (let attempt = 0; !snapshot && attempt < 100; attempt++) {
        await new Promise((resolve) => setTimeout(resolve, 10))
        snapshot = await stream.loadSnapshot(`process-wake-e2e-v1`, {
          expectedContentType: `application/vnd.electric.entity-image+json`,
        })
      }
      expect(snapshot).not.toBeNull()
      expect(snapshot?.offset).toBe(head.exists ? head.offset : undefined)
    }

    gate!.reset()
    await entity.send(`two`)
    await entity.waitForOperation(`state:activation`, `update`, {
      timeoutMs: 30_000,
    })
    const finalHistory = await entity.waitForSettled(30_000)
    const finalState = finalHistory.find(
      `state:activation`,
      (event) =>
        event.key === `result` &&
        (event.headers as { operation?: unknown })?.operation === `update`
    )
    expect(finalState?.value).toMatchObject({
      count: 2,
      eventTypes: [
        `entity_created`,
        `inbox`,
        `entity_created`,
        `inbox`,
        `run`,
        `text`,
        `text_delta`,
        `text`,
        `run`,
        `state:activation`,
        `inbox`,
      ],
      // The currently approved conservative policy deliberately replays the
      // complete raw history. The prior inbox therefore appears again in
      // ctx.events; this is explicit and can erase recovery speedups.
      payloads: [`one`, `one`, `two`],
    })
    expect(
      finalHistory.events
        .filter((event) => event.type === `text_delta`)
        .map((event) => (event.value as { delta?: unknown }).delta)
    ).toEqual([`handled:inbox`, `handled:inbox`])
    const snapshotReads = gate!.requests.filter(
      (request) =>
        request.method === `GET` && request.snapshot === `process-wake-e2e-v1`
    )
    if (snapshotEnabled) {
      expect(snapshotReads).toContainEqual(
        expect.objectContaining({
          status: 200,
          snapshot: `process-wake-e2e-v1`,
        })
      )
    } else {
      expect(snapshotReads).toEqual([])
    }

    const recoveryHead = await stream.head()
    const finalSubscriptionResponse = await fetch(
      new URL(
        `__ds/subscriptions/${encodeURIComponent(subscriptionId)}`,
        root!
      ),
      { headers: { authorization: `Bearer ${runtimeToken}` } }
    )
    expect(finalSubscriptionResponse.status).toBe(200)
    const finalSubscription = (await finalSubscriptionResponse.json()) as {
      streams?: Array<{ path?: string; acked_offset?: string }>
    }
    const finalSubscribed = finalSubscription.streams?.find(
      (entry) => entry.path === streamPath
    )
    expect(finalSubscribed?.acked_offset).toBe(
      recoveryHead.exists ? recoveryHead.offset : undefined
    )

    const cancellationGate = makeCancellationGate(entity.entityUrl)
    await entity.send({ control: `block` })
    await cancellationGate.started
    await runtime.signal(entity.entityUrl, `SIGINT`, {
      reason: `real processWake E2E cancellation`,
    })
    await cancellationGate.observed
    const cancellationHistory = await entity.waitForSettled(30_000)
    expect(
      cancellationHistory.events.some(
        (event) =>
          event.type === `signal` &&
          (event.value as { signal?: unknown })?.signal === `SIGINT`
      )
    ).toBe(true)
    expect(
      cancellationHistory.events
        .filter((event) => event.type === `text_delta`)
        .map((event) => (event.value as { delta?: unknown }).delta)
    ).toEqual([`handled:inbox`, `handled:inbox`])
    const finalHead = await stream.head()
    const cancellationAck = await waitForSubscriptionAck(
      root!,
      runtimeToken,
      subscriptionId,
      streamPath,
      finalHead.exists ? finalHead.offset : null,
      30_000
    )
    expect(cancellationAck).toBe(finalHead.exists ? finalHead.offset : null)

    await fetch(new URL(`/__fixture/records`, callbackGate!), {
      method: `DELETE`,
    })
    const armFailure = await fetch(
      new URL(`/__fixture/fail-next-done`, callbackGate!),
      { method: `POST` }
    )
    expect(armFailure.status).toBe(204)
    runtime.expectWakeError(/Done callback failed \(503\)/)
    await entity.send({ control: `crash` })
    const redeliveryHistory = await entity.waitForTypeCount(`text_delta`, 1, {
      timeoutMs: 90_000,
      predicate: (event) =>
        (event.value as { delta?: unknown })?.delta === `redelivered`,
    })
    expect(externalEffectAttempts.get(entity.entityUrl)).toBeGreaterThanOrEqual(
      2
    )
    expect(
      redeliveryHistory.events.filter(
        (event) =>
          event.type === `text_delta` &&
          (event.value as { delta?: unknown })?.delta === `redelivered`
      )
    ).toHaveLength(1)
    const callbackRecords = (await (
      await fetch(new URL(`/__fixture/records`, callbackGate!))
    ).json()) as Array<{
      method?: string
      pathname?: string
      done?: boolean | null
      status?: number | null
      outcome?: string
    }>
    expect(
      callbackRecords.some(
        (request) =>
          request.method === `POST` &&
          request.pathname?.endsWith(`/callback`) === true &&
          request.done === true &&
          request.status === 503 &&
          request.outcome === `injected-failure`
      )
    ).toBe(true)
    const redeliveryHead = await stream.head()
    const redeliveryAck = await waitForSubscriptionAck(
      root!,
      runtimeToken,
      subscriptionId,
      streamPath,
      redeliveryHead.exists ? redeliveryHead.offset : null,
      30_000
    )
    process.stdout.write(
      `${JSON.stringify({
        kind: `chronicle-process-wake-smoke`,
        mode: snapshotEnabled ? `snapshot-full-raw-replay` : `replay`,
        entityUrl: entity.entityUrl,
        streamPath,
        subscriptionId,
        headOffset: redeliveryHead.exists ? redeliveryHead.offset : null,
        ackedOffset: redeliveryAck,
        snapshot: snapshot
          ? {
              hit: true,
              bytes: snapshot.body.byteLength,
              cut: snapshot.offset,
            }
          : { hit: false },
        assertions: {
          state: true,
          rawInputs: true,
          persistedOutput: true,
          ack: true,
          signal: true,
          cancellation: true,
          ackFailureRedelivery: true,
          exactlyOnceExternalEffectsClaimed: false,
          recoverySnapshotGet200: snapshotEnabled ? true : null,
        },
      })}\n`
    )
  }, 120_000)
})

function makeCancellationGate(entityUrl: string) {
  let markStarted!: () => void
  let markObserved!: () => void
  const gate = {
    started: new Promise<void>((resolve) => {
      markStarted = resolve
    }),
    markStarted,
    observed: new Promise<void>((resolve) => {
      markObserved = resolve
    }),
    markObserved,
  }
  cancellationGates.set(entityUrl, gate)
  return gate
}

async function readSubscriptionAck(
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
  expect(response.status).toBe(200)
  const subscription = (await response.json()) as {
    streams?: Array<{ path?: string; acked_offset?: string }>
  }
  return (
    subscription.streams?.find((entry) => entry.path === streamPath)
      ?.acked_offset ?? null
  )
}

async function waitForSubscriptionAck(
  streamRoot: URL,
  token: string,
  subscriptionId: string,
  streamPath: string,
  expected: string | null | undefined,
  timeoutMs: number
): Promise<string | null> {
  const started = Date.now()
  let actual: string | null = null
  while (Date.now() - started < timeoutMs) {
    actual = await readSubscriptionAck(
      streamRoot,
      token,
      subscriptionId,
      streamPath
    )
    if (actual === (expected ?? null)) return actual
    await new Promise((resolve) => setTimeout(resolve, 10))
  }
  throw new Error(
    `Timed out waiting for subscription ack ${expected ?? `(none)`}; received ${actual ?? `(none)`}`
  )
}
