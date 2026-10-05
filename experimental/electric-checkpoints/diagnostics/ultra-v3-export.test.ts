import { afterEach, beforeEach, expect, it } from "vitest"
import { DurableStreamTestServer } from "@durable-streams/server"
import { DurableStream } from "@durable-streams/client"
import { createStateSchema, createStreamDB } from "../src/stream-db"
import type { StreamDBBootstrap } from "../src/stream-db"
import type { StandardSchemaV1 } from "@standard-schema/spec"

type Item = { id: string; name: string }
const schema: StandardSchemaV1<Item> = {
  "~standard": {
    version: 1,
    vendor: `review`,
    validate: (value) => ({ value: value as Item }),
  },
}
const state = createStateSchema({
  items: { schema, type: `item`, primaryKey: `id` },
})
let server: DurableStreamTestServer
let stream: DurableStream
let url: string
let dbs: Array<{ close: () => void }>

beforeEach(async () => {
  dbs = []
  server = new DurableStreamTestServer({ port: 0 })
  const base = await server.start()
  url = `${base}/review/export`
  stream = await DurableStream.create({ url, contentType: `application/json` })
})

afterEach(async () => {
  for (const db of dbs) db.close()
  await server.stop()
})

it.each([false, true])(
  `exports exact rows/cut inside persisting mutationFn (onCommittedBatch=%s)`,
  async (captureEnabled) => {
    let captured!: StreamDBBootstrap
    let visibleAtCapture: string | undefined
    const db = createStreamDB({
      streamOptions: { url, contentType: `application/json` },
      state,
      ...(captureEnabled ? { onCommittedBatch: () => {} } : {}),
      actions: ({ db: actionDb }) => ({
        insertA: {
          onMutate: () => actionDb.collections.items.insert({ id: `a`, name: `Optimistic A` }),
          mutationFn: async () => {
            await stream.append(JSON.stringify(state.items.insert({
              key: `a`, value: { id: `a`, name: `Canonical A` }, headers: { txid: `a-echo` },
            })))
            await actionDb.utils.awaitTxId(`a-echo`)
            captured = actionDb.exportState()
            visibleAtCapture = actionDb.collections.items.get(`a`)?.name
          },
        },
      }),
    })
    dbs.push(db)
    await db.preload()
    await db.actions.insertA(undefined).isPersisted.promise
    const head = await stream.head()
    if (!head.exists) throw new Error(`Source stream missing`)
    // Hydrate exactly the public export. There is no suffix left to repair an
    // image that includes the input's offset while omitting its canonical row.
    const recovered = createStreamDB({
      streamOptions: { url, contentType: `application/json` },
      state,
      bootstrap: captured,
    })
    dbs.push(recovered)
    await recovered.preload()
    const restored = recovered.exportState().rowsByCollection.items
    console.log(`PUBLIC_EXPORT_ECHO`, JSON.stringify({ captureEnabled, captured, visibleAtCapture, restored }))
    expect(visibleAtCapture).toBe(`Optimistic A`)
    expect(captured.offset).toBe(head.offset)
    expect(captured.nextSeq).toBe(1)
    expect(captured.rowsByCollection.items).toEqual([{ id: `a`, name: `Canonical A`, _seq: 0 }])
    expect(restored).toEqual([{ id: `a`, name: `Canonical A`, _seq: 0 }])
  }
)

it.each([false, true])(
  `exports B at its source cut while unrelated A persists (onCommittedBatch=%s)`,
  async (captureEnabled) => {
    let release!: () => void
    let started!: () => void
    const held = new Promise<void>((resolve) => { release = resolve })
    const pending = new Promise<void>((resolve) => { started = resolve })
    const db = createStreamDB({
      streamOptions: { url, contentType: `application/json` },
      state,
      ...(captureEnabled ? { onCommittedBatch: () => {} } : {}),
      actions: ({ db: actionDb }) => ({
        insertA: {
          onMutate: () => actionDb.collections.items.insert({ id: `a`, name: `Optimistic A` }),
          mutationFn: async () => {
            started()
            await held
            await stream.append(JSON.stringify(state.items.insert({
              key: `a`, value: { id: `a`, name: `Canonical A` }, headers: { txid: `later-a` },
            })))
            await actionDb.utils.awaitTxId(`later-a`)
          },
        },
      }),
    })
    dbs.push(db)
    await db.preload()
    const transaction = db.actions.insertA(undefined)
    await pending
    let captured!: StreamDBBootstrap
    let bOffset: string | undefined
    try {
      await stream.append(JSON.stringify(state.items.insert({
        key: `b`, value: { id: `b`, name: `Canonical B` }, headers: { txid: `b-first` },
      })))
      await db.utils.awaitTxId(`b-first`)
      const head = await stream.head()
      if (!head.exists) throw new Error(`Source stream missing`)
      bOffset = head.offset
      captured = db.exportState()
      console.log(`PUBLIC_EXPORT_B_BEFORE_A`, JSON.stringify({ captureEnabled, captured, visible: db.collections.items.toArray }))
    } finally {
      release()
      await transaction.isPersisted.promise
    }
    const finalImage = db.exportState()
    const replay = createStreamDB({
      streamOptions: { url, contentType: `application/json` },
      state,
      onCommittedBatch: () => {},
    })
    dbs.push(replay)
    await replay.preload()
    const replayImage = replay.exportState()
    console.log(`PUBLIC_EXPORT_ORDER`, JSON.stringify({ captureEnabled, finalImage, replayImage }))
    expect(captured.offset).toBe(bOffset)
    expect(captured.nextSeq).toBe(1)
    expect(captured.rowsByCollection.items).toEqual([{ id: `b`, name: `Canonical B`, _seq: 0 }])
    expect(finalImage.offset).toBe(replayImage.offset)
    expect(JSON.stringify(finalImage)).toBe(JSON.stringify(replayImage))
  }
)
