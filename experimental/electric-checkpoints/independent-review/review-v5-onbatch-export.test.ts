import { expect, it } from 'vitest'
import { DurableStreamTestServer } from '@durable-streams/server'
import { DurableStream } from '@durable-streams/client'
import {
  createEntityStreamDB,
  encodeEntityProjectionImage,
  type EntityProjectionImageV3,
} from '../src/entity-stream-db'
import { ev } from './helpers/event-fixtures'

it(`exports a whole committed image or rejects inside the next synchronous onBatch callback`, async () => {
  const server = new DurableStreamTestServer({ port: 0 })
  const baseUrl = await server.start()
  const streamUrl = `${baseUrl}/streams/onbatch-export`
  let db: ReturnType<typeof createEntityStreamDB> | undefined
  try {
    const stream = await DurableStream.create({
      url: streamUrl,
      contentType: `application/json`,
    })
    await stream.append(JSON.stringify(ev(`state:notes`, `a`, `insert`, { text: `A` })))

    let callbackRan = false
    let captured: EntityProjectionImageV3 | undefined
    let exportError: unknown
    const committedImages: EntityProjectionImageV3[] = []
    db = createEntityStreamDB(streamUrl, { notes: {} }, undefined, {
      projectionVersion: `onbatch-review`,
      // Enables coherent SDK committed-image capture and immediate sync.
      onCommittedSnapshot: (image) => { committedImages.push(image) },
      onBatch: (batch) => {
        if (!batch.items.some((item) => `key` in item && item.key === `b`)) return
        callbackRan = true
        try {
          // Deliberately synchronous: this is the public callback boundary,
          // not an artificially held SDK commit or a persistence race.
          captured = db!.utils.exportSnapshot(`onbatch-review`)
        } catch (error) {
          exportError = error
        }
      },
    })
    await db.preload()
    const beforeB = db.utils.exportSnapshot(`onbatch-review`)
    expect(beforeB.rowsByCollection.notes.map((row) => row.key)).toEqual([`a`])
    expect(beforeB.nextSeq).toBe(1)
    expect(beforeB.nextEventPosition).toBe(1)

    // A has committed before B is appended, forcing a second source batch.
    await stream.append(JSON.stringify(ev(`state:notes`, `b`, `insert`, { text: `B` }, { txid: `b-echo` })))
    await db.utils.awaitTxId(`b-echo`)
    const afterB = db.utils.exportSnapshot(`onbatch-review`)
    const describe = (image: EntityProjectionImageV3 | undefined) => image && ({
      offset: image.offset,
      nextSeq: image.nextSeq,
      nextEventPosition: image.nextEventPosition,
      rows: image.rowsByCollection.notes,
      rowPointers: image.rowPointers.notes,
      timelineOrders: image.timelineOrders.notes,
      previousBatchOffset: image.previousBatchOffset,
    })
    console.log(`SYNCHRONOUS_ONBATCH_EXPORT`, JSON.stringify({
      beforeB: describe(beforeB),
      insideB: describe(captured),
      afterB: describe(afterB),
      exportError: exportError instanceof Error ? exportError.message : exportError,
      committedImageCount: committedImages.length,
    }))

    expect(callbackRan).toBe(true)
    expect(afterB.rowsByCollection.notes.map((row) => row.key).sort()).toEqual([`a`, `b`])
    expect(afterB.nextSeq).toBe(2)
    expect(afterB.nextEventPosition).toBe(2)
    expect(afterB.offset).not.toBe(beforeB.offset)
    expect(committedImages.at(-1)).toEqual(afterB)
    // Both safe contracts are accepted: reject during the batch, or return
    // the complete last committed image. A mixed image must fail this test.
    if (exportError !== undefined) {
      expect(captured).toBeUndefined()
    } else {
      expect(captured).toBeDefined()
      expect(new TextDecoder().decode(encodeEntityProjectionImage(captured!)))
        .toBe(new TextDecoder().decode(encodeEntityProjectionImage(beforeB)))
    }
  } finally {
    db?.close()
    await server.stop()
  }
})
