import { describe, expect, it, vi } from 'vitest'
import {
  createEntityStreamDB,
  decodeEntityProjectionImage,
  encodeEntityProjectionImage,
} from '../src/entity-stream-db'
import type { EntityProjectionImageV3 } from '../src/entity-stream-db'

function response(items: Array<unknown>, offset: string, guarded = false) {
  return new Response(JSON.stringify(items), {
    headers: {
      'content-type': `application/json`,
      'Stream-Next-Offset': offset,
      'Stream-Up-To-Date': `true`,
      ...(guarded
        ? {
            'Stream-Snapshot': `v1`,
            'Stream-Incarnation': `inc-1`,
          }
        : {}),
    },
  })
}

describe(`EntityStreamDB projection snapshots`, () => {
  it(`rejects in-batch export and permits aligned committed exports across two cuts`, async () => {
    const first = response(
      [
        {
          type: `state:notes`,
          key: `a`,
          value: { body: `A` },
          headers: { operation: `insert` },
        },
      ],
      `cut-1`
    )
    first.headers.delete(`stream-up-to-date`)
    const fetchClient = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(first)
      .mockResolvedValueOnce(
        response(
          [
            {
              type: `state:notes`,
              key: `b`,
              value: { body: `B` },
              headers: { operation: `insert` },
            },
          ],
          `cut-2`
        )
      )
      .mockImplementation(
        (_input, init) =>
          new Promise<Response>((_resolve, reject) => {
            init?.signal?.addEventListener(`abort`, () =>
              reject(new DOMException(`Aborted`, `AbortError`))
            )
          })
      )
    const committed: Array<EntityProjectionImageV3> = []
    const db = createEntityStreamDB(
      `https://example.com/atomic`,
      { notes: {} } as any,
      undefined,
      {
        streamOptions: { fetch: fetchClient },
        onBeforeBatch: () => {
          expect(() => db.utils.exportSnapshot(`atomic-v1`)).toThrow(
            `uncommitted stream batch`
          )
        },
        onBatch: () => {
          expect(() => db.utils.exportSnapshot(`atomic-v1`)).toThrow(
            `uncommitted stream batch`
          )
        },
        projectionVersion: `atomic-v1`,
        onCommittedSnapshot: (image) => {
          expect(db.utils.exportSnapshot(`atomic-v1`)).toEqual(image)
          committed.push(image)
        },
      }
    )
    try {
      await db.preload()
      expect(
        committed.map((image) => ({
          offset: image.offset,
          nextSeq: image.nextSeq,
          nextEventPosition: image.nextEventPosition,
          keys: image.rowsByCollection.notes.map((row) => row.key),
          pointers: image.rowPointers.notes,
        }))
      ).toEqual([
        {
          offset: `cut-1`,
          nextSeq: 1,
          nextEventPosition: 1,
          keys: [`a`],
          pointers: [[`a`, { offset: null, subOffset: 1 }]],
        },
        {
          offset: `cut-2`,
          nextSeq: 2,
          nextEventPosition: 2,
          keys: [`a`, `b`],
          pointers: [
            [`a`, { offset: null, subOffset: 1 }],
            [`b`, { offset: null, subOffset: 2 }],
          ],
        },
      ])
    } finally {
      db.close()
    }
  })

  it(`rejects export without explicit coherent capture while retaining legacy reads`, async () => {
    const db = createEntityStreamDB(
      `https://example.com/legacy`,
      { notes: {} } as any,
      undefined,
      {
        streamOptions: {
          fetch: vi.fn<typeof fetch>().mockResolvedValue(
            response(
              [
                {
                  type: `state:notes`,
                  key: `a`,
                  value: { body: `source A` },
                  headers: { operation: `insert` },
                },
              ],
              `cut-1`
            )
          ),
        },
      }
    )
    try {
      await db.preload()
      expect(db.collections.notes.get(`a`)).toMatchObject({ body: `source A` })
      expect(() => db.utils.exportSnapshot(`entity-v8`)).toThrow(
        `requires coherent capture via onCommittedSnapshot`
      )
      expect(() => db.exportState()).toThrow(
        `requires coherent capture via onCommittedBatch`
      )
    } finally {
      db.close()
    }
  })

  it(`round-trips rows, deletion sequence, pointers, order indexes and batch anchor`, async () => {
    let committed: EntityProjectionImageV3 | undefined
    const replay = createEntityStreamDB(
      `https://example.com/entity`,
      { notes: {} } as any,
      undefined,
      {
        streamOptions: {
          fetch: vi.fn<typeof fetch>().mockResolvedValue(
            response(
              [
                {
                  type: `state:notes`,
                  key: `a`,
                  value: {
                    body: `first`,
                    removedLater: true,
                    nested: { stable: true },
                  },
                  headers: { operation: `insert`, offset: `entry-1` },
                },
                {
                  type: `state:notes`,
                  key: `b`,
                  value: { body: `delete me` },
                  headers: { operation: `insert`, offset: `entry-1` },
                },
                {
                  type: `state:notes`,
                  key: `a`,
                  value: { body: `replacement`, nested: { stable: true } },
                  headers: { operation: `update`, offset: `entry-2` },
                },
                {
                  type: `state:notes`,
                  key: `b`,
                  headers: { operation: `delete`, offset: `entry-3` },
                },
              ],
              `cut-4`
            )
          ),
        },
        projectionVersion: `entity-v7`,
        onCommittedSnapshot: (image) => {
          committed = image
        },
      }
    )
    await replay.preload()
    expect(committed).toBeDefined()
    expect(committed?.nextSeq).toBe(4)
    expect(committed?.previousBatchOffset).toBe(`cut-4`)
    expect(committed?.rowsByCollection.notes).toEqual([
      expect.objectContaining({
        key: `a`,
        body: `replacement`,
        _seq: 2,
        removedLater: true,
      }),
    ])
    const retainedImage = committed!
    ;(
      replay.collections.notes.get(`a`) as unknown as {
        nested: { stable: boolean }
      }
    ).nested.stable = false
    expect(
      (
        retainedImage.rowsByCollection.notes[0]!.nested as {
          stable: boolean
        }
      ).stable
    ).toBe(true)
    const pointerA = committed?.rowPointers.notes.find(([key]) => key === `a`)
    expect(pointerA?.[1]).toEqual({ offset: null, subOffset: 3 })
    const orderA = committed?.timelineOrders.notes.find(([key]) => key === `a`)
    replay.close()

    const bytes = encodeEntityProjectionImage(committed!)
    // The old eight-digit ordering codec must never hydrate into a widened
    // suffix, even when every other image field and projectionVersion match.
    const oldCodec = { ...committed!, codec: `electric-entity-image/v2` }
    expect(() =>
      decodeEntityProjectionImage(
        new TextEncoder().encode(JSON.stringify(oldCodec)),
        `entity-v7`
      )
    ).toThrow(`Incompatible entity projection image`)
    expect(() =>
      createEntityStreamDB(`https://example.com/entity`, undefined, undefined, {
        snapshot: { image: oldCodec as any, incarnation: `inc-1` },
      })
    ).toThrow(`Incompatible entity projection image`)
    const decoded = decodeEntityProjectionImage(bytes, `entity-v7`)
    const suffixFetch = vi.fn<typeof fetch>().mockResolvedValue(
      response(
        [
          {
            type: `state:notes`,
            key: `c`,
            value: { body: `suffix` },
            headers: { operation: `insert`, offset: `entry-4` },
          },
        ],
        `cut-5`,
        true
      )
    )
    const hydrated = createEntityStreamDB(
      `https://example.com/entity`,
      { notes: {} } as any,
      undefined,
      {
        streamOptions: { fetch: suffixFetch },
        snapshot: { image: decoded, incarnation: `inc-1` },
        projectionVersion: `entity-v7`,
      }
    )
    await hydrated.preload()
    expect(hydrated.collections.notes.get(`a`)).toEqual(
      expect.objectContaining({ body: `replacement`, _seq: 2 })
    )
    expect(hydrated.collections.notes.get(`c`)).toEqual(
      expect.objectContaining({ body: `suffix`, _seq: 4 })
    )
    expect(hydrated.collections.notes.__electricTimelineOrders?.get(`a`)).toBe(
      orderA?.[1]
    )
    expect(hydrated.collections.notes.__electricRowOffsets?.get(`c`)).toEqual({
      offset: null,
      subOffset: 5,
    })
    hydrated.close()
  })

  it(`rejects malformed and incompatible images before hydration`, () => {
    expect(() =>
      decodeEntityProjectionImage(new TextEncoder().encode(`{`), `entity-v1`)
    ).toThrow(`Invalid entity projection image JSON`)
    const wrongVersion = new TextEncoder().encode(
      JSON.stringify({ codec: `electric-entity-image/v1` })
    )
    expect(() =>
      decodeEntityProjectionImage(wrongVersion, `entity-v2`)
    ).toThrow(`Incompatible entity projection image`)
  })
})
