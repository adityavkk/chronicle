import assert from 'node:assert/strict'
import { test } from 'node:test'
import { apply, decode, emptyState, encode, event, expected, percentile } from './benchmark.mjs'

test('deletes count toward nextSeq; reinserts restore first-insertion order', () => {
  const c = { keys: 2, payload: 8 }
  const state = emptyState()
  for (let i = 0; i < 17; i++) apply(state, event(c, i))
  // Cycle 7 deletes keys 0 and 1 at indices 14/15. Index 16 reinserts key 0.
  assert.deepEqual([...state.rows], [['0', {
    value: { revision: 16, text: '16:xxxxx' }, _seq: 16, _first: 16,
  }]])
  assert.equal(state.nextSeq, 17)
  assert.deepEqual(decode(encode(state)), state)
  assert.deepEqual(expected(c, 17), state)
})

test('independent oracle and restored suffix agree across deletion boundaries', () => {
  for (const keys of [0, 1, 3, 13]) {
    const c = { keys, payload: 16 }
    for (const count of [0, 1, 7, 8, 22, 24, 25, 106, 241]) {
      for (const cut of new Set([0, Math.floor(count / 3), Math.max(0, count - 1), count])) {
        const full = emptyState()
        for (let i = 0; i < cut; i++) apply(full, event(c, i))
        const restored = decode(encode(full))
        for (let i = cut; i < count; i++) apply(restored, event(c, i))
        assert.deepEqual(restored, expected(c, count), `keys=${keys} count=${count} cut=${cut}`)
      }
    }
  }
})

test('skipping, duplicating, or reordering inputs is not accepted as a fast result', () => {
  const c = { keys: 3, payload: 8 }
  assert.throws(() => apply(emptyState(), event(c, 1)))
  const state = emptyState()
  apply(state, event(c, 0))
  assert.throws(() => apply(state, event(c, 0)))
  assert.throws(() => decode(Buffer.from('{"codec":"wrong"}')))
})

test('percentiles use nearest rank, preserve observations, and do not average percentiles', () => {
  const samples = [1, 2, 3, 4, 500]
  assert.equal(percentile(samples, 0.5), 3)
  assert.equal(percentile(samples, 0.95), 500)
  assert.deepEqual(samples, [1, 2, 3, 4, 500])
})
