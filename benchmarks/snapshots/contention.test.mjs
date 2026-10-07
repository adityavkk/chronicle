import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { createServer } from 'node:http'
import { test } from 'node:test'
import { dispatchSchedule, fixedSchedule, recoverCaptured, summarizeArrivals, validateCaptured } from './contention.mjs'
import { encode, event, expected } from './benchmark.mjs'

test('fixed schedule has no drift, duplicate arrivals, or endpoint surprise', () => {
  assert.deepEqual(fixedSchedule(4, 1000), [0, 250, 500, 750])
  assert.deepEqual(fixedSchedule(2, 1100), [0, 500, 1000])
})

test('early timer wakeups cannot dispatch before the scheduled arrival', async () => {
  let clock = 0
  const records = await dispatchSchedule({ schedule: [10.5], maxInflight: 1, maxDeferred: 0,
    now: () => clock,
    wait: async (ms) => { clock += Math.max(1, Math.floor(ms)) },
    task: async () => ({ serviceMs: 0 }),
  })
  assert.ok(records[0].dispatchedMs >= 10.5)
})

test('bounded dispatcher accounts for every deferred, missed, and failed arrival', async () => {
  let clock = 0
  const releases = []
  const recordsPromise = dispatchSchedule({ schedule: [0, 1, 2, 3], maxInflight: 1, maxDeferred: 1,
    now: () => clock,
    wait: async (ms) => { clock += ms },
    task: ({ index }) => new Promise((resolve, reject) => releases.push(() => index === 0 ? reject(new Error('boom')) : resolve({ serviceMs: 1 }))) })
  await new Promise(setImmediate)
  assert.equal(releases.length, 1)
  releases.shift()()
  await new Promise(setImmediate)
  releases.shift()()
  const records = await recordsPromise
  assert.equal(records.length, 4)
  assert.deepEqual(records.map((r) => r.disposition), ['on-time', 'missed', 'missed', 'deferred'])
  const summary = summarizeArrivals(records, 4)
  assert.deepEqual({ offered: summary.offered, completed: summary.completed, errors: summary.errors, missed: summary.missed, deferred: summary.deferred },
    { offered: 4, completed: 1, errors: 1, missed: 2, deferred: 1 })
})

test('captured-cut validation rejects dropped, reordered, and wrong-cut histories', () => {
  const c = { keys: 3, payload: 8 }
  const changes = Array.from({ length: 10 }, (_, i) => event(c, i))
  assert.equal(validateCaptured(c, changes, 10).nextSeq, 10)
  assert.throws(() => validateCaptured(c, changes.slice(1), 9))
  assert.throws(() => validateCaptured(c, [changes[1], changes[0], ...changes.slice(2)], 10))
  assert.throws(() => validateCaptured(c, changes, 9))
})

test('moving-cut recovery uses returned offsets and waits for a late append acknowledgement', async (t) => {
  const c = { keys: 3, payload: 8 }
  const boundaries = new Map([['cut-A', 2]])
  const image = encode(expected(c, 2))
  let fault = ''
  const server = createServer((req, res) => {
    res.setHeader('Stream-Snapshot', 'v1')
    res.setHeader('Stream-Incarnation', 'source-A')
    if (req.url.includes('snapshot=')) {
      res.setHeader('Stream-Snapshot-Offset', fault === 'cut' ? 'wrong-cut' : 'cut-A')
      res.setHeader('Content-Digest', `sha-256=:${createHash('sha256').update(image).digest('base64')}:`)
      res.end(image)
      return
    }
    assert.equal(req.headers['if-stream-incarnation'], 'source-A')
    res.setHeader('Stream-Next-Offset', 'cut-B')
    res.setHeader('Stream-Up-To-Date', 'true')
    const from = req.url.includes('offset=-1') ? 0 : 2
    const changes = Array.from({ length: 4 - from }, (_, i) => event(c, from + i))
    if (fault === 'drop') changes.pop()
    res.end(JSON.stringify(changes))
  })
  await new Promise((done) => server.listen(0, '127.0.0.1', done))
  t.after(() => new Promise((done) => server.close(done)))
  const config = { base: `http://127.0.0.1:${server.address().port}/`, token: 'fixture' }
  let acknowledgementObserved = false
  const fixture = { path: 'source', incarnation: 'source-A', pendingAppend: {
    then(resolve) { acknowledgementObserved = true; boundaries.set('cut-B', 4); resolve() },
  } }
  const restored = await recoverCaptured(config, c, fixture, 'snapshot', boundaries)
  assert.ok(acknowledgementObserved)
  assert.equal(restored.capturedCount, 4)
  assert.equal((await recoverCaptured(config, c, fixture, 'full', boundaries)).capturedCount, 4)
  fault = 'cut'
  await assert.rejects(recoverCaptured(config, c, fixture, 'snapshot', boundaries), /image offset/)
  fault = 'drop'
  await assert.rejects(recoverCaptured(config, c, fixture, 'full', boundaries))
  await assert.rejects(recoverCaptured(config, c, fixture, 'snapshot', boundaries))
})

test('late failed requests remain in drain time rather than overstating achieved rate', () => {
  const result = summarizeArrivals([
    { scheduledMs: 0, completedMs: 10, queueMs: 0, value: { serviceMs: 10 } },
    { scheduledMs: 50, completedMs: 1000, error: 'timeout' },
  ], 100)
  assert.equal(result.elapsedThroughDrainMs, 1000)
  assert.equal(result.achievedPerSecond, 1)
  assert.equal(result.errors, 1)
})
