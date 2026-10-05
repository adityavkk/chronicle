// Fixed-arrival-rate recovery benchmark under advancing snapshot/write load.
// Intentionally separate from benchmark.mjs so its historical baseline is stable.
import assert from 'node:assert/strict'
import { createHash, randomUUID } from 'node:crypto'
import { mkdir, readFile, writeFile } from 'node:fs/promises'
import { availableParallelism, cpus, release, totalmem } from 'node:os'
import { dirname, resolve } from 'node:path'
import { performance } from 'node:perf_hooks'
import { pathToFileURL } from 'node:url'
import {
  append, apply, counters, decode, distribution, emptyState, event, expected,
  publish, request,
} from './benchmark.mjs'

const sleep = (ms) => new Promise((done) => setTimeout(done, ms))
const projection = 'state-v1'

export function fixedSchedule(rate, durationMs) {
  assert.ok(Number.isFinite(rate) && rate > 0)
  assert.ok(Number.isFinite(durationMs) && durationMs > 0)
  const interval = 1000 / rate
  return Array.from({ length: Math.ceil(durationMs / interval) }, (_, i) => i * interval)
}

// Dispatch against absolute due times. Busy arrivals wait in a bounded FIFO;
// once that FIFO is full they are explicitly counted as missed, never dropped.
export async function dispatchSchedule({ schedule, maxInflight, maxDeferred, task, now = () => performance.now(), wait = sleep, origin = now() }) {
  assert.ok(Number.isSafeInteger(maxInflight) && maxInflight > 0)
  assert.ok(Number.isSafeInteger(maxDeferred) && maxDeferred >= 0)
  const pending = []
  const records = []
  let active = 0
  let timerDone = false
  let finish
  const completed = new Promise((done) => { finish = done })
  const check = () => { if (timerDone && active === 0 && pending.length === 0) finish(records) }
  const launch = (arrival) => {
    active++
    const dispatched = now()
    const record = { ...arrival, disposition: arrival.deferred ? 'deferred' : 'on-time', dispatchedMs: dispatched - origin,
      queueMs: Math.max(0, dispatched - arrival.dueAbsolute) }
    records.push(record)
    Promise.resolve().then(() => task(record)).then(
      (value) => { record.value = value },
      (error) => { record.error = String(error?.stack ?? error) },
    ).finally(() => {
      record.completedMs = now() - origin
      active--
      if (pending.length) launch(pending.shift())
      check()
    })
  }
  for (let i = 0; i < schedule.length; i++) {
    const dueAbsolute = origin + schedule[i]
    await wait(Math.max(0, dueAbsolute - now()))
    // Timers truncate fractional delays and may wake early. Never offer a
    // request before its due time or subtract that early start from latency.
    while (now() < dueAbsolute) await wait(dueAbsolute - now())
    const arrival = { index: i, scheduledMs: schedule[i], dueAbsolute, deferred: false }
    if (active < maxInflight) launch(arrival)
    else if (pending.length < maxDeferred) { arrival.deferred = true; pending.push(arrival) }
    else records.push({ ...arrival, disposition: 'missed', queueMs: null })
  }
  timerDone = true
  check()
  return completed
}

export function summarizeArrivals(records, durationMs) {
  const completed = records.filter((r) => r.completedMs !== undefined && !r.error)
  const errors = records.filter((r) => r.error)
  const missed = records.filter((r) => r.disposition === 'missed')
  const deferred = records.filter((r) => r.disposition === 'deferred')
  const elapsedMs = Math.max(durationMs, ...records.map((r) => r.completedMs ?? 0))
  return {
    offered: records.length, completed: completed.length, errors: errors.length,
    missed: missed.length, deferred: deferred.length,
    offeredPerSecond: records.length * 1000 / durationMs,
    achievedPerSecond: completed.length * 1000 / elapsedMs,
    elapsedThroughDrainMs: elapsedMs,
    queueMs: completed.length ? distribution(completed.map((r) => r.queueMs)) : null,
    endToEndMs: completed.length ? distribution(completed.map((r) => r.completedMs - r.scheduledMs)) : null,
    serviceMs: completed.length ? distribution(completed.map((r) => r.value.serviceMs)) : null,
  }
}

export function validateCaptured(c, changes, count) {
  assert.equal(changes.length, count)
  const state = emptyState()
  for (const change of changes) apply(state, change)
  assert.deepEqual(state, expected(c, count))
  return state
}

async function createStream(config, path) {
  const created = await request(config, path, 'PUT', undefined, { 'Content-Type': 'application/json' })
  assert.equal(created.status, 201, created.body.toString())
  const head = await request(config, path, 'HEAD')
  assert.equal(head.status, 200)
  assert.equal(head.headers.get('stream-snapshot'), 'v1')
  const incarnation = head.headers.get('stream-incarnation')
  assert.ok(incarnation)
  return { path, incarnation }
}

async function read(config, fixture, offset, incarnation = '') {
  const response = await request(config, `${fixture.path}?offset=${encodeURIComponent(offset)}`, 'GET', undefined,
    incarnation ? { 'If-Stream-Incarnation': incarnation } : {})
  assert.equal(response.status, 200, response.body.subarray(0, 200).toString())
  assert.equal(response.headers.get('stream-snapshot'), 'v1')
  const actual = response.headers.get('stream-incarnation')
  assert.ok(actual)
  if (incarnation) assert.equal(actual, incarnation)
  assert.equal(response.headers.get('stream-up-to-date'), 'true', 'campaign requires unbounded catch-up')
  const next = response.headers.get('stream-next-offset')
  assert.ok(next)
  return { changes: JSON.parse(response.body), next, incarnation: actual, bytes: response.body.length }
}

export async function recoverCaptured(config, c, fixture, mode, boundaries) {
  const started = performance.now()
  let changes = []
  let bytes = 0
  let requests = 0
  let state = emptyState()
  let terminal
  let imageCount
  let imageOffset
  if (mode === 'snapshot') {
    const saved = await request(config, `${fixture.path}?snapshot=${projection}`)
    assert.equal(saved.status, 200, saved.body.toString())
    assert.equal(saved.headers.get('stream-snapshot'), 'v1')
    assert.equal(saved.headers.get('stream-incarnation'), fixture.incarnation)
    requests++
    bytes += saved.body.length
    const actualDigest = `sha-256=:${createHash('sha256').update(saved.body).digest('base64')}:`
    assert.equal(saved.headers.get('content-digest'), actualDigest)
    state = decode(saved.body)
    imageCount = state.nextSeq
    imageOffset = saved.headers.get('stream-snapshot-offset')
    assert.ok(imageOffset)
    const suffix = await read(config, fixture, imageOffset, fixture.incarnation)
    for (const change of suffix.changes) apply(state, change)
    changes = suffix.changes
    bytes += suffix.bytes
    requests++
    terminal = suffix.next
  } else {
    const full = await read(config, fixture, '-1', fixture.incarnation)
    changes = full.changes
    for (const change of changes) apply(state, change)
    bytes = full.bytes
    requests = 1
    terminal = full.next
  }
  // Both paths include parsing/folding; independent oracle work is outside
  // service latency (but still contributes to client load and completion time).
  const serviceMs = performance.now() - started
  // An append can be visible to GET before its HTTP acknowledgement reaches
  // the writer. Wait for that in-flight append's actual returned offset rather
  // than inventing an offset or treating the acknowledgement race as corruption.
  if (!boundaries.has(terminal) && fixture.pendingAppend) await fixture.pendingAppend
  const capturedCount = boundaries.get(terminal)
  assert.notEqual(capturedCount, undefined, `unknown opaque source boundary ${terminal}`)
  if (mode === 'snapshot') {
    assert.equal(imageCount, boundaries.get(imageOffset), 'image offset does not match its state')
  }
  assert.deepEqual(state, expected(c, capturedCount))
  return { serviceMs, bytes, requests, capturedCount, terminal }
}

async function runPath(config, options, mode, root) {
  const c = { keys: options.keys, payload: options.payload }
  const source = await createStream(config, `${root}/${mode}/source`)
  const unrelated = await createStream(config, `${root}/${mode}/unrelated`)
  const boundaries = new Map([['-1', 0]])
  let count = 0
  let state = emptyState()
  let etag = ''
  const appendSamples = []
  const publishSamples = []
  const seedEnd = options.seedEvents
  for (let start = 0; start < seedEnd; start += 50) {
    const batch = Array.from({ length: Math.min(50, seedEnd - start) }, (_, i) => event(c, start + i))
    const offset = await append(config, source.path, batch)
    for (const change of batch) apply(state, change)
    count += batch.length
    boundaries.set(offset, count)
  }
  let published = await publish(config, source, state, [...boundaries.keys()].at(-1), etag)
  assert.equal(published.response.status, 201, published.response.body.toString())
  etag = published.response.headers.get('etag')
  assert.ok(published.sample.bytes > 700 * 1024 && published.sample.bytes < 1024 * 1024,
    `expected realistic near-limit image, got ${published.sample.bytes}`)

  let unrelatedCount = 0
  const mutationSchedule = fixedSchedule(options.mutationRate, options.durationMs)
  const publicationEvery = options.publicationEvery
  const samples = []
  let peakRSS = process.memoryUsage.rss()
  let peakHeap = process.memoryUsage().heapUsed
  let sampling = true
  const before = await counters(config)
  let serverPeakRSS = before.serverRSSBytes
  let redisPeakMemory = before.redisMemoryBytes
  const memorySampler = (async () => { while (sampling) { const m = process.memoryUsage(); peakRSS = Math.max(peakRSS, m.rss); peakHeap = Math.max(peakHeap, m.heapUsed); await sleep(25) } })()
  const serviceMemorySampler = (async () => {
    while (sampling) {
      await sleep(250)
      if (!sampling) break
      const observed = await counters(config)
      serverPeakRSS = Math.max(serverPeakRSS, observed.serverRSSBytes)
      redisPeakMemory = Math.max(redisPeakMemory, observed.redisMemoryBytes)
    }
  })()
  const origin = performance.now()
  const mutationRun = dispatchSchedule({ schedule: mutationSchedule, maxInflight: 1, maxDeferred: options.maxDeferred,
    origin,
    task: async ({ index }) => {
      const isUnrelated = index % 3 === 1
      const target = isUnrelated ? unrelated : source
      const indexInStream = isUnrelated ? unrelatedCount : count
      const change = event(c, indexInStream)
      const begin = performance.now()
      const operation = append(config, target.path, [change]).then((offset) => {
        appendSamples.push({ kind: isUnrelated ? 'unrelated' : 'source', ms: performance.now() - begin })
        if (isUnrelated) unrelatedCount++
        else {
          apply(state, change)
          count++
          boundaries.set(offset, count) // chronological boundary; offsets remain opaque.
        }
        return offset
      })
      if (!isUnrelated) source.pendingAppend = operation
      const offset = await operation
      if (!isUnrelated) source.pendingAppend = null
      if (!isUnrelated && index % publicationEvery === 0) {
        published = await publish(config, source, state, offset, etag)
        assert.equal(published.response.status, 200, published.response.body.toString())
        etag = published.response.headers.get('etag')
        publishSamples.push(published.sample.totalMs)
      }
      return { serviceMs: performance.now() - begin, kind: isUnrelated ? 'unrelated' : 'source' }
    } })

  const arrivals = await dispatchSchedule({ schedule: fixedSchedule(options.recoveryRate, options.durationMs),
    origin,
    maxInflight: options.maxInflight, maxDeferred: options.maxDeferred,
    task: async () => { const sample = await recoverCaptured(config, c, source, mode, boundaries); samples.push(sample); return sample } })
  const mutationArrivals = await mutationRun
  const after = await counters(config)
  sampling = false
  await Promise.all([memorySampler, serviceMemorySampler])
  serverPeakRSS = Math.max(serverPeakRSS, after.serverRSSBytes)
  redisPeakMemory = Math.max(redisPeakMemory, after.redisMemoryBytes)
  return {
    mode, arrivals, mutationArrivals,
    recovery: summarizeArrivals(arrivals, options.durationMs),
    mutations: summarizeArrivals(mutationArrivals, options.durationMs),
    appendLatencyMs: Object.fromEntries(['source', 'unrelated'].map((kind) => {
      const values = appendSamples.filter((s) => s.kind === kind).map((s) => s.ms)
      return [kind, values.length ? distribution(values) : null]
    })),
    publishLatencyMs: publishSamples.length ? distribution(publishSamples) : null,
    samples, appendSamples, publishSamples, imageBytes: published.sample.bytes,
    before, after,
    memory: { clientPeakRSSBytes: peakRSS, clientPeakHeapUsedBytes: peakHeap,
      serverSampledPeakRSSBytes: serverPeakRSS, redisSampledPeakUsedMemoryBytes: redisPeakMemory,
      sampling: 'client sampled every 25ms; Chronicle RSS and Redis used_memory every 250ms plus endpoints, so sub-interval peaks may be missed; INFO sampling adds slight load; no worker threads are used' },
  }
}

async function main() {
  const args = process.argv.slice(2)
  const option = (name, fallback) => { const i = args.indexOf(name); return i < 0 ? fallback : args[i + 1] }
  const options = {
    durationMs: Number(option('--duration-ms', '30000')), recoveryRate: Number(option('--recovery-rate', '20')),
    mutationRate: Number(option('--mutation-rate', '20')), maxInflight: Number(option('--max-inflight', '8')),
    maxDeferred: Number(option('--max-deferred', '32')), publicationEvery: Number(option('--publication-every', '10')),
    seedEvents: Number(option('--seed-events', '100000')), keys: 1800, payload: 384,
  }
  for (const [name, value] of Object.entries(options)) {
    assert.ok(Number.isFinite(value) && (name === 'maxDeferred' ? value >= 0 : value > 0), `invalid ${name}`)
  }
  const config = { base: process.env.SNAPSHOT_BENCH_URL ?? 'http://127.0.0.1:4438/v1/stream/',
    metrics: process.env.SNAPSHOT_BENCH_METRICS ?? 'http://127.0.0.1:9098/metrics',
    redisPort: Number(process.env.SNAPSHOT_BENCH_REDIS_PORT ?? '6381'),
    token: process.env.SNAPSHOT_BENCH_TOKEN ?? 'snapshot-bench-local-only' }
  for (const url of [config.base, config.metrics]) assert.ok(['127.0.0.1', 'localhost', '[::1]'].includes(new URL(url).hostname))
  const root = `snapshot-bench/contention-${randomUUID()}`
  const order = option('--order', 'full,snapshot').split(',')
  assert.deepEqual([...order].sort(), ['full', 'snapshot'], '--order must contain full and snapshot exactly once')
  const paths = ['full/source', 'full/unrelated', 'snapshot/source', 'snapshot/unrelated'].map((p) => `${root}/${p}`)
  const hashes = {}
  for (const path of [option('--binary', '.tmp/snapshot-bench/chronicle'),
    'handler_snapshot.go', 'store/snapshot.go', 'store/redis/snapshot.go',
    'store/redis/scripts/snapshot_get.lua', 'store/redis/scripts/snapshot_put.lua',
    'store/redis/scripts/snapshot_delete.lua', 'benchmarks/snapshots/benchmark.mjs',
    'benchmarks/snapshots/contention.mjs']) {
    hashes[path] = createHash('sha256').update(await readFile(path)).digest('hex')
  }
  const result = { format: 1, complete: false, options, order, hashes,
    environment: { node: process.version, platform: process.platform, arch: process.arch,
      release: release(), cpus: availableParallelism(), cpuModel: cpus()[0]?.model, totalMemoryBytes: totalmem() },
    startedAt: new Date().toISOString(), paths: {} }
  const output = resolve(option('--out', 'benchmarks/snapshots/results/contention-local.json'))
  const save = async () => {
    await mkdir(dirname(output), { recursive: true })
    await writeFile(output, JSON.stringify(result, null, 2) + '\n')
  }
  await save()
  try {
    // Identical generated arrival schedules and duration; fresh fixtures avoid
    // one path inheriting the other path's warmed/mutated state.
    for (const mode of order) {
      result.paths[mode] = await runPath(config, options, mode, root)
      await save()
      for (const suffix of ['source', 'unrelated']) {
        const deleted = await request(config, `${root}/${mode}/${suffix}`, 'DELETE')
        assert.equal(deleted.status, 204)
      }
    }
    result.complete = !['full', 'snapshot'].some((mode) => result.paths[mode].recovery.errors || result.paths[mode].mutations.errors)
    result.finishedAt = new Date().toISOString()
    await save()
    assert.ok(result.complete, `campaign recorded failures in ${output}`)
    console.log(`PASS: fixed-arrival contention results: ${output}`)
  } catch (error) {
    result.fatalError = String(error?.stack ?? error)
    result.finishedAt = new Date().toISOString()
    await save()
    throw error
  } finally {
    await Promise.all(paths.map(async (path) => {
      const response = await request(config, path, 'DELETE')
      assert.ok([204, 404].includes(response.status), `cleanup failed: ${path}`)
    }))
  }
}

if (process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === import.meta.url) await main()
