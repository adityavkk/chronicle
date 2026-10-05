// Real TCP HTTP -> Chronicle -> Redis, with a fresh JS projection per recovery.
// No Electric SDK code is exercised. See README.md for limits and reproduction.
import assert from 'node:assert/strict'
import { createHash, randomUUID } from 'node:crypto'
import { execFile } from 'node:child_process'
import { mkdir, readFile, writeFile } from 'node:fs/promises'
import { availableParallelism, cpus, release, totalmem } from 'node:os'
import { dirname, resolve } from 'node:path'
import { performance } from 'node:perf_hooks'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { promisify } from 'node:util'
import { isMainThread, parentPort, Worker, workerData } from 'node:worker_threads'

const exec = promisify(execFile)
const sleep = (ms) => new Promise((done) => setTimeout(done, ms))
const hash = (bytes) => createHash('sha256').update(bytes).digest('hex')
const digest = (bytes) => `sha-256=:${createHash('sha256').update(bytes).digest('base64')}:`
const codec = 'benchmark-state/v1'
const projection = 'state-v1'
const maxImageBytes = 1 << 20

export const scenarios = [
  { name: 'short', events: 100, cut: 90, keys: 10, payload: 128 },
  { name: 'updates-10k', events: 10000, cut: 9900, keys: 100, payload: 256 },
  { name: 'updates-100k', events: 100000, cut: 99900, keys: 100, payload: 256 },
  { name: 'stale-half-history', events: 100000, cut: 50000, keys: 100, payload: 256 },
  { name: 'near-limit-image', events: 100000, cut: 99900, keys: 1800, payload: 384 },
  { name: 'large-events', events: 10000, cut: 9900, keys: 100, payload: 4096 },
  { name: 'append-only', events: 2500, cut: 2400, keys: 0, payload: 192 },
  { name: 'oversize-fallback', events: 6000, cut: 5900, keys: 0, payload: 192, oversize: true },
  { name: 'version-miss', events: 10000, cut: 9900, keys: 100, payload: 256, miss: true },
  { name: 'pending-raw-half', events: 100000, cut: 99900, keys: 100, payload: 256, rawFrom: 50000 },
  { name: 'eight-readers', events: 100000, cut: 99900, keys: 100, payload: 256, concurrency: 8 },
  { name: 'append-pressure', events: 100000, cut: 99900, keys: 100, payload: 256, concurrency: 4, pressure: true },
  { name: 'short-plus-5ms', events: 100, cut: 90, keys: 10, payload: 128, addedRTTMs: 5 },
  { name: 'updates-plus-5ms', events: 100000, cut: 99900, keys: 100, payload: 256, addedRTTMs: 5 },
]

function payload(c, index) {
  return `${index}:`.padEnd(c.payload, 'x')
}

export function event(c, index) {
  const key = c.keys ? index % c.keys : index
  const cycle = c.keys ? Math.floor(index / c.keys) : 0
  const deleted = c.keys > 0 && cycle % 11 === 7
  return {
    type: 'item', key: String(key), index,
    headers: { operation: deleted ? 'delete' : cycle === 0 ? 'insert' : 'update' },
    value: deleted ? null : { revision: index, text: payload(c, index) },
  }
}

export function emptyState() {
  return { nextSeq: 0, rows: new Map() }
}

export function apply(state, change) {
  // A skipped, duplicated, or reordered event must fail, not produce a fast run.
  assert.equal(change.index, state.nextSeq++)
  if (change.headers.operation === 'delete') {
    state.rows.delete(change.key)
  } else {
    state.rows.set(change.key, {
      value: change.value,
      _seq: change.index,
      _first: state.rows.get(change.key)?._first ?? change.index,
    })
  }
}

export function encode(state) {
  return Buffer.from(JSON.stringify({ codec, nextSeq: state.nextSeq, rows: [...state.rows] }))
}

export function decode(body) {
  const image = JSON.parse(body)
  assert.equal(image.codec, codec)
  assert.ok(Number.isSafeInteger(image.nextSeq) && image.nextSeq >= 0)
  const rows = new Map(image.rows)
  assert.equal(rows.size, image.rows.length)
  return { nextSeq: image.nextSeq, rows }
}

// Independent oracle: derive the most recent event and first surviving insert
// per key. Does not call apply(), replay the history, or load the saved image.
export function expected(c, count) {
  const rows = []
  for (let key = 0; key < Math.min(c.keys || count, count); key++) {
    const cycle = c.keys ? Math.floor((count - 1 - key) / c.keys) : 0
    if (c.keys && cycle % 11 === 7) continue
    const index = c.keys ? cycle * c.keys + key : key
    const lastDeletion = cycle < 7 ? -1 : 7 + 11 * Math.floor((cycle - 7) / 11)
    const first = c.keys ? (lastDeletion + 1) * c.keys + key : key
    rows.push([String(key), {
      value: { revision: index, text: payload(c, index) }, _seq: index, _first: first,
    }])
  }
  rows.sort((a, b) => a[1]._first - b[1]._first)
  return { nextSeq: count, rows: new Map(rows) }
}

export function percentile(values, fraction) {
  const sorted = [...values].sort((a, b) => a - b)
  return sorted[Math.max(0, Math.ceil(sorted.length * fraction) - 1)]
}

export function distribution(values) {
  return {
    n: values.length, min: Math.min(...values), p50: percentile(values, 0.5),
    p95: percentile(values, 0.95), max: Math.max(...values),
    mean: values.reduce((a, b) => a + b, 0) / values.length,
  }
}

export async function request(config, path, method = 'GET', body, headers = {}, addedRTTMs = 0) {
  if (addedRTTMs) await sleep(addedRTTMs)
  const response = await fetch(config.base + path, {
    method, body, signal: AbortSignal.timeout(60000),
    headers: { Authorization: `Bearer ${config.token}`, ...headers },
  })
  const bytes = Buffer.from(await response.arrayBuffer())
  assert.equal(response.headers.get('content-encoding'), null, 'unexpected compression')
  return { status: response.status, headers: response.headers, body: bytes }
}

export async function append(config, path, changes) {
  const response = await request(config, path, 'POST', JSON.stringify(changes), { 'Content-Type': 'application/json' })
  assert.equal(response.status, 204, response.body.toString())
  const offset = response.headers.get('stream-next-offset')
  assert.ok(offset)
  return offset
}

export async function publish(config, fixture, state, offset, etag = '') {
  const start = performance.now()
  const body = encode(state) // Freeze/export and integrity computation ARE timed.
  const headers = {
    'Content-Type': 'application/json', 'Stream-Snapshot': 'v1',
    'Stream-Snapshot-Offset': offset, 'If-Stream-Incarnation': fixture.incarnation,
    'Content-Digest': digest(body),
    ...(etag ? { 'If-Match': etag } : { 'If-None-Match': '*' }),
  }
  const exportMs = performance.now() - start
  const response = await request(config, fixture.path + '?snapshot=' + projection, 'PUT', body, headers)
  return {
    response, sample: { totalMs: performance.now() - start, exportMs, bytes: body.length },
  }
}

async function seed(config, c, path) {
  const created = await request(config, path, 'PUT', undefined, { 'Content-Type': 'application/json' })
  assert.equal(created.status, 201, created.body.toString())
  const head = await request(config, path, 'HEAD')
  assert.equal(head.status, 200)
  assert.equal(head.headers.get('stream-snapshot'), 'v1')
  const fixture = { path, incarnation: head.headers.get('stream-incarnation'), rawOffset: '-1' }
  assert.ok(fixture.incarnation)
  const boundaries = [...new Set([c.cut, c.events, c.rawFrom ?? 0])].filter((n) => n > 0).sort((a, b) => a - b)
  let count = 0
  for (const boundary of boundaries) {
    while (count < boundary) {
      const end = Math.min(boundary, count + 50)
      const batch = Array.from({ length: end - count }, (_, i) => event(c, count + i))
      fixture.tail = await append(config, path, batch)
      count = end
    }
    if (boundary === c.rawFrom) fixture.rawOffset = fixture.tail
    if (boundary === c.cut) {
      fixture.cutOffset = fixture.tail
      // Prepare the image by actual HTTP replay, not from the expected oracle.
      const cut = await recover(config, { ...c, events: c.cut, rawFrom: undefined }, fixture, 'full')
      const { response, sample } = await publish(config, fixture, cut.state, fixture.tail)
      assert.equal(response.status, c.oversize ? 413 : 201, response.body.toString())
      fixture.imageBytes = sample.bytes
      fixture.initialPublication = sample
      fixture.etag = response.headers.get('etag')
      assert.equal(sample.bytes > maxImageBytes, !!c.oversize)
    }
  }
  return fixture
}

export async function recover(config, c, fixture, mode) {
  let state = emptyState()
  let offset = '-1'
  let incarnation = ''
  let bytes = 0
  let requests = 0
  let applied = 0
  let imageHit = false
  let nextOffset
  const raw = []
  const cpu = process.threadCpuUsage()
  const start = performance.now()
  if (mode === 'snapshot') {
    const saved = await request(config, fixture.path + '?snapshot=' + (c.miss ? 'state-v2' : projection), 'GET', undefined, {}, c.addedRTTMs)
    bytes += saved.body.length
    requests++
    if (saved.status === 200) {
      assert.equal(saved.headers.get('stream-snapshot'), 'v1')
      assert.equal(saved.headers.get('content-digest'), digest(saved.body))
      assert.ok(saved.headers.get('etag')?.startsWith('"'))
      incarnation = saved.headers.get('stream-incarnation')
      offset = saved.headers.get('stream-snapshot-offset')
      assert.ok(incarnation && offset)
      state = decode(saved.body)
      imageHit = true
    } else {
      assert.equal(saved.status, 404, `snapshot failure is not a cache miss: ${saved.status}`)
      assert.ok(c.miss || c.oversize, 'unexpected snapshot miss')
    }
  }
  async function read(from, consume) {
    for (;;) {
      const response = await request(config, fixture.path + '?offset=' + encodeURIComponent(from), 'GET', undefined,
        incarnation ? { 'If-Stream-Incarnation': incarnation } : {}, c.addedRTTMs)
      requests++
      bytes += response.body.length
      assert.equal(response.status, 200, response.body.subarray(0, 200).toString())
      assert.equal(response.headers.get('stream-snapshot'), 'v1')
      const actualIncarnation = response.headers.get('stream-incarnation')
      assert.ok(actualIncarnation)
      if (incarnation) assert.equal(actualIncarnation, incarnation)
      incarnation = actualIncarnation
      const changes = JSON.parse(response.body)
      assert.ok(Array.isArray(changes))
      for (const change of changes) consume(change)
      const next = response.headers.get('stream-next-offset')
      assert.ok(next)
      if (response.headers.get('stream-up-to-date') === 'true') return next
      assert.notEqual(next, from, 'read failed to advance')
      from = next
    }
  }
  nextOffset = await read(offset, (change) => {
    apply(state, change)
    applied++
    if (!imageHit && c.rawFrom !== undefined && change.index >= c.rawFrom) raw.push(change)
  })
  if (imageHit && c.rawFrom !== undefined) {
    // A separate raw-input consumer must not apply this overlapping window twice.
    const rawTail = await read(fixture.rawOffset, (change) => raw.push(change))
    assert.equal(rawTail, nextOffset)
  }
  const sample = {
    totalMs: performance.now() - start, bytes, requests, applied, rawEvents: raw.length, imageHit,
  }
  const used = process.threadCpuUsage(cpu)
  sample.clientThreadCPUms = (used.user + used.system) / 1000
  // Equivalence assertions are deliberately outside latency, but never skipped.
  assert.equal(nextOffset, fixture.tail)
  const oracle = expected(c, c.events)
  assert.equal(state.nextSeq, oracle.nextSeq)
  assert.deepEqual([...state.rows], [...oracle.rows])
  if (c.rawFrom !== undefined) {
    assert.equal(raw.length, c.events - c.rawFrom)
    for (let i = 0; i < raw.length; i++) assert.deepEqual(raw[i], event(c, c.rawFrom + i))
  }
  return { state, sample }
}

export async function counters(config) {
  const [metrics, redis] = await Promise.all([
    fetch(config.metrics, { signal: AbortSignal.timeout(10000) }).then(async (r) => {
      assert.equal(r.status, 200)
      return r.text()
    }),
    exec('redis-cli', ['-h', '127.0.0.1', '-p', String(config.redisPort), '--raw', 'INFO', 'ALL']),
  ])
  const value = (text, name, delimiter) => {
    const found = text.match(new RegExp(`^${name}${delimiter}([0-9.e+]+)`, 'm'))
    assert.ok(found, `missing counter ${name}`)
    return Number(found[1])
  }
  return {
    serverCPUSeconds: value(metrics, 'process_cpu_seconds_total', ' '),
    serverAllocBytes: value(metrics, 'go_memstats_alloc_bytes_total', ' '),
    serverRSSBytes: value(metrics, 'process_resident_memory_bytes', ' '),
    redisCPUSeconds: value(redis.stdout, 'used_cpu_user', ':') + value(redis.stdout, 'used_cpu_sys', ':'),
    redisCommands: value(redis.stdout, 'total_commands_processed', ':'),
    redisMemoryBytes: value(redis.stdout, 'used_memory', ':'),
  }
}

function delta(before, after) {
  return Object.fromEntries(Object.keys(before).map((key) => [key, after[key] - before[key]]))
}

async function measuredPhase(config, c, fixture, mode, samples) {
  // One V8 isolate per concurrent reader; startup and warmup precede measurement.
  const workers = Array.from({ length: c.concurrency ?? 1 }, () => new Worker(new URL(import.meta.url), {
    workerData: { config, c, fixture, mode, samples },
  }))
  const writer = c.pressure ? new Worker(new URL(import.meta.url), {
    workerData: { kind: 'writer', config, path: fixture.path + '-pressure', c },
  }) : null
  const all = writer ? [...workers, writer] : workers
  try {
    await Promise.all(all.map((worker) => new Promise((done, reject) => {
      worker.once('error', reject)
      worker.once('message', (message) => message.ready ? done() : reject(new Error('worker not ready')))
    })))
    const before = await counters(config)
    const cpu = process.cpuUsage()
    const started = performance.now()
    const pressureResult = writer ? new Promise((done, reject) => {
      writer.once('error', reject)
      writer.once('message', done)
      writer.postMessage('go')
    }) : null
    const results = await Promise.all(workers.map((worker) => new Promise((done, reject) => {
      worker.once('error', reject)
      worker.once('message', done)
      worker.postMessage('go')
    })))
    writer?.postMessage('stop')
    const pressure = await pressureResult
    const wallMs = performance.now() - started
    const used = process.cpuUsage(cpu)
    const after = await counters(config)
    return {
      mode, wallMs, clientCPUSeconds: (used.user + used.system) / 1e6,
      counters: delta(before, after), before, after, pressure,
      samples: results.flatMap((result) => result.samples),
    }
  } finally {
    await Promise.all(all.map((worker) => worker.terminate()))
  }
}

async function publications(config, c, fixture, count) {
  // Real replacements at advancing cuts, not repeated identical no-op PUTs.
  const state = expected(c, c.events)
  let etag = fixture.etag
  const samples = []
  for (let i = 0; i < count; i++) {
    const change = event(c, c.events + i)
    const offset = await append(config, fixture.path, [change])
    apply(state, change)
    const { response, sample } = await publish(config, fixture, state, offset, etag)
    assert.equal(response.status, c.oversize ? 413 : 200, response.body.toString())
    etag = response.headers.get('etag')
    samples.push(sample)
  }
  if (!c.oversize) {
    const saved = await request(config, fixture.path + '?snapshot=' + projection)
    assert.equal(saved.status, 200)
    assert.equal(saved.headers.get('content-digest'), digest(saved.body))
    assert.deepEqual(decode(saved.body), expected(c, c.events + count))
  }
  return { rejected: !!c.oversize, latencyMs: distribution(samples.map((s) => s.totalMs)), samples }
}

function summarize(phases) {
  return Object.fromEntries(['full', 'snapshot'].map((mode) => {
    const selected = phases.filter((phase) => phase.mode === mode)
    const samples = selected.flatMap((phase) => phase.samples)
    const average = (field) => samples.reduce((sum, s) => sum + s[field], 0) / samples.length
    const cpuPerRecovery = (field) => selected.reduce((sum, p) => sum + p.counters[field], 0) * 1000 / samples.length
    return [mode, {
      latencyMs: distribution(samples.map((s) => s.totalMs)),
      bytes: average('bytes'), requests: average('requests'), applied: average('applied'),
      rawEvents: average('rawEvents'), clientThreadCPUms: average('clientThreadCPUms'),
      serverCPUms: cpuPerRecovery('serverCPUSeconds'), redisCPUms: cpuPerRecovery('redisCPUSeconds'),
      // Includes equivalence checks, unlike per-recovery latency; do not label fold-only CPU.
      clientCPUmsIncludingVerification: selected.reduce((sum, p) => sum + p.clientCPUSeconds, 0) * 1000 / samples.length,
      serverAllocatedBytes: selected.reduce((sum, p) => sum + p.counters.serverAllocBytes, 0) / samples.length,
      redisCommands: selected.reduce((sum, p) => sum + p.counters.redisCommands, 0) / samples.length,
    }]
  }))
}

async function main() {
  const args = process.argv.slice(2)
  const option = (name, fallback) => {
    const i = args.indexOf(name)
    return i < 0 ? fallback : args[i + 1]
  }
  const samples = Number(option('--samples', '20'))
  const rounds = Number(option('--rounds', '4'))
  const publishSamples = Number(option('--publish-samples', '30'))
  for (const n of [samples, rounds, publishSamples]) assert.ok(Number.isSafeInteger(n) && n > 0)
  const config = {
    base: process.env.SNAPSHOT_BENCH_URL ?? 'http://127.0.0.1:4438/v1/stream/',
    metrics: process.env.SNAPSHOT_BENCH_METRICS ?? 'http://127.0.0.1:9098/metrics',
    redisPort: Number(process.env.SNAPSHOT_BENCH_REDIS_PORT ?? '6381'),
    token: process.env.SNAPSHOT_BENCH_TOKEN ?? 'snapshot-bench-local-only',
  }
  // This tool creates and deletes fixtures; accidental remote runs are forbidden.
  for (const url of [config.base, config.metrics]) assert.ok(['127.0.0.1', 'localhost', '[::1]'].includes(new URL(url).hostname))
  assert.ok(config.base.endsWith('/'))
  const output = resolve(option('--out', 'benchmarks/snapshots/results/local.json'))
  const chosen = option('--cases', '').split(',').filter(Boolean)
  for (const name of chosen) assert.ok(scenarios.some((c) => c.name === name), `unknown scenario ${name}`)
  const selected = scenarios.filter((c) => !chosen.length || chosen.includes(c.name))
  const run = randomUUID()
  const revision = (await exec('git', ['rev-parse', 'HEAD'])).stdout.trim()
  const changedFiles = (await exec('git', ['ls-files', '-m', '-o', '--exclude-standard'])).stdout.trim().split('\n')
  const sourceHashes = {}
  for (const path of changedFiles.filter((p) => /\.(go|lua|mjs)$/.test(p))) sourceHashes[path] = hash(await readFile(path))
  const redis = (await exec('redis-cli', ['-h', '127.0.0.1', '-p', String(config.redisPort), '--raw', 'INFO', 'server'])).stdout
  const result = {
    format: 1, run, startedAt: new Date().toISOString(), complete: false,
    environment: {
      revision, dirty: changedFiles.some(Boolean), sourceHashes, harnessSHA256: hash(await readFile(fileURLToPath(import.meta.url))),
      node: process.version, os: release(), arch: process.arch, cpu: cpus()[0].model,
      availableCPUs: availableParallelism(), memoryBytes: totalmem(),
      redisVersion: redis.match(/^redis_version:(.*)\r?$/m)?.[1].trim(),
      goVersion: (await exec('go', ['version'])).stdout.trim(),
    },
    methodology: {
      samplesPerWorkerPerRound: samples, rounds, publishSamples, warmupsPerWorker: 2,
      transport: 'loopback HTTP/1.1, identity encoding; warm processes/connections, fresh materialized state',
      addedRTT: 'selected cases inject a fixed delay before each HTTP request; not a network emulator',
      latencyIncludes: 'HTTP, body transfer, JSON decode, digest verification, hydrate, fold, raw-input recovery',
      latencyExcludes: 'fixture setup, warmup, oracle assertions, publication, process startup, claim and handler execution',
      cpuCaveat: 'client thread CPU excludes oracle assertions but excludes other V8/native threads; whole-client CPU includes assertions; process counters include pressure writer when active',
      counterCaveat: 'server CPU has process-counter resolution; Redis commands include INFO and Lua subcommands; before/after RSS is not peak memory',
      pressure: '4 readers plus 50 events per 50ms on a separate source; each phase at least 3s; actual achieved rate and scheduling lag are recorded',
      workload: 'synthetic State-shaped changes; not Electric SDK/EntityStreamDB or production traces',
      ordering: 'alternating full/snapshot then snapshot/full by round',
    }, scenarios: [],
  }
  async function save() {
    await mkdir(dirname(output), { recursive: true })
    await writeFile(output, JSON.stringify(result, null, 2) + '\n')
  }
  await save()
  for (const c of selected) {
    console.log(`seeding ${c.name}: ${c.events} events`)
    const path = `snapshot-bench/${run}/${c.name}`
    try {
      const fixture = await seed(config, c, path)
      if (c.pressure) {
        const created = await request(config, path + '-pressure', 'PUT', undefined, { 'Content-Type': 'application/json' })
        assert.equal(created.status, 201)
      }
      const phases = []
      for (let round = 0; round < rounds; round++) {
        for (const mode of round % 2 ? ['snapshot', 'full'] : ['full', 'snapshot']) {
          console.log(`  round ${round + 1}/${rounds} ${mode}`)
          phases.push({ round, ...await measuredPhase(config, c, fixture, mode, samples) })
        }
      }
      const publication = await publications(config, c, fixture, publishSamples)
      const summary = summarize(phases)
      result.scenarios.push({ config: c, imageBytes: fixture.imageBytes, initialPublication: fixture.initialPublication, phases, publication, summary })
      console.log(`  p50 full=${summary.full.latencyMs.p50.toFixed(2)}ms snapshot=${summary.snapshot.latencyMs.p50.toFixed(2)}ms; image=${fixture.imageBytes}B`)
      await save()
    } finally {
      const response = await request(config, path, 'DELETE')
      assert.ok([204, 404].includes(response.status), 'fixture cleanup failed')
      if (c.pressure) {
        const response = await request(config, path + '-pressure', 'DELETE')
        assert.ok([204, 404].includes(response.status), 'pressure fixture cleanup failed')
      }
    }
  }
  result.complete = true
  result.finishedAt = new Date().toISOString()
  await save()
  console.log(`PASS: ${result.scenarios.length} scenarios; all recoveries verified. Results: ${output}`)
}

if (!isMainThread && workerData.kind === 'writer') {
  const { config, path, c } = workerData
  let stopped = false
  parentPort.on('message', (message) => { if (message === 'stop') stopped = true })
  parentPort.postMessage({ ready: true })
  parentPort.once('message', async () => {
    const started = performance.now()
    const latencies = []
    let maxScheduleLagMs = 0
    // Fixed offered load: 50 changes every 50ms, on a separate source. This
    // measures Redis/Chronicle contention without changing the recovery target.
    while (!stopped) {
      const due = started + latencies.length * 50
      await sleep(Math.max(0, due - performance.now()))
      if (stopped) break
      const batch = Array.from({ length: 50 }, (_, i) => event(c, latencies.length * 50 + i))
      maxScheduleLagMs = Math.max(maxScheduleLagMs, performance.now() - due)
      const begin = performance.now()
      await append(config, path, batch)
      latencies.push(performance.now() - begin)
    }
    parentPort.postMessage({
      targetEventsPerSecond: 1000, events: latencies.length * 50,
      elapsedMs: performance.now() - started, maxScheduleLagMs,
      appendLatencyMs: distribution(latencies), latencies,
    })
  })
} else if (!isMainThread) {
  const { config, c, fixture, mode, samples } = workerData
  for (let i = 0; i < 2; i++) await recover(config, c, fixture, mode)
  parentPort.postMessage({ ready: true })
  parentPort.once('message', async () => {
    const results = []
    const until = performance.now() + (c.pressure ? 3000 : 0)
    while (results.length < samples || performance.now() < until) {
      results.push((await recover(config, c, fixture, mode)).sample)
    }
    parentPort.postMessage({ samples: results })
  })
} else if (process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === import.meta.url) {
  await main()
}
