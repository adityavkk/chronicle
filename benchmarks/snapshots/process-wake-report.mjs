// Independent statistics for the Electric harness's raw campaign JSON.
// Checks recorded evidence, not the truth of the harness's assertions.
import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { resolve } from 'node:path'
import { pathToFileURL } from 'node:url'
import { distribution } from './benchmark.mjs'

const fullReplayMode = 'snapshot-full-raw-replay'
const checkpointMode = 'snapshot-checkpointed-inputs'
const checks = ['state', 'hydratedState', 'rawInputs', 'persistedOutput', 'exactPersistedOutputs', 'activationCount', 'ack', 'release']
const trafficKinds = ['snapshotLookup', 'snapshotPublication', 'fullReplay', 'suffix', 'other']
const finite = (value) => Number.isFinite(value) && value >= 0
const count = (value) => Number.isSafeInteger(value) && value >= 0
const text = (value) => typeof value === 'string' && value.length > 0
const stats = (values) => values.length ? distribution(values) : null

// This audits Chronicle's gate evidence, not the opaque-offset Durable SDK API.
function compareChronicleOffsets(left, right) {
  for (const value of [left, right]) assert.match(value, /^\d+_\d+$/, 'missing concrete Chronicle offset')
  const a = left.split('_').map(BigInt)
  const b = right.split('_').map(BigInt)
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return a[i] < b[i] ? -1 : 1
  }
  return 0
}

function validateCheckpointReads(sample) {
  const recovery = sample.recovery
  assert.equal(recovery?.contract, 'bounded-checkpoint-v1', 'sample has wrong recovery contract')
  assert.equal(typeof recovery?.checkpointHit, 'boolean', 'missing checkpoint recovery outcome')
  if (!recovery.checkpointHit) {
    assert.notEqual(sample.assertions.noPrefixReads, true, 'no checkpoint hit to validate')
    if (sample.mode === checkpointMode) assert.ok(text(recovery.fallbackReason), 'fallback must have a reason')
    return
  }
  assert.equal(sample.mode, checkpointMode, 'replay cannot restore a checkpoint')
  assert.equal(sample.snapshot.lookup, 'hit', 'checkpoint was not fetched')
  assert.ok(recovery.fallbackReason == null, 'checkpoint hit also reports fallback')
  assert.equal(recovery.stateCut, sample.snapshot.cut, 'state cut differs from image cut')
  assert.equal(recovery.checkpointSourceCut, recovery.stateCut, 'checkpoint and image cuts are not aligned')
  assert.ok(count(recovery.processedSeq), 'invalid completed sequence')
  assert.ok(compareChronicleOffsets(recovery.processedThrough, recovery.stateCut) <= 0,
    'completed progress exceeds image cut')
  assert.equal(recovery.suffixStartOffset, recovery.stateCut, 'suffix skipped the image boundary')
  assert.ok(text(recovery.incarnation), 'missing checkpoint incarnation')
  assert.ok(Array.isArray(recovery.sourceReads) && recovery.sourceReads.length > 0, 'missing source GET evidence')
  assert.equal(recovery.sourceReads[0].offset, recovery.stateCut, 'first read must resume at image cut')
  for (const read of recovery.sourceReads) {
    assert.ok(compareChronicleOffsets(read.offset, recovery.stateCut) >= 0, 'source GET reads checkpoint prefix')
    assert.ok(compareChronicleOffsets(read.offset, sample.offsets.finalHead) <= 0, 'source GET starts beyond final head')
    assert.equal(read.incarnation, recovery.incarnation, 'source GET has missing or wrong incarnation guard')
    // An admitted error/aborted request can lack a response offset. Its request
    // still counts: omitting it would hide prefix reads that transferred no body.
    if (read.nextOffset !== null) {
      assert.ok(compareChronicleOffsets(read.nextOffset, read.offset) >= 0, 'source GET response offset regressed')
      assert.ok(compareChronicleOffsets(read.nextOffset, sample.offsets.finalHead) <= 0, 'source GET response exceeds final head')
    }
  }
  assert.equal(sample.assertions.noPrefixReads, true, 'missing no-prefix assertion')
}

export function summarizeProcessWake(data) {
  assert.equal(data.kind, 'chronicle-process-wake-benchmark-campaign')
  assert.equal(data.schemaVersion, 1)
  assert.ok(text(data.labels.memoryScope))
  const modes = data.campaign.modes ?? ['replay', fullReplayMode]
  const snapshotMode = modes.find((mode) => mode !== 'replay')
  assert.ok([fullReplayMode, checkpointMode].includes(snapshotMode), 'unknown snapshot mode')
  assert.deepEqual([...modes].sort(), ['replay', snapshotMode].sort(), 'campaign needs replay and one snapshot mode')
  if (snapshotMode === checkpointMode) assert.equal(data.labels.rawInputRecovery, 'bounded-checkpoint-v1')
  assert.deepEqual(data.hashes.drift, [], 'sources changed during campaign')
  assert.deepEqual(data.hashes.before, data.hashes.after, 'frozen source hashes differ')
  assert.ok(Object.keys(data.hashes.before).length > 0, 'missing frozen hashes')
  for (const value of Object.values(data.hashes.before)) assert.match(value, /^[a-f0-9]{64}$/)
  const target = data.campaign.measuredSamplesPerModeScenario
  assert.ok(count(target) && target > 0)
  assert.ok(count(data.campaign.warmupsPerRun) && data.campaign.warmupsPerRun > 0)
  const orders = data.campaign.phaseOrders.map((order) => {
    assert.deepEqual([...order].sort(), [...modes].sort())
    return order.join(',')
  })
  assert.equal(new Set(orders).size, 2, 'both phase orders are required')
  assert.equal(orders.length, 2)
  assert.equal(target % orders.length, 0, 'target must balance phase orders')
  assert.equal(data.campaign.validationTimed, false)
  assert.ok(Array.isArray(data.failures))
  const scenarios = data.campaign.scenarios
  assert.ok(scenarios.length > 0 && scenarios.every(text))
  assert.equal(new Set(scenarios).size, scenarios.length, 'duplicate scenario')
  const sources = new Map()
  const pairs = new Map()

  for (const sample of data.samples) {
    assert.ok(scenarios.includes(sample.scenario), 'unknown scenario')
    assert.ok(modes.includes(sample.mode), 'unknown recovery mode')
    assert.ok(text(sample.phase))
    assert.ok(orders.includes(sample.phaseOrder), 'unknown phase order')
    assert.equal(typeof sample.warmup, 'boolean', 'warmup classification is required')
    assert.ok(Number.isSafeInteger(sample.sampleIndex))
    assert.equal(sample.sampleIndex < 0, sample.warmup, 'warmup index disagrees with classification')
    assert.ok(['passed', 'failed', 'incomplete'].includes(sample.status))
    const key = JSON.stringify([sample.scenario, sample.phase, sample.sampleIndex])
    const pair = pairs.get(key) ?? new Map()
    assert.ok(!pair.has(sample.mode), 'duplicate sample identity')
    for (const other of pair.values()) assert.equal(sample.phaseOrder, other.phaseOrder, 'pair order differs')
    pair.set(sample.mode, sample)
    pairs.set(key, pair)
    // Failed attempts may have partial measurements. Retain them, not fake zeros.
    if (sample.status !== 'passed') {
      assert.ok(text(sample.error), 'failed sample needs an error')
      continue
    }
    assert.ok(sample.error == null, 'successful sample contains an error')
    assert.match(sample.sourceHash, /^[a-f0-9]{64}$/)
    assert.ok(count(sample.sourceEventCount))
    assert.ok(text(sample.sourceHashScope))
    const source = { hash: sample.sourceHash, events: sample.sourceEventCount, scope: sample.sourceHashScope }
    if (sources.has(sample.scenario)) assert.deepEqual(source, sources.get(sample.scenario), 'fixture drift')
    sources.set(sample.scenario, source)
    for (const check of checks) assert.equal(sample.assertions[check], true, `missing ${check} validation`)
    for (const value of Object.values(sample.assertions)) assert.ok(value === true || value === null, 'failed assertion')
    assert.equal(sample.validationTimed, false)
    assert.ok(text(sample.offsets.finalHead))
    assert.equal(sample.offsets.acked, sample.offsets.finalHead, 'ack differs from final head')
    assert.deepEqual(sample.leaseState, { phase: 'idle', holder: '0', leaseUntilNs: '0' }, 'lease not released')
    assert.ok(text(sample.activation.scope))
    for (const field of ['totalMs', 'claimMs', 'preloadMs', 'handlerMs']) {
      assert.ok(finite(sample.activation[field]), `invalid ${field}`)
      assert.ok(sample.activation[field] <= sample.activation.totalMs, `${field} exceeds total`)
    }
    assert.ok(sample.activation.totalMs > 0)
    assert.equal(sample.resources.scope, data.labels.memoryScope)
    for (const field of ['cpuUserMicros', 'cpuSystemMicros', 'samplingIntervalMs']) assert.ok(finite(sample.resources[field]))
    for (const [type, peak] of [['heap', 'peakHeapBytes'], ['rss', 'peakRssBytes']]) {
      const values = [sample.resources[`${type}BeforeBytes`], sample.resources[peak], sample.resources[`${type}AfterBytes`]]
      assert.ok(values.every(count), `invalid ${type} measurement`)
      assert.ok(values[1] >= Math.max(values[0], values[2]), `${type} peak excludes an endpoint`)
    }
    for (const field of ['requests', 'requestBytes', 'responseBytes']) {
      for (const kind of [...trafficKinds, 'total']) assert.ok(count(sample.traffic[kind][field]), `invalid traffic ${field}`)
      assert.equal(trafficKinds.reduce((sum, kind) => sum + sample.traffic[kind][field], 0), sample.traffic.total[field],
        `traffic ${field} categories do not sum to total`)
    }
    assert.ok(['not-requested', 'hit', 'miss-or-error'].includes(sample.snapshot.lookup))
    assert.equal(sample.snapshot.lookup === 'not-requested', sample.mode === 'replay')
    if (sample.snapshot.lookup === 'hit') {
      assert.ok(count(sample.snapshot.imageBytes) && sample.snapshot.imageBytes > 0)
      assert.ok(text(sample.snapshot.cut))
    }
    if (snapshotMode === checkpointMode) validateCheckpointReads(sample)
    const publication = sample.snapshot.publication
    if (publication !== null) {
      assert.ok(count(publication.requestBytes))
      assert.ok(publication.durationMs === null || finite(publication.durationMs))
    }
  }

  const results = scenarios.map((scenario) => {
    const samples = data.samples.filter((s) => s.scenario === scenario)
    const measured = samples.filter((s) => !s.warmup)
    const complete = [...pairs.values()].filter((pair) => pair.size === 2 &&
      [...pair.values()].every((s) => s.scenario === scenario && !s.warmup && s.status === 'passed'))
    const phaseCoverage = orders.map((order) => ({
      order,
      pairs: complete.filter((pair) => pair.get('replay').phaseOrder === order).length,
      warmups: Object.fromEntries(modes.map((mode) => [mode, samples.filter((s) =>
        s.phaseOrder === order && s.mode === mode && s.warmup && s.status === 'passed').length])),
    }))
    return {
      scenario, source: sources.get(scenario) ?? null,
      completePairs: complete.length, belowTarget: complete.length < target,
      phaseCoverage,
      missingPhaseCoverage: phaseCoverage.some((phase) => phase.pairs < target / orders.length ||
        Object.values(phase.warmups).some((n) => n < data.campaign.warmupsPerRun)),
      warmups: samples.filter((s) => s.warmup).length,
      warmupFailures: samples.filter((s) => s.warmup && s.status !== 'passed').length,
      unpairedPassed: measured.filter((s) => s.status === 'passed').length - 2 * complete.length,
      replayFirstPairs: complete.filter((pair) => pair.get('replay').phaseOrder.startsWith('replay,')).length,
      snapshotFirstPairs: complete.filter((pair) => pair.get('replay').phaseOrder.startsWith(`${snapshotMode},`)).length,
      failures: samples.filter((s) => s.status !== 'passed'),
      pairedActivationDeltaMs: stats(complete.map((pair) =>
        pair.get(snapshotMode).activation.totalMs - pair.get('replay').activation.totalMs)),
      paths: Object.fromEntries(modes.map((mode) => {
        const attempts = measured.filter((s) => s.mode === mode)
        const matched = complete.map((pair) => pair.get(mode))
        return [mode, {
          attempted: attempts.length, passed: attempts.filter((s) => s.status === 'passed').length,
          failed: attempts.filter((s) => s.status !== 'passed').length,
          lookups: Object.fromEntries(['not-requested', 'hit', 'miss-or-error', 'unknown'].map((lookup) =>
            [lookup, attempts.filter((s) => (s.snapshot?.lookup ?? 'unknown') === lookup).length])),
          ...(snapshotMode === checkpointMode && {
            checkpointHits: matched.filter((s) => s.recovery.checkpointHit).length,
            checkpointFallbacks: matched.filter((s) => mode === checkpointMode && !s.recovery.checkpointHit)
              .map((s) => ({ phase: s.phase, sampleIndex: s.sampleIndex, reason: s.recovery.fallbackReason })),
            verifiedNoPrefixReads: matched.filter((s) => s.assertions.noPrefixReads === true).length,
          }),
          activation: Object.fromEntries(['totalMs', 'claimMs', 'preloadMs', 'handlerMs'].map((field) =>
            [field, stats(matched.map((s) => s.activation[field]))])),
          cpuMs: stats(matched.map((s) => (s.resources.cpuUserMicros + s.resources.cpuSystemMicros) / 1000)),
          peakHeapBytes: stats(matched.map((s) => s.resources.peakHeapBytes)),
          peakRssBytes: stats(matched.map((s) => s.resources.peakRssBytes)),
          traffic: Object.fromEntries([...trafficKinds, 'total'].map((kind) => [kind,
            Object.fromEntries(['requests', 'requestBytes', 'responseBytes'].map((field) =>
              [field, stats(matched.map((s) => s.traffic[kind][field]))]))])),
          publicationAttempts: matched.filter((s) => s.snapshot.publication !== null).length,
          publicationMs: stats(matched.map((s) => s.snapshot.publication?.durationMs).filter((v) => v != null)),
          exercised: Object.fromEntries(['signal', 'cancellation'].map((check) =>
            [check, matched.filter((s) => s.assertions[check] === true).length])),
        }]
      })),
    }
  })
  return {
    labels: data.labels, pins: data.pins, hashes: data.hashes, runFailures: data.failures,
    method: 'nearest-rank percentiles of complete passing pairs; warmups excluded; failures retained',
    scenarios: results,
  }
}

if (process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === import.meta.url) {
  assert.equal(process.argv.length, 3, 'usage: node process-wake-report.mjs <campaign.json>')
  const report = summarizeProcessWake(JSON.parse(await readFile(process.argv[2], 'utf8')))
  console.log(JSON.stringify(report, null, 2))
  if (report.runFailures.length || report.scenarios.some((s) =>
    s.belowTarget || s.missingPhaseCoverage || s.failures.length || s.unpairedPassed)) {
    process.exitCode = 1
  }
}
