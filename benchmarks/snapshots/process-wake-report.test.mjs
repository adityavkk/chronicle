import assert from 'node:assert/strict'
import { test } from 'node:test'
import { summarizeProcessWake } from './process-wake-report.mjs'

function campaign() {
  const sample = (mode, phase, index, ms) => ({
    scenario: 'updates', mode, phase, sampleIndex: index, warmup: index < 0, status: 'passed',
    phaseOrder: phase === 'replay-first' ? 'replay,snapshot-full-raw-replay' : 'snapshot-full-raw-replay,replay',
    sourceHash: 'a'.repeat(64), sourceEventCount: 101, sourceHashScope: 'normalized semantic events',
    activation: { totalMs: ms, claimMs: 0.1, preloadMs: ms / 2, handlerMs: 0.3, scope: 'entry through done' },
    snapshot: { lookup: mode === 'replay' ? 'not-requested' : 'hit', imageBytes: mode === 'replay' ? null : 80,
      cut: mode === 'replay' ? null : 'cut-A', publication: null },
    traffic: Object.fromEntries(['snapshotLookup', 'snapshotPublication', 'fullReplay', 'suffix', 'other', 'total']
      .map((kind) => [kind, { requests: kind === 'total' ? 5 : 1, requestBytes: kind === 'total' ? 15 : 3,
        responseBytes: kind === 'total' ? 85 : 17 }])),
    resources: { scope: 'combined-node-process', cpuUserMicros: 2900, cpuSystemMicros: 300,
      heapBeforeBytes: 10, peakHeapBytes: 15, heapAfterBytes: 11,
      rssBeforeBytes: 20, peakRssBytes: 25, rssAfterBytes: 21, samplingIntervalMs: 2 },
    offsets: { inputHead: 'cut-A', finalHead: 'cut-B', acked: 'cut-B' },
    leaseState: { phase: 'idle', holder: '0', leaseUntilNs: '0' }, validationTimed: false,
    assertions: { state: true, hydratedState: true, rawInputs: true, persistedOutput: true,
      exactPersistedOutputs: true, activationCount: true, ack: true, release: true, signal: null, cancellation: true },
  })
  return {
    kind: 'chronicle-process-wake-benchmark-campaign', schemaVersion: 1,
    labels: { memoryScope: 'combined-node-process' }, pins: {},
    hashes: { before: { code: 'b'.repeat(64) }, after: { code: 'b'.repeat(64) }, drift: [] },
    campaign: { scenarios: ['updates'], measuredSamplesPerModeScenario: 4, validationTimed: false,
      warmupsPerRun: 1, phaseOrders: [['replay', 'snapshot-full-raw-replay'], ['snapshot-full-raw-replay', 'replay']] },
    failures: [],
    samples: ['replay-first', 'snapshot-first'].flatMap((phase) => [-1, 0, 1].flatMap((index) => {
      const ms = index < 0 ? 99999 : (phase === 'replay-first' ? [11, 73] : [27, 39])[index]
      return [sample('replay', phase, index, ms), sample('snapshot-full-raw-replay', phase, index, ms / 2)]
    })),
  }
}

test('matched observations exclude warmups and preserve phase order and metric scope', () => {
  const input = campaign()
  const before = structuredClone(input)
  const report = summarizeProcessWake(input)
  assert.deepEqual(input, before)
  const result = report.scenarios[0]
  assert.equal(report.labels.memoryScope, 'combined-node-process')
  assert.equal(result.completePairs, 4)
  assert.equal(result.warmups, 4)
  assert.equal(result.replayFirstPairs, 2)
  assert.equal(result.snapshotFirstPairs, 2)
  assert.equal(result.missingPhaseCoverage, false)
  assert.equal(result.pairedActivationDeltaMs.p50, -19.5)
  assert.equal(result.paths.replay.activation.totalMs.p50, 27)
  assert.equal(result.paths.replay.activation.totalMs.p95, 73)
  assert.equal(result.paths['snapshot-full-raw-replay'].activation.totalMs.p50, 13.5)
  assert.equal(result.paths.replay.cpuMs.p50, 3.2)
  assert.deepEqual(result.paths.replay.exercised, { signal: 0, cancellation: 4 })
  assert.equal(result.paths.replay.publicationMs, null)
})

test('failed pairs and unmatched survivors cannot enter paired statistics or meet targets', () => {
  const input = campaign()
  Object.assign(input.samples.at(-1), { status: 'failed', error: 'preload timeout', activation: null, resources: null })
  input.failures.push({ error: 'failed run', exitCode: 1 })
  const report = summarizeProcessWake(input)
  const result = report.scenarios[0]
  assert.equal(result.completePairs, 3)
  assert.equal(result.unpairedPassed, 1)
  assert.equal(result.belowTarget, true)
  assert.equal(result.paths.replay.passed, 4)
  assert.equal(result.paths.replay.activation.totalMs.n, 3)
  assert.equal(result.failures[0].error, 'preload timeout')
  assert.equal(report.runFailures[0].exitCode, 1)
})

test('missing warmups, incomplete attempts, and a missing phase order stay visible', () => {
  const input = campaign()
  input.samples = input.samples.filter((s) => !s.warmup)
  assert.equal(summarizeProcessWake(input).scenarios[0].missingPhaseCoverage, true)
  const incomplete = input.samples[0]
  Object.assign(incomplete, { status: 'incomplete', error: 'claim pending', activation: null })
  assert.equal(summarizeProcessWake(input).scenarios[0].failures[0].status, 'incomplete')
  const oneOrder = campaign()
  oneOrder.samples = oneOrder.samples.filter((s) => s.phase === 'replay-first')
  oneOrder.campaign.measuredSamplesPerModeScenario = 2
  const result = summarizeProcessWake(oneOrder).scenarios[0]
  assert.equal(result.belowTarget, false)
  assert.equal(result.missingPhaseCoverage, true)
})

test('rejects drift, duplicate identities, bad measurements, false success, and unreleased leases', () => {
  for (const mutate of [
    (d) => { d.hashes.after.code = 'c'.repeat(64) },
    (d) => { d.samples[2].sourceEventCount++ },
    (d) => { d.samples[2].sourceHash = 'c'.repeat(64) },
    (d) => { d.samples.push(structuredClone(d.samples[2])) },
    (d) => { d.samples[2].activation.totalMs = Number.NaN },
    (d) => { d.samples[2].activation.preloadMs = null },
    (d) => { d.samples[2].activation.claimMs = -1 },
    (d) => { delete d.samples[2].warmup },
    (d) => { d.samples[2].sampleIndex = -2 },
    (d) => { d.samples[2].assertions.hydratedState = false },
    (d) => { delete d.samples[2].assertions.hydratedState; d.samples[2].assertions.workloadState = true },
    (d) => { d.samples[2].offsets.acked = 'older-cut' },
    (d) => { d.samples[2].assertions.signal = false },
    (d) => { d.samples[2].leaseState.phase = 'live' },
    (d) => { d.samples[2].leaseState.holder = '123' },
    (d) => { d.samples[2].resources.scope = 'runtime-only' },
    (d) => { d.samples[2].resources.peakHeapBytes = 5 },
    (d) => { d.samples[2].traffic.total.responseBytes++ },
    (d) => { d.samples[2].snapshot.lookup = 'hit' },
    (d) => { d.samples[3].phaseOrder = 'snapshot-full-raw-replay,replay' },
  ]) {
    const input = campaign()
    mutate(input)
    assert.throws(() => summarizeProcessWake(input), String(mutate))
  }
})

test('empty campaigns expose missing coverage without inventing zero latency', () => {
  const input = campaign()
  input.samples = []
  const result = summarizeProcessWake(input).scenarios[0]
  assert.equal(result.belowTarget, true)
  assert.equal(result.paths.replay.activation.totalMs, null)
})

function checkpointCampaign() {
  const input = JSON.parse(JSON.stringify(campaign()).replaceAll('snapshot-full-raw-replay', 'snapshot-checkpointed-inputs'))
  input.campaign.modes = ['replay', 'snapshot-checkpointed-inputs']
  input.labels.rawInputRecovery = 'bounded-checkpoint-v1'
  for (const sample of input.samples) {
    sample.offsets.finalHead = sample.offsets.acked = '0000000000000002_0000000000000127'
    sample.assertions.noPrefixReads = sample.mode === 'replay' ? null : true
    sample.recovery = { contract: 'bounded-checkpoint-v1', checkpointHit: false, fallbackReason: null }
    if (sample.mode === 'replay') continue
    sample.snapshot.cut = '0000000000000002_0000000000000100'
    sample.recovery = {
      contract: 'bounded-checkpoint-v1', checkpointHit: true, fallbackReason: null, incarnation: 'inc-current',
      checkpointSourceCut: sample.snapshot.cut,
      processedThrough: '0000000000000002_0000000000000091', processedSeq: 37,
      stateCut: sample.snapshot.cut, suffixStartOffset: sample.snapshot.cut,
      sourceReads: [
        { offset: sample.snapshot.cut, nextOffset: sample.offsets.finalHead, incarnation: 'inc-current' },
        { offset: sample.offsets.finalHead, nextOffset: null, incarnation: 'inc-current' },
      ],
    }
  }
  return input
}

test('checkpoint statistics use the bounded contract and verify all guarded suffix reads, including aborted requests', () => {
  const input = checkpointCampaign()
  const result = summarizeProcessWake(input).scenarios[0]
  assert.equal(result.completePairs, 4)
  assert.equal(result.snapshotFirstPairs, 2)
  assert.equal(result.pairedActivationDeltaMs.p50, -19.5)
  const path = result.paths['snapshot-checkpointed-inputs']
  assert.equal(path.activation.totalMs.p50, 13.5)
  assert.equal(path.checkpointHits, 4)
  assert.equal(path.verifiedNoPrefixReads, 4)
  assert.deepEqual(path.checkpointFallbacks, [])
  assert.equal(result.paths.replay.verifiedNoPrefixReads, 0)
})

test('a claimed no-prefix result cannot hide a pre-cut GET or missing incarnation behind traffic categories', () => {
  const prefixOffsets = [null, '', '-1', 'now', '0000000000000002_0000000000000099', '0000000000000001_9999999999999999']
  for (const offset of prefixOffsets) {
    const input = checkpointCampaign()
    const sample = input.samples[3]
    // Even a classifier reporting no full replay cannot overrule a gate request.
    sample.traffic.suffix.requests += sample.traffic.fullReplay.requests
    sample.traffic.fullReplay.requests = 0
    sample.recovery.sourceReads.push({ offset, nextOffset: null, incarnation: 'inc-current' })
    assert.throws(() => summarizeProcessWake(input), `accepted prefix offset ${offset}`)
  }
  for (const mutate of [
    (d, s) => { delete d.labels.rawInputRecovery },
    (d, s) => { d.labels.rawInputRecovery = 'full-replay' },
    (d, s) => { delete s.recovery },
    (d, s) => { s.recovery.contract = 'full-replay' },
    (d, s) => { delete s.recovery.checkpointSourceCut },
    (d, s) => { s.recovery.checkpointSourceCut = s.offsets.finalHead },
    (d, s) => { s.recovery.processedThrough = s.offsets.finalHead },
    (d, s) => { s.recovery.processedThrough = '-1' },
    (d, s) => { s.recovery.processedSeq = -1 },
    (d, s) => { s.recovery.processedSeq = 3.5 },
    (d, s) => { s.recovery.processedSeq = Number.MAX_SAFE_INTEGER + 1 },
    (d, s) => { s.recovery.sourceReads = [] },
    (d, s) => { s.recovery.sourceReads[1].incarnation = null },
    (d, s) => { s.recovery.sourceReads[1].incarnation = 'inc-old' },
    (d, s) => { s.recovery.sourceReads[0].offset = s.offsets.finalHead },
    (d, s) => { s.recovery.sourceReads[0].nextOffset = '0000000000000002_0000000000000099' },
    (d, s) => { s.recovery.sourceReads[1].offset = '0000000000000002_0000000000000128' },
    (d, s) => { s.recovery.sourceReads[0].nextOffset = '0000000000000002_0000000000000128' },
    (d, s) => { s.recovery.stateCut = s.offsets.finalHead },
    (d, s) => { s.recovery.suffixStartOffset = s.offsets.finalHead },
    (d, s) => { s.recovery.fallbackReason = 'incompatible image' },
    (d, s) => { delete s.assertions.noPrefixReads },
  ]) {
    const input = checkpointCampaign()
    mutate(input, input.samples[3])
    assert.throws(() => summarizeProcessWake(input), String(mutate))
  }
})

test('prefix auditing does not round source offsets above JavaScript integer precision', () => {
  const input = checkpointCampaign()
  const sample = input.samples[3]
  const cut = '0000000000000002_9007199254740993'
  const head = '0000000000000002_9007199254741007'
  sample.snapshot.cut = sample.recovery.stateCut = sample.recovery.suffixStartOffset = cut
  sample.recovery.checkpointSourceCut = cut
  sample.offsets.finalHead = sample.offsets.acked = head
  sample.recovery.sourceReads = [{ offset: cut, nextOffset: head, incarnation: 'inc-current' }]
  assert.equal(summarizeProcessWake(input).scenarios[0].paths['snapshot-checkpointed-inputs'].checkpointHits, 4)
  sample.recovery.sourceReads.push({
    offset: '0000000000000002_9007199254740992', nextOffset: null, incarnation: 'inc-current',
  })
  assert.throws(() => summarizeProcessWake(input), /reads checkpoint prefix/)
})

test('checkpoint fallback remains a measured outcome, never a verified suffix-only hit', () => {
  const input = checkpointCampaign()
  const sample = input.samples.at(-1)
  sample.recovery = { contract: 'bounded-checkpoint-v1', checkpointHit: false, fallbackReason: 'image version mismatch' }
  sample.assertions.noPrefixReads = null
  const path = summarizeProcessWake(input).scenarios[0].paths['snapshot-checkpointed-inputs']
  assert.equal(path.activation.totalMs.n, 4)
  assert.equal(path.checkpointHits, 3)
  assert.equal(path.verifiedNoPrefixReads, 3)
  assert.deepEqual(path.checkpointFallbacks, [{ phase: 'snapshot-first', sampleIndex: 1, reason: 'image version mismatch' }])
  sample.assertions.noPrefixReads = true
  assert.throws(() => summarizeProcessWake(input), /no checkpoint hit/)
})
