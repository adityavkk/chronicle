#!/usr/bin/env node
import { createHash } from 'node:crypto'
import { readFile, writeFile } from 'node:fs/promises'
import { spawnSync } from 'node:child_process'
import { resolve } from 'node:path'

const required = (name) => {
  const value = process.env[name]
  if (!value) throw new Error(`${name} is required`)
  return value
}

const electricCheckout = resolve(required(`ELECTRIC_CHECKOUT`))
const durableCheckout = resolve(required(`DURABLE_STREAMS_CHECKOUT`))
const chronicleBinary = resolve(required(`CHRONICLE_BINARY`))
const outputPath = resolve(required(`CHRONICLE_BENCH_OUTPUT`))
const samplesPerPhase = Number(
  process.env.CHRONICLE_BENCH_SAMPLES_PER_PHASE ?? `15`
)
const warmupsPerRun = Number(process.env.CHRONICLE_BENCH_WARMUPS ?? `1`)
const runtimeDirectory = resolve(electricCheckout, `packages/agents-runtime`)
const testFile = `test/chronicle-process-wake-benchmark.test.ts`
const harnessFiles = [
  testFile,
  `test/chronicle-e2e-gate.ts`,
  `test/runtime-dsl.ts`,
  `test/run-chronicle-process-wake-benchmark.mjs`,
]

for (const [name, value] of [
  [`CHRONICLE_BENCH_SAMPLES_PER_PHASE`, samplesPerPhase],
  [`CHRONICLE_BENCH_WARMUPS`, warmupsPerRun],
]) {
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`${name} must be a non-negative integer`)
  }
}
if (warmupsPerRun < 1) {
  throw new Error(`CHRONICLE_BENCH_WARMUPS must be at least 1`)
}

const scenarios = [`update-heavy`, `append-only`, `large-inbox`]
const phases = [
  {
    name: `replay-first`,
    indexBase: 0,
    modes: [`replay`, `snapshot-full-raw-replay`],
  },
  {
    name: `snapshot-first`,
    indexBase: samplesPerPhase,
    modes: [`snapshot-full-raw-replay`, `replay`],
  },
]
const samples = []
const failures = []
const commands = []
const frozenBefore = await frozenHashes()

for (const scenario of scenarios) {
  for (const phase of phases) {
    for (const mode of phase.modes) {
      const args = [`test`, `--run`, testFile]
      const env = {
        ...process.env,
        CHRONICLE_BENCH_SCENARIO: scenario,
        CHRONICLE_BENCH_MODE: mode,
        CHRONICLE_BENCH_PHASE: phase.name,
        CHRONICLE_BENCH_PHASE_ORDER: phase.modes.join(`,`),
        CHRONICLE_BENCH_INDEX_BASE: String(phase.indexBase),
        CHRONICLE_BENCH_SAMPLES: String(samplesPerPhase),
        CHRONICLE_BENCH_WARMUPS: String(warmupsPerRun),
      }
      const displayCommand = [
        `CHRONICLE_BENCH_SCENARIO=${scenario}`,
        `CHRONICLE_BENCH_MODE=${mode}`,
        `CHRONICLE_BENCH_PHASE=${phase.name}`,
        `CHRONICLE_BENCH_INDEX_BASE=${phase.indexBase}`,
        `CHRONICLE_BENCH_SAMPLES=${samplesPerPhase}`,
        `CHRONICLE_BENCH_WARMUPS=${warmupsPerRun}`,
        `pnpm ${args.join(` `)}`,
      ].join(` `)
      commands.push(displayCommand)
      process.stderr.write(`${displayCommand}\n`)

      const run = spawnSync(`pnpm`, args, {
        cwd: runtimeDirectory,
        env,
        encoding: `utf8`,
        maxBuffer: 128 * 1024 * 1024,
      })
      const parsed = parseOutput(run.stdout ?? ``)
      samples.push(...parsed.samples)
      if (run.status !== 0) {
        failures.push({
          scenario,
          mode,
          phase: phase.name,
          exitCode: run.status,
          signal: run.signal,
          parsedSamples: parsed.samples.length,
          stderrTail: (run.stderr ?? ``).split(`\n`).slice(-40).join(`\n`),
        })
      }
    }
  }
}

const frozenAfter = await frozenHashes()
const drift = Object.keys(frozenBefore).filter(
  (key) => frozenBefore[key] !== frozenAfter[key]
)
if (drift.length > 0) {
  failures.push({ kind: `frozen-input-drift`, changed: drift })
}

const result = {
  schemaVersion: 1,
  kind: `chronicle-process-wake-benchmark-campaign`,
  generatedAt: new Date().toISOString(),
  labels: {
    processStartup: `one fresh Node/Vitest/runtime/agents-server process per scenario/mode/phase run`,
    activationRecovery: `fresh in-process EntityStreamDB per webhook activation`,
    memoryScope: `combined runtime handler and embedded agents-server Node process`,
  },
  pins: {
    electric: `bb397424db0e1c153dc356713fd3dfd40315470c`,
    durableStreams: `461b40267aabd644558f9b19dbb9507dd5f691cf`,
  },
  hashes: {
    electricPatchSha256: frozenBefore.electricPatchSha256,
    durablePatchSha256: frozenBefore.durablePatchSha256,
    harnessSha256: frozenBefore.harnessSha256,
    chronicleBinarySha256: frozenBefore.chronicleBinarySha256,
    electricImageRepoDigest: process.env.ELECTRIC_IMAGE_REPO_DIGEST || null,
    electricImageManifest: process.env.ELECTRIC_IMAGE_MANIFEST || null,
    electricImageConfig: process.env.ELECTRIC_IMAGE_CONFIG || null,
    before: frozenBefore,
    after: frozenAfter,
    drift,
  },
  campaign: {
    scenarios,
    modes: [`replay`, `snapshot-full-raw-replay`],
    measuredSamplesPerModeScenario: samplesPerPhase * phases.length,
    warmupsPerRun,
    phaseOrders: phases.map((phase) => phase.modes),
    validationTimed: false,
  },
  commands,
  samples,
  failures,
}
await writeFile(outputPath, `${JSON.stringify(result, null, 2)}\n`)
process.stderr.write(
  `wrote ${samples.length} samples and ${failures.length} run failures to ${outputPath}\n`
)
if (failures.length > 0) process.exitCode = 1

function parseOutput(stdout) {
  const samples = []
  for (const line of stdout.split(`\n`)) {
    if (!line.startsWith(`{`)) continue
    let value
    try {
      value = JSON.parse(line)
    } catch {
      continue
    }
    if (value.kind === `chronicle-process-wake-benchmark-sample`) {
      samples.push(value)
    }
  }
  return { samples }
}

function trackedDiff(checkout) {
  const diff = spawnSync(`git`, [`diff`, `--binary`, `HEAD`], {
    cwd: checkout,
    encoding: null,
    maxBuffer: 128 * 1024 * 1024,
  })
  if (diff.status !== 0) throw new Error(`git diff failed in ${checkout}`)
  return diff.stdout
}

async function patchHash(checkout) {
  const hash = createHash(`sha256`).update(trackedDiff(checkout))
  const untracked = spawnSync(
    `git`,
    [`ls-files`, `--others`, `--exclude-standard`, `-z`],
    { cwd: checkout, encoding: `utf8` }
  )
  if (untracked.status !== 0) {
    throw new Error(`git ls-files failed in ${checkout}`)
  }
  for (const path of untracked.stdout.split(`\0`).filter(Boolean).sort()) {
    hash.update(`untracked\0${path}\0`)
    hash.update(await readFile(resolve(checkout, path)))
    hash.update(`\0`)
  }
  return hash.digest(`hex`)
}

async function frozenHashes() {
  const [
    electricPatchSha256,
    durablePatchSha256,
    harnessSha256,
    chronicleBinarySha256,
  ] = await Promise.all([
    patchHash(electricCheckout),
    patchHash(durableCheckout),
    filesHash(runtimeDirectory, harnessFiles),
    fileHash(chronicleBinary),
  ])
  return {
    electricPatchSha256,
    durablePatchSha256,
    harnessSha256,
    chronicleBinarySha256,
  }
}

async function filesHash(rootDirectory, paths) {
  const hash = createHash(`sha256`)
  for (const path of [...paths].sort()) {
    hash.update(`${path}\0`)
    hash.update(await readFile(resolve(rootDirectory, path)))
    hash.update(`\0`)
  }
  return hash.digest(`hex`)
}

async function fileHash(path) {
  return createHash(`sha256`)
    .update(await readFile(path))
    .digest(`hex`)
}
