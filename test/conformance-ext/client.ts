// Shared driver for the Write Fencing extension suite (docs/spec/WRITE-FENCING.md).
//
// The suite is black-box: it speaks plain HTTP to two chronicle servers that
// scripts/conformance-ext.sh starts — an enforce-mode one with the ext-suite
// service policy (CONFORMANCE_EXT_URL) and an insecure-mode one for the
// negative controls that need today's open posture (CONFORMANCE_EXT_INSECURE_URL).
// Every stream and subscription name is unique per test, so tests are
// order-independent and the two vitest files can run in parallel.

export const BASE = process.env.CONFORMANCE_EXT_URL ?? "http://localhost:4439"
export const INSECURE_BASE =
  process.env.CONFORMANCE_EXT_INSECURE_URL ?? "http://localhost:4440"
export const BEARER = process.env.CONFORMANCE_EXT_BEARER ?? "conformance-ext-bearer"

export const svc = { Authorization: `Bearer ${BEARER}` }

let seq = 0
/** uniq returns a fresh stream path under the policy's fence/ namespace. */
export function uniq(name: string): string {
  seq += 1
  return `fence/${name}-${Date.now().toString(36)}-${seq}`
}

/** subId returns a fresh subscription id (a plain segment, not a stream path). */
export function subId(name: string): string {
  seq += 1
  return `${name}-${Date.now().toString(36)}-${seq}`
}

export function streamUrl(path: string, base = BASE): string {
  return `${base}/v1/stream/${path}`
}

export interface Res {
  status: number
  headers: Headers
  text: string
  json: () => any
}

async function request(
  method: string,
  url: string,
  headers: Record<string, string>,
  body?: string,
): Promise<Res> {
  const res = await fetch(url, { method, headers, body })
  const text = await res.text()
  return { status: res.status, headers: res.headers, text, json: () => JSON.parse(text) }
}

/** createStream PUTs a JSON stream; extra headers (e.g. Write-Fence) ride along. */
export function createStream(
  path: string,
  headers: Record<string, string> = {},
  base = BASE,
): Promise<Res> {
  return request("PUT", streamUrl(path, base), {
    "Content-Type": "application/json",
    ...(base === BASE ? svc : {}),
    ...headers,
  })
}

/** post appends to a stream with exactly the given headers plus a JSON body. */
export function post(
  path: string,
  headers: Record<string, string>,
  body = `{"n":1}`,
  base = BASE,
): Promise<Res> {
  return request("POST", streamUrl(path, base), { "Content-Type": "application/json", ...headers }, body)
}

/** head reads stream metadata as the service principal. */
export function head(path: string): Promise<Res> {
  return request("HEAD", streamUrl(path), { ...svc })
}

/** tail returns the stream's current Stream-Next-Offset. */
export async function tail(path: string): Promise<string> {
  const res = await head(path)
  if (res.status !== 200) throw new Error(`HEAD ${path} = ${res.status}`)
  return res.headers.get("Stream-Next-Offset") ?? ""
}

/** producer builds the fenced-class header set: write token + producer triple. */
export function producer(
  token: string,
  id: string,
  epoch: number | string,
  seqNo: number | string,
): Record<string, string> {
  return {
    "Write-Token": token,
    "Producer-Id": id,
    "Producer-Epoch": String(epoch),
    "Producer-Seq": String(seqNo),
  }
}

export interface SubConfig {
  type: "pull-wake" | "webhook"
  streams?: string[]
  pattern?: string
  webhook?: { url: string }
  wake_stream?: string
  lease_ttl_ms?: number
}

/** createSub PUTs a __ds subscription as the service principal. */
export async function createSub(id: string, cfg: SubConfig): Promise<Res> {
  return request(
    "PUT",
    `${BASE}/v1/stream/__ds/subscriptions/${id}`,
    { "Content-Type": "application/json", ...svc },
    JSON.stringify(cfg),
  )
}

export interface ClaimResponse {
  wake_id: string
  generation: number
  token: string
  write_token?: string
  wake_token?: string
  streams: unknown[]
  lease_ttl_ms: number
}

/** claim claims a pull-wake subscription and asserts it minted a write token. */
export async function claim(id: string, worker: string): Promise<ClaimResponse> {
  const res = await request(
    "POST",
    `${BASE}/v1/stream/__ds/subscriptions/${id}/claim`,
    { "Content-Type": "application/json", ...svc },
    JSON.stringify({ worker }),
  )
  if (res.status !== 200) throw new Error(`claim ${id} = ${res.status}: ${res.text}`)
  const cr = res.json() as ClaimResponse
  if (!cr.write_token) throw new Error(`claim ${id} minted no write_token: ${res.text}`)
  return cr
}

/** pullWakeSub creates a single-stream pull-wake subscription and claims it. */
export async function pullWakeSub(
  id: string,
  path: string,
  leaseMs = 30_000,
): Promise<ClaimResponse> {
  // The shared wake stream must exist or every wake event write warns; the
  // re-PUT is idempotent (same config), so concurrent tests are fine.
  await createStream("fence/wake")
  const res = await createSub(id, {
    type: "pull-wake",
    streams: [path],
    wake_stream: "fence/wake",
    lease_ttl_ms: leaseMs,
  })
  if (res.status !== 201 && res.status !== 200) {
    throw new Error(`create sub ${id} = ${res.status}: ${res.text}`)
  }
  return claim(id, "worker-A")
}

/** ack posts a heartbeat or done callback with the claim's callback token. */
export function ack(
  id: string,
  callbackToken: string,
  body: { wake_id: string; generation: number; acks?: { stream: string; offset: string }[]; done?: boolean },
): Promise<Res> {
  return request(
    "POST",
    `${BASE}/v1/stream/__ds/subscriptions/${id}/ack`,
    { "Content-Type": "application/json", Authorization: `Bearer ${callbackToken}` },
    JSON.stringify(body),
  )
}

/** done acks the stream at its current tail and completes the wake. */
export async function done(id: string, cr: ClaimResponse, path: string): Promise<Res> {
  return ack(id, cr.token, {
    wake_id: cr.wake_id,
    generation: cr.generation,
    acks: [{ stream: path, offset: await tail(path) }],
    done: true,
  })
}

/** deleteSub deletes a subscription as the owning service principal. */
export function deleteSub(id: string): Promise<Res> {
  return request("DELETE", `${BASE}/v1/stream/__ds/subscriptions/${id}`, { ...svc })
}

/** sleep pauses the test for wall-clock lease/expiry boundaries. */
export function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms))
}
