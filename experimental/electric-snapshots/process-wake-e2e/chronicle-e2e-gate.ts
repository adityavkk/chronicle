import { createServer, request as requestHttp } from 'node:http'
import type { AddressInfo } from 'node:net'
import type { ClientRequest, Server } from 'node:http'

export interface ChronicleGateRequest {
  id: number
  method: string
  pathname: string
  search: string
  snapshot: string | null
  status: number | null
  requestBytes: number
  responseBytes: number
  startedAtUnixMs: number
  durationMs: number | null
  streamNextOffset: string | null
  streamUpToDate: string | null
  streamSnapshotOffset: string | null
  done: boolean | null
  outcome: `pending` | `completed` | `aborted` | `error`
}

export interface ChronicleGateCapture {
  stop(): Array<ChronicleGateRequest>
}

export interface ChronicleGate {
  streamRoot: string
  requests: Array<ChronicleGateRequest>
  startCapture(): ChronicleGateCapture
  failNext(
    predicate: (request: {
      method: string
      pathname: string
      search: string
    }) => boolean,
    status: number
  ): void
  waitFor(
    predicate: (request: ChronicleGateRequest) => boolean,
    timeoutMs: number
  ): Promise<ChronicleGateRequest>
  reset(): void
  stop(): Promise<void>
}

export async function startChronicleGate(
  chronicleStreamRoot: URL
): Promise<ChronicleGate> {
  if (
    ![`127.0.0.1`, `localhost`, `::1`].includes(chronicleStreamRoot.hostname)
  ) {
    throw new Error(`Refusing non-loopback Chronicle root`)
  }
  if (!chronicleStreamRoot.pathname.endsWith(`/`)) {
    throw new Error(`CHRONICLE_STREAM_ROOT must end with /`)
  }

  const requests: Array<ChronicleGateRequest> = []
  const failures: Array<{
    predicate: (request: {
      method: string
      pathname: string
      search: string
    }) => boolean
    status: number
  }> = []
  const waiters = new Set<{
    predicate: (request: ChronicleGateRequest) => boolean
    resolve: (request: ChronicleGateRequest) => void
  }>()
  let nextRequestId = 1

  const server = createServer((request, response) => {
    const startedAt = performance.now()
    const target = new URL(request.url ?? `/`, chronicleStreamRoot.origin)
    const recorded: ChronicleGateRequest = {
      id: nextRequestId++,
      method: request.method ?? `GET`,
      pathname: target.pathname,
      search: target.search,
      snapshot: target.searchParams.get(`snapshot`),
      status: null,
      requestBytes: 0,
      responseBytes: 0,
      startedAtUnixMs: Date.now(),
      durationMs: null,
      streamNextOffset: null,
      streamUpToDate: null,
      streamSnapshotOffset: null,
      done: null,
      outcome: `pending`,
    }
    requests.push(recorded)

    const requestChunks: Array<Buffer> = []
    request.on(`data`, (chunk: Buffer) => {
      recorded.requestBytes += chunk.byteLength
      if (recorded.requestBytes <= 64 * 1024) requestChunks.push(chunk)
    })
    request.on(`end`, () => {
      recorded.done = parseDone(requestChunks)
    })

    const summary = {
      method: recorded.method,
      pathname: recorded.pathname,
      search: recorded.search,
    }
    const failureIndex = failures.findIndex(({ predicate }) =>
      predicate(summary)
    )
    if (failureIndex >= 0) {
      const failure = failures.splice(failureIndex, 1)[0]!
      request.resume()
      request.on(`end`, () => {
        recorded.status = failure.status
        recorded.outcome = `completed`
        recorded.durationMs = +(performance.now() - startedAt).toFixed(2)
        response.writeHead(failure.status)
        response.end(`injected Chronicle gate failure`)
        notify(recorded)
      })
      return
    }

    let upstream: ClientRequest | null = requestHttp(
      target,
      {
        method: request.method,
        headers: { ...request.headers, host: target.host },
      },
      (upstreamResponse) => {
        recorded.status = upstreamResponse.statusCode ?? 502
        recorded.streamNextOffset = stringHeader(
          upstreamResponse.headers[`stream-next-offset`]
        )
        recorded.streamUpToDate = stringHeader(
          upstreamResponse.headers[`stream-up-to-date`]
        )
        recorded.streamSnapshotOffset = stringHeader(
          upstreamResponse.headers[`stream-snapshot-offset`]
        )
        response.writeHead(recorded.status, upstreamResponse.headers)
        upstreamResponse.on(`data`, (chunk: Buffer) => {
          recorded.responseBytes += chunk.byteLength
        })
        upstreamResponse.on(`end`, () => {
          if (recorded.outcome !== `pending`) return
          recorded.outcome = `completed`
          recorded.durationMs = +(performance.now() - startedAt).toFixed(2)
          notify(recorded)
        })
        upstreamResponse.on(`aborted`, () => finish(`aborted`))
        upstreamResponse.on(`error`, () => finish(`error`))
        upstreamResponse.pipe(response)
      }
    )

    const finish = (outcome: `aborted` | `error`) => {
      if (recorded.outcome !== `pending`) return
      recorded.outcome = outcome
      recorded.durationMs = +(performance.now() - startedAt).toFixed(2)
      notify(recorded)
    }
    request.on(`aborted`, () => {
      finish(`aborted`)
      upstream?.destroy()
      upstream = null
    })
    response.on(`close`, () => {
      if (response.writableEnded) return
      finish(`aborted`)
      upstream?.destroy()
      upstream = null
    })
    upstream.on(`error`, (error) => {
      finish(`error`)
      if (!response.headersSent) response.writeHead(502)
      response.end(error.message)
    })
    request.pipe(upstream)
  })

  await listen(server)
  const { port } = server.address() as AddressInfo
  const streamRoot = new URL(
    chronicleStreamRoot.pathname,
    `http://127.0.0.1:${port}`
  ).toString()

  return {
    streamRoot,
    requests,
    startCapture() {
      const firstId = nextRequestId
      let stopped = false
      return {
        stop() {
          if (stopped) throw new Error(`Chronicle gate capture already stopped`)
          stopped = true
          const lastId = nextRequestId - 1
          return requests
            .filter((request) => request.id >= firstId && request.id <= lastId)
            .map((request) => ({ ...request }))
        },
      }
    },
    failNext(predicate, status) {
      failures.push({ predicate, status })
    },
    waitFor(predicate, timeoutMs) {
      const existing = requests.find(
        (request) => request.outcome !== `pending` && predicate(request)
      )
      if (existing) return Promise.resolve({ ...existing })
      return new Promise<ChronicleGateRequest>((resolve, reject) => {
        const waiter = { predicate, resolve }
        waiters.add(waiter)
        setTimeout(() => {
          if (waiters.delete(waiter)) {
            reject(
              new Error(
                `Timed out waiting for Chronicle gate request; observed ${JSON.stringify(
                  requests.map(
                    ({ method, pathname, status, done, outcome }) => ({
                      method,
                      pathname,
                      status,
                      done,
                      outcome,
                    })
                  )
                )}`
              )
            )
          }
        }, timeoutMs)
      })
    },
    reset() {
      requests.length = 0
    },
    async stop() {
      await close(server)
    },
  }

  function notify(recorded: ChronicleGateRequest): void {
    for (const waiter of waiters) {
      if (waiter.predicate(recorded)) {
        waiters.delete(waiter)
        waiter.resolve({ ...recorded })
      }
    }
  }
}

function parseDone(chunks: Array<Buffer>): boolean | null {
  if (chunks.length === 0) return null
  try {
    const body = JSON.parse(Buffer.concat(chunks).toString(`utf8`)) as {
      done?: unknown
    }
    return typeof body.done === `boolean` ? body.done : null
  } catch {
    return null
  }
}

function stringHeader(
  value: string | Array<string> | undefined
): string | null {
  return Array.isArray(value) ? (value[0] ?? null) : (value ?? null)
}

async function listen(server: Server): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.once(`error`, reject)
    server.listen(0, `127.0.0.1`, () => {
      server.off(`error`, reject)
      resolve()
    })
  })
}

async function close(server: Server): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()))
    server.closeIdleConnections()
    server.closeAllConnections()
  })
}
