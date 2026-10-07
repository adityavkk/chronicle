#!/usr/bin/env node
import { createServer, request as httpRequest } from 'node:http'

const listen = new URL(
  process.env.CHRONICLE_CALLBACK_GATE_LISTEN ?? `http://127.0.0.1:18439`
)
const upstream = new URL(
  process.env.CHRONICLE_CALLBACK_GATE_UPSTREAM ?? `http://127.0.0.1:18438`
)
for (const url of [listen, upstream]) {
  if (![`127.0.0.1`, `localhost`, `::1`].includes(url.hostname)) {
    throw new Error(`Chronicle callback gate refuses non-loopback URLs`)
  }
}

let failNextDone = false
const records = []

const server = createServer(async (request, response) => {
  const incoming = new URL(request.url ?? `/`, listen)
  if (incoming.pathname === `/__fixture/fail-next-done`) {
    if (request.method !== `POST`)
      return send(response, 405, `method not allowed`)
    failNextDone = true
    return send(response, 204, ``)
  }
  if (incoming.pathname === `/__fixture/records`) {
    if (request.method === `DELETE`) {
      records.length = 0
      return send(response, 204, ``)
    }
    if (request.method === `GET`) {
      response.writeHead(200, { 'content-type': `application/json` })
      return response.end(JSON.stringify(records))
    }
    return send(response, 405, `method not allowed`)
  }

  const chunks = []
  let bytes = 0
  for await (const chunk of request) {
    bytes += chunk.byteLength
    if (bytes > 1024 * 1024)
      return send(response, 413, `fixture body too large`)
    chunks.push(chunk)
  }
  const body = Buffer.concat(chunks)
  const done = parseDone(body)
  const record = {
    method: request.method ?? `GET`,
    pathname: incoming.pathname,
    done,
    status: null,
    outcome: `pending`,
  }
  records.push(record)

  if (
    failNextDone &&
    done === true &&
    incoming.pathname.endsWith(`/callback`)
  ) {
    failNextDone = false
    record.status = 503
    record.outcome = `injected-failure`
    return send(response, 503, `injected callback failure`)
  }

  const target = new URL(`${incoming.pathname}${incoming.search}`, upstream)
  const forwarded = httpRequest(
    target,
    {
      method: request.method,
      headers: { ...request.headers, host: target.host },
    },
    (upstreamResponse) => {
      record.status = upstreamResponse.statusCode ?? 502
      response.writeHead(record.status, upstreamResponse.headers)
      upstreamResponse.pipe(response)
      upstreamResponse.on(`end`, () => {
        record.outcome = `completed`
      })
    }
  )
  forwarded.on(`error`, (error) => {
    record.status = 502
    record.outcome = `error`
    if (!response.headersSent) response.writeHead(502)
    response.end(error.message)
  })
  response.on(`close`, () => {
    if (!response.writableEnded) forwarded.destroy()
  })
  forwarded.end(body)
})

server.listen(Number(listen.port), listen.hostname, () => {
  process.stdout.write(
    `Chronicle callback gate listening on ${listen.origin}\n`
  )
})

function parseDone(body) {
  if (body.byteLength === 0) return null
  try {
    const value = JSON.parse(body.toString(`utf8`))
    return typeof value.done === `boolean` ? value.done : null
  } catch {
    return null
  }
}

function send(response, status, body) {
  response.writeHead(status, { 'content-type': `text/plain` })
  response.end(body)
}
