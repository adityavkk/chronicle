import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { createServer } from 'node:http'
import { test } from 'node:test'
import { event, recover } from './benchmark.mjs'

test('HTTP recovery verifies the image, guards both suffix and raw inputs, and rejects server failures', async (t) => {
  const c = { events: 3, keys: 2, payload: 8, rawFrom: 0 }
  const image = JSON.stringify({
    codec: 'benchmark-state/v1', nextSeq: 2,
    rows: [
      ['0', { value: { revision: 0, text: '0:xxxxxx' }, _seq: 0, _first: 0 }],
      ['1', { value: { revision: 1, text: '1:xxxxxx' }, _seq: 1, _first: 1 }],
    ],
  })
  let fault = ''
  const offsets = []
  const server = createServer((req, res) => {
    const url = new URL(req.url, 'http://localhost')
    res.setHeader('Stream-Snapshot', 'v1')
    res.setHeader('Stream-Incarnation', 'lifetime-A')
    if (url.searchParams.has('snapshot')) {
      if (fault === 'error' || fault === 'miss') {
        res.writeHead(fault === 'error' ? 500 : 404)
        res.end('no image')
        return
      }
      res.setHeader('ETag', '"image-A"')
      res.setHeader('Stream-Snapshot-Offset', 'cut-A')
      const digest = createHash('sha256').update(image).digest('base64')
      res.setHeader('Content-Digest', fault === 'digest' ? 'sha-256=:wrong:' : `sha-256=:${digest}:`)
      res.end(image)
      return
    }
    offsets.push({ offset: url.searchParams.get('offset'), guard: req.headers['if-stream-incarnation'] })
    res.setHeader('Stream-Next-Offset', 'tail-A')
    res.setHeader('Stream-Up-To-Date', 'true')
    if (fault === 'incarnation') res.setHeader('Stream-Incarnation', 'lifetime-B')
    const from = url.searchParams.get('offset') === 'cut-A' ? 2 : 0
    res.end(JSON.stringify(Array.from({ length: 3 - from }, (_, i) => event(c, i + from))))
  })
  await new Promise((done) => server.listen(0, '127.0.0.1', done))
  t.after(() => new Promise((done) => server.close(done)))
  const config = { base: `http://127.0.0.1:${server.address().port}/`, token: 'fixture' }
  const fixture = { path: 'source', rawOffset: '-1', tail: 'tail-A' }
  const result = await recover(config, c, fixture, 'snapshot')
  assert.equal(result.sample.applied, 1)
  assert.equal(result.sample.rawEvents, 3)
  assert.equal(result.sample.requests, 3)
  assert.equal(result.state.rows.get('0').value.revision, 2)
  assert.deepEqual(offsets, [
    { offset: 'cut-A', guard: 'lifetime-A' },
    { offset: '-1', guard: 'lifetime-A' },
  ])

  fault = 'digest'
  await assert.rejects(recover(config, c, fixture, 'snapshot'), /sha-256/)
  fault = 'error'
  await assert.rejects(recover(config, c, fixture, 'snapshot'), /not a cache miss/)
  fault = 'incarnation'
  await assert.rejects(recover(config, c, fixture, 'snapshot'), /lifetime/)
  fault = 'miss'
  await assert.rejects(recover(config, c, fixture, 'snapshot'), /unexpected snapshot miss/)
  const miss = await recover(config, { ...c, miss: true }, fixture, 'snapshot')
  assert.equal(miss.sample.imageHit, false)
  assert.equal(miss.sample.applied, 3)
  assert.equal(miss.sample.requests, 2)
})
