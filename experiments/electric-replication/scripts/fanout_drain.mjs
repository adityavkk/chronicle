// Finite correctness probe, NOT ds-bench or a throughput benchmark. Keep every
// reader's sequence ledger and treat EOF, parse errors and timeout as failures.
import { writeFileSync } from 'node:fs'
import assert from 'node:assert/strict'

const [target, output, countRaw = '1000', recordsRaw = '100'] = process.argv.slice(2)
const count = Number(countRaw), records = Number(recordsRaw)
const url = `${target}/finite-fanout`
const result = { verdict: 'FAIL', target, count, records, appends: [], readers: [] }
const deadline = AbortSignal.timeout(60000)
const tasks = []

async function consume(response, id) {
  const row = { id, status: response.status, received: [] }
  result.readers.push(row)
  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let buffered = ''
  try {
    assert.equal(response.status, 200)
    while (row.received.length < records) {
      const next = await reader.read()
      assert.equal(next.done, false, 'EOF before the complete sequence')
      buffered += decoder.decode(next.value, { stream: true })
      let end
      while ((end = buffered.indexOf('\n\n')) >= 0) {
        const frame = buffered.slice(0, end)
        buffered = buffered.slice(end + 2)
        if (!frame.split('\n').includes('event: data')) continue
        const values = JSON.parse(frame.split('\n').filter(s => s.startsWith('data:')).map(s => s.slice(5).trimStart()).join('\n'))
        assert.ok(Array.isArray(values))
        for (const item of values) {
          row.received.push(item.seq)
          assert.equal(item.seq, row.received.length - 1, 'gap, duplicate or reorder')
          assert.equal(item.payload, `record-${item.seq * 73 + 19}`, 'payload mismatch')
        }
      }
    }
    assert.equal(row.received.length, records)
  } catch (error) {
    row.error = String(error)
  } finally {
    await reader.cancel().catch(() => {})
  }
}

try {
  assert.equal((await fetch(url, { method: 'PUT', headers: { 'content-type': 'application/json' }, signal: deadline })).status, 201)
  // Successful headers from ALL readers before writing, as in pinned ds-bench.
  await Promise.all(Array.from({ length: count }, async (_, id) => {
    const response = await fetch(`${url}?offset=-1&live=sse`, { signal: deadline })
    tasks.push(consume(response, id))
  }))
  for (let seq = 0; seq < records; seq++) {
    const event = { seq, started: Date.now(), outcome: 'unknown' }
    result.appends.push(event)
    const response = await fetch(url, { method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ seq, payload: `record-${seq * 73 + 19}` }), signal: deadline })
    event.status = response.status
    event.ended = Date.now()
    assert.ok(response.ok)
    event.outcome = 'ok'
    await response.arrayBuffer()
    await new Promise(resolve => setTimeout(resolve, 20))
  }
  await Promise.all(tasks)
  assert.equal(result.readers.length, count)
  assert.ok(result.readers.every(r => !r.error && r.received.length === records), 'incomplete reader; see ledgers')
  result.verdict = 'PASS'
} catch (error) {
  result.error = String(error)
} finally {
  writeFileSync(output, JSON.stringify(result, null, 2) + '\n')
  console.log(JSON.stringify({ verdict: result.verdict, readers: result.readers.length,
    complete: result.readers.filter(r => !r.error && r.received.length === records).length, error: result.error }))
}
process.exit(result.verdict === 'PASS' ? 0 : 1)
