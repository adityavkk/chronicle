-- ack.lua — fence, apply acks forward-only, then either release the lease
-- (done='1') or extend it as a heartbeat (done='0'). Doubles as the webhook
-- callback and the pull-wake ack (PROTOCOL §7.1, §7.2). The fence — not the
-- lease — is the correctness mechanism: a stale wake/generation is rejected and
-- cannot advance a cursor.
--
-- Claim granularity (the third axis, design 08): KEYS[1] is the per-(subId,g)
-- SHARDSTATE hash whose (generation, wake_id) fence this ack checks and whose
-- idle/lease fields the done/heartbeat branches write; ARGV[1] is the per-shard
-- schedule MEMBER for the lease/retry ZREM/ZADD. The cursor hash (KEYS[2]) is
-- shared across a subscription's shards — cursors are forward-only watermarks, so
-- an ack only ever advances the streams it names, fenced by its own shard's
-- register. At G=1 / shard 0, KEYS[1]==sub hash and ARGV[1]==id (today). The
-- due-set ZREM in the done branch (Move 2, KEYS[5]) uses this same ARGV[1] member,
-- so a per-shard due mark is cleared by its own shard's ack.
-- KEYS: 1=shardstate 2=links 3=lease_zset 4=retry_zset 5=due_zset
-- ARGV: 1=member 2=req_gen 3=req_wake 4=token_gen 5=done('0'/'1') 6=now_ns
--       7=lease_ttl_ms 8=num_acks then (path, offset)*
-- Reply: {status} ; OK | FENCED | NOSUB
local sub = KEYS[1]
if redis.call('EXISTS', sub) == 0 then
  return { 'NOSUB' }
end
local gen = redis.call('HGET', sub, 'generation')
local wake = redis.call('HGET', sub, 'wake_id')
if fenced(gen, wake, ARGV[2], ARGV[3], ARGV[4]) then
  return { 'FENCED' }
end
local n = tonumber(ARGV[8])
local i = 9
for _ = 1, n do
  local path = ARGV[i]
  local off = ARGV[i + 1]
  local cur = redis.call('HGET', KEYS[2], path)
  if cur ~= false then
    local lt, curoff = split_link(cur)
    if offset_greater(off, curoff) then
      redis.call('HSET', KEYS[2], path, lt .. ':' .. off)
    end
  end
  i = i + 2
end
if ARGV[5] == '1' then
  redis.call('HSET', sub, 'phase', 'idle', 'holder', '0', 'holder_worker', '',
    'wake_id', '', 'lease_until_ns', '0', 'status', 'active',
    'retry_count', '0', 'first_fail_ns', '0', 'next_attempt_ns', '0')
  redis.call('ZREM', KEYS[3], ARGV[1])
  redis.call('ZREM', KEYS[4], ARGV[1])
  redis.call('ZREM', KEYS[5], ARGV[1]) -- clear the due-set wake mark (Move 2)
else
  local until_ns = tonumber(ARGV[6]) + tonumber(ARGV[7]) * 1000000
  redis.call('HSET', sub, 'lease_until_ns', tostring(until_ns), 'phase', 'live')
  redis.call('ZADD', KEYS[3], until_ns, ARGV[1])
end
return { 'OK' }
