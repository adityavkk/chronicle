-- ack.lua — fence, apply acks forward-only, then either release the lease
-- (done='1') or extend it as a heartbeat (done='0'). Doubles as the webhook
-- callback and the pull-wake ack (PROTOCOL §7.1, §7.2). The fence — not the
-- lease — is the correctness mechanism: a stale wake/generation is rejected and
-- cannot advance a cursor.
-- KEYS: 1=sub 2=links 3=lease_zset 4=retry_zset 5=due_zset 6=ownership_slot
-- ARGV: 1=id 2=req_gen 3=req_wake 4=token_gen 5=done('0'/'1') 6=now_ns
--       7=lease_ttl_ms 8=num_acks 9=shard 10=lease_member
--       11=claim_mode('legacy'|'sharded') 12=owner_id 13=owner_epoch
--       then (path, offset)*
-- Reply: {status} ; OK | FENCED | NOSUB
local sub = KEYS[1]
if redis.call('EXISTS', sub) == 0 then
  return { 'NOSUB' }
end
if owner_fenced(KEYS[6], ARGV[12], ARGV[13]) then
  return { 'FENCED' }
end
local conflict = claim_mode_conflict(sub, ARGV[11])
if conflict then
  return { 'FENCED' }
end
local shard = ARGV[9]
local phase_field = shard_field('phase', shard)
local gen_field = shard_field('generation', shard)
local wake_field = shard_field('wake_id', shard)
local holder_field = shard_field('holder', shard)
local holder_worker_field = shard_field('holder_worker', shard)
local lease_until_field = shard_field('lease_until_ns', shard)
local gen = redis.call('HGET', sub, gen_field) or '0'
local wake = redis.call('HGET', sub, wake_field) or ''
if fenced(gen, wake, ARGV[2], ARGV[3], ARGV[4]) then
  return { 'FENCED' }
end
local n = tonumber(ARGV[8])
local i = 14
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
  redis.call('HSET', sub, phase_field, 'idle', holder_field, '0', holder_worker_field, '',
    wake_field, '', lease_until_field, '0')
  redis.call('ZREM', KEYS[3], ARGV[10])
  if shard == '0' then
    redis.call('HSET', sub, 'status', 'active',
      'retry_count', '0', 'first_fail_ns', '0', 'next_attempt_ns', '0')
    redis.call('ZREM', KEYS[4], ARGV[1])
  end
  redis.call('ZREM', KEYS[5], ARGV[1])
else
  local until_ns = tonumber(ARGV[6]) + tonumber(ARGV[7]) * 1000000
  local until_ns_str = string.format('%.0f', until_ns)
  redis.call('HSET', sub, lease_until_field, until_ns_str, phase_field, 'live')
  redis.call('ZADD', KEYS[3], until_ns_str, ARGV[10])
end
return { 'OK' }
