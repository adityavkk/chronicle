-- ack.lua — fence, apply acks forward-only, then either release the lease
-- (done='1') or extend it as a heartbeat (done='0'). Doubles as the webhook
-- callback and the pull-wake ack (PROTOCOL §7.1, §7.2). The fence — not the
-- lease — is the correctness mechanism: a stale wake/generation is rejected and
-- cannot advance a cursor.
--
-- Claim granularity (the third axis, design 08): k_shardstate is the per-(subId,g)
-- SHARDSTATE hash whose (generation, wake_id) fence this ack checks and whose
-- idle/lease fields the done/heartbeat branches write; a_member is the per-shard
-- schedule MEMBER for the lease/retry ZREM/ZADD. The cursor hash (k_links) is
-- shared across a subscription's shards — cursors are forward-only watermarks, so
-- an ack only ever advances the streams it names, fenced by its own shard's
-- register. At G=1 / shard 0, k_shardstate==sub hash and a_member==id (today). The
-- due-set ZREM in the done branch (Move 2, k_due_zset) uses this same a_member member,
-- so a per-shard due mark is cleared by its own shard's ack.
local k_shardstate = KEYS[1]
local k_links = KEYS[2]
local k_lease_zset = KEYS[3]
local k_retry_zset = KEYS[4]
local k_due_zset = KEYS[5]
local k_sub_config = KEYS[6]
local k_slot = KEYS[7]
local a_member = ARGV[1]
local a_req_gen = ARGV[2]
local a_req_wake = ARGV[3]
local a_token_gen = ARGV[4]
local a_done = ARGV[5]
local a_now_ns = ARGV[6]
local a_lease_ttl_ms = ARGV[7]
local a_num_acks = ARGV[8]
local sub = k_shardstate
-- Owner-epoch fence (issue #14, TOCTOU): the replica_id/expected_epoch are the
-- LAST two ARGV (after the variable-length acks). For the load-balanced external
-- ack path epoch is '' and this is a no-op — the (gen,wake_id) fence below is the
-- guard. A reused-as-FENCED reply is indistinguishable from the gen fence on the
-- wire, which is fine: both grant and mutate nothing.
if owner_fenced(k_slot, ARGV[#ARGV - 1], ARGV[#ARGV]) then
  return { 'FENCED' }
end
if redis.call('EXISTS', sub) == 0 then
  return { 'NOSUB' }
end
if redis.call('EXISTS', k_sub_config) == 0 then
  return { 'NOSUB' }
end
local cfg_inc = redis.call('HGET', k_sub_config, 'incarnation')
local shard_inc = redis.call('HGET', sub, 'incarnation')
if k_shardstate ~= k_sub_config then
  if cfg_inc == false or cfg_inc == '' or shard_inc == false or shard_inc == '' or shard_inc ~= cfg_inc then
    return { 'FENCED' }
  end
else
  if cfg_inc ~= false and cfg_inc ~= '' and shard_inc ~= cfg_inc then
    return { 'FENCED' }
  end
end
local gen = redis.call('HGET', sub, 'generation')
local wake = redis.call('HGET', sub, 'wake_id')
if fenced(gen, wake, a_req_gen, a_req_wake, a_token_gen) then
  return { 'FENCED' }
end
-- A callback token deliberately outlives the lease. A heartbeat may renew only
-- a lease that is still live at this instant; otherwise a late worker could
-- revive it before the expiry worker or a takeover clears it. Pull-wake claims
-- additionally require their holder state. Webhook delivery owns no holder and
-- legitimately heartbeats from either waking or live phase.
-- Done acknowledgements retain their existing fence-only behavior so a worker
-- may still report completed work after its lease deadline.
if a_done ~= '1' then
  local dispatch_type = redis.call('HGET', k_sub_config, 'type')
  local phase = redis.call('HGET', sub, 'phase')
  local holder = redis.call('HGET', sub, 'holder')
  local lease_until_ns = tonumber(redis.call('HGET', sub, 'lease_until_ns'))
  local now_ns = tonumber(a_now_ns)
  if lease_until_ns == nil or now_ns == nil or lease_until_ns <= now_ns then
    return { 'FENCED' }
  end
  if dispatch_type == 'webhook' then
    if (phase ~= 'waking' and phase ~= 'live') or holder ~= '0' then
      return { 'FENCED' }
    end
  elseif phase ~= 'live' or holder ~= '1' then
    return { 'FENCED' }
  end
end
local n = tonumber(a_num_acks)
local i = 9
for _ = 1, n do
  local path = ARGV[i]
  local off = ARGV[i + 1]
  local cur = redis.call('HGET', k_links, path)
  if cur ~= false then
    local lt, curoff = split_link(cur)
    if offset_greater(off, curoff) then
      redis.call('HSET', k_links, path, lt .. ':' .. off)
    end
  end
  i = i + 2
end
if a_done == '1' then
  redis.call('HSET', sub, 'phase', 'idle', 'holder', '0', 'holder_worker', '',
    'wake_id', '', 'lease_until_ns', '0', 'status', 'active',
    'retry_count', '0', 'first_fail_ns', '0', 'next_attempt_ns', '0')
  redis.call('ZREM', k_lease_zset, a_member)
  redis.call('ZREM', k_retry_zset, a_member)
  redis.call('ZREM', k_due_zset, a_member) -- clear the due-set wake mark (Move 2)
else
  local until_ns = tonumber(a_now_ns) + tonumber(a_lease_ttl_ms) * 1000000
  redis.call('HSET', sub, 'lease_until_ns', tostring(until_ns), 'phase', 'live')
  redis.call('ZADD', k_lease_zset, until_ns, a_member)
end
return { 'OK' }
