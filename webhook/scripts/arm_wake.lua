-- arm_wake.lua — issue a new wake generation when the subscription is idle
-- (PROTOCOL §7: pending work creates a wake unless one is already in flight or a
-- lease is held). Coalescing falls out of the phase check. For webhook delivery
-- the lease is armed here (arm_lease='1'); for pull-wake it is not (the lease
-- starts at claim, PROTOCOL §7.3).
local k_sub = KEYS[1]
local k_lease_zset = KEYS[2]
local k_due_zset = KEYS[3]
local k_slot = KEYS[4]
local a_id = ARGV[1]
local a_now_ns = ARGV[2]
local a_lease_ttl_ms = ARGV[3]
local a_arm_lease = ARGV[4]
local a_new_wake_id = ARGV[5]
local a_replica_id = ARGV[6]
local a_expected_epoch = ARGV[7]
local sub = k_sub
-- Owner-epoch fence (issue #14, TOCTOU): when an owner-scoped caller (epoch ~= '')
-- arms a wake for a slot it no longer owns, suppress it atomically with the write.
if owner_fenced(k_slot, a_replica_id, a_expected_epoch) then
  return { 'FENCED' }
end
if redis.call('EXISTS', sub) == 0 then
  return { 'NOSUB' }
end
if redis.call('HGET', sub, 'phase') ~= 'idle' then
  -- coalesce: a wake is already in flight, so the due-set mark is left as-is.
  return { 'BUSY', redis.call('HGET', sub, 'generation'), redis.call('HGET', sub, 'wake_id') }
end
local gen = redis.call('HINCRBY', sub, 'generation', 1)
redis.call('HSET', sub, 'wake_id', a_new_wake_id, 'phase', 'waking', 'holder', '0', 'holder_worker', '')
-- Outbox the wake: score = now_ns at arm time, so dueWorker re-fires it in O(owed)
-- if this wake is lost (Move 2). The ack(done)/release ZREMs it; a re-arm after a
-- FENCED re-ZADDs at the new now_ns. Same {__ds} slot, so still single-slot.
redis.call('ZADD', k_due_zset, a_now_ns, a_id)
if a_arm_lease == '1' then
  local until_ns = tonumber(a_now_ns) + tonumber(a_lease_ttl_ms) * 1000000
  redis.call('HSET', sub, 'lease_until_ns', tostring(until_ns))
  redis.call('ZADD', k_lease_zset, until_ns, a_id)
else
  -- pull-wake: mark the wake event as not yet emitted. The lease is not armed
  -- (it starts at claim), so nothing in the schedule recovers a crash between
  -- this arm and the wake-stream append; the recovery sweep keys off this flag
  -- to re-emit a stranded wake.
  redis.call('HSET', sub, 'wake_event_sent_ns', '0')
end
return { 'ARMED', tostring(gen), a_new_wake_id }
