-- arm_wake.lua — issue a new wake generation when the subscription is idle
-- (PROTOCOL §7: pending work creates a wake unless one is already in flight or a
-- lease is held). Coalescing falls out of the phase check. For webhook delivery
-- the lease is armed here (arm_lease='1'); for pull-wake it is not (the lease
-- starts at claim, PROTOCOL §7.3).
-- KEYS: 1=sub 2=lease_zset 3=due_zset 4=slot (ds:{ownership}:slot:<h>)
-- ARGV: 1=id 2=now_ns 3=lease_ttl_ms 4=arm_lease('0'/'1') 5=new_wake_id
--       6=replica_id 7=expected_epoch (epoch '' on the load-balanced path => skip)
-- Reply: {status, generation, wake_id} ; ARMED | BUSY | NOSUB | FENCED
local sub = KEYS[1]
-- Owner-epoch fence (issue #14, TOCTOU): when an owner-scoped caller (epoch ~= '')
-- arms a wake for a slot it no longer owns, suppress it atomically with the write.
if owner_fenced(KEYS[4], ARGV[6], ARGV[7]) then
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
redis.call('HSET', sub, 'wake_id', ARGV[5], 'phase', 'waking', 'holder', '0', 'holder_worker', '')
-- Outbox the wake: score = now_ns at arm time, so dueWorker re-fires it in O(owed)
-- if this wake is lost (Move 2). The ack(done)/release ZREMs it; a re-arm after a
-- FENCED re-ZADDs at the new now_ns. Same {__ds} slot, so still single-slot.
redis.call('ZADD', KEYS[3], ARGV[2], ARGV[1])
if ARGV[4] == '1' then
  local until_ns = tonumber(ARGV[2]) + tonumber(ARGV[3]) * 1000000
  redis.call('HSET', sub, 'lease_until_ns', tostring(until_ns))
  redis.call('ZADD', KEYS[2], until_ns, ARGV[1])
else
  -- pull-wake: mark the wake event as not yet emitted. The lease is not armed
  -- (it starts at claim), so nothing in the schedule recovers a crash between
  -- this arm and the wake-stream append; the recovery sweep keys off this flag
  -- to re-emit a stranded wake.
  redis.call('HSET', sub, 'wake_event_sent_ns', '0')
end
return { 'ARMED', tostring(gen), ARGV[5] }
