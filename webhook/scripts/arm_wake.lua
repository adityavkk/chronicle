-- arm_wake.lua — issue a new wake generation when the subscription is idle
-- (PROTOCOL §7: pending work creates a wake unless one is already in flight or a
-- lease is held). Coalescing falls out of the phase check. For webhook delivery
-- the lease is armed here (arm_lease='1'); for pull-wake it is not (the lease
-- starts at claim, PROTOCOL §7.3).
-- KEYS: 1=sub 2=lease_zset
-- ARGV: 1=id 2=now_ns 3=lease_ttl_ms 4=arm_lease('0'/'1') 5=new_wake_id
-- Reply: {status, generation, wake_id} ; ARMED | BUSY | NOSUB
local sub = KEYS[1]
if redis.call('EXISTS', sub) == 0 then
  return { 'NOSUB' }
end
if redis.call('HGET', sub, 'phase') ~= 'idle' then
  return { 'BUSY', redis.call('HGET', sub, 'generation'), redis.call('HGET', sub, 'wake_id') }
end
local gen = redis.call('HINCRBY', sub, 'generation', 1)
redis.call('HSET', sub, 'wake_id', ARGV[5], 'phase', 'waking', 'holder', '0', 'holder_worker', '')
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
