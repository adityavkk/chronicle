-- claim.lua — pull-wake claim with a compare-and-set lease (PROTOCOL §7.2). A
-- claim is rejected while another worker holds an unexpired lease. On grant the
-- lease is armed; if no wake is currently in flight (idle, or wake already
-- cleared) a fresh generation/wake_id is minted so the worker has a valid fence,
-- otherwise the in-flight wake is reused so two workers racing one wake event
-- collide rather than both "succeeding".
-- KEYS: 1=sub 2=lease_zset
-- ARGV: 1=id 2=worker 3=now_ns 4=lease_ttl_ms 5=new_wake_id
-- Reply: {status, generation, wake_id, holder} ; CLAIMED | BUSY | NOSUB
local sub = KEYS[1]
if redis.call('EXISTS', sub) == 0 then
  return { 'NOSUB' }
end
local phase = redis.call('HGET', sub, 'phase')
local holder = redis.call('HGET', sub, 'holder')
local lease_until = tonumber(redis.call('HGET', sub, 'lease_until_ns')) or 0
local now = tonumber(ARGV[3])
if phase == 'live' and holder == '1' and lease_until > now then
  return { 'BUSY', redis.call('HGET', sub, 'generation'), '', redis.call('HGET', sub, 'holder_worker') }
end
local gen = redis.call('HGET', sub, 'generation')
local wake = redis.call('HGET', sub, 'wake_id')
if phase == 'idle' or wake == '' then
  gen = tostring(redis.call('HINCRBY', sub, 'generation', 1))
  wake = ARGV[5]
  redis.call('HSET', sub, 'wake_id', wake)
end
local until_ns = now + tonumber(ARGV[4]) * 1000000
redis.call('HSET', sub, 'phase', 'live', 'holder', '1', 'holder_worker', ARGV[2], 'lease_until_ns', tostring(until_ns))
redis.call('ZADD', KEYS[2], until_ns, ARGV[1])
return { 'CLAIMED', gen, wake, ARGV[2] }
