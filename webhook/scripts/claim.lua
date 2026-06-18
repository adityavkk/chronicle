-- claim.lua — pull-wake claim with a compare-and-set lease (PROTOCOL §7.2). A
-- claim is rejected while another worker holds an unexpired lease. On grant the
-- lease is armed, and the fence is rotated UNLESS this is the normal first claim
-- of an already-issued pull-wake event. Concretely:
--   * phase == 'waking' with a wake set: reuse the in-flight generation/wake_id,
--     so two workers racing the same wake event collide on one fence instead of
--     both "succeeding".
--   * every other grantable case — idle, a cleared wake, or TAKING OVER an
--     expired live lease — mints a fresh generation + wake_id. Rotating on
--     expired-lease takeover fences out the deposed holder: its still-unexpired
--     token carries the old generation, so a late ack from it returns FENCED and
--     cannot disturb the new holder's lease (the single-holder invariant).
-- KEYS: 1=sub 2=lease_zset
-- ARGV: 1=id 2=worker 3=now_ns 4=lease_ttl_ms 5=new_wake_id 6=shard
--       7=lease_member 8=claim_mode('legacy'|'sharded')
-- Reply: {status, generation, wake_id, holder, lease_lapsed('0'|'1')} ; CLAIMED | BUSY | NOSUB | MODE_CONFLICT
local sub = KEYS[1]
if redis.call('EXISTS', sub) == 0 then
  return { 'NOSUB' }
end
local conflict, existing_mode = claim_mode_conflict(sub, ARGV[8])
if conflict then
  return { 'MODE_CONFLICT', existing_mode }
end
if existing_mode == '' then
  redis.call('HSET', sub, 'claim_mode', ARGV[8])
end
local shard = ARGV[6]
local phase_field = shard_field('phase', shard)
local gen_field = shard_field('generation', shard)
local wake_field = shard_field('wake_id', shard)
local holder_field = shard_field('holder', shard)
local holder_worker_field = shard_field('holder_worker', shard)
local lease_until_field = shard_field('lease_until_ns', shard)
local phase = redis.call('HGET', sub, phase_field) or 'idle'
local holder = redis.call('HGET', sub, holder_field) or '0'
local lease_until = tonumber(redis.call('HGET', sub, lease_until_field)) or 0
local now = tonumber(ARGV[3])
if phase == 'live' and holder == '1' and lease_until > now then
  return { 'BUSY', redis.call('HGET', sub, gen_field) or '0', '', redis.call('HGET', sub, holder_worker_field) or '' }
end
local gen = redis.call('HGET', sub, gen_field) or '0'
local wake = redis.call('HGET', sub, wake_field) or ''
local lapsed = (phase == 'live' and holder == '1' and lease_until <= now)
-- Reaching here with phase == 'live' means the lease is expired (the BUSY guard
-- above already returned for an unexpired live lease), so that case rotates too.
if not (phase == 'waking' and wake ~= '') then
  gen = tostring(redis.call('HINCRBY', sub, gen_field, 1))
  wake = ARGV[5]
  redis.call('HSET', sub, wake_field, wake)
end
local until_ns = now + tonumber(ARGV[4]) * 1000000
redis.call('HSET', sub, phase_field, 'live', holder_field, '1', holder_worker_field, ARGV[2], lease_until_field, tostring(until_ns))
redis.call('ZADD', KEYS[2], until_ns, ARGV[7])
return { 'CLAIMED', gen, wake, ARGV[2], lapsed and '1' or '0' }
