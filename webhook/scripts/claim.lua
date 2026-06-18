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
--
-- Claim granularity (the third axis, design 08): a claim NAMES a shard. The fence
-- lives in the per-(subId,g) shardstate hash (KEYS[2]); NOSUB is decided by the
-- subscription's CONFIG hash (KEYS[1]) existing, so a fresh never-claimed g>0
-- shard is grantable (its fence starts idle, minted here) rather than NOSUB. The
-- lease member (ARGV[1]) is the per-shard schedule member. At G=1 / shard 0,
-- KEYS[1]==KEYS[2]==sub hash and ARGV[1]==id, so this is byte-for-byte the
-- single-holder claim — the split is purely additive (08 §4).
-- KEYS: 1=sub(config) 2=shardstate 3=lease_zset
-- ARGV: 1=member 2=worker 3=now_ns 4=lease_ttl_ms 5=new_wake_id
-- Reply: {status, generation, wake_id, holder} ; CLAIMED | BUSY | NOSUB
local cfg = KEYS[1]
local sub = KEYS[2]
if redis.call('EXISTS', cfg) == 0 then
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
-- Reaching here with phase == 'live' means the lease is expired (the BUSY guard
-- above already returned for an unexpired live lease), so that case rotates too.
if not (phase == 'waking' and wake ~= '') then
  gen = tostring(redis.call('HINCRBY', sub, 'generation', 1))
  wake = ARGV[5]
  redis.call('HSET', sub, 'wake_id', wake)
end
local until_ns = now + tonumber(ARGV[4]) * 1000000
redis.call('HSET', sub, 'phase', 'live', 'holder', '1', 'holder_worker', ARGV[2], 'lease_until_ns', tostring(until_ns))
redis.call('ZADD', KEYS[3], until_ns, ARGV[1])
return { 'CLAIMED', gen, wake, ARGV[2] }
