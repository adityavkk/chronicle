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
-- lives in the per-(subId,g) shardstate hash (k_shardstate); NOSUB is decided by the
-- subscription's CONFIG hash (k_sub_config) existing, so a fresh never-claimed g>0
-- shard is grantable (its fence starts idle, minted here) rather than NOSUB. The
-- lease member (a_member) is the per-shard schedule member. At G=1 / shard 0,
-- k_sub_config==k_shardstate==sub hash and a_member==id, so this is byte-for-byte the
-- single-holder claim — the split is purely additive (08 §4).
local k_sub_config = KEYS[1]
local k_shardstate = KEYS[2]
local k_lease_zset = KEYS[3]
local k_incarnation_counter = KEYS[4]
local k_shard_registry = KEYS[5]
local k_links = KEYS[6]
local a_member = ARGV[1]
local a_worker = ARGV[2]
local a_now_ns = ARGV[3]
local a_lease_ttl_ms = ARGV[4]
local a_new_wake_id = ARGV[5]
local a_shard_index = ARGV[6]
local a_expected_owner = ARGV[7]
local a_expected_incarnation = ARGV[8]
local a_expected_cfg_hash = ARGV[9]
local a_num_paths = ARGV[10]
local i = 11
local expected_paths = {}
for _ = 1, tonumber(a_num_paths) or 0 do
  expected_paths[#expected_paths + 1] = ARGV[i]
  i = i + 1
end
local cfg = k_sub_config
local sub = k_shardstate
-- Bind the route's authorization decision to the exact subscription object and
-- protected resource set it inspected. No lease or fence state changes before
-- this comparison. A delete/recreate or concurrent link mutation is denied
-- rather than transferring the prior decision to a different resource set.
local expectation = subscription_expectation_status(
  cfg, k_links, a_expected_owner, a_expected_incarnation,
  a_expected_cfg_hash, a_num_paths, expected_paths)
if expectation == 'NOSUB' then
  return { 'NOSUB' }
end
if expectation ~= 'MATCHED' then
  return { 'FORBIDDEN' }
end
local cfg_inc = a_expected_incarnation
local shard_inc = redis.call('HGET', sub, 'incarnation')
if k_sub_config ~= k_shardstate and cfg_inc ~= '' and shard_inc ~= cfg_inc then
  redis.call('DEL', sub)
end
local phase = redis.call('HGET', sub, 'phase')
local holder = redis.call('HGET', sub, 'holder')
local lease_until = tonumber(redis.call('HGET', sub, 'lease_until_ns')) or 0
local now = tonumber(a_now_ns)
if phase == 'live' and holder == '1' and lease_until > now then
  return { 'BUSY', redis.call('HGET', sub, 'generation'), '', redis.call('HGET', sub, 'holder_worker') }
end
local incarnation = redis.call('HGET', cfg, 'incarnation')
if incarnation == false or incarnation == '' then
  incarnation = tostring(redis.call('INCR', k_incarnation_counter))
  redis.call('HSET', cfg, 'incarnation', incarnation)
end
local gen = redis.call('HGET', sub, 'generation')
local wake = redis.call('HGET', sub, 'wake_id')
-- Reaching here with phase == 'live' means the lease is expired (the BUSY guard
-- above already returned for an unexpired live lease), so that case rotates too.
if not (phase == 'waking' and wake ~= '') then
  gen = tostring(redis.call('HINCRBY', sub, 'generation', 1))
  wake = a_new_wake_id
  redis.call('HSET', sub, 'wake_id', wake)
end
local until_ns = now + tonumber(a_lease_ttl_ms) * 1000000
redis.call('HSET', sub, 'phase', 'live', 'holder', '1', 'holder_worker', a_worker, 'lease_until_ns', tostring(until_ns), 'incarnation', incarnation)
if k_sub_config ~= k_shardstate then
  redis.call('SADD', k_shard_registry, a_shard_index)
end
redis.call('ZADD', k_lease_zset, until_ns, a_member)
return { 'CLAIMED', gen, wake, a_worker }
