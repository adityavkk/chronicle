-- create_sub.lua — create or idempotently re-confirm a subscription (PROTOCOL §6.2).
-- gate (issue #126 TB3) atomically off this one reply: MATCHED/CONFLICT did
-- not mutate, and the owner is immutable once set at create.
local k_sub = KEYS[1]
local k_subs_set = KEYS[2]
local k_links = KEYS[3]
local k_incarnation_counter = KEYS[4]
local a_id = ARGV[1]
local a_cfg_hash = ARGV[2]
local a_now_ns = ARGV[3]
local a_type = ARGV[4]
local a_pattern = ARGV[5]
local a_webhook_url = ARGV[6]
local a_wake_stream = ARGV[7]
local a_lease_ttl_ms = ARGV[8]
local a_description = ARGV[9]
local a_owner = ARGV[10]
local a_num_links = ARGV[11]
local sub = k_sub
if redis.call('EXISTS', sub) == 1 then
  local owner = redis.call('HGET', sub, 'owner')
  if redis.call('HGET', sub, 'cfg_hash') == a_cfg_hash then
    return { 'MATCHED', owner or '' }
  end
  return { 'CONFLICT', owner or '' }
end
local incarnation = tostring(redis.call('INCR', k_incarnation_counter))
redis.call('HSET', sub,
  'id', a_id, 'cfg_hash', a_cfg_hash, 'created_ns', a_now_ns,
  'type', a_type, 'pattern', a_pattern, 'webhook_url', a_webhook_url,
  'wake_stream', a_wake_stream, 'lease_ttl_ms', a_lease_ttl_ms, 'description', a_description,
  'owner', a_owner,
  'incarnation', incarnation,
  'status', 'active', 'phase', 'idle', 'generation', '0', 'wake_id', '',
  'holder', '0', 'holder_worker', '', 'lease_until_ns', '0',
  'retry_count', '0', 'first_fail_ns', '0', 'next_attempt_ns', '0')
redis.call('SADD', k_subs_set, a_id)
local n = tonumber(a_num_links)
local i = 12
for _ = 1, n do
  redis.call('HSET', k_links, ARGV[i], ARGV[i + 1] .. ':' .. ARGV[i + 2])
  i = i + 3
end
return { 'CREATED', a_owner }
