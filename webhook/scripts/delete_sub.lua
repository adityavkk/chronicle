-- delete_sub.lua — tombstone a subscription (PROTOCOL §6.3): drop its record,
-- links, id-set membership, shard fence records, and schedule entries. In-flight
-- callback/ack/release requests then fence (the record is gone) and cannot
-- advance cursors. The Go caller removes the per-stream fan-out index entries
-- (read before deletion) separately, since those keys are reconciled by the sweep.
local k_sub = KEYS[1]
local k_subs_set = KEYS[2]
local k_links = KEYS[3]
local k_lease_zset = KEYS[4]
local k_retry_zset = KEYS[5]
local k_due_zset = KEYS[6]
local k_shard_registry = KEYS[7]
local a_id = ARGV[1]
local a_authorized = ARGV[2]
local a_expected_owner = ARGV[3]
local a_expected_incarnation = ARGV[4]
local a_expected_cfg_hash = ARGV[5]
local a_num_paths = ARGV[6]
local i = 7
local expected_paths = {}
for _ = 1, tonumber(a_num_paths) or 0 do
  expected_paths[#expected_paths + 1] = ARGV[i]
  i = i + 1
end
if a_authorized == '1' then
  local expectation = subscription_expectation_status(
    k_sub, k_links, a_expected_owner, a_expected_incarnation,
    a_expected_cfg_hash, a_num_paths, expected_paths)
  if expectation == 'NOSUB' then return { 'NOSUB' } end
  if expectation ~= 'MATCHED' then return { 'FORBIDDEN' } end
end
redis.call('DEL', k_sub)
redis.call('DEL', k_links)
redis.call('SREM', k_subs_set, a_id)
redis.call('ZREM', k_lease_zset, a_id)
redis.call('ZREM', k_retry_zset, a_id)
redis.call('ZREM', k_due_zset, a_id)
local shards = redis.call('SMEMBERS', k_shard_registry)
for _, g in ipairs(shards) do
  local member = a_id .. ':g:' .. g
  redis.call('DEL', k_sub .. ':g:' .. g)
  redis.call('ZREM', k_lease_zset, member)
  redis.call('ZREM', k_retry_zset, member)
  redis.call('ZREM', k_due_zset, member)
end
redis.call('DEL', k_shard_registry)
return { 'OK' }
