-- record_success.lua — clear webhook failure bookkeeping after a delivery is
-- accepted, but only if the completed wake is still current. A stale success from
-- a superseded generation must not erase the current retry state.
local k_sub = KEYS[1]
local k_retry_zset = KEYS[2]
local k_slot = KEYS[3]
local a_id = ARGV[1]
local a_generation = ARGV[2]
local a_wake_id = ARGV[3]
local a_replica_id = ARGV[4]
local a_expected_epoch = ARGV[5]
local sub = k_sub
-- Owner-epoch fence (issue #14, TOCTOU): retry-worker successes are external POST
-- completions, so the pre-POST check_owner is only a hint. The post-POST retry
-- cleanup must inline the current owner check atomically with the cleanup.
if owner_fenced(k_slot, a_replica_id, a_expected_epoch) then
  return { 'FENCED' }
end
if redis.call('EXISTS', sub) == 0 then
  return { 'NOSUB' }
end
if redis.call('HGET', sub, 'generation') ~= a_generation or redis.call('HGET', sub, 'wake_id') ~= a_wake_id then
  return { 'STALE' }
end
redis.call('HSET', sub, 'status', 'active', 'retry_count', '0', 'first_fail_ns', '0', 'next_attempt_ns', '0')
redis.call('ZREM', k_retry_zset, a_id)
return { 'OK' }
