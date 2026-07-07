-- schedule_retry.lua — record a webhook delivery failure and persist the next
-- attempt time (PROTOCOL §7.1: "Retry metadata, including next_attempt_at, MUST
-- be persisted across ... eviction"). status flips to failed (PROTOCOL §6.3),
-- but only if the failed wake is still current. A stale failure from a duplicate
-- delivery must not resurrect retry state already cleared by a success/ack.
local k_sub = KEYS[1]
local k_retry_zset = KEYS[2]
local k_slot = KEYS[3]
local a_id = ARGV[1]
local a_now_ns = ARGV[2]
local a_next_attempt_ns = ARGV[3]
local a_generation = ARGV[4]
local a_wake_id = ARGV[5]
local a_replica_id = ARGV[6]
local a_expected_epoch = ARGV[7]
local sub = k_sub
-- Owner-epoch fence (issue #14, TOCTOU): an owner-scoped retry scheduler that has
-- been deposed must not re-arm the retry schedule it no longer owns; epoch ''
-- (the external delivery-failure path) skips the check.
if owner_fenced(k_slot, a_replica_id, a_expected_epoch) then
  return { 'FENCED' }
end
if redis.call('EXISTS', sub) == 0 then
  return { 'NOSUB' }
end
local gen = redis.call('HGET', sub, 'generation')
local wake = redis.call('HGET', sub, 'wake_id')
if fenced(gen, wake, a_generation, a_wake_id, a_generation) then
  return { 'STALE' }
end
redis.call('HINCRBY', sub, 'retry_count', 1)
local first = redis.call('HGET', sub, 'first_fail_ns')
if first == '0' or first == false then
  redis.call('HSET', sub, 'first_fail_ns', a_now_ns)
  first = a_now_ns
end
redis.call('HSET', sub, 'status', 'failed', 'next_attempt_ns', a_next_attempt_ns)
redis.call('ZADD', k_retry_zset, tonumber(a_next_attempt_ns), a_id)
return { 'OK', redis.call('HGET', sub, 'retry_count'), first }
