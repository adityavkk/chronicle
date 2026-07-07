-- schedule_retry.lua — record a webhook delivery failure and persist the next
-- attempt time (PROTOCOL §7.1: "Retry metadata, including next_attempt_at, MUST
-- be persisted across ... eviction"). status flips to failed (PROTOCOL §6.3),
-- but only if the failed wake is still current. A stale failure from a duplicate
-- delivery must not resurrect retry state already cleared by a success/ack.
-- KEYS: 1=sub 2=retry_zset [3=slot (ds:{__ds:h}:ownership:slot:<h>) when owned]
-- ARGV: 1=id 2=now_ns 3=next_attempt_ns 4=generation 5=wake_id
--       6=replica_id 7=expected_epoch
-- Reply: {status, retry_count, first_fail_ns} ; OK | NOSUB | STALE | FENCED
local sub = KEYS[1]
-- Owner-epoch fence (issue #14, TOCTOU): an owner-scoped retry scheduler that has
-- been deposed must not re-arm the retry schedule it no longer owns; epoch ''
-- (the external delivery-failure path) skips the check.
if owner_fenced(KEYS[3], ARGV[6], ARGV[7]) then
  return { 'FENCED' }
end
if redis.call('EXISTS', sub) == 0 then
  return { 'NOSUB' }
end
local gen = redis.call('HGET', sub, 'generation')
local wake = redis.call('HGET', sub, 'wake_id')
if fenced(gen, wake, ARGV[4], ARGV[5], ARGV[4]) then
  return { 'STALE' }
end
redis.call('HINCRBY', sub, 'retry_count', 1)
local first = redis.call('HGET', sub, 'first_fail_ns')
if first == '0' or first == false then
  redis.call('HSET', sub, 'first_fail_ns', ARGV[2])
  first = ARGV[2]
end
redis.call('HSET', sub, 'status', 'failed', 'next_attempt_ns', ARGV[3])
redis.call('ZADD', KEYS[2], tonumber(ARGV[3]), ARGV[1])
return { 'OK', redis.call('HGET', sub, 'retry_count'), first }
