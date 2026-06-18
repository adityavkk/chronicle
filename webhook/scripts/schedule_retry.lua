-- schedule_retry.lua — record a webhook delivery failure and persist the next
-- attempt time (PROTOCOL §7.1: "Retry metadata, including next_attempt_at, MUST
-- be persisted across ... eviction"). status flips to failed (PROTOCOL §6.3).
-- KEYS: 1=sub 2=retry_zset 3=slot (ds:{ownership}:slot:<h>)
-- ARGV: 1=id 2=now_ns 3=next_attempt_ns 4=replica_id 5=expected_epoch
-- Reply: {status, retry_count, first_fail_ns} ; OK | NOSUB | FENCED
local sub = KEYS[1]
-- Owner-epoch fence (issue #14, TOCTOU): an owner-scoped retry scheduler that has
-- been deposed must not re-arm the retry schedule it no longer owns; epoch ''
-- (the external delivery-failure path) skips the check.
if owner_fenced(KEYS[3], ARGV[4], ARGV[5]) then
  return { 'FENCED' }
end
if redis.call('EXISTS', sub) == 0 then
  return { 'NOSUB' }
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
