-- schedule_retry.lua — record a webhook delivery failure and persist the next
-- attempt time (PROTOCOL §7.1: "Retry metadata, including next_attempt_at, MUST
-- be persisted across ... eviction"). status flips to failed (PROTOCOL §6.3).
-- KEYS: 1=sub 2=retry_zset
-- ARGV: 1=id 2=now_ns 3=next_attempt_ns
-- Reply: {status, retry_count, first_fail_ns} ; OK | NOSUB
local sub = KEYS[1]
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
