-- record_success.lua — clear webhook failure bookkeeping after a delivery is
-- accepted; status returns to active and the retry schedule is dropped.
-- KEYS: 1=sub 2=retry_zset
-- ARGV: 1=id
-- Reply: {status} ; OK | NOSUB
local sub = KEYS[1]
if redis.call('EXISTS', sub) == 0 then
  return { 'NOSUB' }
end
redis.call('HSET', sub, 'status', 'active', 'retry_count', '0', 'first_fail_ns', '0', 'next_attempt_ns', '0')
redis.call('ZREM', KEYS[2], ARGV[1])
return { 'OK' }
