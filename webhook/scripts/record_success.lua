-- record_success.lua — clear webhook failure bookkeeping after a delivery is
-- accepted, but only if the completed wake is still current. A stale success from
-- a superseded generation must not erase the current retry state.
-- KEYS: 1=sub 2=retry_zset [3=slot (ds:{__ds:h}:ownership:slot:<h>) when owned]
-- ARGV: 1=id 2=generation 3=wake_id 4=replica_id 5=expected_epoch
-- Reply: {status} ; OK | STALE | FENCED | NOSUB
local sub = KEYS[1]
-- Owner-epoch fence (issue #14, TOCTOU): retry-worker successes are external POST
-- completions, so the pre-POST check_owner is only a hint. The post-POST retry
-- cleanup must inline the current owner check atomically with the cleanup.
if owner_fenced(KEYS[3], ARGV[4], ARGV[5]) then
  return { 'FENCED' }
end
if redis.call('EXISTS', sub) == 0 then
  return { 'NOSUB' }
end
if redis.call('HGET', sub, 'generation') ~= ARGV[2] or redis.call('HGET', sub, 'wake_id') ~= ARGV[3] then
  return { 'STALE' }
end
redis.call('HSET', sub, 'status', 'active', 'retry_count', '0', 'first_fail_ns', '0', 'next_attempt_ns', '0')
redis.call('ZREM', KEYS[2], ARGV[1])
return { 'OK' }
