-- expire_lease.lua — clear an expired lease (PROTOCOL §7.3): if the deadline has
-- passed, drop the holder and wake token and return the subscription to idle so
-- a re-wake can be issued if pending work remains. Pull-wake "waking" with no
-- lease (lease_until_ns=0) is left untouched — its wake event is already in the
-- wake stream for workers to claim.
-- KEYS: 1=sub 2=lease_zset 3=due_zset
-- ARGV: 1=id 2=now_ns 3=pending('0'/'1')
-- Reply: {status} ; EXPIRED | ACTIVE | NOSUB
local sub = KEYS[1]
if redis.call('EXISTS', sub) == 0 then
  return { 'NOSUB' }
end
local lease_until = tonumber(redis.call('HGET', sub, 'lease_until_ns')) or 0
local phase = redis.call('HGET', sub, 'phase')
if (phase == 'live' or phase == 'waking') and lease_until > 0 and lease_until <= tonumber(ARGV[2]) then
  redis.call('HSET', sub, 'phase', 'idle', 'holder', '0', 'holder_worker', '',
    'wake_id', '', 'lease_until_ns', '0')
  redis.call('ZREM', KEYS[2], ARGV[1])
  if ARGV[3] == '1' then
    redis.call('ZADD', KEYS[3], ARGV[2], ARGV[1])
  else
    redis.call('ZREM', KEYS[3], ARGV[1])
  end
  return { 'EXPIRED' }
end
return { 'ACTIVE' }
