-- release.lua — voluntary lease release without acking (PROTOCOL §7.2). Fenced
-- like ack. The caller re-issues a wake afterward if pending work remains.
-- KEYS: 1=sub 2=lease_zset 3=retry_zset 4=due_zset
-- ARGV: 1=id 2=req_gen 3=req_wake 4=token_gen
-- Reply: {status} ; OK | FENCED | NOSUB
local sub = KEYS[1]
if redis.call('EXISTS', sub) == 0 then
  return { 'NOSUB' }
end
local gen = redis.call('HGET', sub, 'generation')
local wake = redis.call('HGET', sub, 'wake_id')
if fenced(gen, wake, ARGV[2], ARGV[3], ARGV[4]) then
  return { 'FENCED' }
end
redis.call('HSET', sub, 'phase', 'idle', 'holder', '0', 'holder_worker', '',
  'wake_id', '', 'lease_until_ns', '0')
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('ZREM', KEYS[4], ARGV[1])
return { 'OK' }
