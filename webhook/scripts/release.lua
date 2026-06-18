-- release.lua — voluntary lease release without acking (PROTOCOL §7.2). Fenced
-- like ack. The caller re-issues a wake afterward if pending work remains.
-- KEYS: 1=sub 2=lease_zset 3=retry_zset
-- ARGV: 1=id 2=req_gen 3=req_wake 4=token_gen 5=shard 6=lease_member
--       7=claim_mode('legacy'|'sharded')
-- Reply: {status} ; OK | FENCED | NOSUB
local sub = KEYS[1]
if redis.call('EXISTS', sub) == 0 then
  return { 'NOSUB' }
end
local conflict = claim_mode_conflict(sub, ARGV[7])
if conflict then
  return { 'FENCED' }
end
local shard = ARGV[5]
local phase_field = shard_field('phase', shard)
local gen_field = shard_field('generation', shard)
local wake_field = shard_field('wake_id', shard)
local holder_field = shard_field('holder', shard)
local holder_worker_field = shard_field('holder_worker', shard)
local lease_until_field = shard_field('lease_until_ns', shard)
local gen = redis.call('HGET', sub, gen_field) or '0'
local wake = redis.call('HGET', sub, wake_field) or ''
if fenced(gen, wake, ARGV[2], ARGV[3], ARGV[4]) then
  return { 'FENCED' }
end
redis.call('HSET', sub, phase_field, 'idle', holder_field, '0', holder_worker_field, '',
  wake_field, '', lease_until_field, '0')
redis.call('ZREM', KEYS[2], ARGV[6])
if shard == '0' then
  redis.call('ZREM', KEYS[3], ARGV[1])
end
return { 'OK' }
