-- reconcile_lease.lua — rebuild volatile schedule entries from the durable
-- subscription HASH after a failover drops a lease or due ZSET tail. It does
-- not change the durable lease/fence state; it only mirrors it back into the
-- schedules the workers consume.
-- KEYS: 1=sub 2=lease_zset 3=due_zset
-- ARGV: 1=id 2=now_ns 3=shard 4=lease_member 5=pending('0'/'1')
-- Reply: {status, lease_repaired('0'|'1'), due_op('add'|'remove'|'none')} ; RECONCILED | SKIPPED | NOSUB
local sub = KEYS[1]
if redis.call('EXISTS', sub) == 0 then
  return { 'NOSUB', '0', 'none' }
end
local shard = ARGV[3]
local phase_field = shard_field('phase', shard)
local lease_until_field = shard_field('lease_until_ns', shard)
local phase = redis.call('HGET', sub, phase_field) or 'idle'
local lease_until = tonumber(redis.call('HGET', sub, lease_until_field)) or 0
if (phase ~= 'live' and phase ~= 'waking') or lease_until <= 0 then
  return { 'SKIPPED', '0', 'none' }
end

local repaired = '0'
if redis.call('ZSCORE', KEYS[2], ARGV[4]) == false then
  redis.call('ZADD', KEYS[2], string.format('%.0f', lease_until), ARGV[4])
  repaired = '1'
end

local due_op = 'remove'
if ARGV[5] == '1' then
  redis.call('ZADD', KEYS[3], ARGV[2], ARGV[1])
  due_op = 'add'
else
  redis.call('ZREM', KEYS[3], ARGV[1])
end

return { 'RECONCILED', repaired, due_op }
