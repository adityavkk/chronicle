-- claim_shard.lua — CAS takeover of a slot-ownership lease. Grants only when
-- the current owner is expired, missing, or the caller itself; bumps
-- owner_epoch on every transfer and never on same-owner renew.
-- KEYS: 1=slot (ds:{ownership}:slot:<h>)
-- ARGV: 1=replica_id 2=now_ns 3=lease_ttl_ms
-- Reply: {status, owner_id, owner_epoch, lease_expiry_ns} ; CLAIMED | RENEWED | BUSY
local slot = KEYS[1]
local me = ARGV[1]
local now = tonumber(ARGV[2])
local owner = redis.call('HGET', slot, 'owner_id')
local exp = tonumber(redis.call('HGET', slot, 'lease_expiry_ns')) or 0

if owner ~= false and owner ~= me and exp > now then
  return { 'BUSY', owner, redis.call('HGET', slot, 'owner_epoch'), tostring(exp) }
end

local epoch
if owner == me then
  epoch = redis.call('HGET', slot, 'owner_epoch')
  if epoch == false then
    epoch = tostring(redis.call('HINCRBY', slot, 'owner_epoch', 1))
  end
else
  epoch = tostring(redis.call('HINCRBY', slot, 'owner_epoch', 1))
end

local until_ns = now + tonumber(ARGV[3]) * 1000000
local until_ns_str = string.format('%.0f', until_ns)
redis.call('HSET', slot, 'owner_id', me, 'lease_expiry_ns', until_ns_str)
return { (owner == me) and 'RENEWED' or 'CLAIMED', me, epoch, until_ns_str }
