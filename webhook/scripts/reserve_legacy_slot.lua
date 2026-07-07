-- reserve_legacy_slot.lua — rollout guard for the pre-#146 owner key.
--
-- New pods claim ds:{__ds:h}:ownership:slot:<h>. During a rolling deploy, old pods
-- still claim ds:{ownership}:slot:<h>. This single-key guard makes the old record
-- authoritative during migration: a live old owner blocks a new claim, and a new
-- claimant reserves the old key before claiming the new key so old pods see BUSY.
local k_legacy_slot = KEYS[1]
local a_replica_id = ARGV[1]
local a_now_ns = ARGV[2]
local a_slot_lease_ttl_ms = ARGV[3]
local slot, me = k_legacy_slot, a_replica_id
local now = tonumber(a_now_ns)
local owner = redis.call('HGET', slot, 'owner_id')
local epoch = redis.call('HGET', slot, 'owner_epoch')
local exp = tonumber(redis.call('HGET', slot, 'lease_expiry_ns')) or 0
if owner ~= false and owner ~= me and exp > now then
  return { 'BUSY', owner, epoch, redis.call('HGET', slot, 'lease_expiry_ns') }
end
if epoch == false or epoch == '' then epoch = '0' end
local until_ns = now + tonumber(a_slot_lease_ttl_ms) * 1000000
redis.call('HSET', slot, 'owner_id', me, 'owner_epoch', epoch, 'lease_expiry_ns', tostring(until_ns))
return { 'RESERVED', me, epoch, tostring(until_ns) }
