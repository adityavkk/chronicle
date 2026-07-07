-- check_owner.lua — verify the caller still owns slot h at the expected epoch
-- before an EXTERNAL side effect (the webhook POST), where an atomic inline check
-- is impossible because the effect crosses the network (issue #14). Schedule/due
-- writes do NOT call this — they inline the same check atomically with the write
-- (see arm_wake/ack/expire_lease/schedule_retry/release), because a separate
-- round-trip would not fence a GC pause between the check and the write.
--
-- This is the owner-epoch fence layered ABOVE the (gen,wake_id) fence: it
-- suppresses a deposed owner's wasted work, but the (gen,wake_id) fence on the
-- returned ack stays the real safety boundary that makes any leaked duplicate
-- harmless. It reads owner_id/owner_epoch only, never the lease clock, so its
-- OWNER verdict is time-free and exact (model_shard.go checks it strictly).
local k_slot = KEYS[1]
local a_replica_id = ARGV[1]
local a_expected_epoch = ARGV[2]
local owner = redis.call('HGET', k_slot, 'owner_id')
if owner == false then return { 'UNOWNED' } end
if owner ~= a_replica_id or redis.call('HGET', k_slot, 'owner_epoch') ~= a_expected_epoch then
  return { 'FENCED' }
end
return { 'OWNER' }
