-- claim_shard.lua — CAS takeover of a slot-ownership lease (issue #14, the
-- {ownership}-tagged analogue of claim.lua's expired-lease takeover). It grants
-- the lease ONLY when the current owner is expired, missing, or the caller
-- itself, and bumps owner_epoch on every *transfer* (never on a same-owner
-- renew) — so a deposed-but-resumed owner carries a STALE epoch and is fenced by
-- check_owner.lua / the inlined schedule-write checks.
--
-- This shards which REPLICA runs autonomous background work for a slot; it is
-- ORTHOGONAL to claim.lua's per-(subId,g) claim granularity (#11). The owner-
-- epoch fence it mints is layered ABOVE the per-subscription (gen,wake_id) fence,
-- never replacing it (06 correction #1): it only suppresses a deposed owner's
-- wasted work, while the (gen,wake_id) fence stays the safety boundary that makes
-- any leaked duplicate harmless.
--
-- Mirrors claim.lua:24-38: the BUSY guard returns for an unexpired live foreign
-- lease; every other grantable case (unowned, expired-takeover, or self) writes
-- the lease, and a TRANSFER (owner ~= me) rotates the fence via HINCRBY — minting
-- epoch 1 on the very first claim and bumping on every later takeover, exactly as
-- claim.lua HINCRBYs the generation on an expired-lease takeover. A same-owner
-- renew keeps the epoch (bump-on-transfer-only) so it never gratuitously fences
-- the owner's own outstanding work. The model in jepsen/checker/model_shard.go
-- (T3) checks these semantics exactly.
-- KEYS: 1=slot (ds:{ownership}:slot:<h>)
-- ARGV: 1=replica_id 2=now_ns 3=slot_lease_ttl_ms
-- Reply: {status, owner_id, owner_epoch, lease_expiry_ns} ; CLAIMED | RENEWED | BUSY
local slot, me = KEYS[1], ARGV[1]
local now = tonumber(ARGV[2])
local owner = redis.call('HGET', slot, 'owner_id')
local exp = tonumber(redis.call('HGET', slot, 'lease_expiry_ns')) or 0
if owner ~= false and owner ~= me and exp > now then
  -- A live foreign owner holds an unexpired lease: nothing is granted (mirrors
  -- claim.lua's BUSY for an unexpired live lease, claim.lua:33-34).
  return { 'BUSY', owner, redis.call('HGET', slot, 'owner_epoch'), redis.call('HGET', slot, 'lease_expiry_ns') }
end
local epoch
if owner == me then
  epoch = redis.call('HGET', slot, 'owner_epoch')                 -- renew: keep the epoch
else
  epoch = tostring(redis.call('HINCRBY', slot, 'owner_epoch', 1)) -- transfer: bump (1 on first claim)
end
local until_ns = now + tonumber(ARGV[3]) * 1000000
redis.call('HSET', slot, 'owner_id', me, 'lease_expiry_ns', tostring(until_ns))
return { (owner == me) and 'RENEWED' or 'CLAIMED', me, epoch, tostring(until_ns) }
