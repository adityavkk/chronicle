-- expire_lease.lua — clear an expired lease (PROTOCOL §7.3): if the deadline has
-- passed, drop the holder and wake token and return the subscription to idle so
-- a re-wake can be issued if pending work remains. Pull-wake "waking" with no
-- lease (lease_until_ns=0) is left untouched — its wake event is already in the
-- wake stream for workers to claim.
local k_sub = KEYS[1]
local k_lease_zset = KEYS[2]
local k_due_zset = KEYS[3]
local k_slot = KEYS[4]
local a_id = ARGV[1]
local a_now_ns = ARGV[2]
local a_replica_id = ARGV[3]
local a_expected_epoch = ARGV[4]
local sub = k_sub
-- Owner-epoch fence (issue #14, TOCTOU): the lease worker is the primary
-- owner-scoped caller — a deposed owner expiring/re-owing leases it no longer owns
-- is suppressed here, atomically with the ZREM/ZADD, so the new owner alone drives
-- the schedule. epoch '' (no scope) skips the check; the full sweep is the backstop.
if owner_fenced(k_slot, a_replica_id, a_expected_epoch) then
  return { 'FENCED' }
end
if redis.call('EXISTS', sub) == 0 then
  return { 'NOSUB' }
end
local lease_until = tonumber(redis.call('HGET', sub, 'lease_until_ns')) or 0
local phase = redis.call('HGET', sub, 'phase')
if (phase == 'live' or phase == 'waking') and lease_until > 0 and lease_until <= tonumber(a_now_ns) then
  redis.call('HSET', sub, 'phase', 'idle', 'holder', '0', 'holder_worker', '',
    'wake_id', '', 'lease_until_ns', '0')
  redis.call('ZREM', k_lease_zset, a_id)
  -- Re-owe: the in-flight wake lapsed, so re-mark the sub due (score = now_ns) for
  -- the dueWorker to re-fire (Move 2). The single-slot script cannot read stream
  -- tails to test pending work, so the ZADD is unconditional; the dueWorker
  -- reconciles it (DecideDue) — firing only if pending, else clearing the mark.
  redis.call('ZADD', k_due_zset, a_now_ns, a_id)
  return { 'EXPIRED' }
end
return { 'ACTIVE' }
