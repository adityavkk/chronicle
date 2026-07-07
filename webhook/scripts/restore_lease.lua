-- restore_lease.lua — the failover-aware eager reconcile (issue #13): re-derive a
-- stranded subscription's dropped schedule entries from the durable sub hash. A
-- failover can ZREM the lease/due tail while leaving the sub hash intact (the L3
-- dropLeaseTail fault); the lease worker is then blind to the lapsed lease — its
-- ZADD is gone — so the subscription can never expire back to idle to be re-fired.
--
-- This re-ZADDs the lease entry at the hash's own lease_until_ns so the lease
-- worker sees the lapse on its next tick, and (when the caller computed pending
-- work, owed='1') re-owes the due mark so the dueWorker re-fires once idle. Both
-- entries are re-derived; the due mark is the same {__ds}:due outbox #12 owns
-- (this adds no new due-set lifecycle mutation — it restores a dropped entry).
--
-- Re-derived from the hash and CONDITIONED on the live/waking phase, so a sub a
-- concurrent release/ack/expire has since idled is left untouched: no stale entry
-- is leaked back into the schedule (which claim_due would then churn forever). A
-- re-ZADD of an entry still present is idempotent (it rewrites the same score), so
-- the pass is fence-safe to run on every recovery event.
local k_sub = KEYS[1]
local k_lease_zset = KEYS[2]
local k_due_zset = KEYS[3]
local a_id = ARGV[1]
local a_now_ns = ARGV[2]
local a_owed = ARGV[3]
local sub = k_sub
if redis.call('EXISTS', sub) == 0 then
  return { 'NOSUB' }
end
local phase = redis.call('HGET', sub, 'phase')
local lease_until = tonumber(redis.call('HGET', sub, 'lease_until_ns')) or 0
if (phase == 'live' or phase == 'waking') and lease_until > 0 then
  redis.call('ZADD', k_lease_zset, lease_until, a_id)
  if a_owed == '1' then
    redis.call('ZADD', k_due_zset, a_now_ns, a_id)
  end
  return { 'RESTORED' }
end
return { 'INTACT' }
