-- release.lua — voluntary lease release without acking (PROTOCOL §7.2). Fenced
-- like ack. The caller re-issues a wake afterward if pending work remains.
-- release.lua is in the TOCTOU inline-check set (GAP3 consistency, issue #14): it
-- idles the sub and ZREMs the due mark exactly like ack(done), so an owner-scoped
-- release must inline the same owner-epoch check (epoch '' => skip on the external
-- path, where the (gen,wake_id) fence below is the guard).
local k_sub = KEYS[1]
local k_lease_zset = KEYS[2]
local k_retry_zset = KEYS[3]
local k_due_zset = KEYS[4]
local k_slot = KEYS[5]
local a_id = ARGV[1]
local a_req_gen = ARGV[2]
local a_req_wake = ARGV[3]
local a_token_gen = ARGV[4]
local a_replica_id = ARGV[5]
local a_expected_epoch = ARGV[6]
local sub = k_sub
if owner_fenced(k_slot, a_replica_id, a_expected_epoch) then
  return { 'FENCED' }
end
if redis.call('EXISTS', sub) == 0 then
  return { 'NOSUB' }
end
local gen = redis.call('HGET', sub, 'generation')
local wake = redis.call('HGET', sub, 'wake_id')
if fenced(gen, wake, a_req_gen, a_req_wake, a_token_gen) then
  return { 'FENCED' }
end
redis.call('HSET', sub, 'phase', 'idle', 'holder', '0', 'holder_worker', '',
  'wake_id', '', 'lease_until_ns', '0')
redis.call('ZREM', k_lease_zset, a_id)
redis.call('ZREM', k_retry_zset, a_id)
-- GAP3: release idles the sub exactly like ack(done), so it must also clear the
-- due-set wake mark — otherwise a voluntarily-released sub strands a phantom mark
-- the dueWorker would re-fire forever (claim_due never ZREMs). Same {__ds} slot.
redis.call('ZREM', k_due_zset, a_id)
return { 'OK' }
