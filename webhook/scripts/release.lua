-- release.lua — voluntary lease release without acking (PROTOCOL §7.2). Fenced
-- like ack. The caller re-issues a wake afterward if pending work remains.
-- KEYS: 1=sub 2=lease_zset 3=retry_zset 4=due_zset 5=slot (ds:{ownership}:slot:<h>)
-- ARGV: 1=id 2=req_gen 3=req_wake 4=token_gen 5=replica_id 6=expected_epoch
-- Reply: {status} ; OK | FENCED | NOSUB
-- release.lua is in the TOCTOU inline-check set (GAP3 consistency, issue #14): it
-- idles the sub and ZREMs the due mark exactly like ack(done), so an owner-scoped
-- release must inline the same owner-epoch check (epoch '' => skip on the external
-- path, where the (gen,wake_id) fence below is the guard).
local sub = KEYS[1]
if owner_fenced(KEYS[5], ARGV[5], ARGV[6]) then
  return { 'FENCED' }
end
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
-- GAP3: release idles the sub exactly like ack(done), so it must also clear the
-- due-set wake mark — otherwise a voluntarily-released sub strands a phantom mark
-- the dueWorker would re-fire forever (claim_due never ZREMs). Same {__ds} slot.
redis.call('ZREM', KEYS[4], ARGV[1])
return { 'OK' }
