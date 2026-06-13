-- record_wake_sent.lua — stamp that the current pull-wake event was durably
-- appended to the wake stream, fenced on the current generation/wake so a stamp
-- from a superseded wake is ignored. Lets the recovery sweep tell "event emitted"
-- from "stranded between arm and emit" (where wake_event_sent_ns stays 0).
-- KEYS: 1=sub
-- ARGV: 1=now_ns 2=generation 3=wake_id
-- Reply: {status} ; OK | STALE | NOSUB
local sub = KEYS[1]
if redis.call('EXISTS', sub) == 0 then
  return { 'NOSUB' }
end
if redis.call('HGET', sub, 'generation') ~= ARGV[2] or redis.call('HGET', sub, 'wake_id') ~= ARGV[3] then
  return { 'STALE' }
end
redis.call('HSET', sub, 'wake_event_sent_ns', ARGV[1])
return { 'OK' }
