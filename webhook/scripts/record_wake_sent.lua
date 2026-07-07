-- record_wake_sent.lua — stamp that the current pull-wake event was durably
-- appended to the wake stream, fenced on the current generation/wake so a stamp
-- from a superseded wake is ignored. Lets the recovery sweep tell "event emitted"
-- from "stranded between arm and emit" (where wake_event_sent_ns stays 0).
local k_sub = KEYS[1]
local a_now_ns = ARGV[1]
local a_generation = ARGV[2]
local a_wake_id = ARGV[3]
local sub = k_sub
if redis.call('EXISTS', sub) == 0 then
  return { 'NOSUB' }
end
if redis.call('HGET', sub, 'generation') ~= a_generation or redis.call('HGET', sub, 'wake_id') ~= a_wake_id then
  return { 'STALE' }
end
redis.call('HSET', sub, 'wake_event_sent_ns', a_now_ns)
return { 'OK' }
