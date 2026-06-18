-- claim_due.lua — atomically take due members from a schedule ZSET (lease or
-- retry) by RE-SCORING them forward to an in-flight visibility window, never
-- ZREM-ing them (docs/research/07 §6.1). A worker that dies after claiming a due
-- member leaves it to fall due again and be reclaimed — at-least-once by
-- construction. Single-threaded Redis makes this the leaderless claim: exactly
-- one replica's re-score wins a given member per tick.
-- KEYS: 1=zset 2=ownership_slot
-- ARGV: 1=now_ns 2=limit 3=visibility_ns 4=owner_id 5=owner_epoch
-- Reply: array of member ids now leased to the caller, or { 'FENCED' }
if owner_fenced(KEYS[2], ARGV[4], ARGV[5]) then
  return { 'FENCED' }
end
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
local vis = tonumber(ARGV[1]) + tonumber(ARGV[3])
for _, m in ipairs(due) do
  redis.call('ZADD', KEYS[1], vis, m)
end
return due
