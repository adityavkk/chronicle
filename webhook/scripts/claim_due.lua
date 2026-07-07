-- claim_due.lua — atomically take due members from a schedule ZSET (lease or
-- retry) by RE-SCORING them forward to an in-flight visibility window, never
-- ZREM-ing them (docs/research/07 §6.1). A worker that dies after claiming a due
-- member leaves it to fall due again and be reclaimed — at-least-once by
-- construction. Single-threaded Redis makes this the leaderless claim: exactly
-- one replica's re-score wins a given member per tick.
local k_zset = KEYS[1]
local a_now_ns = ARGV[1]
local a_limit = ARGV[2]
local a_visibility_ns = ARGV[3]
local due = redis.call('ZRANGEBYSCORE', k_zset, '-inf', a_now_ns, 'LIMIT', 0, tonumber(a_limit))
local vis = tonumber(a_now_ns) + tonumber(a_visibility_ns)
for _, m in ipairs(due) do
  redis.call('ZADD', k_zset, vis, m)
end
return due
