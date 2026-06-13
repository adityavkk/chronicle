-- link_stream.lua — link a stream to a subscription at the given offset if it is
-- not already linked; an explicit link upgrades an existing glob link (explicit
-- takes precedence, PROTOCOL §6.1), preserving the cursor.
-- KEYS: 1=links
-- ARGV: 1=path 2=link_type 3=offset
-- Reply: {status} ; LINKED | UPGRADED | EXISTS
local cur = redis.call('HGET', KEYS[1], ARGV[1])
if cur == false then
  redis.call('HSET', KEYS[1], ARGV[1], ARGV[2] .. ':' .. ARGV[3])
  return { 'LINKED' }
end
if ARGV[2] == 'explicit' then
  local _, off = split_link(cur)
  redis.call('HSET', KEYS[1], ARGV[1], 'explicit:' .. off)
  return { 'UPGRADED' }
end
return { 'EXISTS' }
