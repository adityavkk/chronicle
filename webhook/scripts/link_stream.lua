-- link_stream.lua — link a stream to a subscription at the given offset if it is
-- not already linked; an explicit link upgrades an existing glob link (explicit
-- takes precedence, PROTOCOL §6.1), preserving the cursor.
local k_links = KEYS[1]
local a_path = ARGV[1]
local a_link_type = ARGV[2]
local a_offset = ARGV[3]
local cur = redis.call('HGET', k_links, a_path)
if cur == false then
  redis.call('HSET', k_links, a_path, a_link_type .. ':' .. a_offset)
  return { 'LINKED' }
end
if a_link_type == 'explicit' then
  local _, off = split_link(cur)
  redis.call('HSET', k_links, a_path, 'explicit:' .. off)
  return { 'UPGRADED' }
end
return { 'EXISTS' }
