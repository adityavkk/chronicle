-- unlink_stream.lua — remove an explicit stream link (PROTOCOL §6.4). If the
-- subscription's glob pattern still matches the path (still_glob='1'), the link
-- is kept as a glob link with its cursor preserved; otherwise it is removed.
local k_links = KEYS[1]
local a_path = ARGV[1]
local a_still_glob = ARGV[2]
local cur = redis.call('HGET', k_links, a_path)
if cur == false then
  return { 'GONE' }
end
if a_still_glob == '1' then
  local _, off = split_link(cur)
  redis.call('HSET', k_links, a_path, 'glob:' .. off)
  return { 'GLOB' }
end
redis.call('HDEL', k_links, a_path)
return { 'REMOVED' }
