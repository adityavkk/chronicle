-- unlink_stream.lua — remove an explicit stream link (PROTOCOL §6.4). If the
-- subscription's glob pattern still matches the path (still_glob='1'), the link
-- is kept as a glob link with its cursor preserved; otherwise it is removed.
-- KEYS: 1=links
-- ARGV: 1=path 2=still_glob('0'/'1')
-- Reply: {status} ; REMOVED | GLOB | GONE
local cur = redis.call('HGET', KEYS[1], ARGV[1])
if cur == false then
  return { 'GONE' }
end
if ARGV[2] == '1' then
  local _, off = split_link(cur)
  redis.call('HSET', KEYS[1], ARGV[1], 'glob:' .. off)
  return { 'GLOB' }
end
redis.call('HDEL', KEYS[1], ARGV[1])
return { 'REMOVED' }
