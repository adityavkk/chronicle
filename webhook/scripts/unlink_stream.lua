-- unlink_stream.lua — remove an explicit stream link (PROTOCOL §6.4). If the
-- subscription's glob pattern still matches the path (still_glob='1'), the link
-- is kept as a glob link with its cursor preserved; otherwise it is removed.
local k_sub = KEYS[1]
local k_links = KEYS[2]
local a_path = ARGV[1]
local a_still_glob = ARGV[2]
local a_authorized = ARGV[3]
local a_expected_owner = ARGV[4]
local a_expected_incarnation = ARGV[5]
local a_expected_cfg_hash = ARGV[6]
local a_num_paths = ARGV[7]
local i = 8
local expected_paths = {}
for _ = 1, tonumber(a_num_paths) or 0 do
  expected_paths[#expected_paths + 1] = ARGV[i]
  i = i + 1
end
if a_authorized == '1' then
  local expectation = subscription_expectation_status(
    k_sub, k_links, a_expected_owner, a_expected_incarnation,
    a_expected_cfg_hash, a_num_paths, expected_paths)
  if expectation == 'NOSUB' then return { 'NOSUB' } end
  if expectation ~= 'MATCHED' then return { 'FORBIDDEN' } end
end
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
