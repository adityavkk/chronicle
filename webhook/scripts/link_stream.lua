-- link_stream.lua — link a stream to a subscription at the given offset if it is
-- not already linked; an explicit link upgrades an existing glob link (explicit
-- takes precedence, PROTOCOL §6.1), preserving the cursor.
local k_sub = KEYS[1]
local k_links = KEYS[2]
local a_path = ARGV[1]
local a_link_type = ARGV[2]
local a_offset = ARGV[3]
local a_authorized = ARGV[4]
local a_expected_owner = ARGV[5]
local a_expected_incarnation = ARGV[6]
local a_expected_cfg_hash = ARGV[7]
local a_num_paths = ARGV[8]
local i = 9
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
  redis.call('HSET', k_links, a_path, a_link_type .. ':' .. a_offset)
  return { 'LINKED' }
end
if a_link_type == 'explicit' then
  local _, off = split_link(cur)
  redis.call('HSET', k_links, a_path, 'explicit:' .. off)
  return { 'UPGRADED' }
end
return { 'EXISTS' }
