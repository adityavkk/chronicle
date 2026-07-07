-- rotate_key.lua — the atomic key-rotation transition (#123/#126 TBrot):
-- compare-and-set on the family's active_kid so two replicas rotating
-- concurrently mint exactly one successor. The winner's predecessor is
-- re-marked "retiring" with its retire_after stamp appended to the material
-- ("<priv>:<created>:retiring:<retire_after_unix>"); the successor is
-- installed and the active pointer flipped, all in one script in the {__ds}
-- slot (ADR-0001).
local k_family_hash = KEYS[1]
local k_active_kid = KEYS[2]
local a_expected_active_kid = ARGV[1]
local a_new_kid = ARGV[2]
local a_new_material = ARGV[3]
local a_retire_after_unix = ARGV[4]
local cur = redis.call('GET', k_active_kid)
if not cur or cur ~= a_expected_active_kid then
  return { 'conflict', cur or '' }
end
local old = redis.call('HGET', k_family_hash, cur)
if not old then
  return { 'conflict', '' }
end
local priv, created = string.match(old, '^([^:]+):([^:]+):')
if not priv then
  return { 'conflict', '' }
end
redis.call('HSET', k_family_hash, cur, priv .. ':' .. created .. ':retiring:' .. a_retire_after_unix)
redis.call('HSET', k_family_hash, a_new_kid, a_new_material)
redis.call('SET', k_active_kid, a_new_kid)
return { 'rotated', a_new_kid }
