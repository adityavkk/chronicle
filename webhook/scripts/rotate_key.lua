-- rotate_key.lua — the atomic key-rotation transition (#123/#126 TBrot):
-- compare-and-set on the family's active_kid so two replicas rotating
-- concurrently mint exactly one successor. The winner's predecessor is
-- re-marked "retiring" with its retire_after stamp appended to the material
-- ("<priv>:<created>:retiring:<retire_after_unix>"); the successor is
-- installed and the active pointer flipped, all in one script in the {__ds}
-- slot (ADR-0001).
-- KEYS: 1=family_hash 2=active_kid
-- ARGV: 1=expected_active_kid 2=new_kid 3=new_material 4=retire_after_unix
-- Reply: {'rotated', new_kid} | {'conflict', current_kid_or_empty}
local cur = redis.call('GET', KEYS[2])
if not cur or cur ~= ARGV[1] then
  return { 'conflict', cur or '' }
end
local old = redis.call('HGET', KEYS[1], cur)
if not old then
  return { 'conflict', '' }
end
local priv, created = string.match(old, '^([^:]+):([^:]+):')
if not priv then
  return { 'conflict', '' }
end
redis.call('HSET', KEYS[1], cur, priv .. ':' .. created .. ':retiring:' .. ARGV[4])
redis.call('HSET', KEYS[1], ARGV[2], ARGV[3])
redis.call('SET', KEYS[2], ARGV[2])
return { 'rotated', ARGV[2] }
