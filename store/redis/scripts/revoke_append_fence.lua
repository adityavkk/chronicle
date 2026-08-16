-- revoke_append_fence.lua - tombstone one stream-slot claim marker.
--
-- KEYS: 1=append-fence marker (same Redis Cluster slot as its stream)
-- ARGV: 1=generation 2=wakeId 3=holder 4=retentionMs
--
-- A delayed revocation cannot fence a newer generation. A same-generation
-- request must name the exact claim so one malformed caller cannot revoke a
-- different holder. The tombstone prevents a delayed renewal from reviving it.

local generation = ARGV[1]
local wake_id = ARGV[2]
local holder = ARGV[3]
local current = redis.call('HMGET', KEYS[1],
  'generation', 'wake_id', 'holder')

if current[1] ~= false then
  local generation_cmp = int_cmp(generation, current[1])
  if generation_cmp < 0 then return { 'STALE' } end
  if generation_cmp == 0
    and (current[2] ~= wake_id or current[3] ~= holder) then
    return { 'STALE' }
  end
end

redis.call('HSET', KEYS[1],
  'state', 'revoked',
  'generation', generation,
  'wake_id', wake_id,
  'holder', holder,
  'lease_until_ns', '0')
redis.call('PEXPIRE', KEYS[1], ARGV[4])
return { 'OK' }
