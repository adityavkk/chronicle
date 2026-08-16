-- grant_append_fence.lua - install or renew one stream-slot claim marker.
--
-- KEYS: 1=stream meta 2=append-fence marker (same Redis Cluster slot)
-- ARGV: 1=generation 2=wakeId 3=holder 4=leaseUntilNs 5=nowNs 6=retentionMs
--
-- A same-generation grant may only renew the exact live claim. A higher
-- generation supersedes the old marker. Revoked generations cannot be revived.

local generation = ARGV[1]
local wake_id = ARGV[2]
local holder = ARGV[3]
local lease_until_ns = ARGV[4]
local now_ns = tonumber(ARGV[5])

if tonumber(lease_until_ns) <= now_ns then return { 'FENCED' } end

local stream_incarnation = redis.call('HGET', KEYS[1], 'incarnation')
if stream_incarnation == false then return { 'NOTFOUND' } end

local current = redis.call('HMGET', KEYS[2],
  'state', 'generation', 'wake_id', 'holder')
if current[2] ~= false then
  local generation_cmp = int_cmp(generation, current[2])
  if generation_cmp < 0 then return { 'FENCED' } end
  if generation_cmp == 0 then
    if current[1] ~= 'live'
      or current[3] ~= wake_id
      or current[4] ~= holder then
      return { 'FENCED' }
    end
  end
end

redis.call('HSET', KEYS[2],
  'state', 'live',
  'generation', generation,
  'wake_id', wake_id,
  'holder', holder,
  'lease_until_ns', lease_until_ns,
  'stream_incarnation', stream_incarnation)
redis.call('PEXPIRE', KEYS[2], ARGV[6])
return { 'OK' }
