-- seal_append_fence.lua - tombstone one stream-slot claim marker and, on a
-- write-fenced stream, seal its generation for this authority (#183).
--
-- KEYS: 1=stream meta 2=append-fence marker (same Redis Cluster slot)
-- ARGV: 1=generation 2=wakeId 3=holder 4=retentionMs
--
-- Reply: {'STALE'} | {'NOTFOUND'} | {'OK', outcome, sealGen, sealOff}
--        outcome one of sealed|already|unfenced
--
-- Check order: marker staleness (no mutation on STALE) → tombstone → stream
-- existence → fenced → seal monotone per authority. Runs at done, release,
-- delete and unlink, before the control-plane idle, so a late write of the
-- sealed generation is refused atomically with the append; a redelivered done
-- is idempotent ('already'). The seal records the class's definite last
-- offset (wfLastOff, falling back to the tail when the class never wrote).
-- Nothing is published: stream content does not change.

local generation, wake_id, holder = ARGV[1], ARGV[2], ARGV[3]
local m = meta_map(KEYS[1])

local cur = redis.call('HMGET', KEYS[2], 'generation', 'wake_id', 'holder')
if cur[1] ~= false then
  local generation_cmp = int_cmp(generation, cur[1])
  if generation_cmp < 0
    or (generation_cmp == 0 and (cur[2] ~= wake_id or cur[3] ~= holder)) then
    return { 'STALE' }
  end
end

redis.call('HSET', KEYS[2],
  'state', 'revoked',
  'generation', generation,
  'wake_id', wake_id,
  'holder', holder,
  'lease_until_ns', '0')
redis.call('PEXPIRE', KEYS[2], ARGV[4])

if m == nil then return { 'NOTFOUND' } end
if m.wf ~= '1' then return { 'OK', 'unfenced', '0', '' } end

local auth = fence_auth(KEYS[2])
local seal = m['wfseal:' .. auth]
if seal then
  local seal_gen, _, seal_off = seal_parts(seal)
  if int_cmp(generation, seal_gen) <= 0 then return { 'OK', 'already', seal_gen, seal_off } end
end

local off = m.wfLastOff or m.tail
redis.call('HSET', KEYS[1],
  'wfseal:' .. auth, generation .. ':' .. wake_id .. ':' .. off,
  'wfSealGen', generation,
  'wfSealOff', off)
return { 'OK', 'sealed', generation, off }
