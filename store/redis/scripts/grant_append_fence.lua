-- grant_append_fence.lua - install or renew one stream-slot claim marker.
--
-- KEYS: 1=stream meta 2=append-fence marker (same Redis Cluster slot)
-- ARGV: 1=generation 2=wakeId 3=holder 4=leaseUntilNs 5=nowNs 6=retentionMs
--
-- Reply: {'OK'} | {'OK','SUPERSEDED',<gen>} | {'NOTFOUND'} | {'FENCED'}
--
-- Check order: lease → stream existence → seal → marker generation →
-- same-generation exactness → supersession seal → install. A sealed
-- generation of this authority is never re-granted, however late the grant
-- arrives (the seal outlives the marker tombstone). A same-generation grant
-- may only renew the exact live claim and never shortens its lease. A higher
-- generation supersedes the old marker and, on a write-fenced stream, seals
-- the predecessor at its definite last fenced offset (#183). The key TTL is
-- the remaining lease plus the retention window.

local generation, wake_id, holder = ARGV[1], ARGV[2], ARGV[3]
local lease_until_ns, now_ns, retention_ms = ARGV[4], tonumber(ARGV[5]), tonumber(ARGV[6])

if tonumber(lease_until_ns) <= now_ns then return { 'FENCED' } end

local m = meta_map(KEYS[1])
if m == nil then return { 'NOTFOUND' } end

local auth = fence_auth(KEYS[2])
local seal = m['wfseal:' .. auth]
local seal_gen = seal and (seal_parts(seal)) or nil
if seal_gen and int_cmp(generation, seal_gen) <= 0 then return { 'FENCED' } end

local current = redis.call('HMGET', KEYS[2],
  'state', 'generation', 'wake_id', 'holder', 'lease_until_ns')
local superseded = nil
if current[2] ~= false then
  local generation_cmp = int_cmp(generation, current[2])
  if generation_cmp < 0 then return { 'FENCED' } end
  if generation_cmp == 0 then
    if current[1] ~= 'live'
      or current[3] ~= wake_id
      or current[4] ~= holder then
      return { 'FENCED' }
    end
    -- Renewal never shortens the marker lease: a delayed older re-grant is
    -- harmless.
    if tonumber(current[5]) > tonumber(lease_until_ns) then lease_until_ns = current[5] end
  elseif m.wf == '1' and (seal_gen == nil or int_cmp(current[2], seal_gen) > 0) then
    -- Supersession: fix the predecessor's definite last fenced offset.
    local off = m.wfLastOff or m.tail
    redis.call('HSET', KEYS[1],
      'wfseal:' .. auth, current[2] .. ':' .. current[3] .. ':' .. off,
      'wfSealGen', current[2],
      'wfSealOff', off)
    superseded = current[2]
  end
end

redis.call('HSET', KEYS[2],
  'state', 'live',
  'generation', generation,
  'wake_id', wake_id,
  'holder', holder,
  'lease_until_ns', lease_until_ns,
  'stream_incarnation', m.incarnation)
redis.call('PEXPIRE', KEYS[2], math.ceil((tonumber(lease_until_ns) - now_ns) / 1e6) + retention_ms)
if superseded then return { 'OK', 'SUPERSEDED', superseded } end
return { 'OK' }
