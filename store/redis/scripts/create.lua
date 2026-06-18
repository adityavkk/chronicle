-- create.lua — atomic stream creation (idempotent PUT). Go resolves fork
-- parameters beforehand (source lives in a different cluster slot); this
-- script only touches the new stream's own keys.
--
-- KEYS: 1=meta 2=msg 3=prod 4=forks
-- ARGV: 1=nowNs 2=notifyChannel
--       config-match probe (vs an existing stream, mirroring ConfigMatches):
--       3=normCT 4=ttl(''=nil) 5=expAtNs(''=nil) 6=closed('1'/'0')
--       7=forkedFrom 8=forkOffRequested(''=omitted) 9=forkSubOff
--       10=N (meta field pair count) 11..10+2N=meta field/value pairs
--       11+2N..=initial frames (pre-encoded "<offset>|<data>")
--
-- Reply: {'EXISTS'} | {'MISMATCH'} | {'MATCHED', metaFlat, prodFlat} |
--        {'CREATED'}

local now = tonumber(ARGV[1])
local channel = ARGV[2]

-- config_matches mirrors StreamMetadata.ConfigMatches against the probe
-- ARGVs. All values compare as canonical strings written by Go.
local function config_matches(m)
  if norm_ct(m.ct) ~= ARGV[3] then return false end
  if (m.ttl or '') ~= ARGV[4] then return false end
  if (m.expAtNs or '') ~= ARGV[5] then return false end
  local closed = (m.closed == '1') and '1' or '0'
  if closed ~= ARGV[6] then return false end
  if (m.forkedFrom or '') ~= ARGV[7] then return false end
  if ARGV[7] ~= '' then
    if ARGV[8] ~= '' then
      -- Compare the user-supplied fork offset against forkOffReq, falling
      -- back to the resolved forkOff (pre-ForkOffsetRequested metadata).
      local stored = m.forkOffReq or m.forkOff
      if stored ~= ARGV[8] then return false end
    end
    if (m.forkSubOff or '0') ~= ARGV[9] then return false end
  end
  return true
end

local m = meta_map(KEYS[1])
if m ~= nil then
  if is_expired(m, now) then
    -- Expired: reap in place and recreate — unless forks still reference it
    -- (expire_cleanup flips it to soft-deleted), which blocks re-creation.
    expire_cleanup(m)
    if tonumber(m.refCount or '0') > 0 then return { 'EXISTS' } end
  elseif m.softDel == '1' then
    return { 'EXISTS' }
  elseif config_matches(m) then
    return { 'MATCHED', redis.call('HGETALL', KEYS[1]), redis.call('HGETALL', KEYS[3]) }
  else
    return { 'MISMATCH' }
  end
end

-- Fresh create. DEL is defensive (expired leftovers are already gone).
redis.call('DEL', KEYS[1], KEYS[2], KEYS[3], KEYS[4])

local n = tonumber(ARGV[10])
local hset = { 'HSET', KEYS[1] }
for i = 11, 10 + 2 * n do hset[#hset + 1] = ARGV[i] end
redis.call(unpack(hset))

local first_frame = 11 + 2 * n
if #ARGV >= first_frame then
  local i = first_frame
  while i <= #ARGV do
    local stop = math.min(i + 999, #ARGV)
    local zargs = { 'ZADD', KEYS[2] }
    for j = i, stop do
      zargs[#zargs + 1] = '0'
      zargs[#zargs + 1] = ARGV[j]
    end
    redis.call(unpack(zargs))
    i = stop + 1
  end
  redis.call('PUBLISH', channel, 'a')
end

local nm = meta_map(KEYS[1])
refresh_backstop(nm, now)

return { 'CREATED' }
