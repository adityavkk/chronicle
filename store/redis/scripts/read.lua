-- read.lua: bounded read page plus root snapshot validation.
--
-- KEYS: 1=meta 2=msg 3=prod 4=forks
-- ARGV:
--   1=nowNs
--   2=lexMin
--   3=lexMax
--   4=targetBytes
--   5=maxFrames
--   6=expectedIncarnation (empty captures the current incarnation)
--   7=rootRead ('1' checks expiry and root visibility)
--   8=fetchFrames ('1' reads the bounded ZSET range)
--   9=allowOversized ('1' lets the first returned frame exceed targetBytes)
--   10=requireCandidate ('1' means the selected upper bound is a known frame)
--   11=touchRoot ('1' refreshes the root stream's sliding TTL)
--   12=legacyIncarnation (random ID used only by HSETNX migration)
--
-- Reply:
--   {'NOTFOUND'} | {'SOFTDEL'} | {'SNAPSHOT'} | {'MISSING'}
--   {'OK', metaFlat, members, fetchedBytes, returnedBytes}

local now = tonumber(ARGV[1])
local target_bytes = tonumber(ARGV[4])
local max_frames = tonumber(ARGV[5])
local expected_incarnation = ARGV[6]
local root_read = ARGV[7] == '1'
local fetch_frames = ARGV[8] == '1'
local allow_oversized = ARGV[9] == '1'
local require_candidate = ARGV[10] == '1'
local touch_root = ARGV[11] == '1'

local m = meta_map(KEYS[1])
if m == nil then return { 'NOTFOUND' } end

if root_read then
  if is_expired(m, now) then
    expire_cleanup(m)
    return { 'NOTFOUND' }
  end
  if m.softDel == '1' then return { 'SOFTDEL' } end
end

local incarnation = m.incarnation
if incarnation == nil or incarnation == '' then
  if root_read then
    redis.call('HSETNX', KEYS[1], 'incarnation', ARGV[12])
    incarnation = redis.call('HGET', KEYS[1], 'incarnation')
    m.incarnation = incarnation
  else
    -- Inherited fork sources are read against metadata fetched before this
    -- script. Keep the compatibility identity stable until a direct root read
    -- performs the one-time persisted migration.
    incarnation = m.createdAtNs
  end
end
if expected_incarnation ~= '' and incarnation ~= expected_incarnation then
  return { 'SNAPSHOT' }
end

if root_read and touch_root and should_touch_read(m) then
  -- Refresh once when the logical client read captures its root snapshot.
  -- Continuations, sealing, and inherited ranges must not extend a TTL.
  m.accessedAtNs = string.format('%.0f', now)
  redis.call('HSET', KEYS[1], 'accessedAtNs', m.accessedAtNs)
  refresh_backstop(m, now)
end

local members = {}
local fetched_bytes = 0
local returned_bytes = 0
if fetch_frames and max_frames > 0 and target_bytes > 0 then
  -- Add a small payload budget to the fixed-width byte-offset field without
  -- converting the 16-digit field to a Lua number. Lua numbers cannot exactly
  -- represent every valid offset.
  local function add_offset_bytes(offset, delta)
    if #offset ~= 33 or string.sub(offset, 17, 17) ~= '_' then return nil end
    local digits = {}
    local carry = delta
    for i = 33, 18, -1 do
      local digit = string.byte(offset, i) - string.byte('0')
      if digit < 0 or digit > 9 then return nil end
      local sum = digit + carry
      digits[i - 17] = string.char(string.byte('0') + (sum % 10))
      carry = math.floor(sum / 10)
    end
    if carry > 0 then return nil end
    return string.sub(offset, 1, 17) .. table.concat(digits)
  end

  local function accept(member)
    -- A frame is the 33-byte offset, one separator byte, then its payload.
    -- Keep malformed members in the reply so Go reports the corruption.
    local payload_bytes = #member - 34
    if payload_bytes < 0 then payload_bytes = 0 end
    fetched_bytes = fetched_bytes + payload_bytes

    local fits = returned_bytes + payload_bytes <= target_bytes
    if fits or (#members == 0 and allow_oversized) then
      members[#members + 1] = member
      returned_bytes = returned_bytes + payload_bytes
      return returned_bytes < target_bytes
    end
    return false
  end

  -- Fetch the first candidate alone. This makes an oversized first frame cost
  -- one member, not max_frames members.
  local first = redis.call(
    'ZRANGEBYLEX', KEYS[2], ARGV[2], ARGV[3], 'LIMIT', 0, 1
  )
  if #first == 0 then
    if require_candidate or (
        m.tail ~= '0000000000000000_0000000000000000'
        and redis.call('ZCARD', KEYS[2]) == 0
      ) then
      return { 'MISSING' }
    end
  end
  local accepting = true
  if #first == 1 then accepting = accept(first[1]) end

  -- Once the first frame is known, its end offset is an exact byte anchor.
  -- Bulk-fetch only members whose cumulative end offset can fit the remaining
  -- returned-byte budget. If the first frame reaches the segment upper bound,
  -- the common one-frame refresh needs no second range call.
  if #first == 1 and accepting and #members < max_frames then
    local first_offset = string.sub(first[1], 1, 33)
    local segment_upper = string.sub(ARGV[3], 2, 34)
    if first_offset < segment_upper then
      local byte_max_offset = add_offset_bytes(
        first_offset,
        target_bytes - returned_bytes
      )
      if byte_max_offset ~= nil then
        local byte_max = '[' .. byte_max_offset .. string.char(255)
        if byte_max > ARGV[3] then byte_max = ARGV[3] end
        local candidates = redis.call(
          'ZRANGEBYLEX',
          KEYS[2],
          '(' .. first_offset .. string.char(255),
          byte_max,
          'LIMIT',
          0,
          max_frames - #members
        )
        for _, member in ipairs(candidates) do
          if not accept(member) then
            accepting = false
            break
          end
        end
      else
        accepting = false
      end
    end
  end
end

return {
  'OK',
  meta_flat(m),
  members,
  tostring(fetched_bytes),
  tostring(returned_bytes)
}
