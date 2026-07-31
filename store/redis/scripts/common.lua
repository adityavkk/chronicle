-- common.lua — shared prelude prepended to every chronicle script by
-- scripts.go. Convention: KEYS[1]=meta HASH, KEYS[2]=msg ZSET,
-- KEYS[3]=prod HASH, KEYS[4]=forks SET (extra script-specific keys follow).
--
-- Lua numbers are doubles, so producer epoch/seq values are kept as decimal
-- strings and compared with the helpers below. UnixNano timestamps still use
-- numbers and carry ~256ns rounding (irrelevant at ms-granularity expiry).

-- meta_map loads the meta HASH into a table, or nil if the stream is absent.
local function meta_map(key)
  local flat = redis.call('HGETALL', key)
  if #flat == 0 then return nil end
  local m = {}
  for i = 1, #flat, 2 do m[flat[i]] = flat[i + 1] end
  if m.tail == nil then return nil end
  return m
end

-- meta_flat returns the already-loaded metadata for a script reply. Reads must
-- not issue a second HGETALL merely to cross the Redis/Go decoding boundary.
local function meta_flat(m)
  local flat = {}
  for field, value in pairs(m) do
    flat[#flat + 1] = field
    flat[#flat + 1] = value
  end
  return flat
end

-- is_expired mirrors StreamMetadata.IsExpired (lazy expiry source of truth).
local function is_expired(m, now_ns)
  if m.expAtNs and now_ns > tonumber(m.expAtNs) then return true end
  if m.ttl and now_ns > tonumber(m.accessedAtNs) + tonumber(m.ttl) * 1e9 then
    return true
  end
  return false
end

-- should_touch_read mirrors store.ShouldRenewReadAccess. Absolute expiry is a
-- fixed deadline; only a configured sliding TTL renews on access.
local function should_touch_read(m)
  return m.ttl ~= nil
end

-- expire_cleanup handles a stream discovered expired: fork sources
-- (refCount > 0) flip to soft-deleted so fork readers keep working;
-- otherwise all keys are deleted. Callers then report NOTFOUND.
local function expire_cleanup(m)
  if tonumber(m.refCount or '0') > 0 then
    redis.call('HSET', KEYS[1], 'softDel', '1')
    redis.call('PERSIST', KEYS[1])
    redis.call('PERSIST', KEYS[2])
    redis.call('PERSIST', KEYS[3])
    redis.call('PERSIST', KEYS[4])
  else
    redis.call('DEL', KEYS[1], KEYS[2], KEYS[3], KEYS[4])
  end
end

-- refresh_backstop sets the GC key TTL (lazy expiry stays the source of
-- truth; the key TTL only reaps idle expired streams). Streams referenced
-- by forks or soft-deleted never carry key TTLs.
local max_backstop_ttl_seconds = 9000000000000000

local function refresh_backstop(m, now_ns)
  local ks = { KEYS[1], KEYS[2], KEYS[3], KEYS[4] }
  if tonumber(m.refCount or '0') > 0 or m.softDel == '1' then
    for _, k in ipairs(ks) do redis.call('PERSIST', k) end
    return
  end
  local pttl = nil
  if m.ttl then
    -- Redis PEXPIRE accepts a signed int64 millisecond value. Stream-TTL is
    -- a signed int64 count of seconds, so large valid protocol TTLs cannot be
    -- represented as a Redis key TTL after the seconds-to-milliseconds
    -- conversion. Keep those keys persistent and rely on lazy expiry, which
    -- is the source of truth. The conservative bound is exactly representable
    -- by Lua and leaves ample room below MaxInt64 after adding the backstop.
    local ttl = tonumber(m.ttl)
    if ttl <= max_backstop_ttl_seconds then pttl = ttl * 1000 + 60000 end
  end
  if m.expAtNs then
    local rem = math.floor((tonumber(m.expAtNs) - now_ns) / 1e6) + 60000
    if rem < 1 then rem = 1 end
    if pttl == nil or rem < pttl then pttl = rem end
  end
  for _, k in ipairs(ks) do
    if pttl then redis.call('PEXPIRE', k, pttl) else redis.call('PERSIST', k) end
  end
end

-- norm_ct mirrors store.ContentTypeMatches normalization: empty defaults to
-- application/octet-stream, parameters stripped at the first ';', ASCII
-- lowercase (Redis runs in the C locale so string.lower is ASCII-only).
local function norm_ct(ct)
  if ct == nil or ct == '' then return 'application/octet-stream' end
  return string.lower(string.match(ct, '^[^;]*'))
end

-- make_reply builds the fixed-shape 9-element reply used by the mutation
-- scripts: {status, tail, producerResult, currentEpoch, expectedSeq,
-- receivedSeq, lastSeq, closed, alreadyClosed} — all strings so int64
-- fidelity survives the Lua double round-trip.
local function make_reply(status, tail, presult, cur_epoch, exp_seq, rcv_seq, last_seq, closed, already)
  return { status, tail or '', presult or '0', cur_epoch or '0',
    exp_seq or '0', rcv_seq or '0', last_seq or '0', closed or '0', already or '0' }
end

local function int_parts(s)
  local neg = string.sub(s, 1, 1) == '-'
  if neg then s = string.sub(s, 2) end
  s = string.gsub(s, '^0+', '')
  if s == '' then return false, '0' end
  return neg, s
end

local function int_cmp(a, b)
  local an, ad = int_parts(a)
  local bn, bd = int_parts(b)
  if an and not bn then return -1 end
  if not an and bn then return 1 end

  local c = 0
  if #ad < #bd then c = -1 elseif #ad > #bd then c = 1
  elseif ad < bd then c = -1 elseif ad > bd then c = 1 end
  if an and bn then return -c end
  return c
end

-- offset_cmp compares canonical "readSeq_byteOffset" values without passing
-- either uint64 component through a Lua number. It mirrors store.Compare.
local function offset_cmp(a, b)
  local ar, ab = string.match(a or '', '^(%d+)_(%d+)$')
  local br, bb = string.match(b or '', '^(%d+)_(%d+)$')
  if ar == nil or br == nil then error('invalid stream offset') end
  local read_seq_cmp = int_cmp(ar, br)
  if read_seq_cmp ~= 0 then return read_seq_cmp end
  return int_cmp(ab, bb)
end

-- classify_root_read_range mirrors store.ClassifyRootReadRange. page_tail is
-- the atomically loaded tail for a first page or the caller's fixed snapshot
-- tail for a continuation.
local function classify_root_read_range(m, requested, page_tail, is_now)
  if is_now or offset_cmp(requested, page_tail) >= 0 then return 'empty' end
  if m.forkedFrom and m.forkedFrom ~= '' and
      offset_cmp(requested, m.forkOff or '0000000000000000_0000000000000000') < 0 then
    return 'inherited'
  end
  return 'root'
end

local function int_is_zero(s)
  local _, d = int_parts(s)
  return d == '0'
end

local function abs_add_one(d)
  local carry = 1
  local out = {}
  for i = #d, 1, -1 do
    local n = string.byte(d, i) - 48 + carry
    if n == 10 then
      out[#out + 1] = '0'
      carry = 1
    else
      out[#out + 1] = string.char(48 + n)
      carry = 0
    end
  end
  if carry == 1 then out[#out + 1] = '1' end
  local s = ''
  for i = #out, 1, -1 do s = s .. out[i] end
  return s
end

local function abs_sub_one(d)
  local borrow = 1
  local out = {}
  for i = #d, 1, -1 do
    local n = string.byte(d, i) - 48 - borrow
    if n < 0 then
      out[#out + 1] = '9'
      borrow = 1
    else
      out[#out + 1] = string.char(48 + n)
      borrow = 0
    end
  end
  local s = ''
  for i = #out, 1, -1 do s = s .. out[i] end
  s = string.gsub(s, '^0+', '')
  if s == '' then return '0' end
  return s
end

local function int_add_one(s)
  local neg, d = int_parts(s)
  if not neg then return abs_add_one(d) end
  if d == '1' then return '0' end
  return '-' .. abs_sub_one(d)
end

-- validate_producer mirrors store.ValidateProducer exactly. state_str is the
-- prod HASH value ("epoch:lastSeq:lastUpdated") or false/nil on first
-- contact. Returns (outcome, detail1, detail2):
--   'ACCEPT'     — accepted; caller persists "epoch:seq:now"
--   'DUP'        — duplicate; detail1 = state lastSeq string (no write)
--   'STALE_EPOCH'— detail1 = current epoch string
--   'EPOCH_SEQ'  — new epoch not starting at 0
--   'SEQ_GAP'    — detail1 = expected seq string, detail2 = received seq string
local function validate_producer(state_str, epoch, seq)
  if not state_str then
    if not int_is_zero(seq) then return 'SEQ_GAP', '0', seq end
    return 'ACCEPT'
  end
  local s_epoch_s, s_seq_s = string.match(state_str, '^(-?%d+):(-?%d+):')
  local epoch_cmp = int_cmp(epoch, s_epoch_s)
  if epoch_cmp < 0 then return 'STALE_EPOCH', s_epoch_s end
  if epoch_cmp > 0 then
    if not int_is_zero(seq) then return 'EPOCH_SEQ' end
    return 'ACCEPT'
  end
  if int_cmp(seq, s_seq_s) <= 0 then return 'DUP', s_seq_s end
  local expected = int_add_one(s_seq_s)
  if seq == expected then return 'ACCEPT' end
  return 'SEQ_GAP', expected, seq
end
