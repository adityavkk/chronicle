-- common.lua — shared prelude prepended to every chronicle script by
-- scripts.go. Convention: KEYS[1]=meta HASH, KEYS[2]=msg ZSET,
-- KEYS[3]=prod HASH, KEYS[4]=forks SET (extra script-specific keys follow).
--
-- Lua numbers are doubles: producer epoch/seq comparisons are exact only
-- below 2^53 (documented limit, far beyond practical values), and UnixNano
-- timestamps carry ~256ns rounding (irrelevant at ms-granularity expiry).

-- meta_map loads the meta HASH into a table, or nil if the stream is absent.
local function meta_map(key)
  local flat = redis.call('HGETALL', key)
  if #flat == 0 then return nil end
  local m = {}
  for i = 1, #flat, 2 do m[flat[i]] = flat[i + 1] end
  if m.tail == nil then return nil end
  return m
end

-- is_expired mirrors StreamMetadata.IsExpired (lazy expiry source of truth).
local function is_expired(m, now_ns)
  if m.expAtNs and now_ns > tonumber(m.expAtNs) then return true end
  if m.ttl and now_ns > tonumber(m.accessedAtNs) + tonumber(m.ttl) * 1e9 then
    return true
  end
  return false
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
local function refresh_backstop(m, now_ns)
  local ks = { KEYS[1], KEYS[2], KEYS[3], KEYS[4] }
  if tonumber(m.refCount or '0') > 0 or m.softDel == '1' then
    for _, k in ipairs(ks) do redis.call('PERSIST', k) end
    return
  end
  local pttl = nil
  if m.ttl then pttl = tonumber(m.ttl) * 1000 + 60000 end
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
    if seq ~= 0 then return 'SEQ_GAP', '0', tostring(seq) end
    return 'ACCEPT'
  end
  local s_epoch_s, s_seq_s = string.match(state_str, '^(-?%d+):(-?%d+):')
  local s_epoch, s_seq = tonumber(s_epoch_s), tonumber(s_seq_s)
  if epoch < s_epoch then return 'STALE_EPOCH', s_epoch_s end
  if epoch > s_epoch then
    if seq ~= 0 then return 'EPOCH_SEQ' end
    return 'ACCEPT'
  end
  if seq <= s_seq then return 'DUP', s_seq_s end
  if seq == s_seq + 1 then return 'ACCEPT' end
  return 'SEQ_GAP', tostring(s_seq + 1), tostring(seq)
end
