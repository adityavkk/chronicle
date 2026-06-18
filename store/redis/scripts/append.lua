-- append.lua — atomic append: full validation chain in spec precedence
-- order, then write + meta update + notify. Frames arrive pre-encoded
-- ("<offset>|<data>") computed by Go against ARGV[10] (expected tail); if
-- the tail moved concurrently the script returns RETRY and Go re-frames.
--
-- valOnly ('1') runs the validation chain only and replies VALONLY instead
-- of writing: Go uses it when JSON-mode parsing fails, so closed/producer/
-- seq errors keep spec precedence over ErrInvalidJSON (upstream parses JSON
-- after validation).
--
-- KEYS: 1=meta 2=msg 3=prod 4=forks
-- ARGV: 1=nowNs 2=notifyChannel 3=reqCT(normalized media type, ''=skip)
--       4=streamSeq(''=none) 5=close('1'/'0') 6=hasProducer('1'/'0')
--       7=producerId 8=producerEpoch 9=producerSeq
--       10=expectedTail 11=newTail 12=valOnly('1'/'0') 13..=frames
--
-- Reply: make_reply (see common.lua); status one of OK|VALONLY|RETRY|
-- NOTFOUND|SOFTDEL|CLOSED|CTMISMATCH|SEQCONFLICT|STALE_EPOCH|EPOCH_SEQ|
-- SEQ_GAP.

local now = tonumber(ARGV[1])
local channel = ARGV[2]
local req_ct = ARGV[3]
local stream_seq = ARGV[4]
local closing = ARGV[5] == '1'
local has_producer = ARGV[6] == '1'
local producer_id = ARGV[7]
local p_epoch = tonumber(ARGV[8])
local p_seq = tonumber(ARGV[9])
local expected_tail = ARGV[10]
local new_tail = ARGV[11]
local val_only = ARGV[12] == '1'

-- 1. Existence.
local m = meta_map(KEYS[1])
if m == nil then return make_reply('NOTFOUND') end

-- 2. Soft-deleted (before expiry, mirroring MemoryStore order).
if m.softDel == '1' then return make_reply('SOFTDEL') end

-- 3. Lazy expiry.
if is_expired(m, now) then
  expire_cleanup(m)
  return make_reply('NOTFOUND')
end

-- 4. Sliding-TTL touch (upstream touches before the closed check, so even
-- rejected appends refresh the window).
m.accessedAtNs = string.format('%.0f', now)
redis.call('HSET', KEYS[1], 'accessedAtNs', m.accessedAtNs)
refresh_backstop(m, now)

-- 5. Closed: duplicate of the closing producer tuple is idempotent success;
-- anything else is CLOSED carrying the final tail offset.
if m.closed == '1' then
  if has_producer and m.cbEpoch ~= nil
    and m.cbId == producer_id and m.cbEpoch == ARGV[8] and m.cbSeq == ARGV[9] then
    return make_reply('OK', m.tail, '2', nil, nil, nil, ARGV[9], '1')
  end
  return make_reply('CLOSED', m.tail, nil, nil, nil, nil, nil, '1')
end

-- 6. Content-type match (skipped when the request carries none).
if req_ct ~= '' and norm_ct(m.ct) ~= req_ct then
  return make_reply('CTMISMATCH', m.tail)
end

-- 7. Producer validation — BEFORE Stream-Seq so retries dedupe even when
-- Stream-Seq would conflict. Duplicates return with NO write.
if has_producer then
  local state = redis.call('HGET', KEYS[3], producer_id)
  local outcome, d1, d2 = validate_producer(state, p_epoch, p_seq)
  if outcome == 'DUP' then
    return make_reply('OK', m.tail, '2', nil, nil, nil, d1, '0')
  elseif outcome == 'STALE_EPOCH' then
    return make_reply('STALE_EPOCH', m.tail, nil, d1)
  elseif outcome == 'EPOCH_SEQ' then
    return make_reply('EPOCH_SEQ', m.tail)
  elseif outcome == 'SEQ_GAP' then
    return make_reply('SEQ_GAP', m.tail, nil, nil, d1, d2)
  end
end

-- 8. Stream-Seq bytewise-lex regression check (C locale => memcmp order).
if stream_seq ~= '' and m.lastSeq ~= nil and m.lastSeq ~= '' and stream_seq <= m.lastSeq then
  return make_reply('SEQCONFLICT', m.tail)
end

-- 9. Validation-only mode stops here (all checks passed, nothing written).
if val_only then return make_reply('VALONLY', m.tail) end

-- 10. Optimistic frame check: Go framed against expected_tail.
if m.tail ~= expected_tail then return make_reply('RETRY') end

-- 11. Write frames (chunked: unpack is C-stack bounded) and commit metadata.
if #ARGV >= 13 then
  local i = 13
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
  redis.call('HSET', KEYS[1], 'tail', new_tail)
end

local hset = {}
if stream_seq ~= '' then
  hset[#hset + 1] = 'lastSeq'
  hset[#hset + 1] = stream_seq
end
if closing then
  hset[#hset + 1] = 'closed'
  hset[#hset + 1] = '1'
  if has_producer then
    hset[#hset + 1] = 'cbId'
    hset[#hset + 1] = producer_id
    hset[#hset + 1] = 'cbEpoch'
    hset[#hset + 1] = ARGV[8]
    hset[#hset + 1] = 'cbSeq'
    hset[#hset + 1] = ARGV[9]
  end
end
if #hset > 0 then redis.call('HSET', KEYS[1], unpack(hset)) end

local result_last_seq = '0'
if has_producer then
  redis.call('HSET', KEYS[3], producer_id,
    ARGV[8] .. ':' .. ARGV[9] .. ':' .. string.format('%.0f', math.floor(now / 1e9)))
  -- prod key may have just been created: align its backstop TTL.
  refresh_backstop(m, now)
  result_last_seq = ARGV[9]
end

redis.call('PUBLISH', channel, closing and 'c' or 'a')

return make_reply('OK', new_tail, has_producer and '1' or '0',
  nil, nil, nil, result_last_seq, closing and '1' or '0')
