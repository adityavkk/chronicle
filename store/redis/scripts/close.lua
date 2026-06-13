-- close.lua — close-only path (CloseStream / CloseStreamWithProducer),
-- including closedBy producer-tuple dedup. Mirrors MemoryStore: close does
-- NOT refresh the sliding TTL and does NOT check softDeleted.
--
-- KEYS: 1=meta 2=msg 3=prod 4=forks
-- ARGV: 1=nowNs 2=notifyChannel 3=hasProducer('1'/'0')
--       4=producerId 5=producerEpoch 6=producerSeq
--
-- Reply: make_reply; status one of OK|NOTFOUND|CLOSED|STALE_EPOCH|
-- EPOCH_SEQ|SEQ_GAP. CLOSED = already closed by a different producer tuple.

local now = tonumber(ARGV[1])
local channel = ARGV[2]
local has_producer = ARGV[3] == '1'
local producer_id = ARGV[4]
local p_epoch = tonumber(ARGV[5])
local p_seq = tonumber(ARGV[6])

local m = meta_map(KEYS[1])
if m == nil then return make_reply('NOTFOUND') end

if is_expired(m, now) then
  expire_cleanup(m)
  return make_reply('NOTFOUND')
end

if m.closed == '1' then
  if has_producer then
    if m.cbEpoch ~= nil and m.cbId == producer_id
      and m.cbEpoch == ARGV[5] and m.cbSeq == ARGV[6] then
      -- Duplicate of the closing request: idempotent success.
      return make_reply('OK', m.tail, '2', nil, nil, nil, ARGV[6], '1', '1')
    end
    return make_reply('CLOSED', m.tail, nil, nil, nil, nil, nil, '1', '1')
  end
  -- Plain close is idempotent; upstream still notifies waiters.
  redis.call('PUBLISH', channel, 'c')
  return make_reply('OK', m.tail, '0', nil, nil, nil, nil, '1', '1')
end

if has_producer then
  local state = redis.call('HGET', KEYS[3], producer_id)
  local outcome, d1, d2 = validate_producer(state, p_epoch, p_seq)
  if outcome == 'DUP' then
    -- Duplicate while the stream is open: succeed WITHOUT closing.
    return make_reply('OK', m.tail, '2', nil, nil, nil, d1, '0', '0')
  elseif outcome == 'STALE_EPOCH' then
    return make_reply('STALE_EPOCH', m.tail, nil, d1)
  elseif outcome == 'EPOCH_SEQ' then
    return make_reply('EPOCH_SEQ', m.tail)
  elseif outcome == 'SEQ_GAP' then
    return make_reply('SEQ_GAP', m.tail, nil, nil, d1, d2)
  end
  -- Accepted: commit producer state, close, record the closing tuple.
  redis.call('HSET', KEYS[3], producer_id,
    ARGV[5] .. ':' .. ARGV[6] .. ':' .. string.format('%.0f', math.floor(now / 1e9)))
  redis.call('HSET', KEYS[1], 'closed', '1',
    'cbId', producer_id, 'cbEpoch', ARGV[5], 'cbSeq', ARGV[6])
  refresh_backstop(m, now)
  redis.call('PUBLISH', channel, 'c')
  return make_reply('OK', m.tail, '1', nil, nil, nil, ARGV[6], '1', '0')
end

redis.call('HSET', KEYS[1], 'closed', '1')
redis.call('PUBLISH', channel, 'c')
return make_reply('OK', m.tail, '0', nil, nil, nil, nil, '1', '0')
