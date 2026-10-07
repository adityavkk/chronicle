-- Atomic mirror of MemoryStore.PutSnapshot. KEYS[5] is the source-slot image
-- HASH. d:<projection> stores only the descriptor; b:<projection> is raw bytes.
-- ARGV: now, projection, incarnation, offset, ETag, descriptor, body,
-- condition kind, prior ETag, max versions, max total body bytes.
local now, projection, incarnation, off = tonumber(ARGV[1]), ARGV[2], ARGV[3], ARGV[4]
local dk, bk = 'd:' .. projection, 'b:' .. projection
local m = meta_map(KEYS[1])
if m == nil then return { 'NOTFOUND' } end
if is_expired(m, now) then expire_cleanup(m); return { 'NOTFOUND' } end
if m.softDel == '1' then return { 'SOFTDEL' } end
if (m.incarnation or tostring(m.createdAtNs)) ~= incarnation then return { 'SNAPSHOT' } end
local boundary = off == '0000000000000000_0000000000000000' and (m.forkedFrom or '') == ''
if not boundary then
  local rows = redis.call('ZRANGEBYLEX', KEYS[2], '[' .. off .. '|', '[' .. off .. '\255', 'LIMIT', 0, 1)
  boundary = #rows == 1
end
if not boundary or offset_cmp(off, m.tail) > 0 then return { 'CONFLICT' } end
local old = redis.call('HGET', KEYS[5], dk)
local has_body = redis.call('HEXISTS', KEYS[5], bk) == 1
local oldbytes = redis.call('HSTRLEN', KEYS[5], bk)
local previous
if old then
  local ok
  ok, previous = pcall(cjson.decode, old)
  if not ok or type(previous) ~= 'table' or type(previous.Offset) ~= 'string' or
      type(previous.ETag) ~= 'string' or not has_body or previous.Incarnation ~= incarnation then
    return { 'CORRUPT' }
  end
elseif has_body then return { 'CORRUPT' } end
local countRaw, bytesRaw = redis.call('HGET', KEYS[5], '__count'), redis.call('HGET', KEYS[5], '__bytes')
local fields = redis.call('HLEN', KEYS[5])
if fields > 0 and (not countRaw or not bytesRaw) then return { 'CORRUPT' } end
local count, bytes = tonumber(countRaw or '0'), tonumber(bytesRaw or '0')
if not count or not bytes or count < 0 or bytes < oldbytes or
    count % 1 ~= 0 or bytes % 1 ~= 0 or (fields > 0 and fields ~= 2 * count + 2) then
  return { 'CORRUPT' }
end
if ARGV[8] == 'none' then
  if old then return { 'PRECONDITION' } end
elseif not old or previous.ETag ~= ARGV[9] then return { 'PRECONDITION' } end
if old then
  if offset_cmp(off, previous.Offset) < 0 then return { 'CONFLICT' } end
  if off == previous.Offset then
    if old ~= ARGV[6] or redis.call('HGET', KEYS[5], bk) ~= ARGV[7] then return { 'CONFLICT' } end
    return { 'OK', '0' }
  end
end
local newcount = count + (old and 0 or 1)
local newbytes = bytes - oldbytes + string.len(ARGV[7])
if newcount > tonumber(ARGV[10]) or newbytes > tonumber(ARGV[11]) then return { 'QUOTA' } end
redis.call('HSET', KEYS[5], dk, ARGV[6], bk, ARGV[7], '__count', newcount, '__bytes', newbytes)
-- Publication is not source activity: copy, never renew, the source backstop.
local ttl = redis.call('PTTL', KEYS[1])
if ttl >= 0 then redis.call('PEXPIRE', KEYS[5], ttl)
else redis.call('PERSIST', KEYS[5]) end
return { 'OK', old and '0' or '1' }
