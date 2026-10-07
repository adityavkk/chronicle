-- Atomic mirror of MemoryStore.DeleteSnapshot. Retire only the image named by
-- a strong ETag in the current source incarnation; never touch source activity.
local now, projection, incarnation, etag = tonumber(ARGV[1]), ARGV[2], ARGV[3], ARGV[4]
local dk, bk = 'd:' .. projection, 'b:' .. projection
local m = meta_map(KEYS[1])
if m == nil then return { 'NOTFOUND' } end
if is_expired(m, now) then expire_cleanup(m); return { 'NOTFOUND' } end
if m.softDel == '1' then return { 'SOFTDEL' } end
if (m.incarnation or tostring(m.createdAtNs)) ~= incarnation then return { 'SNAPSHOT' } end
local old = redis.call('HGET', KEYS[5], dk)
if not old then return { 'PRECONDITION' } end
if redis.call('HEXISTS', KEYS[5], bk) == 0 then return { 'CORRUPT' } end
local bodybytes = redis.call('HSTRLEN', KEYS[5], bk)
local ok, descriptor = pcall(cjson.decode, old)
if not ok or type(descriptor) ~= 'table' or descriptor.Incarnation ~= incarnation then return { 'CORRUPT' } end
if descriptor.ETag ~= etag then return { 'PRECONDITION' } end
local count, bytes = tonumber(redis.call('HGET', KEYS[5], '__count')), tonumber(redis.call('HGET', KEYS[5], '__bytes'))
if not count or not bytes or count < 1 or bytes < bodybytes or
    count % 1 ~= 0 or bytes % 1 ~= 0 or redis.call('HLEN', KEYS[5]) ~= 2 * count + 2 then
  return { 'CORRUPT' }
end
if count == 1 then
  redis.call('DEL', KEYS[5])
else
  redis.call('HDEL', KEYS[5], dk, bk)
  redis.call('HSET', KEYS[5], '__count', count - 1, '__bytes', bytes - bodybytes)
end
-- HDEL/HSET preserve the existing TTL. No source or companion TTL is renewed.
return { 'OK' }
