-- Source validation and image lookup share one atomic source-slot operation.
-- Go verifies the stored ETag and bounds the saved offset by this captured tail.
local now = tonumber(ARGV[1])
local m = meta_map(KEYS[1])
if m == nil then return { 'NOTFOUND' } end
if is_expired(m, now) then expire_cleanup(m); return { 'NOTFOUND' } end
if m.softDel == '1' then return { 'SOFTDEL' } end
if redis.call('EXISTS', KEYS[5]) == 1 and redis.call('HEXISTS', KEYS[5], '__count') == 0 then
  return { 'CORRUPT' }
end
local envelope = redis.call('HGET', KEYS[5], 'd:' .. ARGV[2])
if not envelope then
  if redis.call('HEXISTS', KEYS[5], 'b:' .. ARGV[2]) == 1 then return { 'CORRUPT' } end
  return { 'MISSING' }
end
local body = redis.call('HGET', KEYS[5], 'b:' .. ARGV[2])
if not body then return { 'CORRUPT' } end
return { 'OK', m.incarnation or tostring(m.createdAtNs), m.tail, envelope, body }
