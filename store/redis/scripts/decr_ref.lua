-- decr_ref.lua — release a fork reference on a source stream (fork deleted
-- or fork creation rolled back). If the source is soft-deleted and the
-- refcount hits zero, the source is hard-deleted and the reply carries its
-- own forkedFrom so Go can cascade up the chain.
--
-- KEYS: 1=meta 2=msg 3=prod 4=forks   (all of the SOURCE stream)
-- ARGV: 1=nowNs 2=notifyChannel 3=forkPath
--
-- Reply: {'GONE'} (source vanished — nothing to do) |
--        {'UNDERFLOW'} (refcount went negative, clamped to 0) |
--        {'CASCADE', forkedFrom} (source hard-deleted, recurse) |
--        {'OK', newRefCount}

local now = tonumber(ARGV[1])

local m = meta_map(KEYS[1])
if m == nil then return { 'GONE' } end

local rc = redis.call('HINCRBY', KEYS[1], 'refCount', -1)
redis.call('SREM', KEYS[4], ARGV[3])

if rc < 0 then
  redis.call('HSET', KEYS[1], 'refCount', '0')
  return { 'UNDERFLOW' }
end

if rc == 0 and m.softDel == '1' then
  -- Last fork detached from a soft-deleted source: cascade hard delete.
  local parent = m.forkedFrom or ''
  redis.call('DEL', KEYS[1], KEYS[2], KEYS[3], KEYS[4])
  redis.call('PUBLISH', ARGV[2], 'd')
  return { 'CASCADE', parent }
end

if rc == 0 then
  -- No forks reference this stream anymore: the TTL backstop applies again.
  m.refCount = '0'
  refresh_backstop(m, now)
end

return { 'OK', tostring(rc) }
