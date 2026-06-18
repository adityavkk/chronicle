-- delete.lua — refcount-aware delete of one stream's own keys. The fork
-- cascade (decrementing the source's refcount across slots) is orchestrated
-- by Go via decr_ref.lua; the reply carries forkedFrom for that walk.
--
-- KEYS: 1=meta 2=msg 3=prod 4=forks
-- ARGV: 1=notifyChannel
--
-- Reply: {'NOTFOUND'} | {'SOFTDEL'} (already soft-deleted) |
--        {'SOFTDELETED'} (refCount>0, flipped instead of deleting) |
--        {'DELETED', forkedFrom}

local m = meta_map(KEYS[1])
if m == nil then return { 'NOTFOUND' } end

-- Upstream Delete has no expiry check: deleting an expired-but-present
-- stream just deletes it.

if m.softDel == '1' then return { 'SOFTDEL' } end

if tonumber(m.refCount or '0') > 0 then
  -- Forks still reference this stream: soft-delete, retain data, drop TTLs.
  redis.call('HSET', KEYS[1], 'softDel', '1')
  redis.call('PERSIST', KEYS[1])
  redis.call('PERSIST', KEYS[2])
  redis.call('PERSIST', KEYS[3])
  redis.call('PERSIST', KEYS[4])
  return { 'SOFTDELETED' }
end

redis.call('DEL', KEYS[1], KEYS[2], KEYS[3], KEYS[4])
redis.call('PUBLISH', ARGV[1], 'd')
return { 'DELETED', m.forkedFrom or '' }
