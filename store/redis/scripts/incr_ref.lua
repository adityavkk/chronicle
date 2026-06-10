-- incr_ref.lua — take a fork reference on a source stream: re-validate
-- atomically (Go validated against a snapshot first), increment refCount,
-- register the fork, and drop key TTLs (referenced sources must not be
-- reaped by the backstop).
--
-- KEYS: 1=meta 2=msg 3=prod 4=forks   (all of the SOURCE stream)
-- ARGV: 1=nowNs 2=forkPath
--
-- Reply: {'NOTFOUND'} | {'SOFTDEL'} | {'OK', newRefCount}

local now = tonumber(ARGV[1])

local m = meta_map(KEYS[1])
if m == nil then return { 'NOTFOUND' } end

if m.softDel == '1' then return { 'SOFTDEL' } end

if is_expired(m, now) then
  expire_cleanup(m)
  return { 'NOTFOUND' }
end

local rc = redis.call('HINCRBY', KEYS[1], 'refCount', 1)
redis.call('SADD', KEYS[4], ARGV[2])
redis.call('PERSIST', KEYS[1])
redis.call('PERSIST', KEYS[2])
redis.call('PERSIST', KEYS[3])
redis.call('PERSIST', KEYS[4])
return { 'OK', tostring(rc) }
