-- read.lua — Read fast path: lazy-expiry check, sliding-TTL touch, then
-- meta + own frames in one atomic step. Fork chain stitching (inherited
-- ranges) happens in Go with plain ZRANGEBYLEX on the source slots.
--
-- KEYS: 1=meta 2=msg 3=prod 4=forks
-- ARGV: 1=nowNs 2=lexMin (exclusive bound "(<offset>\xff")
--
-- Reply: {'NOTFOUND'} | {'SOFTDEL'} | {'OK', metaFlat, members}

local now = tonumber(ARGV[1])

local m = meta_map(KEYS[1])
if m == nil then return { 'NOTFOUND' } end

if is_expired(m, now) then
  expire_cleanup(m)
  return { 'NOTFOUND' }
end

if m.softDel == '1' then return { 'SOFTDEL' } end

-- Sliding-TTL touch: Read counts as access (Get does not).
m.accessedAtNs = string.format('%.0f', now)
redis.call('HSET', KEYS[1], 'accessedAtNs', m.accessedAtNs)
refresh_backstop(m, now)

local members = redis.call('ZRANGEBYLEX', KEYS[2], ARGV[2], '+')
return { 'OK', redis.call('HGETALL', KEYS[1]), members }
