-- create_sub.lua — create or idempotently re-confirm a subscription (PROTOCOL §6.2).
-- KEYS: 1=sub  2=subs_set  3=links
-- ARGV: 1=id 2=cfg_hash 3=now_ns 4=type 5=pattern 6=webhook_url 7=wake_stream
--       8=lease_ttl_ms 9=consistency_tier 10=description 11=num_links then
--       (path,link_type,offset)*, legacy_cfg_hash_without_consistency_tier
-- Reply: {status} ; CREATED | MATCHED | CONFLICT
local sub = KEYS[1]
local n = tonumber(ARGV[11])
local legacy_hash = ARGV[12 + n * 3] or ''
if redis.call('EXISTS', sub) == 1 then
  local stored_hash = redis.call('HGET', sub, 'cfg_hash')
  if stored_hash == ARGV[2] then
    if (redis.call('HGET', sub, 'consistency_tier') or '') == '' then
      redis.call('HSET', sub, 'consistency_tier', ARGV[9])
    end
    return { 'MATCHED' }
  end
  if ARGV[9] == 'A' and stored_hash == legacy_hash and (redis.call('HGET', sub, 'consistency_tier') or '') == '' then
    redis.call('HSET', sub, 'cfg_hash', ARGV[2], 'consistency_tier', ARGV[9])
    return { 'MATCHED' }
  end
  return { 'CONFLICT' }
end
redis.call('HSET', sub,
  'id', ARGV[1], 'cfg_hash', ARGV[2], 'created_ns', ARGV[3],
  'type', ARGV[4], 'pattern', ARGV[5], 'webhook_url', ARGV[6],
  'wake_stream', ARGV[7], 'lease_ttl_ms', ARGV[8], 'consistency_tier', ARGV[9],
  'description', ARGV[10],
  'status', 'active', 'phase', 'idle', 'generation', '0', 'wake_id', '',
  'holder', '0', 'holder_worker', '', 'lease_until_ns', '0',
  'retry_count', '0', 'first_fail_ns', '0', 'next_attempt_ns', '0')
redis.call('SADD', KEYS[2], ARGV[1])
local i = 12
for _ = 1, n do
  redis.call('HSET', KEYS[3], ARGV[i], ARGV[i + 1] .. ':' .. ARGV[i + 2])
  i = i + 3
end
return { 'CREATED' }
