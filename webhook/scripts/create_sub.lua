-- create_sub.lua — create or idempotently re-confirm a subscription (PROTOCOL §6.2).
-- KEYS: 1=sub  2=subs_set  3=links
-- ARGV: 1=id 2=cfg_hash 3=now_ns 4=type 5=pattern 6=webhook_url 7=wake_stream
--       8=lease_ttl_ms 9=description 10=num_links then (path,link_type,offset)*
-- Reply: {status} ; CREATED | MATCHED | CONFLICT
local sub = KEYS[1]
if redis.call('EXISTS', sub) == 1 then
  if redis.call('HGET', sub, 'cfg_hash') == ARGV[2] then
    return { 'MATCHED' }
  end
  return { 'CONFLICT' }
end
redis.call('HSET', sub,
  'id', ARGV[1], 'cfg_hash', ARGV[2], 'created_ns', ARGV[3],
  'type', ARGV[4], 'pattern', ARGV[5], 'webhook_url', ARGV[6],
  'wake_stream', ARGV[7], 'lease_ttl_ms', ARGV[8], 'description', ARGV[9],
  'status', 'active', 'phase', 'idle', 'generation', '0', 'wake_id', '',
  'holder', '0', 'holder_worker', '', 'lease_until_ns', '0',
  'retry_count', '0', 'first_fail_ns', '0', 'next_attempt_ns', '0')
redis.call('SADD', KEYS[2], ARGV[1])
local n = tonumber(ARGV[10])
local i = 11
for _ = 1, n do
  redis.call('HSET', KEYS[3], ARGV[i], ARGV[i + 1] .. ':' .. ARGV[i + 2])
  i = i + 3
end
return { 'CREATED' }
