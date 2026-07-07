-- create_sub.lua — create or idempotently re-confirm a subscription (PROTOCOL §6.2).
-- KEYS: 1=sub  2=subs_set  3=links  4=incarnation_counter
-- ARGV: 1=id 2=cfg_hash 3=now_ns 4=type 5=pattern 6=webhook_url 7=wake_stream
--       8=lease_ttl_ms 9=description 10=owner 11=num_links then (path,link_type,offset)*
-- Reply: {status, owner} ; CREATED | MATCHED | CONFLICT. owner is the stored
-- owner subject ('' when ownerless) so the caller can apply the ownership
-- gate (issue #126 TB3) atomically off this one reply: MATCHED/CONFLICT did
-- not mutate, and the owner is immutable once set at create.
local sub = KEYS[1]
if redis.call('EXISTS', sub) == 1 then
  local owner = redis.call('HGET', sub, 'owner')
  if redis.call('HGET', sub, 'cfg_hash') == ARGV[2] then
    return { 'MATCHED', owner or '' }
  end
  return { 'CONFLICT', owner or '' }
end
local incarnation = tostring(redis.call('INCR', KEYS[4]))
redis.call('HSET', sub,
  'id', ARGV[1], 'cfg_hash', ARGV[2], 'created_ns', ARGV[3],
  'type', ARGV[4], 'pattern', ARGV[5], 'webhook_url', ARGV[6],
  'wake_stream', ARGV[7], 'lease_ttl_ms', ARGV[8], 'description', ARGV[9],
  'owner', ARGV[10],
  'incarnation', incarnation,
  'status', 'active', 'phase', 'idle', 'generation', '0', 'wake_id', '',
  'holder', '0', 'holder_worker', '', 'lease_until_ns', '0',
  'retry_count', '0', 'first_fail_ns', '0', 'next_attempt_ns', '0')
redis.call('SADD', KEYS[2], ARGV[1])
local n = tonumber(ARGV[11])
local i = 12
for _ = 1, n do
  redis.call('HSET', KEYS[3], ARGV[i], ARGV[i + 1] .. ':' .. ARGV[i + 2])
  i = i + 3
end
return { 'CREATED', ARGV[10] }
