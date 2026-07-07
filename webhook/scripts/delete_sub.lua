-- delete_sub.lua — tombstone a subscription (PROTOCOL §6.3): drop its record,
-- links, id-set membership, shard fence records, and schedule entries. In-flight
-- callback/ack/release requests then fence (the record is gone) and cannot
-- advance cursors. The Go caller removes the per-stream fan-out index entries
-- (read before deletion) separately, since those keys are reconciled by the sweep.
local k_sub = KEYS[1]
local k_subs_set = KEYS[2]
local k_links = KEYS[3]
local k_lease_zset = KEYS[4]
local k_retry_zset = KEYS[5]
local k_due_zset = KEYS[6]
local k_shard_registry = KEYS[7]
local a_id = ARGV[1]
redis.call('DEL', k_sub)
redis.call('DEL', k_links)
redis.call('SREM', k_subs_set, a_id)
redis.call('ZREM', k_lease_zset, a_id)
redis.call('ZREM', k_retry_zset, a_id)
redis.call('ZREM', k_due_zset, a_id)
local shards = redis.call('SMEMBERS', k_shard_registry)
for _, g in ipairs(shards) do
  local member = a_id .. ':g:' .. g
  redis.call('DEL', k_sub .. ':g:' .. g)
  redis.call('ZREM', k_lease_zset, member)
  redis.call('ZREM', k_retry_zset, member)
  redis.call('ZREM', k_due_zset, member)
end
redis.call('DEL', k_shard_registry)
return { 'OK' }
