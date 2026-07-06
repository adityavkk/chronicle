-- delete_sub.lua — tombstone a subscription (PROTOCOL §6.3): drop its record,
-- links, id-set membership, shard fence records, and schedule entries. In-flight
-- callback/ack/release requests then fence (the record is gone) and cannot
-- advance cursors. The Go caller removes the per-stream fan-out index entries
-- (read before deletion) separately, since those keys are reconciled by the sweep.
-- KEYS: 1=sub 2=subs_set 3=links 4=lease_zset 5=retry_zset 6=due_zset
--       7=shard_registry
-- ARGV: 1=id
-- Reply: {status} ; OK
redis.call('DEL', KEYS[1])
redis.call('DEL', KEYS[3])
redis.call('SREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[4], ARGV[1])
redis.call('ZREM', KEYS[5], ARGV[1])
redis.call('ZREM', KEYS[6], ARGV[1])
local shards = redis.call('SMEMBERS', KEYS[7])
for _, g in ipairs(shards) do
  local member = ARGV[1] .. ':g:' .. g
  redis.call('DEL', KEYS[1] .. ':g:' .. g)
  redis.call('ZREM', KEYS[4], member)
  redis.call('ZREM', KEYS[5], member)
  redis.call('ZREM', KEYS[6], member)
end
redis.call('DEL', KEYS[7])
return { 'OK' }
