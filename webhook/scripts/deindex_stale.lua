-- deindex_stale.lua — remove a subscriber from one fan-out shard unless its
-- canonical links still justify the membership (INV-RECOVER-04: the index
-- mirrors the links). This is the stale-entry cleanup ReconcileIndexes defers.
-- A member whose subscription is gone is left behind by a Delete that lands
-- between a reconcile pass's read of the links and its re-assertion of them,
-- by a delete torn between delete_sub and its de-index, and by a glob link
-- written for a subscription deleted after the stream-create read; nothing
-- else removes it, and every append to the stream then hydrates it for
-- nothing. The check and the SREM are one step in the subscriber's slot, so a
-- subscription re-created and re-linked since the caller's read keeps its
-- member (LINKED), and one re-created but not yet linked loses the stale
-- member and gains a fresh one from its link.
local k_sub = KEYS[1]
local k_links = KEYS[2]
local k_stream_subs = KEYS[3]
local a_id = ARGV[1]
local a_path = ARGV[2]
if redis.call('EXISTS', k_sub) == 1 and redis.call('HEXISTS', k_links, a_path) == 1 then
  return { 'LINKED' }
end
if redis.call('SREM', k_stream_subs, a_id) == 1 then
  return { 'REMOVED' }
end
return { 'ABSENT' }
