-- common.lua — shared prelude for the __ds subscription scripts.
--
-- Every subscription-control key shares the {__ds} hash tag, so all of these
-- scripts touch a single slot and are cluster-safe. The per-stream fan-out
-- index (ds:{__ds}:stream:<path>) is maintained from Go as a best-effort index
-- reconciled by the recovery sweep, so it is never touched here.
--
-- ONE EXCEPTION (issue #14): the schedule/due-mutating scripts (arm_wake, ack,
-- expire_lease, schedule_retry, release) also take the {ownership} slot key as an
-- extra KEY to inline the owner-epoch fence (owner_fenced below). That key carries
-- the literal {ownership} tag, a DIFFERENT cluster slot, so those EVALs are
-- single-slot only on a single-node Redis (the deploy/test substrate); the
-- ownership keyspace is deliberately not slot-homed (05:311), and co-locating it
-- for a real cluster is out of scope here (state shard #15, DR #16). The slot key
-- is read ONLY when the caller is acting as a slot owner (epoch ~= ''), so the
-- load-balanced external paths that pass epoch '' never touch the second slot.

-- offset_greater reports a > b for opaque, fixed-width, lexicographically
-- sortable offsets (PROTOCOL §8), treating the "-1"/"" beginning sentinel as
-- less than any real offset. Redis Lua compares strings bytewise (C locale),
-- which equals stream order for zero-padded offsets. Mirrors state.go.
local function offset_greater(a, b)
  if a == b then return false end
  if b == '-1' or b == '' then return a ~= '-1' and a ~= '' end
  if a == '-1' or a == '' then return false end
  return a > b
end

-- split_link splits a links-hash value "<link_type>:<offset>" on the first
-- colon (link_type has no colon; an offset may). Returns link_type, offset.
local function split_link(v)
  local sep = string.find(v, ':', 1, true)
  return string.sub(v, 1, sep - 1), string.sub(v, sep + 1)
end

-- fenced reports whether a callback/ack/release is stale and must be rejected
-- (PROTOCOL §7.3): token generation, request generation, and request wake_id
-- must all match current subscription state. Mirrors FenceDecision in state.go.
local function fenced(cur_gen, cur_wake, req_gen, req_wake, token_gen)
  return token_gen ~= cur_gen or req_gen ~= cur_gen or req_wake == '' or req_wake ~= cur_wake
end

-- owner_fenced is the owner-epoch fence the schedule/due-mutating scripts inline
-- at the top to resolve the TOCTOU (issue #14, 05:372-385): a deposed-but-resumed
-- owner's write is rejected ATOMICALLY with the write itself, which a separate
-- check_owner round-trip could not do across a GC pause between check and write.
-- epoch == '' means the caller is NOT acting as a slot owner (the load-balanced
-- external/hot path) — the check is skipped and the (gen,wake_id) fence is the
-- guard. Otherwise the caller must be slot's current owner_id at the expected
-- owner_epoch, else its write is FENCED. Layered ABOVE the (gen,wake_id) fence,
-- NEVER replacing it: it only SUPPRESSES a deposed owner's wasted work. The
-- slot-ownership axis it enforces is orthogonal to the per-(subId,g) claim
-- granularity (#11).
local function owner_fenced(slot, me, epoch)
  if epoch == '' or epoch == false or epoch == nil then return false end
  if redis.call('HGET', slot, 'owner_id') ~= me then return true end
  return redis.call('HGET', slot, 'owner_epoch') ~= epoch
end
