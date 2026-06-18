-- common.lua — shared prelude for the __ds subscription scripts.
--
-- Every subscription-control key shares the {__ds} hash tag, so all of these
-- scripts touch a single slot and are cluster-safe. Owner slot records checked
-- inline by these scripts are co-located under that same tag; membership stays
-- under {ownership} because it is not part of schedule/due mutations. The
-- per-stream fan-out index (ds:{__ds}:stream:<path>) is maintained from Go as a
-- best-effort index reconciled by the recovery sweep, so it is never touched
-- here.

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

-- shard_field returns the subscription HASH field for one claim shard. Shard 0
-- uses the legacy unqualified field names so existing state remains readable.
local function shard_field(base, shard)
  if shard == '0' then return base end
  return base .. ':' .. shard
end

-- legacy_base_used reports whether pre-upgrade shard-0 state already proves the
-- subscription has used the original unsharded pull-claim contract.
local function legacy_base_used(sub)
  local gen = tonumber(redis.call('HGET', sub, 'generation')) or 0
  local phase = redis.call('HGET', sub, 'phase') or 'idle'
  local wake = redis.call('HGET', sub, 'wake_id') or ''
  local holder = redis.call('HGET', sub, 'holder') or '0'
  local lease_until = tonumber(redis.call('HGET', sub, 'lease_until_ns')) or 0
  return gen ~= 0 or phase ~= 'idle' or wake ~= '' or holder == '1' or lease_until ~= 0
end

-- claim_mode_conflict reports whether the request tries to mix the original
-- unsharded pull-claim contract with the explicit-shard extension. The first
-- post-upgrade claim fixes claim_mode unless shard-0 legacy fields already
-- imply legacy mode; ack/release fence on a later mismatch.
local function claim_mode_conflict(sub, mode)
  local cur = redis.call('HGET', sub, 'claim_mode') or ''
  if cur == '' and legacy_base_used(sub) then cur = 'legacy' end
  return cur ~= '' and cur ~= mode, cur
end

-- owner_fenced reports whether an owner-epoch guarded schedule/due mutation
-- should be rejected. Empty owner_id means the caller is an explicit no-owner
-- path (route callback or full-sweep backstop) and bypasses this optimization
-- fence; non-empty callers must match the slot ownership record exactly.
local function owner_fenced(slot, owner_id, owner_epoch)
  if owner_id == '' then return false end
  local owner = redis.call('HGET', slot, 'owner_id')
  if owner == false then return true end
  return owner ~= owner_id or redis.call('HGET', slot, 'owner_epoch') ~= owner_epoch
end
