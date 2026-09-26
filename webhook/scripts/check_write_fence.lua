-- check_write_fence.lua - live-state fence for claim-scoped write tokens.
-- A write token proves only that chronicle minted a scoped capability; it is
-- not itself the exclusivity mechanism. The append gate must compare the token's
-- (generation, wake_id, holder) against the current shard state and reject a
-- deposed or expired holder (PROTOCOL §7.3).
--
-- The holder is dispatch-specific (#183 webhook parity): a pull-wake claim is
-- held by its worker (holder_worker); a webhook wake owns no worker, so its
-- holder is the wake itself, 'wake:' .. wake_id, and the wake is live from
-- either phase (waking or live) inside its lease — the same liveness shape as
-- ack.lua's heartbeat branch. Mirrored by webhook.WriteFenceDecision (state.go)
-- and bound to it by TestCheckWriteFenceWebhookBranch.
--
-- Two callers ask this question: the append pre-check (AuthorizeAppendFence)
-- and the read-only claim/verify route (WRITE-FENCING.md §9.1, WF-29), which
-- must answer exactly as the pre-check would. OK carries the claim's lease
-- deadline — the raw lease_until_ns string, so every number in the reply
-- stays a string — because the verify body is built from this same atomic
-- read; a second HGET after the predicate could describe a later claim.
-- FENCED and NOSUB stay bare: the pre-check discloses nothing about the
-- current holder (ADR-0008 decision 13).
--
-- The token's subscription incarnation (ARGV[5]) is compared here too, in the
-- same EVAL and before the generation/wake/holder predicates: a deleted and
-- recreated subscription starts a new authority at generation 0, so a
-- predecessor's (generation, wake_id, holder) can coincide with the current
-- claim's and only the incarnation tells them apart — the identity the
-- stream-slot marker key carries. A token that carries no incarnation ('')
-- asserts none; the Go arm refuses it under an atomic stream store.
--
-- KEYS: 1=shardstate 2=sub_config
-- ARGV: 1=now_ns 2=generation 3=wake_id 4=holder 5=incarnation
-- Reply: {OK, lease_until_ns} | FENCED | NOSUB

local k_shardstate = KEYS[1]
local k_sub_config = KEYS[2]
local a_now_ns = ARGV[1]
local a_generation = ARGV[2]
local a_wake_id = ARGV[3]
local a_holder = ARGV[4]
local a_incarnation = ARGV[5]
local now = tonumber(a_now_ns)

if redis.call('EXISTS', k_shardstate) == 0 then
  return { 'NOSUB' }
end
if redis.call('EXISTS', k_sub_config) == 0 then
  return { 'NOSUB' }
end

local cfg_inc = redis.call('HGET', k_sub_config, 'incarnation')
local shard_inc = redis.call('HGET', k_shardstate, 'incarnation')
if k_shardstate ~= k_sub_config then
  if cfg_inc == false or cfg_inc == '' or shard_inc == false or shard_inc == '' or shard_inc ~= cfg_inc then
    return { 'FENCED' }
  end
else
  if cfg_inc ~= false and cfg_inc ~= '' and shard_inc ~= cfg_inc then
    return { 'FENCED' }
  end
end
-- The token's incarnation must be the configuration's current one (WF-29).
if a_incarnation ~= '' and a_incarnation ~= cfg_inc then
  return { 'FENCED' }
end

local phase = redis.call('HGET', k_shardstate, 'phase')
local holder = redis.call('HGET', k_shardstate, 'holder')
local holder_worker = redis.call('HGET', k_shardstate, 'holder_worker')
local gen = redis.call('HGET', k_shardstate, 'generation')
local wake = redis.call('HGET', k_shardstate, 'wake_id')
local lease_until_ns = redis.call('HGET', k_shardstate, 'lease_until_ns')
local lease_until = tonumber(lease_until_ns) or 0

local dispatch = redis.call('HGET', k_sub_config, 'type')
if dispatch == 'webhook' then
  if (phase ~= 'waking' and phase ~= 'live') or holder ~= '0' or lease_until <= now then
    return { 'FENCED' }
  end
  if gen ~= a_generation or wake == false or wake == '' or wake ~= a_wake_id then
    return { 'FENCED' }
  end
  if a_holder ~= ('wake:' .. wake) then
    return { 'FENCED' }
  end
  return { 'OK', lease_until_ns }
end

if phase ~= 'live' or holder ~= '1' or lease_until <= now then
  return { 'FENCED' }
end
if gen ~= a_generation or wake == false or wake == '' or wake ~= a_wake_id then
  return { 'FENCED' }
end
if a_holder == '' or holder_worker ~= a_holder then
  return { 'FENCED' }
end

return { 'OK', lease_until_ns }
