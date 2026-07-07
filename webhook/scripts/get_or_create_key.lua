-- get_or_create_key.lua — atomically adopt the persisted active signing key or
-- install the caller's candidate as the active key (PROTOCOL §6.5: private keys
-- SHOULD persist across restarts so the kid stays stable). The first server to
-- run this wins; all others adopt the stored key.
local k_jwks_hash = KEYS[1]
local k_active_kid = KEYS[2]
local a_candidate_kid = ARGV[1]
local a_candidate_material = ARGV[2]
local active = redis.call('GET', k_active_kid)
if active and redis.call('HEXISTS', k_jwks_hash, active) == 1 then
  return { active, redis.call('HGET', k_jwks_hash, active) }
end
redis.call('HSET', k_jwks_hash, a_candidate_kid, a_candidate_material)
redis.call('SET', k_active_kid, a_candidate_kid)
return { a_candidate_kid, a_candidate_material }
