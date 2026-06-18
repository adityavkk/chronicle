-- get_or_create_key.lua — atomically adopt the persisted active signing key or
-- install the caller's candidate as the active key (PROTOCOL §6.5: private keys
-- SHOULD persist across restarts so the kid stays stable). The first server to
-- run this wins; all others adopt the stored key.
-- KEYS: 1=jwks_hash 2=active_kid
-- ARGV: 1=candidate_kid 2=candidate_material
-- Reply: {kid, material}
local active = redis.call('GET', KEYS[2])
if active and redis.call('HEXISTS', KEYS[1], active) == 1 then
  return { active, redis.call('HGET', KEYS[1], active) }
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('SET', KEYS[2], ARGV[1])
return { ARGV[1], ARGV[2] }
