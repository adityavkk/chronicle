-- check_owner.lua — verify the caller still owns a slot at the expected epoch.
-- This is reserved for the external webhook POST gate; schedule/due mutations
-- inline owner_fenced in their own script.
-- KEYS: 1=slot (ds:{ownership}:slot:<h>)
-- ARGV: 1=replica_id 2=expected_epoch
-- Reply: {status} ; OWNER | FENCED | UNOWNED
local owner = redis.call('HGET', KEYS[1], 'owner_id')
if owner == false then
  return { 'UNOWNED' }
end
if owner ~= ARGV[1] or redis.call('HGET', KEYS[1], 'owner_epoch') ~= ARGV[2] then
  return { 'FENCED' }
end
return { 'OWNER' }
