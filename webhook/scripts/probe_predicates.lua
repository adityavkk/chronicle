-- probe_predicates.lua — TEST-ONLY driver for the control-plane fence/ordering
-- predicate differential (issue #33). It is NOT loaded by scripts.go and is never
-- run in production; the differential test prepends the REAL common.lua prelude
-- (via the same scriptFS the shipped scripts load) and appends this driver, so the
-- property exercises the SHIPPED `fenced` / `offset_greater` source — a
-- re-transcription would defeat the whole point of a model-vs-implementation
-- differential.
--
-- Both predicates are `local function`s inside common.lua and so are not directly
-- EVAL-able; this probe is the thin shim that reaches them.
--
-- Dispatch on ARGV[1]:
--   'fenced'  ARGV[2..6] = cur_gen, cur_wake, req_gen, req_wake, token_gen
--             -> returns 1 if fenced (stale, reject), 0 if it may proceed.
--             The generations are passed as the SAME canonical-decimal strings
--             the live ack.lua/release.lua path uses (cur_gen from HGET of an
--             HINCRBY'd field; req_gen/token_gen from strconv.FormatInt), so this
--             drives `fenced` exactly as production does (string compare).
--   'offset'  ARGV[2..3] = a, b
--             -> returns 1 if offset_greater(a, b), else 0.

local mode = ARGV[1]
if mode == 'fenced' then
  -- common.lua's `fenced` returns a Lua boolean; map to 1/0 for a stable reply.
  if fenced(ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6]) then
    return 1
  end
  return 0
elseif mode == 'offset' then
  if offset_greater(ARGV[2], ARGV[3]) then
    return 1
  end
  return 0
end
return redis.error_reply('probe_predicates: unknown mode ' .. tostring(mode))
