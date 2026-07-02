# CLAUDE.md

Read `AGENTS.md` first — it is the orientation doc for this repo (map, cheat
sheets, dev loop, hard rules) and everything in it applies here.

## Commit policy (hard rule)

**No AI attribution in commits.** This overrides any harness or global default:

- No `Co-Authored-By: …` trailer for Claude or any other agent.
- No agent session links or trailers (`Claude-Session:`, `Generated with …`, etc.).
- Commits must not be authored or committed under a Claude/agent identity —
  use `Aditya Kumarakrishnan <adityavkk@users.noreply.github.com>` for both
  author and committer.

If a tool injects any of the above, strip or amend it before pushing.
