# Demo notes — running the existing UI against local chronicle

This records how the upstream `examples/test-ui` was spun up and connected to a local
chronicle instance, and what was verified. It is the "if one exists, spin it up and connect
it to a local instance of chronicle" step from the request.

## What was run

No Docker or Redis was needed. chronicle was run with its in-memory store.

1. **chronicle server** (built from this worktree):

   ```
   make build
   ./bin/chronicle --listen :4437 --store memory --subscriptions=false
   ```

   Notes:
   - `--store memory` avoids Redis and Docker entirely (Colima was not running).
   - `--subscriptions=false` is required with the memory store, because the `__ds`
     subscription control plane needs the Redis backend. The test-ui does not use the
     subscription API, so this does not affect the demo.
   - chronicle log line confirmed: `chronicle listening addr=:4437 root=/v1/stream/
     store=memory subscriptions=false`.

2. **The existing UI** (from the upstream clone, `node_modules` already present):

   ```
   cd /Users/auk000v/dev/durable-streams/examples/test-ui
   pnpm dev      # Vite 7 dev server on http://localhost:3000
   ```

   It connects to `http://localhost:4437` because the UI computes its server URL as
   `http://${window.location.hostname}:4437`, and chronicle also listens on `:4437`. This is
   the lucky-port path; it only works on the default port, which is the limitation the new UI
   must remove.

Both processes were started as Orca-managed terminals inside this worktree's workspace.

## What was verified

- chronicle round-trip with `curl`: `PUT /v1/stream/demo` returned `201`, append returned
  `204`, read returned `200` with the JSON body. CORS is open
  (`Access-Control-Allow-Origin: *`), so the browser UI on `:3000` can call `:4437`.
- Through the UI: created a stream `ui-demo`. chronicle then reported
  `HEAD /v1/stream/ui-demo` as `200`, and the UI's discovery event appeared in chronicle's
  `__registry__` stream:
  `{"type":"stream","key":"ui-demo","value":{"path":"ui-demo","contentType":"text/plain", ...}}`.
- Through the UI: sent the message "hello from the existing UI, written into chronicle".
  Reading `/v1/stream/ui-demo?offset=-1` from chronicle returned that exact text, and the UI
  live-tail updated to "1 entries · 51 B · 1/min". Full create, write, and live-read path
  works against chronicle.

## How to stop the demo

```
orca terminal stop --worktree path:/Users/auk000v/orca/workspaces/chronicle/durable-streams-ui
```

or stop the individual `chronicle :4437 (memory)` and `test-ui :3000` terminals in the Orca
app.
