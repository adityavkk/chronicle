# Unmodified upstream protocol suite

This harness pins `@durable-streams/server-conformance-tests` 0.3.5 and Vitest
4.1.8, with the full dependency graph/integrities in `package-lock.json`. It does
not rewrite tests or omit failing core cases. The six optional subscription cases
are disabled by upstream's default; Chronicle's Go subscription API is not
implemented here. Transport errors retain their cause and are rethrown unchanged.

From `rust/chronicle`, against a disposable, trusted-private deployment:

```sh
make conformance CONFORMANCE_TEST_URL="$PRIVATE_CHRONICLE_URL/v1/stream/unused-tenant"
```

The suite concatenates `/v1/stream/...` to `baseUrl`; the nested path becomes the
stream identity within this tenant. It creates/deletes data. Never target a
production tenant. Fork references and same-URL recreation need explicit review
against this service's tenant/incarnation contract; a nested base URL alone is
not proof that every suite assumption is compatible.

`npm ci --ignore-scripts --legacy-peer-deps` reproduces installation; npm 10.9.9
otherwise failed its peer-dependency graph construction with `edgesOut` on this
orb. No dependency lifecycle scripts are required. `longPollTimeoutMs` controls
some upstream test timeouts, not the server's configured waiting period or every
test's independently hard-coded timeout.

For live k3d tests, prefer an in-cluster client or a temporary private NodePort
over `kubectl port-forward`. During this run, a broken-pipe connection terminated
the whole forwarding process and caused unrelated tests to fail while its
supervisor restarted it. The pods had not restarted. A local NodePort avoided
that tunnel-wide failure. One guarded local setup is:

```sh
test "$(ops/kubectl.sh config current-context)" = k3d-chronicle-rust
ops/kubectl.sh -n chronicle expose service chronicle-http \
  --name=chronicle-conformance --type=NodePort --port=8080 --target-port=8080
ops/kubectl.sh -n chronicle get service chronicle-conformance
sudo docker inspect k3d-chronicle-rust-agent-0 \
  --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}'
# Set PRIVATE_CHRONICLE_URL from that private Docker address and assigned port.
# Wait for /healthz, then run the suite above. No public portal or host port mapping.
ops/kubectl.sh -n chronicle delete service chronicle-conformance
```

Preserve every run's default and JSON reports, including failed transport runs:

```sh
CONFORMANCE_TEST_URL="$PRIVATE_CHRONICLE_URL/v1/stream/another-unused-tenant" \
  npm --prefix tests/conformance test -- --reporter=default --reporter=json \
  --outputFile=../../evidence/NEW-RUN.json > evidence/NEW-RUN.txt 2>&1
```

Use unused output filenames; the upstream reporter overwrites an existing report.
Current failures and limitations are recorded in `evidence/CONFORMANCE.md`, not
converted into skips. A passing narrower SSE/history test does not override them.
