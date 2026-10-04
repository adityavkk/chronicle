# Fixed-tenant mount verification

Source `92500e7`; image `chronicle-raft:mount`, ID
`sha256:e2d68b495d29042b2ba6c1f7b6ba7f47d652171171e6646c18aec3a0ed5ce4ca`;
release binary SHA256
`1518d9558348f2fd3189ed770dae2e7ccca83e44dcbac46283976406be5ca442`.

Before upgrade, `tests/mount_http.py --phase prepare` created the same path under
two distinct canonical tenants with asymmetric payloads. All five pods stopped;
PVCs retained. The new image and `STREAM_TENANT=conformance-mounted` were applied
before restarting. `--phase verify` passed: the shortened mounted URL read the
original configured tenant's bytes; a URL spelling the other tenant did not read
its data; mounted creation/read also worked. Histories are
`mount-{prepare,verify}.jsonl`, schema-3 observations, not Porcupine input.

`make check` passed before deployment. The full unchanged upstream suite against
the origin (no nested tenant prefix) returned **247 passed, 79 failed, 6 upstream
default skips**, retained in `conformance-mount.{json,txt}`. Fork operations remain
unsupported, so namespace alignment is not fork acceptance.

The cluster is currently in this fixed-tenant mode. All forwarding nodes must
share the configuration. Restore ordinary canonical routing only by stopping all
pods, removing `STREAM_TENANT` from the StatefulSet, then restarting; changing a
subset can route requests into the wrong namespace. This setting is trusted
deployment configuration, not tenant authentication or a client-selectable header.
