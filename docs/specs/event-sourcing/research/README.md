# Event sourcing & projections — landscape research

Prior-art study for deciding whether/how Chronicle's **durable streams** primitive should grow
toward **true event-sourced entities** (snapshots, projections, per-entity optimistic
concurrency, bundled handlers). Follows the repo convention `docs/specs/<topic>/research/NN-*.md`
+ a top-level synthesis.

Orca-orchestrated, `/search`-grounded (sources cited inline).

## Layout
- `00-chronicle-ground-truth.md` — what Chronicle is **today** (the fixed reference).
- `01-eventstoredb.md` — EventStoreDB / KurrentDB (the closest analog).
- `02-akka.md` — Akka Persistence event-sourced actors + Akka Projection.
- `03-axon-marten.md` — Axon Framework (JVM/CQRS) + Marten (.NET/Postgres) projection taxonomy.
- `04-log-tanstack-durable.md` — Kafka/Pulsar log lineage, TanStack DB as client-side
  projections, durable-execution adjacency (Restate/Temporal).
- `_prompts/` — the exact research briefs each worker was given (provenance).
- `../LANDSCAPE.md` — **synthesis**: similarities, gaps, and a concrete extension design.

## What each research file must contain
1. **Model** — event/stream model; how events are identified, typed, ordered.
2. **Optimistic concurrency** — the "expected version" mechanism.
3. **Snapshots** — built-in or app-level; how recovery folds snapshot + tail.
4. **Projections / read models** — *when* computed (inline/write-time vs async vs read-time);
   checkpoints/offsets; replay/rebuild.
5. **Handlers & consumers** — handlers bundled with the entity/stream? subscription model
   (push/pull, competing consumers, checkpointing, delivery guarantees).
6. **Lessons for Chronicle** — 4–6 sharp, opinionated bullets of design pressure.
