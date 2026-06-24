# TanStack DB, the State Protocol, and differential dataflow: a from-first-principles guide

This document teaches the client side of the Durable Streams stack. It explains four things, in
an order where each one builds on the last:

1. The **State Protocol**: how you turn a raw append-only stream into a feed of typed changes.
2. **MaterializedState**: the simplest way to turn that change feed back into current state.
3. **TanStack DB**: the client store you use when a plain key lookup is not enough and you need
   queries, joins, and instant writes.
4. **Differential dataflow** and the **d2ts** engine: the thing that makes TanStack DB's live
   queries fast, explained from scratch.

It ends by tying all of this back to Chronicle and Durable Streams, and by stating plainly what
this stack does not solve. Every code sample is taken from the official docs and cited at the end.

A note on where this fits: Durable Streams, the State Protocol, and TanStack DB are all from the
same team (ElectricSQL). They are designed to stack together. Chronicle implements the base Durable
Streams protocol, so this client stack is the layer that would sit on top of a Chronicle server.

---

## 0. The problem, and the shape of the solution

Start with the problem this whole stack exists to solve. You are building an app with a screen that
shows data, for example a list of todos. The data lives on a server. You face a familiar choice, and
both options are bad.

The first option is to build one server endpoint per screen, each returning exactly the shape that
screen needs. This pushes view logic into the backend, and it leads to request chains, where one
request has to finish before the next can start.

The second option is to load a lot of data into the browser and filter it there in JavaScript. The
first load is slow, and filtering large arrays on every change is slow.

The stack in this document is a third option. You sync the data you need into the browser once, you
keep it current with small updates instead of refetching, and you compute each screen's view locally
and quickly. To make that work, four pieces fit together:

- A **durable stream** carries an ordered, append-only log of changes. (This is Chronicle.)
- The **State Protocol** gives those changes a fixed shape: insert, update, or delete on a named
  entity.
- A **materializer** folds the change feed back into current state. The simplest one is
  `MaterializedState`, an in-memory map.
- **TanStack DB** is the materializer you use when you need real queries. It keeps the data in
  collections and answers live queries over them, and it makes writes feel instant.

The idea underneath all of it is one sentence: **the log of changes is the source of truth, and any
view of current state is something you compute from that log.** Keep that sentence in mind. The rest
of this document is just the mechanics of computing those views well.

---

## 1. From an append-only stream to current state

A Durable Stream is a URL you can append bytes to and read bytes from, in order. Bytes already
written never change. You read from a saved position called an offset, which is an opaque token you
can store and resume from later. That is the whole base primitive. It knows nothing about records or
updates. It just stores bytes in order.

Because the stream only records changes, current state is not stored anywhere. You derive it. You
read every change in order and apply each one to a local copy. Reading the history and applying it is
called **materialization**, and the current value of any entity is simply the last value written for
it.

This is the same model as event sourcing, which the companion docs in this folder cover. The new
material here is the layers above the stream that make this model pleasant to use on a client.

---

## 2. The State Protocol: giving the bytes a meaning

A raw stream carries bytes with no agreed meaning. The State Protocol is a small convention that
gives them one. The rule is: make the stream a JSON stream, and write each message as a JSON object
with a fixed shape. Once you do that, the stream reads as a database-style change feed.

Each message is a **change event**. It describes one operation on one entity. Here is the exact shape
from the protocol specification:

```json
{
  "type": "<entity-type>",
  "key": "<entity-key>",
  "value": <any-json-value>,
  "old_value": <any-json-value>,  // optional
  "headers": {
    "operation": "insert" | "update" | "delete",
    "txid": "<transaction-id>",  // optional
    "timestamp": "<rfc3339-timestamp>"  // optional
  }
}
```

The fields mean this:

- `type` says which kind of entity changed, for example `"user"`. It routes the event to a
  collection.
- `key` is the entity's id, for example `"user:123"`.
- `value` is the entity's new data. It is required for insert and update.
- `old_value` is the prior data. It is optional, and it is there for auditing or for detecting a
  conflict, not for resolving one.
- `headers.operation` is the one required header. It is `insert`, `update`, or `delete`.
- `headers.txid` and `headers.timestamp` are optional. The `txid` is an opaque transaction id, which
  matters later for confirming a write.

A concrete insert looks like this:

```json
{
  "type": "user",
  "key": "user:123",
  "value": { "name": "Alice", "email": "alice@example.com" },
  "headers": { "operation": "insert", "timestamp": "2025-01-15T10:30:00Z" }
}
```

There is a second kind of message, the **control event**, which manages the stream rather than
carrying data. It has only headers, and its `control` field is one of three values. `snapshot-start`
and `snapshot-end` mark that the messages between them are a complete picture of state at one moment.
`reset` tells the client to throw away what it has and start over from a given offset, which is useful
after a schema change. Control events let a server say "here is a full baseline" and "start over"
without inventing new data messages.

That is the entire State Protocol. It adds no new network features. It only fixes the shape of the
messages and the rule for folding them. The strength of that is interoperability: any client and any
server that agree on this shape can sync state with each other.

One thing the protocol deliberately leaves out, which is important and comes up again at the end: it
has **no version number per entity and no conflict resolution**. The `old_value` and `txid` fields
support detecting and grouping changes, but deciding what happens when two writers change the same
entity is left to the application.

---

## 3. MaterializedState: the simplest materializer

`MaterializedState` is the reference materializer that ships with the State Protocol. It is an
in-memory store that applies change events to reconstruct current values. It is worth reading its
actual code, because it is short and it makes the model concrete.

```typescript
export class MaterializedState {
  private data: Map<string, Map<string, unknown>>   // type -> (key -> value)

  apply(event: ChangeEvent): void {
    const { type, key, value, headers } = event

    let typeMap = this.data.get(type)
    if (!typeMap) {
      typeMap = new Map()
      this.data.set(type, typeMap)
    }

    switch (headers.operation) {
      case `insert`: typeMap.set(key, value); break
      case `update`: typeMap.set(key, value); break
      case `upsert`: typeMap.set(key, value); break
      case `delete`: typeMap.delete(key); break
    }
  }

  get<T>(type: string, key: string): T | undefined { /* lookup */ }
  getType(type: string): Map<string, unknown> { /* all of a type */ }
}
```

The data structure is a map from entity type to a map from key to value. The `apply` method reads the
operation and does the obvious thing. Notice the part that surprises people: **insert, update, and
upsert are identical.** All three call `typeMap.set(key, value)`, which is a full overwrite. Only
delete is different. So an update does not merge fields. If a producer sends a partial value on an
update, the fields it leaves out are lost. The operation name is mostly a description for the
producer and reader, not a different code path.

Here is the full lifecycle with the real API. You apply a change, then you read current state:

```typescript
import { MaterializedState } from "@durable-streams/state"

const state = new MaterializedState()

state.apply({
  type: "token",
  key: "stream-1",
  value: { content: "Hello", model: "claude-3" },
  headers: { operation: "insert" },
})

const token = state.get("token", "stream-1")   // { content: "Hello", model: "claude-3" }
const allTokens = state.getType("token")        // Map of every token
```

Now work through a tiny example by hand to see "current value is the last write." Suppose these three
events arrive in order for `key: "user:1"`:

1. `insert` with `value: { name: "Ana", plan: "free" }`. The map now holds `{ name: "Ana", plan:
   "free" }`.
2. `update` with `value: { name: "Ana", plan: "pro" }`. The map is overwritten, so it now holds
   `{ name: "Ana", plan: "pro" }`. The update replaced the whole value, it did not patch one field.
3. `delete`. The key is removed. `get("user", "user:1")` now returns `undefined`.

Replaying those three events always lands on the same result, because the events are ordered and the
bytes never change. That determinism is the point: every client that reads the same feed converges on
the same state.

`MaterializedState` is the right tool when key lookups are all you need, for example a feed of AI
tokens, a presence list, or a key value config. It is small and has no dependencies. Its limit is also
clear: it can only look up by key. It cannot filter, sort, join, or aggregate, and it is not
reactive, meaning it does not tell your UI when something changed. For that you need a query engine,
which is TanStack DB.

---

## 4. TanStack DB: collections, live queries, and instant writes

TanStack DB is a reactive in-memory store that you put between your components and your backend. It
has three core ideas. You load data into **collections**, you read with **live queries**, and you
write with **optimistic mutations**.

### Collections

A collection is a typed set of objects of one kind, the client-side equivalent of a database table,
for example all todos or all lists. Its job is to separate getting data into your app from binding
data to your components. You give a collection a way to compute each row's key and, ideally, a schema.

A collection has a source, which is where its data comes from. The source is what differs between
collection types:

- `queryCollection` fetches from a REST API through TanStack Query.
- `electricCollection` syncs in real time from Postgres through ElectricSQL. (A Chronicle-backed
  collection would sit in this same slot, as a sync source.)
- `localOnlyCollection` holds in-memory UI state with no server.
- `localStorageCollection` keeps small state in the browser's local storage and syncs it across tabs.

Here is a REST-backed collection, taken from the docs. The `onInsert` handler is how a local write
reaches your server:

```tsx
import { createCollection } from "@tanstack/react-db"
import { queryCollectionOptions } from "@tanstack/query-db-collection"

const todoCollection = createCollection(
  queryCollectionOptions({
    queryKey: ["todos"],
    queryFn: async () => fetch("/api/todos"),
    getKey: (item) => item.id,
    schema: todoSchema,
    onInsert: async ({ transaction }) => {
      const { changes: newTodo } = transaction.mutations[0]
      await api.todos.create(newTodo)   // send the local write to your API
    },
  })
)
```

### Live queries

A live query reads from one or more collections with a builder that looks like SQL: `from` picks the
collections, `where` filters, `join` combines, `groupBy` aggregates, `select` shapes the output. The
result is reactive. When the underlying data changes in a way that affects the result, the result
updates and your component re-renders. In React you read it with the `useLiveQuery` hook.

This query joins two collections and is recomputed live:

```ts
import { useLiveQuery } from "@tanstack/react-db"
import { eq } from "@tanstack/db"

const { data: todos } = useLiveQuery((q) =>
  q
    .from({ todos: todoCollection })
    .join({ lists: listCollection }, ({ todos, lists }) => eq(lists.id, todos.listId), "inner")
    .where(({ lists }) => eq(lists.active, true))
    .select(({ todos, lists }) => ({ id: todos.id, title: todos.title, listName: lists.name }))
)
```

A real relational join across two client-side collections is the thing a plain fetch-and-cache layer
cannot do for you. It is the concrete payoff of "load normalized collections, then denormalize on the
client exactly as each screen needs."

Two more things follow. First, every live query is itself a collection, so you can query the result
of a query. A query that filters or aggregates a collection becomes a derived collection that other
queries can read, the way a database view builds on a table. Second, the reason these queries can be
recomputed on every change without being slow is the engine in Part 5.

### Optimistic mutations

You write by calling `insert`, `update`, or `delete` on a collection. The change shows in the UI
immediately, before the server has confirmed anything:

```ts
const todoCollection = createCollection({
  id: "todos",
  onUpdate: async ({ transaction }) => {
    const { original, changes } = transaction.mutations[0]
    await api.todos.update(original.id, changes)
  },
})

// Applies optimistic state instantly, then runs onUpdate in the background.
todoCollection.update(todo.id, (draft) => {
  draft.completed = true
})
```

The lifecycle has five steps. First, the optimistic change is applied to local state and the UI
updates at once. Second, the matching handler runs. Third, the handler writes to your backend. Fourth,
the handler waits for that change to come back through sync. Fifth, the optimistic change is dropped
and replaced by the confirmed server state. If the handler throws, the optimistic change is rolled
back automatically.

Internally the collection keeps two layers: the confirmed synced data, which it treats as immutable,
and your pending optimistic changes on top. A live query reads a merged view of the two. This is why
the UI can show an instant change without corrupting the source data, and why rollback is clean. You
just discard the top layer.

There is one rule you must follow, and it is the most common source of bugs: **a write handler must
not finish until the server's change has synced back into the collection.** If it finishes early, the
optimistic row is removed before the confirmed row arrives, and the row flickers out and back in. How
you wait depends on the collection type. A `queryCollection` refetches after the handler. An
`electricCollection` waits for a transaction id, which Part 6 explains.

### How this differs from TanStack Query, a Redux store, and a local database

TanStack DB is built on top of TanStack Query and extends it. TanStack Query is a cache of server
responses keyed by a query key, and it does not understand relationships between your data, so a
cross-entity view needs manual array filtering and a write needs a hand-written sequence of cancel,
snapshot, set, roll back, and invalidate. TanStack DB keeps a normalized store, runs real relational
live queries, and owns the optimistic state so a write is a single call with automatic rollback. The
docs show the contrast directly:

```typescript
// Before, with plain TanStack Query: write the optimistic update by hand.
const addTodoMutation = useMutation({
  mutationFn: async (newTodo) => api.todos.create(newTodo),
  onMutate: async (newTodo) => {
    await queryClient.cancelQueries({ queryKey: ["todos"] })
    const previousTodos = queryClient.getQueryData(["todos"])
    queryClient.setQueryData(["todos"], (old) => [...(old || []), newTodo])
    return { previousTodos }
  },
  onError: (err, newTodo, context) => {
    queryClient.setQueryData(["todos"], context.previousTodos)
  },
  onSettled: () => queryClient.invalidateQueries({ queryKey: ["todos"] }),
})

// After, with TanStack DB: the collection owns the optimistic state and rollback.
todoCollection.insert({ id: crypto.randomUUID(), text: "🔥 Make app faster", completed: false })
```

Against a Redux or Zustand store, TanStack DB can replace them for client state, because it manages
both server and client state through collections and live queries, instead of you writing reducers,
selectors, and memoized derivations. Against a local SQLite or IndexedDB database, TanStack DB is a
different thing: it is an in-memory reactive query engine, not on-disk storage. For durability or
offline use you pair it with `localStorageCollection` for small data, or with a sync engine. The two
are combined, not chosen between.

---

## 5. Differential dataflow and d2ts: why live queries are fast

This is the part most people have not seen before, so it starts from zero.

### The problem: keeping a query result current is expensive the naive way

A live query result is a saved answer to a question, for example "all incomplete todos, sorted by
date." When one todo changes, you want the answer to update. The simple way is to run the whole query
again over every todo. If there are 100,000 todos, you do 100,000 rows of work to react to a one-row
change. That cost grows with the size of the data, not the size of the change, so it gets slow.

Keeping a saved query result correct as the data changes, without rebuilding it from scratch, is
called **incremental view maintenance**. The goal is to do work proportional to the change, not to
the whole dataset.

### The technique: differential dataflow

Differential dataflow is a method for doing exactly that. It came from Microsoft Research, in a 2013
paper by Frank McSherry and colleagues, built on a system called Naiad. McSherry later wrote the
standard Rust implementation, and the database company Materialize is built on it. It has three parts.

First, you express the query once as a **graph of small operators**: map transforms a row, filter
keeps rows that pass a test, join matches rows from two streams by key, count and reduce aggregate by
key, groupBy groups and aggregates. Data flows along the edges of this graph from one operator to the
next.

Second, the things flowing between operators are not whole tables. They are **changes**, called
deltas. A delta is a row paired with a number called its multiplicity. A multiplicity of `+1` means
"this row was added." A multiplicity of `-1` means "this row was removed." So a deletion is just a row
with a negative weight. This is why deletions cost the same as insertions: they travel through the
same operators, with a minus sign.

Third, every operator that needs to remember things keeps a **small internal index** of what it has
seen. So when a single `+1` or `-1` arrives, the operator looks up only the rows involved, computes
the matching change to its own output, and emits just that. It never scans everything.

Each change also carries a **version**, which is a logical timestamp that records the order changes
happened in, so aggregates know when a result is ready to emit.

**d2ts** is a TypeScript implementation of differential dataflow, written by the ElectricSQL team. It
brings the same operator graph and the same delta model to JavaScript, so it can run in a browser.
TanStack DB compiles each live query into a d2ts operator graph. When a collection row changes, only
the affected part of the result is recomputed. The docs report that updating one row in a sorted
100,000-item collection takes about 0.7 milliseconds on an M1 Pro laptop, which is why optimistic
writes feel instant.

### What the operator graph looks like in code

Here is the smallest d2ts pipeline, from its README. Each `[value, multiplicity]` pair is a delta.

```typescript
import { D2, map, filter, debug, MultiSet } from "@electric-sql/d2ts"

const graph = new D2({ initialFrontier: 0 })
const input = graph.newInput<number>()

const output = input.pipe(
  map((x) => x + 5),
  filter((x) => x % 2 === 0),
  debug("output"),
)
graph.finalize()

input.sendData(0, new MultiSet([[1, 1], [2, 1], [3, 1]]))   // add 1, 2, 3 (each +1)
input.sendFrontier(1)
graph.run()
// map adds 5 -> 6, 7, 8 ; filter keeps evens -> emits 6 and 8
```

### A worked example: one new row through filter, join, then count

This is the example to remember, because it shows the whole payoff. Setup: a stream of comments is
joined to a stream of users by user id, and then comments are counted per user. Suppose user `u1`,
named `Ana`, already has 2 comments in the result, and the join and count operators already hold that
state in their indexes.

Now one new comment arrives. It is a single delta: the row `{ commentId: "c3", userId: "u1", visible:
true }` with multiplicity `+1`.

- **filter (keep visible comments).** The comment is visible, so the filter emits one delta, the same
  row with `+1`. If it were not visible, the filter would emit nothing, and the pipeline would stop
  here.
- **join with users on `userId = "u1"`.** The join does not scan all users. It takes the one comment
  delta, looks up key `u1` in its stored index of users, finds `{ name: "Ana" }`, and emits one joined
  delta `[(c3, Ana), +1]`.
- **count by user name.** `Ana` previously had a count of 2. The count operator updates its stored
  aggregate to 3 and emits **two** deltas to express the change: `[(Ana, 2), -1]` to retract the old
  count, and `[(Ana, 3), +1]` to assert the new one.

The total work for the whole pipeline is a few index lookups and three emitted deltas, no matter how
many comments or users exist. If the comment were deleted instead, the identical path runs with `-1`
weights. That is incremental view maintenance in action.

The pattern in the count step, where a changed aggregate emits a `-1` for the old result and a `+1`
for the new result, is general. It is how aggregates stay correct under both additions and removals.

### The cost

Differential dataflow trades memory and complexity for speed. Every stateful operator (join, count,
reduce) must hold an index of its inputs, so a join keeps an index of both sides. Large live datasets
with many joins can use a lot of memory, which matters in a browser. The first pass over existing data
is not free either: the operators have to process everything once to build their indexes before the
fast incremental updates begin. And the speed number is environment-specific, measured on one dataset
and one laptop, so it varies with query shape, data size, and hardware.

---

## 6. The end-to-end flow: a write travels the whole stack

Now put the pieces together and follow a single write from a click to every other client's screen.
The path depends on the collection type, so here is the sync-engine path, which is the one that
matches a Durable Streams or ElectricSQL backend.

1. The user clicks. Your code calls `collection.insert(newTodo)`.
2. The collection applies the change to its optimistic layer. The live query that reads the collection
   recomputes the affected part of its result through d2ts, and the UI updates at once.
3. The `onInsert` handler runs. It writes the change to your backend inside a database transaction,
   and the backend returns a transaction id. The handler returns `{ txid }`.
4. The backend's committed change is published to the stream as a State Protocol change event.
5. Sync delivers that change event to every client that is subscribed, including the one that made the
   write. The client matches the returned `txid` to the change arriving in the stream. When it
   matches, the client drops the optimistic layer and keeps the confirmed row.
6. On every other client, the same change event arrives, their collections update, and their live
   queries recompute. Their screens now show the new todo.

The `txid` deserves a note, because it is the only concurrency-related mechanism in the stack, and it
is often misunderstood. It is a confirmation token, not a version check. The backend generates it
inside the same transaction as the write, returns it, and the client waits for that exact id to come
back through the feed. Here is the correct pattern and the common bug:

```typescript
// ✅ Correct: get the txid INSIDE the same transaction as the write.
const result = await sql.begin(async (tx) => {
  txid = await generateTxId(tx)                       // SELECT pg_current_xact_id()
  const [todo] = await tx`INSERT INTO todos ${tx(data)} RETURNING *`
  return todo
})
return { todo: result, txid }
// If you read the txid in a SEPARATE transaction, it never appears in the stream,
// and the optimistic write stalls forever with no error.
```

The `txid` ties one optimistic write to the one confirmed change that replaces it. It does not say
"the entity was at version N." It carries no freshness or ordering guarantee to clients. The protocol
states that clients must not rely on transaction semantics unless the server documents them.

---

## 7. How this connects to Chronicle, and what it does not solve

Lay the stack next to Chronicle and the division of labor is clean.

- The **durable stream** is the log. Chronicle is a durable-streams server. It carries ordered,
  append-only, resumable bytes, and it is the part Chronicle already implements and proves correct.
- The **State Protocol** is a message convention, mostly a client and producer concern. The server's
  job is only to carry JSON messages in order. Chronicle needs to add little or nothing for this.
- **MaterializedState** and **TanStack DB** run on the client. They fold the change feed into current
  state, and TanStack DB makes that state queryable and writable.

This is why the recommendation in `LANDSCAPE.md` is to keep projections on the client. The read model
is computed on each client by d2ts, incrementally. The server never runs the fold, so it never hits
the single-hub serialization limit that a server-side projection would. The server's job shrinks to
shipping the log well, which Chronicle already does with offset reads and live tailing.

Now the honest limit, which connects back to the earlier discussion of concurrency. This stack solves
the **read side** of the problem very well. It does not solve the **write side**. There is no
per-entity version and no expected-version check anywhere in it. `MaterializedState` is plain
last-write-wins: two writers who change the same key simply overwrite each other, and the last change
to arrive wins. TanStack DB's "optimistic" writes are optimistic UI, which means apply locally and roll
back on rejection. They are not optimistic concurrency control. Deciding whether two conflicting writes
are allowed is left entirely to the application and the server.

So if you build event-sourced entities on Chronicle and you need the guarantee that an entity stays
internally consistent under concurrent writers, this client stack will not give it to you. You add it
on the server, either with a numeric expected-version on append, or by making one writer own the
entity. The companion documents in this folder cover that choice.

---

## 8. Glossary

- **Durable Stream**: a URL-addressable, append-only, ordered byte stream you can resume from an
  offset. The base log. Chronicle implements it.
- **Offset**: an opaque, sortable token marking a position in a stream, used to resume reading.
- **State Protocol**: a JSON convention that makes each stream message an insert, update, or delete on
  a typed, keyed entity.
- **Change event**: one State Protocol message. Fields: `type`, `key`, `value`, optional `old_value`,
  and `headers` with `operation`.
- **Materialization**: folding the change feed in order to reconstruct current state.
- **MaterializedState**: the reference in-memory materializer, a map from type to a map from key to
  value, applying changes last-write-wins.
- **Collection**: in TanStack DB, a typed set of rows of one kind, with a sync source and write
  handlers.
- **Live query**: a reactive query over collections that recomputes incrementally when data changes.
- **Optimistic mutation**: a write that updates the UI at once and reconciles with the server in the
  background, rolling back on error.
- **Incremental view maintenance**: keeping a query result current by doing work proportional to the
  change, not the whole dataset.
- **Differential dataflow**: the technique behind that, using a graph of operators that pass deltas
  (rows with a `+1` or `-1` multiplicity) and keep indexed state.
- **d2ts**: ElectricSQL's TypeScript implementation of differential dataflow. The engine under
  TanStack DB live queries.
- **txid**: an optional transaction id used to confirm that a write came back through the feed. A
  confirmation token, not a version.

## 9. Sources

- TanStack DB overview, collections, live queries, mutations:
  [tanstack.com/db](https://tanstack.com/db/latest/docs/overview),
  [query-collection](https://tanstack.com/db/latest/docs/collections/query-collection),
  [electric-collection](https://tanstack.com/db/latest/docs/collections/electric-collection),
  [live-queries](https://tanstack.com/db/latest/docs/guides/live-queries),
  [mutations](https://tanstack.com/db/latest/docs/guides/mutations).
- d2ts and differential dataflow: [github.com/electric-sql/d2ts](https://github.com/electric-sql/d2ts),
  the 2013 CIDR paper "Differential Dataflow," and McSherry's Rust
  [differential-dataflow](https://github.com/TimelyDataflow/differential-dataflow).
- ElectricSQL on TanStack DB and differential dataflow:
  [Super-fast apps on sync with TanStack DB](https://electric.ax/blog/2025/07/29/super-fast-apps-on-sync-with-tanstack-db).
- Durable Streams and the State Protocol:
  [PROTOCOL.md](https://github.com/durable-streams/durable-streams/blob/main/PROTOCOL.md),
  the `@durable-streams/state` package (STATE-PROTOCOL.md, `materialized-state.ts`, `types.ts`,
  README), and [Durable Streams 0.1.0 and the State Protocol](https://electric.ax/blog/2025/12/23/durable-streams-0.1.0).

---

*Companion reading in this folder: [`README.md`](README.md) answers the framing questions,
[`LANDSCAPE.md`](LANDSCAPE.md) is the design reference, and [`research/`](research/) holds the grounded
per-system deep dives. This document is the client-side materialization layer of that story.*
