import { createCollection, createOptimisticAction } from "@tanstack/db"
import { DurableStream as DurableStreamClass } from "@durable-streams/client"
import { isChangeEvent, isControlEvent } from "./types"
import type { Collection, SyncConfig } from "@tanstack/db"
import type { ChangeEvent, StateEvent } from "./types"
import type { StandardSchemaV1 } from "@standard-schema/spec"
import type {
  DurableStream,
  DurableStreamOptions,
  JsonBatch,
  LiveMode,
  StreamResponse,
} from "@durable-streams/client"
import type { CollectionDefinition, StreamStateDefinition } from "./schema"

// Schema definitions and event construction are db-free and live in ./schema.
// Re-export them here so the TanStack-backed `@durable-streams/state/db`
// surface stays a superset of the db-free main entry.
export { createStateSchema } from "./schema"
export type {
  CollectionDefinition,
  CollectionEventHelpers,
  CollectionWithHelpers,
  StreamStateDefinition,
  StateSchema,
} from "./schema"

// ============================================================================
// Type Definitions
// ============================================================================

/**
 * Definition for a single action that can be passed to createOptimisticAction
 */
export interface ActionDefinition<TParams = any, TContext = any> {
  onMutate: (params: TParams) => void
  mutationFn: (params: TParams, context: TContext) => Promise<any>
}

/**
 * Factory function for creating actions with access to db and stream context
 */
export type ActionFactory<
  TDef extends StreamStateDefinition,
  TActions extends Record<string, ActionDefinition<any>>,
> = (context: { db: StreamDB<TDef>; stream: DurableStream }) => TActions

/**
 * Map action definitions to callable action functions
 */
export type ActionMap<TActions extends Record<string, ActionDefinition<any>>> =
  {
    [K in keyof TActions]: ReturnType<typeof createOptimisticAction<any>>
  }

/**
 * Options for creating a stream DB
 */
export interface CreateStreamDBOptions<
  TDef extends StreamStateDefinition = StreamStateDefinition,
  TActions extends Record<string, ActionDefinition<any>> = Record<
    string,
    never
  >,
> {
  /** Options for creating a new durable stream. Ignored when `stream` is provided. */
  streamOptions?: DurableStreamOptions
  /** Pre-existing DurableStream instance to reuse (avoids creating a second connection). */
  stream?: DurableStream
  /** Live read mode used by the StreamDB consumer. Defaults to true. */
  live?: LiveMode
  /** The stream state definition */
  state: TDef
  /** Optional factory function to create actions with db and stream context */
  actions?: ActionFactory<TDef, TActions>
  /** Called for every ChangeEvent as it flows through the stream consumer. */
  onEvent?: (event: ChangeEvent) => void
  /**
   * Called once per consumed stream batch before items are dispatched.
   * Useful when external consumers need batch metadata available during
   * downstream collection/effect processing.
   */
  onBeforeBatch?: (batch: JsonBatch<StateEvent>) => void
  /**
   * Called once per consumed stream batch after items have been dispatched.
   * Useful for tracking safe offsets for external ack/lease protocols.
   */
  onBatch?: (batch: JsonBatch<StateEvent>) => void
  /** Reports terminal stream or batch failures after the consumer starts. */
  onError?: (error: Error) => void
  /** Validated committed state to install before opening the source stream. */
  bootstrap?: StreamDBBootstrap
  /** Source incarnation guard to retain even when no bootstrap is installed. */
  incarnation?: string
  /**
   * Enables coherent canonical capture, required by exportState(). Runs only
   * after collection writes and the corresponding offset commit. Source
   * commits apply immediately while preserving the separate optimistic overlay;
   * omitting this option retains legacy sync timing and disables export.
   */
  onCommittedBatch?: (cut: StreamDBCommittedCut) => void
}

export interface StreamDBBootstrap {
  offset: string
  incarnation?: string
  nextSeq: number
  rowsByCollection: Record<string, Array<Record<string, unknown>>>
}

export interface StreamDBCommittedCut {
  batch: JsonBatch<StateEvent>
  image: StreamDBBootstrap
}

/**
 * Extract the value type from a CollectionDefinition
 */
type ExtractCollectionType<T extends CollectionDefinition> =
  T extends CollectionDefinition<infer U> ? U : unknown

/**
 * Map collection definitions to TanStack DB Collection types
 */
type CollectionMap<TDef extends StreamStateDefinition> = {
  [K in keyof TDef]: Collection<ExtractCollectionType<TDef[K]> & object, string>
}

/**
 * The StreamDB interface - provides typed access to collections
 */
export type StreamDB<TDef extends StreamStateDefinition> = {
  collections: CollectionMap<TDef>
} & StreamDBMethods

/**
 * StreamDB with actions
 */
export type StreamDBWithActions<
  TDef extends StreamStateDefinition,
  TActions extends Record<string, ActionDefinition<any>>,
> = StreamDB<TDef> & {
  actions: ActionMap<TActions>
}

/**
 * Utility methods available on StreamDB
 */
export interface StreamDBUtils {
  /**
   * Wait for a specific transaction ID to be synced through the stream
   * @param txid The transaction ID to wait for (UUID string)
   * @param timeout Optional timeout in milliseconds (defaults to 5000ms)
   * @returns Promise that resolves when the txid is synced
   */
  awaitTxId: (txid: string, timeout?: number) => Promise<void>
}

/**
 * Methods available on a StreamDB instance
 */
export interface StreamDBMethods {
  /**
   * The underlying DurableStream instance
   */
  stream: DurableStream

  /**
   * Current stream offset (tracks the last consumed position).
   */
  readonly offset: string

  /**
   * Freeze the current committed rows and exact next-read offset together.
   * Throws unless onCommittedBatch explicitly enabled coherent capture, or
   * while a source batch is being dispatched before its committed callback.
   */
  exportState: () => StreamDBBootstrap

  /**
   * Preload all collections by consuming the stream until up-to-date
   */
  preload: () => Promise<void>

  /**
   * Close the stream connection and cleanup
   */
  close: () => void

  /**
   * Utility methods for advanced stream operations
   */
  utils: StreamDBUtils
}

/**
 * Build a TanStack collection id for a StreamDB collection.
 *
 * Collection ids must be unique per source stream, not just per schema key,
 * otherwise joining the same collection name from two different streams can
 * collapse to one logical source inside TanStack DB.
 */
export function getStreamDBCollectionId(
  streamUrl: string,
  collectionName: string
): string {
  return `stream-db:${streamUrl}:${collectionName}`
}

// ============================================================================
// Internal Event Dispatcher
// ============================================================================

/**
 * Handler for collection sync events
 */
interface CollectionSyncHandler {
  begin: () => void
  write: (
    value: object,
    type: `insert` | `update` | `delete`,
    cursor?: string
  ) => void
  commit: () => void
  markReady: () => void
  truncate: () => void
  primaryKey: string
}

/**
 * Internal event dispatcher that routes stream events to collection handlers
 */
class EventDispatcher {
  /** Map from event type to collection handler */
  private handlers = new Map<string, CollectionSyncHandler>()

  /** Handlers that have pending writes (need commit) */
  private pendingHandlers = new Set<CollectionSyncHandler>()

  /** Whether we've received the initial up-to-date signal */
  private isUpToDate = false

  /** Resolvers and rejecters for preload promises */
  private preloadResolvers: Array<() => void> = []
  private preloadRejecters: Array<(error: Error) => void> = []

  /** Set of all txids that have been seen and committed */
  private seenTxids = new Set<string>()

  /** Txids collected during current batch (before commit) */
  private pendingTxids = new Set<string>()

  /** Resolvers waiting for specific txids */
  private txidResolvers = new Map<
    string,
    Array<{
      resolve: () => void
      reject: (error: Error) => void
      timeoutId: ReturnType<typeof setTimeout>
    }>
  >()

  /** Track existing keys per collection for upsert logic */
  private existingKeys = new Map<string, Set<string>>()

  /** Global sequence counter for insertion ordering */
  private seq = 0

  /**
   * Register a handler for a specific event type
   */
  registerHandler(eventType: string, handler: CollectionSyncHandler): void {
    this.handlers.set(eventType, handler)
    // Initialize key tracking for upsert logic
    if (!this.existingKeys.has(eventType)) {
      this.existingKeys.set(eventType, new Set())
    }
  }

  hydrate(
    definitions: StreamStateDefinition,
    rowsByCollection: Record<string, Array<Record<string, unknown>>>,
    nextSeq: number
  ): void {
    for (const [collectionName, definition] of Object.entries(definitions)) {
      const handler = this.handlers.get(definition.type)
      if (!handler) throw new Error(`Missing handler for ${collectionName}`)
      const rows = rowsByCollection[collectionName] ?? []
      if (rows.length === 0) continue
      handler.begin()
      for (const row of rows) {
        const key = row[definition.primaryKey]
        if (typeof key !== `string` && typeof key !== `number`) {
          throw new Error(`Invalid bootstrap key for ${collectionName}`)
        }
        handler.write(structuredClone(row), `insert`)
        this.existingKeys.get(definition.type)?.add(String(key))
      }
      handler.commit()
    }
    this.seq = nextSeq
  }

  /**
   * Dispatch a change event to the appropriate collection.
   * Writes are buffered until commit() is called via markUpToDate().
   */
  dispatchChange(event: StateEvent, cursor?: string): void {
    if (!isChangeEvent(event)) return

    const eventCursor = event.headers.offset ?? cursor

    // Check for txid in headers and collect it
    if (event.headers.txid && typeof event.headers.txid === `string`) {
      this.pendingTxids.add(event.headers.txid)
    }

    const handler = this.handlers.get(event.type)
    if (!handler) {
      // Unknown event type - ignore silently
      return
    }

    let operation = event.headers.operation

    // Validate that values are objects (required for key tracking)
    if (operation !== `delete`) {
      if (typeof event.value !== `object` || event.value === null) {
        throw new Error(
          `StreamDB collections require object values; got ${typeof event.value} for type=${event.type}, key=${event.key}`
        )
      }
    }

    // Get value, ensuring it's an object
    const originalValue = event.value ?? {}

    // Create a shallow copy to avoid mutating the original
    const value = { ...originalValue }

    // Set the primary key field on the value object from the event key
    ;(value as any)[handler.primaryKey] = event.key

    // Stamp global insertion order for cross-collection sorting
    ;(value as any)._seq = this.seq++

    // Begin transaction on first write to this handler
    if (!this.pendingHandlers.has(handler)) {
      handler.begin()
      this.pendingHandlers.add(handler)
    }

    // Handle upsert by converting to insert or update
    if (operation === `upsert`) {
      const keys = this.existingKeys.get(event.type)
      const existing = keys?.has(event.key)
      operation = existing ? `update` : `insert`
    }

    const keys = this.existingKeys.get(event.type)

    // Live stream reconnects can replay an already-synced insert for the same
    // canonical row. Normalize that case to update so observation replays
    // remain idempotent instead of tripping TanStack DB's duplicate-key path.
    // Do not consult the collection's visible state here: it includes pending
    // optimistic inserts. Treating the authoritative echo of such an insert as
    // an update leaves the canonical synced base empty, so a committed snapshot
    // silently omits the row.
    if (operation === `insert` && keys?.has(event.key)) {
      operation = `update`
    }

    // Track key existence for upsert logic
    if (operation === `insert` || operation === `update`) {
      keys?.add(event.key)
    } else {
      // Must be delete
      keys?.delete(event.key)
    }

    try {
      handler.write(value, operation, eventCursor)
    } catch (error) {
      console.error(`[StreamDB] Error in handler.write():`, error)
      console.error(`[StreamDB] Event that caused error:`, {
        type: event.type,
        key: event.key,
        operation,
      })
      throw error
    }
  }

  /**
   * Handle control events from the stream JSON items
   */
  dispatchControl(event: StateEvent): void {
    if (!isControlEvent(event)) return

    switch (event.headers.control) {
      case `reset`:
        // Truncate all collections
        for (const handler of this.handlers.values()) {
          handler.truncate()
        }
        // Clear key tracking
        for (const keys of this.existingKeys.values()) {
          keys.clear()
        }
        this.pendingHandlers.clear()
        this.isUpToDate = false
        break

      case `snapshot-start`:
      case `snapshot-end`:
        // These are hints for snapshot boundaries
        break
    }
  }

  /**
   * Commit all pending writes and handle up-to-date signal
   */
  commit(markReady: boolean, strict = false): void {
    // Commit all handlers that have pending writes
    for (const handler of this.pendingHandlers) {
      try {
        handler.commit()
      } catch (error) {
        console.error(`[StreamDB] Error in handler.commit():`, error)

        // WORKAROUND for TanStack DB groupBy bug
        // If it's the known "already exists in collection live-query" error, log and continue
        if (
          !strict &&
          error instanceof Error &&
          error.message.includes(`already exists in the collection`) &&
          error.message.includes(`live-query`)
        ) {
          console.warn(
            `[StreamDB] Known TanStack DB groupBy bug detected - continuing despite error`
          )
          console.warn(
            `[StreamDB] Queries with groupBy may show stale data until fixed`
          )
          continue // Don't throw, let other handlers commit
        }

        throw error
      }
    }
    this.pendingHandlers.clear()

    // Commit pending txids
    for (const txid of this.pendingTxids) {
      this.seenTxids.add(txid)

      // Resolve any promises waiting for this txid
      const resolvers = this.txidResolvers.get(txid)
      if (resolvers) {
        for (const { resolve, timeoutId } of resolvers) {
          clearTimeout(timeoutId)
          resolve()
        }
        this.txidResolvers.delete(txid)
      }
    }
    this.pendingTxids.clear()

    if (markReady && !this.isUpToDate) {
      this.isUpToDate = true
      // Mark all collections as ready
      for (const handler of this.handlers.values()) {
        handler.markReady()
      }
      // Resolve all preload promises
      for (const resolve of this.preloadResolvers) {
        resolve()
      }
      this.preloadResolvers = []
    }
  }

  /**
   * Wait for the stream to reach up-to-date state
   */
  waitForUpToDate(): Promise<void> {
    if (this.isUpToDate) {
      return Promise.resolve()
    }
    return new Promise((resolve, reject) => {
      this.preloadResolvers.push(resolve)
      this.preloadRejecters.push(reject)
    })
  }

  /**
   * Reject all waiting preload promises with an error
   */
  rejectAll(error: Error): void {
    for (const reject of this.preloadRejecters) {
      reject(error)
    }
    this.preloadResolvers = []
    this.preloadRejecters = []

    // Also reject all pending txid promises
    for (const resolvers of this.txidResolvers.values()) {
      for (const { reject, timeoutId } of resolvers) {
        clearTimeout(timeoutId)
        reject(error)
      }
    }
    this.txidResolvers.clear()
  }

  /**
   * Check if we've received up-to-date
   */
  get ready(): boolean {
    return this.isUpToDate
  }

  get nextSequence(): number {
    return this.seq
  }

  /**
   * Wait for a specific txid to be seen in the stream
   */
  awaitTxId(txid: string, timeout: number = 5000): Promise<void> {
    // Check if we've already seen this txid
    if (this.seenTxids.has(txid)) {
      return Promise.resolve()
    }

    return new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        // Remove this resolver from the map
        const resolvers = this.txidResolvers.get(txid)
        if (resolvers) {
          const index = resolvers.findIndex((r) => r.timeoutId === timeoutId)
          if (index !== -1) {
            resolvers.splice(index, 1)
          }
          if (resolvers.length === 0) {
            this.txidResolvers.delete(txid)
          }
        }
        reject(new Error(`Timeout waiting for txid: ${txid}`))
      }, timeout)

      // Add to resolvers map
      if (!this.txidResolvers.has(txid)) {
        this.txidResolvers.set(txid, [])
      }
      this.txidResolvers.get(txid)!.push({ resolve, reject, timeoutId })
    })
  }
}

// ============================================================================
// Sync Factory
// ============================================================================

/**
 * Create a sync config for a stream-backed collection
 */
function createStreamSyncConfig<T extends object>(
  eventType: string,
  dispatcher: EventDispatcher,
  primaryKey: string,
  immediate: boolean
): SyncConfig<T, string> {
  return {
    // Preserve TanStack's existing partial-update contract. Canonical export
    // reads syncedData, including fields retained by historical partial writes.
    sync: ({ begin, write, commit, markReady, truncate }) => {
      // Register this collection's handler with the dispatcher
      dispatcher.registerHandler(eventType, {
        // Canonical capture must apply the source batch to syncedData before
        // advancing its cut, even while a manual optimistic write persists.
        // TanStack keeps the optimistic overlay separately. Legacy readers
        // without committed-image capture retain their existing sync timing.
        begin: () => begin(immediate ? { immediate: true } : undefined),
        write: (value, type, _cursor) => {
          write({
            value: value as T,
            type,
          })
        },
        commit,
        markReady,
        truncate,
        primaryKey,
      })

      // If the dispatcher is already up-to-date, mark ready immediately
      if (dispatcher.ready) {
        markReady()
      }

      // Return cleanup function
      return () => {
        // No cleanup needed - stream lifecycle managed by StreamDB
      }
    },
  }
}

// ============================================================================
// Main Implementation
// ============================================================================

/**
 * Create a stream-backed database with TanStack DB collections
 *
 * This function is synchronous - it creates the stream handle and collections
 * but does not start the stream connection. Call `db.preload()` to connect
 * and sync initial data.
 *
 * @example
 * ```typescript
 * const stateSchema = createStateSchema({
 *   users: { schema: userSchema, type: "user", primaryKey: "id" },
 *   messages: { schema: messageSchema, type: "message", primaryKey: "id" },
 * })
 *
 * // Create a stream DB (synchronous - stream is created lazily on preload)
 * const db = createStreamDB({
 *   streamOptions: {
 *     url: "https://api.example.com/streams/my-stream",
 *     contentType: "application/json",
 *   },
 *   state: stateSchema,
 * })
 *
 * // preload() creates the stream and loads initial data
 * await db.preload()
 * const user = await db.collections.users.get("123")
 * ```
 */
export function createStreamDB<
  TDef extends StreamStateDefinition,
  TActions extends Record<string, ActionDefinition<any>> = Record<
    string,
    never
  >,
>(
  options: CreateStreamDBOptions<TDef, TActions>
): TActions extends Record<string, never>
  ? StreamDB<TDef>
  : StreamDBWithActions<TDef, TActions> {
  const {
    streamOptions,
    state,
    actions: actionsFactory,
    live = true,
    onEvent,
    onBeforeBatch,
    onBatch,
    onError,
    bootstrap,
    incarnation,
    onCommittedBatch,
  } = options

  // Reuse provided stream or create a new one
  const stream =
    options.stream ??
    (() => {
      if (!streamOptions) {
        throw new Error(`createStreamDB requires stream or streamOptions`)
      }
      return new DurableStreamClass(streamOptions)
    })()

  // Create the event dispatcher
  const dispatcher = new EventDispatcher()

  const streamIdentity = stream.url

  // Create TanStack DB collections for each definition
  const collectionInstances: Record<string, Collection<object, string>> = {}

  for (const [name, definition] of Object.entries(state)) {
    const collection: Collection<object, string> = createCollection({
      id: getStreamDBCollectionId(streamIdentity, name),
      schema: definition.schema as StandardSchemaV1<object>,
      getKey: (item: any) => String(item[definition.primaryKey]),
      sync: createStreamSyncConfig(
        definition.type,
        dispatcher,
        definition.primaryKey,
        onCommittedBatch !== undefined
      ),
      startSync: true, // Start syncing immediately
      // Disable GC - we manage lifecycle via db.close()
      // DB would otherwise clean up the collections independently of each other, we
      // cant recover one and not the others from a single log.
      gcTime: 0,
    })

    collectionInstances[name] = collection
  }

  // Stream consumer state (lazy initialization)
  let streamResponse: StreamResponse<StateEvent> | null = null
  const abortController = new AbortController()
  let consumerStarted = false
  let lastConsumedOffset = bootstrap?.offset ?? `-1`
  let hydrated = false
  let failedBatchError: Error | null = null
  let batchInProgress = false
  let lastCommittedImage: StreamDBBootstrap | null =
    onCommittedBatch && bootstrap ? structuredClone(bootstrap) : null

  const captureCommittedState = (): StreamDBBootstrap => ({
    offset: lastConsumedOffset,
    ...(streamResponse?.incarnation
      ? { incarnation: streamResponse.incarnation }
      : bootstrap?.incarnation
        ? { incarnation: bootstrap.incarnation }
        : incarnation
          ? { incarnation }
          : {}),
    nextSeq: dispatcher.nextSequence,
    rowsByCollection: Object.fromEntries(
      Object.entries(collectionInstances).map(([name, collection]) => [
        name,
        [...collection._state.syncedData.values()].map((row) =>
          structuredClone(row as Record<string, unknown>)
        ),
      ])
    ),
  })
  const exportState = (): StreamDBBootstrap => {
    if (failedBatchError) {
      throw new Error(`Cannot export state after a failed stream batch`, {
        cause: failedBatchError,
      })
    }
    if (!onCommittedBatch) {
      throw new Error(
        `exportState requires coherent capture via onCommittedBatch`
      )
    }
    if (batchInProgress) {
      throw new Error(`Cannot export state during an uncommitted stream batch`)
    }
    return structuredClone(lastCommittedImage ?? captureCommittedState())
  }
  const isAbortLikeError = (err: unknown): boolean => {
    if (abortController.signal.aborted) {
      return true
    }
    if (!(err instanceof Error)) {
      return false
    }
    return (
      err.name === `AbortError` ||
      err.name === `FetchBackoffAbortError` ||
      err.message === `Stream request was aborted`
    )
  }

  /**
   * Start the stream consumer (called lazily on first preload)
   */
  const startConsumer = async (): Promise<void> => {
    if (consumerStarted) return
    consumerStarted = true

    if (bootstrap && !hydrated) {
      const knownCollections = new Set(Object.keys(state))
      for (const name of Object.keys(bootstrap.rowsByCollection)) {
        if (!knownCollections.has(name)) {
          throw new Error(`Unknown bootstrap collection: ${name}`)
        }
      }
      if (!Number.isSafeInteger(bootstrap.nextSeq) || bootstrap.nextSeq < 0) {
        throw new Error(`Invalid bootstrap nextSeq`)
      }
      for (const [name, definition] of Object.entries(state)) {
        for (const row of bootstrap.rowsByCollection[name] ?? []) {
          const result = await definition.schema[`~standard`].validate(row)
          if (`issues` in result && result.issues) {
            throw new Error(`Invalid bootstrap row for ${name}`)
          }
        }
      }
      dispatcher.hydrate(state, bootstrap.rowsByCollection, bootstrap.nextSeq)
      hydrated = true
    }

    // Start streaming (this is where the connection actually happens)
    streamResponse = await stream.stream<StateEvent>({
      live,
      json: true,
      ...(bootstrap ? { offset: bootstrap.offset } : {}),
      ...((bootstrap?.incarnation ?? incarnation)
        ? { incarnation: bootstrap?.incarnation ?? incarnation }
        : {}),
      signal: abortController.signal,
    })
    // StreamDB consumes batches via subscribeJson(); it does not await the
    // session's closed promise. Swallow that terminal rejection so aborting
    // the live session during db.close() doesn't surface as an unhandled
    // rejection after the real error has already been routed elsewhere.
    void streamResponse.closed.catch((err) => {
      if (isAbortLikeError(err)) {
        return undefined
      }
      const error = err instanceof Error ? err : new Error(String(err))
      console.error(`[StreamDB] Stream consumer closed unexpectedly:`, error)
      dispatcher.rejectAll(error)
      onError?.(error)
      return undefined
    })
    // Process events as they come in
    streamResponse.subscribeJson((batch) => {
      batchInProgress = true
      try {
        onBeforeBatch?.(batch)

        for (const event of batch.items) {
          if (isChangeEvent(event)) {
            dispatcher.dispatchChange(event, batch.offset)
            onEvent?.(event)
          } else if (isControlEvent(event)) {
            dispatcher.dispatchControl(event)
          }
        }

        onBatch?.(batch)
        dispatcher.commit(batch.upToDate, onCommittedBatch !== undefined)
        lastConsumedOffset = batch.offset
        if (onCommittedBatch) {
          lastCommittedImage = captureCommittedState()
          // Related projections may advance metadata during dispatch. Only
          // expose export after both rows and this source cut are coherent.
          batchInProgress = false
          onCommittedBatch({
            batch,
            image: structuredClone(lastCommittedImage),
          })
        }
      } catch (error) {
        console.error(`[StreamDB] Error processing batch:`, error)
        failedBatchError =
          error instanceof Error ? error : new Error(String(error))
        dispatcher.rejectAll(failedBatchError)
        onError?.(failedBatchError)
        abortController.abort()
      } finally {
        batchInProgress = false
      }
      return Promise.resolve()
    })
  }

  // Build the StreamDB object with methods
  const dbMethods: StreamDBMethods = {
    stream,
    get offset() {
      return lastConsumedOffset
    },
    exportState,
    preload: async () => {
      await startConsumer()
      await dispatcher.waitForUpToDate()
    },
    close: () => {
      // Reject all pending operations before aborting
      dispatcher.rejectAll(new Error(`StreamDB closed`))
      abortController.abort()
    },
    utils: {
      awaitTxId: (txid: string, timeout?: number) =>
        dispatcher.awaitTxId(txid, timeout),
    },
  }

  const db = Object.create(null) as StreamDB<TDef>
  Object.defineProperty(db, `collections`, {
    value: collectionInstances,
    enumerable: true,
    configurable: false,
    writable: false,
  })
  Object.defineProperties(db, Object.getOwnPropertyDescriptors(dbMethods))

  // If actions factory is provided, wrap actions and return db with actions
  if (actionsFactory) {
    const actionDefs = actionsFactory({ db, stream })
    const wrappedActions: Record<
      string,
      ReturnType<typeof createOptimisticAction>
    > = {}
    for (const [name, def] of Object.entries(actionDefs)) {
      wrappedActions[name] = createOptimisticAction({
        onMutate: def.onMutate,
        mutationFn: def.mutationFn,
      })
    }

    Object.defineProperty(db, `actions`, {
      value: wrappedActions,
      enumerable: true,
      configurable: false,
      writable: false,
    })
    return db as any
  }

  return db as any
}
