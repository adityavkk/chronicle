// Real HTTP/Redis validation against the extracted Chronicle source archive.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
const requiredEnv = (name: string): string => {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
};
const streamRoot = new URL(requiredEnv(`CHRONICLE_STREAM_ROOT`));
if (![`127.0.0.1`, `localhost`, `::1`].includes(streamRoot.hostname)) {
  throw new Error(
    `Refusing to run destructive E2E against a non-loopback host`,
  );
}
if (!streamRoot.pathname.endsWith(`/`)) {
  throw new Error(`CHRONICLE_STREAM_ROOT must end with /`);
}
const base = new URL(`e2e/snapshot-${randomUUID()}`, streamRoot).toString();

const durableRoot = requiredEnv(`DURABLE_STREAMS_CHECKOUT`);
const electricRoot = requiredEnv(`ELECTRIC_CHECKOUT`);
const { DurableStream, DurableStreamError } = await import(
  `${durableRoot}/packages/client/src/index.ts`
);
const {
  createEntityStreamDB,
  decodeEntityProjectionImage,
  encodeEntityProjectionImage,
} = await import(
  `${electricRoot}/packages/agents-runtime/src/entity-stream-db.ts`
);

const tokenDir = requiredEnv(`CHRONICLE_TOKEN_DIR`);
const projection = `entity-e2e-v1`;
const mediaType = `application/vnd.electric.entity-image+json`;
const token = async (name: string) =>
  (await readFile(`${tokenDir}/.${name}-token`, `utf8`)).trim();
const bearer = (value: string) => ({ Authorization: `Bearer ${value}` });
const writerHeaders = bearer(await token(`writer`));
const publisherHeaders = bearer(await token(`publisher`));
const browserHeaders = bearer(await token(`browser`));

const writer = new DurableStream({
  url: base,
  headers: writerHeaders,
  contentType: `application/json`,
  batching: false,
});

const event = (
  operation: `insert` | `update` | `delete`,
  key: string,
  value?: Record<string, unknown>,
) => ({
  type: `state:notes`,
  key,
  ...(value ? { value } : {}),
  headers: { operation },
});

let sourceExists = false;
let result: Record<string, unknown> | undefined;
try {
  await writer.create();
  sourceExists = true;
  await writer.append(
    JSON.stringify(event(`insert`, `a`, { body: `first`, removed: true })),
  );
  await writer.append(
    JSON.stringify(event(`insert`, `b`, { body: `delete-me` })),
  );
  await writer.append(
    JSON.stringify(event(`update`, `a`, { body: `replacement` })),
  );
  await writer.append(JSON.stringify(event(`delete`, `b`)));

  let fullImage: ReturnType<
    ReturnType<typeof createEntityStreamDB>[`utils`][`exportSnapshot`]
  >;
  let fullIncarnation: string | undefined;
  const replay = createEntityStreamDB(base, { notes: {} } as any, undefined, {
    streamOptions: { headers: writerHeaders },
    projectionVersion: projection,
    onCommittedSnapshot: (image, incarnation) => {
      fullImage = image;
      fullIncarnation = incarnation;
    },
  });
  await replay.preload();
  assert.ok(
    fullIncarnation,
    `full replay reader must establish an incarnation`,
  );
  assert.equal(fullImage!.nextSeq, 4);
  assert.equal(fullImage!.rowsByCollection.notes.length, 1);
  assert.equal(fullImage!.rowsByCollection.notes[0]!.body, `replacement`);
  assert.ok(!(`removed` in fullImage!.rowsByCollection.notes[0]!));
  const fullCut = fullImage!.offset;
  replay.close();

  const snapshots = new DurableStream({
    url: base,
    headers: writerHeaders,
    contentType: `application/json`,
  });
  const before = await snapshots.loadSnapshot(projection);
  assert.equal(before, null);
  const encoded = encodeEntityProjectionImage(fullImage!);
  const published = await snapshots.publishSnapshot(projection, {
    body: encoded,
    contentType: mediaType,
    offset: fullCut,
    incarnation: fullIncarnation!,
    etag: null,
    headers: publisherHeaders,
  });
  assert.equal(published.created, true);

  const loaded = await snapshots.loadSnapshot(projection);
  assert.ok(loaded);
  assert.equal(loaded.offset, fullCut);
  assert.equal(loaded.incarnation, fullIncarnation);
  const decoded = decodeEntityProjectionImage(loaded.body, projection);

  await writer.append(JSON.stringify(event(`insert`, `c`, { body: `suffix` })));
  let hydratedImage: typeof fullImage;
  const hydrated = createEntityStreamDB(base, { notes: {} } as any, undefined, {
    streamOptions: { headers: writerHeaders },
    snapshot: { image: decoded, incarnation: loaded.incarnation },
    projectionVersion: projection,
    onCommittedSnapshot: (image, incarnation) => {
      hydratedImage = image;
      assert.equal(incarnation, loaded.incarnation);
    },
  });
  await hydrated.preload();
  assert.equal(hydrated.collections.notes.get(`a`)?.body, `replacement`);
  assert.equal(hydrated.collections.notes.get(`c`)?.body, `suffix`);
  assert.equal((hydrated.collections.notes.get(`c`) as any)._seq, 4);
  assert.equal(hydratedImage!.nextSeq, 5);
  assert.equal(hydratedImage!.rowsByCollection.notes.length, 2);
  hydrated.close();

  let stalePublishStatus: number | undefined;
  try {
    await snapshots.publishSnapshot(projection, {
      body: encodeEntityProjectionImage(hydratedImage!),
      contentType: mediaType,
      offset: hydratedImage!.offset,
      incarnation: loaded.incarnation,
      etag: `"stale-etag"`,
      headers: publisherHeaders,
    });
  } catch (error) {
    assert.ok(error instanceof DurableStreamError);
    stalePublishStatus = error.status;
  }
  assert.equal(stalePublishStatus, 412);

  const retire = async (headers: HeadersInit, includeMatch = true) =>
    fetch(`${base}?snapshot=${encodeURIComponent(projection)}`, {
      method: `DELETE`,
      headers: {
        ...Object.fromEntries(new Headers(headers)),
        "Stream-Snapshot": `v1`,
        "If-Stream-Incarnation": loaded.incarnation,
        ...(includeMatch ? { "If-Match": loaded.etag } : {}),
      },
    });

  assert.equal((await retire(writerHeaders)).status, 403);
  assert.equal((await retire(browserHeaders)).status, 403);
  assert.equal((await retire(publisherHeaders, false)).status, 428);
  assert.equal(
    (
      await fetch(`${base}?snapshot=${encodeURIComponent(projection)}`, {
        method: `DELETE`,
        headers: {
          ...publisherHeaders,
          "Stream-Snapshot": `v1`,
          "If-Stream-Incarnation": loaded.incarnation,
          "If-Match": `"stale-etag"`,
        },
      })
    ).status,
    412,
  );
  assert.equal((await retire(publisherHeaders)).status, 204);
  assert.equal((await retire(publisherHeaders)).status, 412);
  assert.equal(await snapshots.loadSnapshot(projection), null);

  let oversizedStatus: number | undefined;
  try {
    await snapshots.publishSnapshot(`${projection}-oversized`, {
      body: new Uint8Array(1024 * 1024 + 1),
      contentType: mediaType,
      offset: fullCut,
      incarnation: fullIncarnation!,
      etag: null,
      headers: publisherHeaders,
    });
  } catch (error) {
    oversizedStatus = (error as { status?: number }).status;
  }
  assert.equal(oversizedStatus, 413);

  const rawHead = await fetch(base, { method: `HEAD`, headers: writerHeaders });
  assert.equal(rawHead.headers.get(`Stream-Snapshot-Max-Versions`), `8`);
  assert.equal(
    rawHead.headers.get(`Stream-Snapshot-Max-Total-Bytes`),
    `${4 * 1024 * 1024}`,
  );

  await writer.delete();
  sourceExists = false;
  await writer.create();
  sourceExists = true;
  await writer.append(
    JSON.stringify(event(`insert`, `new`, { body: `new-incarnation` })),
  );

  let recreationStatus: number | undefined;
  const staleHydration = createEntityStreamDB(
    base,
    { notes: {} } as any,
    undefined,
    {
      streamOptions: { headers: writerHeaders },
      snapshot: { image: decoded, incarnation: loaded.incarnation },
      projectionVersion: projection,
    },
  );
  try {
    await staleHydration.preload();
  } catch (error) {
    recreationStatus = (error as { status?: number }).status;
  } finally {
    staleHydration.close();
  }
  assert.equal(recreationStatus, 412);

  const missingAfterRecreate = await snapshots.loadSnapshot(projection);
  assert.equal(missingAfterRecreate, null);

  const head = await writer.head();
  assert.equal(head.exists, true);
  assert.equal(head.snapshotVersion, `v1`);
  assert.notEqual(head.incarnation, loaded.incarnation);
  assert.equal(head.snapshotMaxBytes, 1024 * 1024);

  result = {
    fullCut,
    snapshotBytes: encoded.byteLength,
    suffixRows: hydratedImage!.rowsByCollection.notes.length,
    nextSeq: hydratedImage!.nextSeq,
    stalePublishStatus,
    writerRetireStatus: 403,
    browserRetireStatus: 403,
    missingIfMatchStatus: 428,
    staleRetireStatus: 412,
    successfulRetireStatus: 204,
    missingImageRetireStatus: 412,
    oversizedStatus,
    snapshotMaxVersions: 8,
    snapshotMaxTotalBytes: 4 * 1024 * 1024,
    recreationStatus,
    incarnationChanged: head.incarnation !== loaded.incarnation,
    snapshotMaxBytes: head.snapshotMaxBytes,
  };
} finally {
  if (sourceExists) {
    try {
      await writer.delete();
    } catch (error) {
      if (!(error instanceof DurableStreamError) || error.status !== 404) {
        throw error;
      }
    }
  }
}
assert.ok(result);
console.log(JSON.stringify({ ...result, cleanedUp: true }));
