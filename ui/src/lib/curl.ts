/**
 * Pure curl reproduction for an {@link Operation} descriptor.
 *
 * Every write/tail/read the UI performs is first described as an
 * {@link Operation} ({method, url, headers, body?}). This module turns that
 * descriptor into the exact equivalent `curl` command, so a control can show
 * "the same thing on the command line" right next to the button — before or
 * after the request runs. It is deliberately dependency-free and side-effect
 * free, which makes it trivially unit-testable.
 *
 * (Note: lib/protocol.ts also has a `toCurl`, but that one reproduces a
 * *completed* {@link HttpExchange}. This one works from the *intended*
 * Operation, which is what the write actions and the Playground build. Import
 * whichever matches what you have in hand.)
 *
 * No DOM, no store, no I/O.
 */

import type { Operation } from "./types";

/**
 * Reproduce an {@link Operation} as a copy-pastable, single-line curl command.
 *
 *  - HEAD uses `-I`; any non-GET method adds `-X METHOD`.
 *  - Each header becomes a `-H 'Name: value'` flag, in insertion order.
 *  - A string body is emitted as `--data-raw '<body>'` (no implicit @file or
 *    URL-encoding, so the bytes match exactly what the UI sends).
 *  - A binary (Uint8Array) body cannot be inlined safely on one line, so it is
 *    emitted as `--data-binary @-` with a leading note that the caller pipes
 *    the bytes in — honest rather than corrupting the payload.
 *  - The URL is single-quoted last so a query string with `&` survives a paste.
 */
export function toCurl(op: Operation): string {
	const parts: string[] = ["curl"];

	const method = op.method.toUpperCase();
	if (method === "HEAD") {
		parts.push("-I");
	} else if (method !== "GET") {
		parts.push("-X", method);
	}

	for (const [name, value] of Object.entries(op.headers)) {
		parts.push("-H", shellQuote(`${name}: ${value}`));
	}

	if (op.body !== undefined) {
		if (typeof op.body === "string") {
			parts.push("--data-raw", shellQuote(op.body));
		} else {
			// Binary payload: bytes can't round-trip inside a shell-quoted string,
			// so read from stdin and tell the reader to pipe the bytes in.
			parts.push("--data-binary", "@-", `# ${op.body.byteLength} bytes piped on stdin`);
		}
	}

	parts.push(shellQuote(op.url));
	return parts.join(" ");
}

/** Single-quote a string for a POSIX shell, escaping embedded single quotes. */
function shellQuote(s: string): string {
	return `'${s.replace(/'/g, "'\\''")}'`;
}
