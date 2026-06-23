import { cleanup, fireEvent, render, screen } from "@testing-library/preact";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { GridRow } from "../lib/types";
import { ExportMenu } from "./ExportMenu";

const ROWS: readonly GridRow[] = [
	{ index: 0, byteSize: 7, preview: "{a:1}", kind: "json", value: { a: 1 } },
	{ index: 1, byteSize: 7, preview: "{b:2}", kind: "json", value: { b: 2 } },
];

const realCreate = (globalThis.URL as { createObjectURL?: unknown }).createObjectURL;
const realRevoke = (globalThis.URL as { revokeObjectURL?: unknown }).revokeObjectURL;

afterEach(() => {
	cleanup();
	vi.restoreAllMocks();
	(globalThis.URL as { createObjectURL?: unknown }).createObjectURL = realCreate;
	(globalThis.URL as { revokeObjectURL?: unknown }).revokeObjectURL = realRevoke;
});

/** Stub the object-URL API + capture the <a download> the click targets. */
function stubDownload(): { create: ReturnType<typeof vi.fn>; downloads: { name: string }[] } {
	const create = vi.fn(() => "blob:test");
	(globalThis.URL as { createObjectURL?: unknown }).createObjectURL = create;
	(globalThis.URL as { revokeObjectURL?: unknown }).revokeObjectURL = vi.fn();
	const downloads: { name: string }[] = [];
	vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(function (
		this: HTMLAnchorElement,
	) {
		downloads.push({ name: this.download });
	});
	return { create, downloads };
}

describe("ExportMenu", () => {
	it("opens a menu offering NDJSON, CSV, and (with rawBytes) Save raw body", () => {
		render(
			<ExportMenu
				rows={ROWS}
				kind="json"
				streamPath="orders/created"
				offset="-1"
				rawBytes={new Uint8Array([1, 2, 3])}
			/>,
		);
		fireEvent.click(screen.getByRole("button", { name: "Export the loaded rows" }));
		expect(screen.getByRole("menuitem", { name: /NDJSON/ })).toBeTruthy();
		expect(screen.getByRole("menuitem", { name: /CSV/ })).toBeTruthy();
		expect(screen.getByRole("menuitem", { name: /Save raw body/ })).toBeTruthy();
	});

	it("omits Save raw body when no rawBytes are provided (the tail buffer)", () => {
		render(<ExportMenu rows={ROWS} kind="json" streamPath="s" offset="tail-now" />);
		fireEvent.click(screen.getByRole("button", { name: "Export the loaded rows" }));
		expect(screen.queryByRole("menuitem", { name: /Save raw body/ })).toBeNull();
	});

	it("downloads NDJSON named from the stream path + offset", () => {
		const { create, downloads } = stubDownload();
		render(<ExportMenu rows={ROWS} kind="json" streamPath="orders/created" offset="-1" />);
		fireEvent.click(screen.getByRole("button", { name: "Export the loaded rows" }));
		fireEvent.click(screen.getByRole("menuitem", { name: /NDJSON/ }));
		expect(create).toHaveBeenCalledTimes(1);
		expect(downloads).toHaveLength(1);
		expect(downloads[0]?.name).toBe("orders-created@-1.ndjson");
	});

	it("uses a kind-appropriate extension for Save raw body", () => {
		const { downloads } = stubDownload();
		render(
			<ExportMenu
				rows={ROWS}
				kind="binary"
				streamPath="blob"
				offset="now"
				rawBytes={new Uint8Array([9, 9])}
			/>,
		);
		fireEvent.click(screen.getByRole("button", { name: "Export the loaded rows" }));
		fireEvent.click(screen.getByRole("menuitem", { name: /Save raw body/ }));
		expect(downloads[0]?.name).toBe("blob@now.bin");
	});

	it("disables the trigger when there is nothing to export", () => {
		render(<ExportMenu rows={[]} kind="json" streamPath="s" offset="-1" />);
		const trigger = screen.getByRole("button", { name: "Export the loaded rows" });
		expect((trigger as HTMLButtonElement).disabled).toBe(true);
	});

	it("closes itself if it becomes disabled while open (the tail buffer is cleared)", () => {
		const { rerender } = render(
			<ExportMenu rows={ROWS} kind="json" streamPath="s" offset="tail-now" />,
		);
		fireEvent.click(screen.getByRole("button", { name: "Export the loaded rows" }));
		expect(screen.getByRole("menu")).toBeTruthy();
		// The live buffer empties (e.g. Clear) while the popover is open.
		rerender(<ExportMenu rows={[]} kind="json" streamPath="s" offset="tail-now" />);
		expect(screen.queryByRole("menu")).toBeNull();
	});
});
