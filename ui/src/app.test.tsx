import { cleanup, fireEvent, render, screen } from "@testing-library/preact";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { App } from "./app";
import { activeConnectionId, commandPaletteOpen, connections } from "./state/store";

beforeEach(() => {
	connections.value = [];
	activeConnectionId.value = null;
	commandPaletteOpen.value = false;
});

afterEach(() => {
	cleanup();
	commandPaletteOpen.value = false;
});

describe("App global command-palette hotkey", () => {
	it("opens the palette on Cmd/Ctrl-K from anywhere (no input focused)", () => {
		render(<App />);
		expect(screen.queryByRole("dialog", { name: "Command palette" })).toBeNull();
		fireEvent.keyDown(document.body, { key: "k", ctrlKey: true });
		expect(screen.getByRole("dialog", { name: "Command palette" })).toBeTruthy();
	});

	it("ignores the hotkey while typing in an input, textarea, or select", () => {
		render(<App />);
		for (const tag of ["input", "textarea", "select"] as const) {
			const field = document.createElement(tag);
			document.body.appendChild(field);
			field.focus();
			fireEvent.keyDown(field, { key: "k", metaKey: true });
			expect(screen.queryByRole("dialog", { name: "Command palette" })).toBeNull();
			field.remove();
		}
	});

	it("does not open on a bare K without a modifier", () => {
		render(<App />);
		fireEvent.keyDown(document.body, { key: "k" });
		expect(screen.queryByRole("dialog", { name: "Command palette" })).toBeNull();
	});

	it("closes the palette on Escape (via the Modal shell)", () => {
		render(<App />);
		fireEvent.keyDown(document.body, { key: "k", ctrlKey: true });
		expect(screen.getByRole("dialog", { name: "Command palette" })).toBeTruthy();
		fireEvent.keyDown(document.body, { key: "Escape" });
		expect(screen.queryByRole("dialog", { name: "Command palette" })).toBeNull();
	});

	it("restores focus to the pre-open trigger after the palette closes", () => {
		render(<App />);
		const trigger = document.createElement("button");
		document.body.appendChild(trigger);
		trigger.focus();
		expect(document.activeElement).toBe(trigger);

		fireEvent.keyDown(document.body, { key: "k", ctrlKey: true });
		expect(screen.getByRole("combobox", { name: /search commands/i })).toBe(document.activeElement);

		fireEvent.keyDown(document.body, { key: "Escape" });
		expect(screen.queryByRole("dialog", { name: "Command palette" })).toBeNull();
		expect(document.activeElement).toBe(trigger);
		trigger.remove();
	});
});
