/**
 * menuKeyboard — shared keyboard wiring for the role="menu" popovers
 * (ConnectionManager's connection switcher and StreamActionsMenu).
 *
 * It applies the roving-tabindex idiom (see lib/roving) to ARIA menus, the same
 * way Inspector (tabs) and Navigator (tree) do for their lists:
 *  - exactly one menuitem is the tab stop (tabIndex 0), the rest are -1;
 *  - ArrowUp/ArrowDown move focus between items (wrapping), Home/End jump;
 *  - Tab/Shift+Tab are trapped so focus cannot leak out of the open menu.
 * Escape (close + restore focus to the trigger) stays on the document keydown
 * listener each menu already installs.
 *
 * These are DOM helpers (focus is inherently imperative), so they live here in
 * the component layer rather than in lib/; the pure index math they lean on is
 * in lib/roving and is unit-tested there.
 */

import { nextRovingIndex, wrapIndex } from "../lib/roving";

/** Enabled menuitems — the roving ring for Arrow/Home/End. */
const MENUITEM_SELECTOR =
	'[role="menuitem"]:not([disabled]),[role="menuitemradio"]:not([disabled])';
/** Every menuitem (incl. disabled) — for assigning the roving tabIndex. */
const ALL_MENUITEM_SELECTOR = '[role="menuitem"],[role="menuitemradio"]';
/** Everything focusable inside the popover — the Tab trap cycles this set. */
const FOCUSABLE_SELECTOR =
	'a[href],button:not([disabled]),summary,input:not([disabled]),select:not([disabled]),textarea:not([disabled]),[tabindex]:not([tabindex="-1"])';

function menuItems(pop: HTMLElement): HTMLElement[] {
	return Array.from(pop.querySelectorAll<HTMLElement>(MENUITEM_SELECTOR));
}

/** Make `target` the sole tab stop among the menu's items, then focus it. */
export function focusMenuItem(pop: HTMLElement, target: HTMLElement): void {
	pop.querySelectorAll<HTMLElement>(ALL_MENUITEM_SELECTOR).forEach((el) => {
		el.tabIndex = el === target ? 0 : -1;
	});
	target.focus();
}

/** On open: move focus to the first enabled menuitem, if there is one. */
export function focusFirstMenuItem(pop: HTMLElement | null): void {
	if (pop === null) return;
	const first = pop.querySelector<HTMLElement>(MENUITEM_SELECTOR);
	if (first !== null) focusMenuItem(pop, first);
}

/**
 * Handle a keydown on the popover. Arrow/Home/End rove between menuitems; Tab is
 * trapped to cycle every focusable element inside the menu so focus never
 * escapes. Returns true when the key was handled (and default prevented).
 */
export function handleMenuKeydown(e: KeyboardEvent, pop: HTMLElement | null): boolean {
	if (pop === null) return false;

	if (e.key === "Tab") {
		const focusable = Array.from(pop.querySelectorAll<HTMLElement>(FOCUSABLE_SELECTOR));
		if (focusable.length === 0) return false;
		e.preventDefault();
		const at = focusable.indexOf(document.activeElement as HTMLElement);
		const next = wrapIndex((at < 0 ? 0 : at) + (e.shiftKey ? -1 : 1), focusable.length);
		focusable[next]?.focus();
		return true;
	}

	const list = menuItems(pop);
	const current = list.indexOf(document.activeElement as HTMLElement);
	const target = nextRovingIndex(e.key, current, list.length);
	if (target === null) return false;
	e.preventDefault();
	const el = list[target];
	if (el !== undefined) focusMenuItem(pop, el);
	return true;
}
