import "@testing-library/preact";

// jsdom does not implement matchMedia; the theme system queries it. Provide a
// minimal, typed stub so component tests can mount without crashing.
if (typeof window !== "undefined" && typeof window.matchMedia !== "function") {
	window.matchMedia = (query: string): MediaQueryList =>
		({
			matches: false,
			media: query,
			onchange: null,
			addEventListener: () => {},
			removeEventListener: () => {},
			addListener: () => {},
			removeListener: () => {},
			dispatchEvent: () => false,
		}) as unknown as MediaQueryList;
}

// jsdom does not implement scrollIntoView; the command palette scrolls its
// active row into view as the selection moves. Provide a no-op so component
// tests can mount and navigate without crashing.
if (typeof Element !== "undefined" && typeof Element.prototype.scrollIntoView !== "function") {
	Element.prototype.scrollIntoView = (): void => {};
}
