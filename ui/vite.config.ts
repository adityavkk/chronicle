import preact from "@preact/preset-vite";
import { defineConfig } from "vitest/config";

// The production build is emitted into cmd/dsui/embedded so the Go binary can
// embed it with //go:embed and serve it as a single self-contained executable.
// Keep outDir + emptyOutDir in sync with cmd/dsui/main.go (fs.Sub "embedded").
export default defineConfig({
	plugins: [preact()],
	server: { port: 3001 },
	build: {
		outDir: "../cmd/dsui/embedded",
		emptyOutDir: true,
	},
	test: {
		environment: "jsdom",
		globals: true,
		setupFiles: ["./vitest.setup.ts"],
		css: true,
	},
});
