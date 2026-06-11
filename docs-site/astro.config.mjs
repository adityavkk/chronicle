// @ts-check
import { defineConfig } from "astro/config";
import mdx from "@astrojs/mdx";
import expressiveCode from "astro-expressive-code";
import { pluginLineNumbers } from "@expressive-code/plugin-line-numbers";

// GitHub Pages project site: https://adityavkk.github.io/chronicle/
// Override with SITE / BASE env vars if hosting elsewhere.
const SITE = process.env.SITE ?? "https://adityavkk.github.io";
const BASE = process.env.BASE ?? "/chronicle";

export default defineConfig({
  site: SITE,
  base: BASE,
  trailingSlash: "ignore",
  integrations: [
    // expressive-code must come BEFORE mdx() so it can wire its renderer
    // into MDX's markdown pipeline. Provides Shiki highlighting + editor
    // chrome (copy button, file tabs, line highlights, diff markers).
    expressiveCode({
      themes: ["min-light"],
      plugins: [pluginLineNumbers()],
      styleOverrides: {
        borderRadius: "8px",
        borderColor: "var(--rule)",
        codeFontFamily: "var(--mono)",
        codeFontSize: "13.5px",
        codeLineHeight: "1.45",
        codePaddingBlock: "16px",
        codePaddingInline: "20px",
        frames: {
          shadowColor: "rgba(31, 29, 26, 0.08)",
          editorTabBarBackground: "var(--paper-warm)",
          editorActiveTabBackground: "var(--paper)",
          editorActiveTabForeground: "var(--ink)",
          editorTabBorderRadius: "6px 6px 0 0",
          terminalBackground: "var(--paper-warm)",
          terminalTitlebarBackground: "var(--paper-warm)",
        },
      },
      defaultProps: {
        wrap: false,
        showLineNumbers: false,
      },
    }),
    mdx(),
  ],
  output: "static",
  build: { format: "directory", assets: "_assets" },
  vite: {
    css: {
      preprocessorOptions: {
        scss: {
          api: "modern-compiler",
        },
      },
    },
  },
});
