# docs-site

The Chronicle documentation site, published to
[adityavkk.github.io/chronicle](https://adityavkk.github.io/chronicle/).

An Astro 5 static site, isolated from the Go module: its own `package.json`,
`bun.lock`, and build. Nothing in here is imported by the server.

```bash
bun install
bun run dev        # local dev server
bun run build      # static build into dist/
```

Deployment is `.github/workflows/deploy-docs.yml`: every push to `main` that
touches `docs-site/**` builds the site with `withastro/action` and deploys the
artifact to GitHub Pages (Pages source is "GitHub Actions"; no `gh-pages`
branch). `astro.config.mjs` sets `site`/`base` for the project-page URL —
override with `SITE`/`BASE` env vars to host elsewhere.

Layout:

```
src/pages/index.astro    the one route; wraps _overview.mdx in the Doc layout
src/pages/_overview.mdx  the overview content (underscore = not routed)
src/layouts/Doc.astro    page chrome: head, masthead, hero, footer
src/components/          Hero, SectionHead, Callout, Tldr, Table, Glyph, …
src/styles/tokens.scss   the design tokens (palette, type, layout vars)
src/styles/base.scss     global styles: shell, prose rail, tldr, print
```
