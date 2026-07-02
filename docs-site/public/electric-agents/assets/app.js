/* Electric Agents Explainer — shared behavior: theme, scroll-spy, mermaid */
(function () {
  // ---------- Theme ----------
  const stored = localStorage.getItem('ea-theme');
  const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
  const theme = stored || (prefersDark ? 'dark' : 'light');
  document.documentElement.setAttribute('data-theme', theme);

  function mermaidVars() {
    const s = getComputedStyle(document.documentElement);
    const v = (name) => s.getPropertyValue(name).trim();
    return {
      background: v('--surface'),
      primaryColor: v('--node-a'),
      primaryTextColor: v('--text'),
      primaryBorderColor: v('--accent'),
      secondaryColor: v('--node-b'),
      tertiaryColor: v('--node-c'),
      lineColor: v('--text-dim'),
      textColor: v('--text'),
      fontFamily: v('--sans') || 'Inter, sans-serif',
      fontSize: '14.5px',
      clusterBkg: v('--surface-2'),
      clusterBorder: v('--border'),
      edgeLabelBackground: v('--surface'),
      actorBkg: v('--node-a'),
      actorBorder: v('--accent'),
      actorTextColor: v('--text'),
      signalColor: v('--text-dim'),
      signalTextColor: v('--text'),
      labelBoxBkgColor: v('--surface-2'),
      labelTextColor: v('--text'),
      noteBkgColor: v('--node-b'),
      noteTextColor: v('--text'),
      noteBorderColor: v('--border'),
      activationBkgColor: v('--surface-2'),
      activationBorderColor: v('--accent'),
      sequenceNumberColor: v('--surface'),
    };
  }

  let mermaidSources = null;

  function renderMermaid() {
    if (!window.mermaid) return;
    const nodes = document.querySelectorAll('.mermaid');
    if (!nodes.length) return;
    if (mermaidSources === null) {
      mermaidSources = [];
      nodes.forEach((n) => mermaidSources.push(n.textContent));
    }
    // Mermaid's style/classDef parser can't handle var(--x); resolve to
    // concrete values for the active theme before each render.
    const rootStyle = getComputedStyle(document.documentElement);
    const resolveVars = (src) =>
      src.replace(/var\((--[a-z0-9-]+)\)/gi, (_, name) => rootStyle.getPropertyValue(name).trim() || '#888');
    nodes.forEach((n, i) => {
      n.removeAttribute('data-processed');
      n.textContent = resolveVars(mermaidSources[i]);
    });
    window.mermaid.initialize({
      startOnLoad: false,
      theme: 'base',
      securityLevel: 'loose',
      themeVariables: mermaidVars(),
      flowchart: { curve: 'basis', htmlLabels: true, padding: 10 },
      sequence: { mirrorActors: false, boxMargin: 8 },
    });
    window.mermaid.run({ nodes: document.querySelectorAll('.mermaid') });
  }

  window.addEventListener('DOMContentLoaded', () => {
    // theme toggle button
    const btn = document.querySelector('.theme-toggle');
    if (btn) {
      const setLabel = () => {
        const t = document.documentElement.getAttribute('data-theme');
        btn.textContent = t === 'dark' ? '☀ light' : '☾ dark';
      };
      setLabel();
      btn.addEventListener('click', () => {
        const cur = document.documentElement.getAttribute('data-theme');
        const next = cur === 'dark' ? 'light' : 'dark';
        document.documentElement.setAttribute('data-theme', next);
        localStorage.setItem('ea-theme', next);
        setLabel();
        renderMermaid();
      });
    }

    renderMermaid();

    // ---------- Scroll spy ----------
    const tocLinks = Array.from(document.querySelectorAll('nav.toc a[href^="#"]'));
    if (tocLinks.length) {
      const sections = tocLinks
        .map((a) => document.getElementById(a.getAttribute('href').slice(1)))
        .filter(Boolean);
      const byId = {};
      tocLinks.forEach((a) => (byId[a.getAttribute('href').slice(1)] = a));
      const activate = (id) => {
        tocLinks.forEach((a) => a.classList.remove('active'));
        const link = byId[id];
        if (link) {
          link.classList.add('active');
          const toc = link.closest('nav.toc');
          if (toc && getComputedStyle(toc).display === 'flex') {
            link.scrollIntoView({ block: 'nearest', inline: 'center', behavior: 'smooth' });
          }
        }
      };
      const obs = new IntersectionObserver(
        (entries) => {
          entries.forEach((e) => {
            if (e.isIntersecting) activate(e.target.id);
          });
        },
        { rootMargin: '-10% 0px -80% 0px' }
      );
      sections.forEach((s) => obs.observe(s));
    }
  });
})();
