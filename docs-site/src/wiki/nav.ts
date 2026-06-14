// Single source of truth for the Code Wiki's left-hand navigation. Each page
// is one route under /wiki; its `sections` are the in-page anchors the sidebar
// expands (and the scroll-spy tracks) when that page is active. Keep the `id`s
// in lockstep with the <section class="block" id="…"> ids in the MDX.

export interface WikiSection {
  /** Matches the id on the page's <section class="block">. */
  id: string;
  label: string;
}

export interface WikiPage {
  /** Route slug under /wiki ("" is the index). */
  slug: string;
  title: string;
  /** One-line subtitle shown under the page title in the rail. */
  blurb: string;
  sections: WikiSection[];
}

export const WIKI_NAV: WikiPage[] = [
  {
    slug: "",
    title: "Overview",
    blurb: "Architecture & module map",
    sections: [
      { id: "what", label: "What Chronicle is" },
      { id: "architecture", label: "System architecture" },
      { id: "shell-core", label: "Shell & core" },
      { id: "modules-map", label: "The module map" },
      { id: "next", label: "How to read this" },
    ],
  },
  {
    slug: "modules",
    title: "Modules",
    blurb: "Package by package",
    sections: [
      { id: "shell", label: "The imperative shell" },
      { id: "protocol", label: "The pure core" },
      { id: "store", label: "The storage contract" },
      { id: "redis", label: "The Redis backend" },
      { id: "subs", label: "Subscriptions & webhooks" },
    ],
  },
  {
    slug: "tracers",
    title: "Tracer bullets",
    blurb: "Requests, end to end",
    sections: [
      { id: "append", label: "A write lands" },
      { id: "read", label: "A reader tails" },
      { id: "wake", label: "A subscription wakes" },
    ],
  },
];
