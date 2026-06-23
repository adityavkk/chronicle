package redis

import (
	"context"
	"strconv"
	"testing"
	"time"

	"pgregory.net/rapid"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// config_match_differential_test.go is the live-Lua leg of ConfigMatches
// idempotent-create soundness (INV-CFG-01). The pure-Go decision is pinned in
// store/config_match_property_test.go; here we cross-check the Go
// StreamMetadata.ConfigMatches verdict against the live create.lua config_matches
// reply (MATCHED / MISMATCH) over the same kind of generated (metadata, options)
// pairs, with explicit coverage of the ForkOffsetRequested-vs-ForkOffset fallback
// (incl. the pre-PR nil-requested path) and the nil-vs-0 ForkSubOffset
// equivalence.
//
// Strategy: seed an existing stream's meta HASH DIRECTLY via the production
// metaToFields encoding (so the stored forkOff / forkOffReq / forkSubOff /
// ct / ttl / expAtNs / closed / forkedFrom are under full control, including the
// JSON-sub-offset advanced-offset and pre-PR-nil-requested shapes), then call the
// REAL createScript with probe ARGVs built exactly as the production createArgs
// builds them. Because the stream already exists and is neither expired nor
// soft-deleted, create.lua reaches config_matches and returns MATCHED or
// MISMATCH — which we compare to the Go oracle ConfigMatches(seeded, opts).
// Skipped under -short / when Redis is unreachable (newTestStore handles both).

// configDiffCase mirrors the pure-Go configCase: the options O and the stored
// metadata M that should be recognized (or not) as an idempotent re-create.
type configDiffCase struct {
	meta store.StreamMetadata
	opts store.CreateOptions
}

// metaFromOptsRedis builds the stored StreamMetadata that a faithful create from
// O would persist, mirroring store.metaFromOpts (kept local so the redis package
// has no test dependency on the store package's test helpers). advanceForkOff
// models a JSON sub-offset fork whose internal ForkOffset is pushed past the
// user value; recordRequested toggles whether ForkOffsetRequested was persisted
// (post-PR) or left nil (pre-PR fallback path).
func metaFromOptsRedis(o store.CreateOptions, advanceForkOff, recordRequested bool) store.StreamMetadata {
	now := time.Now()
	m := store.StreamMetadata{
		Path:           "seeded",
		ContentType:    o.ContentType,
		CurrentOffset:  store.ZeroOffset,
		CreatedAt:      now,
		LastAccessedAt: now,
		Closed:         o.Closed,
		ForkedFrom:     o.ForkedFrom,
		Producers:      map[string]*store.ProducerState{},
	}
	if o.TTLSeconds != nil {
		v := *o.TTLSeconds
		m.TTLSeconds = &v
	}
	if o.ExpiresAt != nil {
		tm := *o.ExpiresAt
		m.ExpiresAt = &tm
	}
	if o.ForkedFrom != "" {
		var requested store.Offset
		if o.ForkOffset != nil {
			requested = *o.ForkOffset
		}
		advanced := advanceForkOff && o.ForkSubOffset != nil && *o.ForkSubOffset > 0
		internal := requested
		if advanced {
			internal = requested.Add(*o.ForkSubOffset * 7)
		}
		m.ForkOffset = internal
		m.CurrentOffset = internal
		// Mirror the real create path: ForkOffsetRequested is recorded whenever a
		// fork offset was supplied; the pre-PR nil-requested fallback is only a
		// reachable state when the internal offset was NOT advanced.
		if o.ForkOffset != nil && (recordRequested || advanced) {
			req := requested
			m.ForkOffsetRequested = &req
		}
		if o.ForkSubOffset != nil {
			m.ForkSubOffset = *o.ForkSubOffset
		}
	}
	// The content-type stored by a real create is never empty (it defaults to
	// octet-stream, or inherits the source for forks). Mirror the non-fork
	// default so the seeded HASH is realistic; ConfigMatches normalizes both
	// sides anyway, so this does not weaken the agreement.
	if m.ContentType == "" {
		m.ContentType = "application/octet-stream"
	}
	return m
}

// configDiffOptsGen draws options across the full comparison surface, biased so
// forks (and their fork-offset / sub-offset shapes) are well represented.
func configDiffOptsGen() *rapid.Generator[store.CreateOptions] {
	return rapid.Custom(func(t *rapid.T) store.CreateOptions {
		o := store.CreateOptions{
			ContentType: rapid.SampledFrom([]string{
				"application/json", "Application/JSON", "text/plain",
				"text/plain; charset=utf-8", "image/png", "application/octet-stream",
			}).Draw(t, "ct"),
			Closed: rapid.Bool().Draw(t, "closed"),
		}
		if rapid.Bool().Draw(t, "hasTTL") {
			// TTL >= 1 for the SEEDED stream: a 0-second TTL means "expire
			// immediately", so the lazy is_expired (now > accessedAt + 0) would
			// reap the seeded stream in the microseconds between the HSET and the
			// script run, before config_matches is reached. The TTL=0 boundary is
			// covered by the pure-Go config-match property, which does not seed a
			// live stream. ConfigMatches compares the TTL value either way.
			v := rapid.Int64Range(1, 1<<40).Draw(t, "ttl")
			o.TTLSeconds = &v
		}
		if rapid.Bool().Draw(t, "hasExp") {
			// Seed FUTURE expiries only: the seeded stream must not be lazily
			// reaped by is_expired before create.lua reaches config_matches.
			// ConfigMatches compares ExpiresAt by equality, so the exact future
			// instant is irrelevant to the verdict — only that it is not past.
			ahead := rapid.Int64Range(int64(time.Hour), int64(1000*time.Hour)).Draw(t, "expAhead")
			tm := time.Now().Add(time.Duration(ahead)).UTC()
			o.ExpiresAt = &tm
		}
		if rapid.Bool().Draw(t, "isFork") {
			o.ForkedFrom = rapid.SampledFrom([]string{"/src/a", "/src/b", "/parent"}).Draw(t, "forkedFrom")
			if rapid.Bool().Draw(t, "hasForkOff") {
				b := rapid.Uint64Range(0, 1<<40).Draw(t, "forkOff")
				o.ForkOffset = &store.Offset{ByteOffset: b}
			}
			switch rapid.IntRange(0, 2).Draw(t, "subOffKind") {
			case 1:
				z := uint64(0)
				o.ForkSubOffset = &z
			case 2:
				v := rapid.Uint64Range(1, 1<<40).Draw(t, "subOff")
				o.ForkSubOffset = &v
			}
		}
		return o
	})
}

// configDiffCaseGen draws a (stored metadata, re-create options) pair. ~half are
// idempotent (options re-create the seeded metadata exactly, across all four
// advance×recordRequested fork shapes); ~half perturb exactly one comparison
// input so the verdict must flip. The Go oracle ConfigMatches is the ground
// truth either way — the differential asserts live Lua agrees with it.
func configDiffCaseGen() *rapid.Generator[configDiffCase] {
	return rapid.Custom(func(t *rapid.T) configDiffCase {
		o := configDiffOptsGen().Draw(t, "opts")
		advance := rapid.Bool().Draw(t, "advance")
		recordReq := rapid.Bool().Draw(t, "recordRequested")
		m := metaFromOptsRedis(o, advance, recordReq)

		if rapid.Bool().Draw(t, "matched") {
			return configDiffCase{meta: m, opts: o}
		}
		perturbConfigOpts(t, &m, &o)
		return configDiffCase{meta: m, opts: o}
	})
}

// perturbConfigOpts mutates exactly one comparison input on an (m, o) pair so the
// ConfigMatches verdict flips. The Go oracle re-decides the verdict, so the only
// requirement is that the perturbation is valid for the shape (no impossible
// forks). It deliberately includes the two subtle fork branches.
func perturbConfigOpts(t *rapid.T, m *store.StreamMetadata, o *store.CreateOptions) {
	choices := []string{"ct", "ttl", "exp", "closed"}
	if o.ForkedFrom != "" {
		choices = append(choices, "forkSubOff", "forkedFromChange")
		if o.ForkOffset != nil {
			choices = append(choices, "forkOffReq")
		}
	} else {
		choices = append(choices, "forkedFromAdd")
	}
	switch rapid.SampledFrom(choices).Draw(t, "perturb") {
	case "ct":
		o.ContentType = "application/x-perturbed"
	case "ttl":
		if o.TTLSeconds == nil {
			v := int64(7)
			o.TTLSeconds = &v
		} else {
			o.TTLSeconds = nil
		}
	case "exp":
		if o.ExpiresAt == nil {
			tm := time.Unix(0, 123456789).UTC()
			o.ExpiresAt = &tm
		} else {
			o.ExpiresAt = nil
		}
	case "closed":
		o.Closed = !m.Closed
	case "forkSubOff":
		v := m.ForkSubOffset + 1
		o.ForkSubOffset = &v
	case "forkOffReq":
		bumped := o.ForkOffset.Add(13)
		o.ForkOffset = &bumped
	case "forkedFromChange":
		o.ForkedFrom += "-other"
	case "forkedFromAdd":
		o.ForkedFrom = "/newly/forked"
		z := store.Offset{}
		o.ForkOffset = &z
	}
}

// seedMeta writes the stored metadata directly into its meta HASH so the
// subsequent createScript call finds an existing, non-expired, non-soft-deleted
// stream and reaches config_matches. It returns the path used.
func seedMeta(t *rapid.T, s *Store, ctx context.Context, m store.StreamMetadata) string {
	path := testPath("cfgdiff")
	m.Path = path
	fields := metaToFields(&m)
	// Flatten to an HSET argument list.
	args := make([]any, 0, 2*len(fields))
	for k, v := range fields {
		args = append(args, k, v)
	}
	if err := s.client.HSet(ctx, metaKey(path), args...).Err(); err != nil {
		t.Fatalf("seed meta HASH: %v", err)
	}
	return path
}

// configProbeArgs builds the create.lua argument list for a config-match probe:
// the same ARGV[3..9] the production createArgs assembles, then N=0 meta fields
// (the write path is never reached because the stream exists and matches or
// mismatches). It does NOT reuse createArgs because that bundles a full meta
// write; the probe only needs the comparison ARGVs.
func configProbeArgs(s *Store, path string, opts store.CreateOptions) []any {
	probeTTL := ""
	if opts.TTLSeconds != nil {
		probeTTL = strconv.FormatInt(*opts.TTLSeconds, 10)
	}
	probeExp := ""
	if opts.ExpiresAt != nil {
		probeExp = strconv.FormatInt(opts.ExpiresAt.UnixNano(), 10)
	}
	probeClosed := "0"
	if opts.Closed {
		probeClosed = "1"
	}
	probeForkOff := ""
	if opts.ForkOffset != nil {
		probeForkOff = opts.ForkOffset.String()
	}
	probeSubOff := "0"
	if opts.ForkSubOffset != nil {
		probeSubOff = strconv.FormatUint(*opts.ForkSubOffset, 10)
	}
	return []any{
		s.nowNsArg(), notifyChannel(path),
		normalizeCT(opts.ContentType), probeTTL, probeExp, probeClosed,
		opts.ForkedFrom, probeForkOff, probeSubOff,
		"0", // N = 0 meta fields (write path unreached)
	}
}

// TestDifferentialConfigMatches cross-checks the Go ConfigMatches verdict against
// the live create.lua config_matches reply (INV-CFG-01). For each generated case
// it seeds the stored metadata directly, runs the real createScript with probe
// ARGVs, and asserts: Go ConfigMatches true <=> Lua returns MATCHED; Go false <=>
// Lua returns MISMATCH. CREATED/EXISTS would mean the stream wasn't found or was
// soft-deleted/expired — a test setup error, flagged loudly.
func TestDifferentialConfigMatches(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rapid.Check(t, func(t *rapid.T) {
		c := configDiffCaseGen().Draw(t, "case")
		want := c.meta.ConfigMatches(c.opts) // Go oracle

		path := seedMeta(t, s, ctx, c.meta)
		args := configProbeArgs(s, path, c.opts)
		raw, err := createScript.Run(ctx, s.client, keysFor(path), args...).Result()
		if err != nil {
			t.Fatalf("createScript probe: %v\n  meta=%+v\n  opts=%+v", err, c.meta, c.opts)
		}
		status, _, err := decodeStatusReply(raw)
		if err != nil {
			t.Fatalf("decode create reply: %v (raw %v)", err, raw)
		}

		switch status {
		case stMatched:
			if !want {
				t.Fatalf("INV-CFG-01 DIVERGENCE: live config_matches=MATCHED but Go ConfigMatches=false\n"+
					"  meta=%+v\n  opts=%+v", c.meta, c.opts)
			}
		case stMismatch:
			if want {
				t.Fatalf("INV-CFG-01 DIVERGENCE: live config_matches=MISMATCH but Go ConfigMatches=true\n"+
					"  meta=%+v\n  opts=%+v", c.meta, c.opts)
			}
		default:
			t.Fatalf("config-match probe returned %q (expected MATCHED/MISMATCH); "+
				"seeded stream was likely not found/expired/soft-deleted — test setup error\n"+
				"  meta=%+v\n  opts=%+v", status, c.meta, c.opts)
		}
	})
}

// TestDifferentialConfigMatchesForkBranches drives the two adversarial-critic
// fork branches through the live config_matches explicitly (INV-CFG-01): the
// ForkOffsetRequested-vs-ForkOffset fallback (post-PR recorded requested offset,
// AND the pre-PR nil-requested fallback to the internal offset) and the nil-vs-0
// ForkSubOffset equivalence. Each sub-case asserts the live Lua reply matches the
// Go oracle, so the subtle fallback can never silently drift between the two
// implementations.
func TestDifferentialConfigMatchesForkBranches(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	run := func(t *rapid.T, m store.StreamMetadata, o store.CreateOptions, label string) {
		want := m.ConfigMatches(o)
		path := seedMeta(t, s, ctx, m)
		args := configProbeArgs(s, path, o)
		raw, err := createScript.Run(ctx, s.client, keysFor(path), args...).Result()
		if err != nil {
			t.Fatalf("%s: createScript probe: %v", label, err)
		}
		status, _, err := decodeStatusReply(raw)
		if err != nil {
			t.Fatalf("%s: decode reply: %v", label, err)
		}
		gotMatched := status == stMatched
		if status != stMatched && status != stMismatch {
			t.Fatalf("%s: unexpected status %q (setup error)\n  meta=%+v\n  opts=%+v", label, status, m, o)
		}
		if gotMatched != want {
			t.Fatalf("INV-CFG-01 %s DIVERGENCE: live MATCHED=%v but Go ConfigMatches=%v\n  meta=%+v\n  opts=%+v",
				label, gotMatched, want, m, o)
		}
	}

	rapid.Check(t, func(t *rapid.T) {
		requested := store.Offset{ByteOffset: rapid.Uint64Range(0, 1<<40).Draw(t, "requested")}
		sub := rapid.Uint64Range(1, 1<<32).Draw(t, "sub")
		advanced := requested.Add(sub * 7)

		jsonFork := func() (store.StreamMetadata, store.CreateOptions) {
			now := time.Now()
			m := store.StreamMetadata{
				ContentType:    "application/json",
				CurrentOffset:  advanced,
				CreatedAt:      now,
				LastAccessedAt: now,
				ForkedFrom:     "/src",
				ForkOffset:     advanced,
				ForkSubOffset:  sub,
				Producers:      map[string]*store.ProducerState{},
			}
			o := store.CreateOptions{
				ContentType:   "application/json",
				ForkedFrom:    "/src",
				ForkOffset:    &store.Offset{ByteOffset: requested.ByteOffset},
				ForkSubOffset: &sub,
			}
			return m, o
		}

		// Fallback, post-PR: requested recorded; re-create with the requested
		// offset MATCHES, with the advanced internal offset MISMATCHES.
		m, o := jsonFork()
		req := requested
		m.ForkOffsetRequested = &req
		run(t, m, o, "fallback/post-PR/requested-matches")
		if !advanced.Equal(requested) {
			o2 := o
			adv := store.Offset{ByteOffset: advanced.ByteOffset}
			o2.ForkOffset = &adv
			run(t, m, o2, "fallback/post-PR/advanced-mismatches")
		}

		// Fallback, pre-PR: requested NOT recorded; comparison falls back to the
		// internal ForkOffset, so re-create with the internal offset MATCHES.
		mPre, _ := jsonFork()
		mPre.ForkOffsetRequested = nil
		adv := store.Offset{ByteOffset: advanced.ByteOffset}
		oPre := store.CreateOptions{
			ContentType:   "application/json",
			ForkedFrom:    "/src",
			ForkOffset:    &adv,
			ForkSubOffset: &sub,
		}
		run(t, mPre, oPre, "fallback/pre-PR/internal-matches")

		// nil-vs-0 sub-offset: stored 0, re-create with nil and with explicit 0
		// must both MATCH; nonzero must MISMATCH.
		now := time.Now()
		base := store.StreamMetadata{
			ContentType:    "application/octet-stream",
			CreatedAt:      now,
			LastAccessedAt: now,
			ForkedFrom:     "/src",
			ForkOffset:     store.Offset{},
			ForkSubOffset:  0,
			Producers:      map[string]*store.ProducerState{},
		}
		nilOpts := store.CreateOptions{
			ContentType: "application/octet-stream",
			ForkedFrom:  "/src",
			ForkOffset:  &store.Offset{},
		}
		run(t, base, nilOpts, "nil-vs-0/nil-matches")
		zero := uint64(0)
		zeroOpts := nilOpts
		zeroOpts.ForkSubOffset = &zero
		run(t, base, zeroOpts, "nil-vs-0/explicit-0-matches")
		nonzero := rapid.Uint64Range(1, 1<<40).Draw(t, "nonzero")
		nonOpts := nilOpts
		nonOpts.ForkSubOffset = &nonzero
		run(t, base, nonOpts, "nil-vs-0/nonzero-mismatches")
	})
}
