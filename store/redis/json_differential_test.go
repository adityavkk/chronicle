package redis

import (
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"pgregory.net/rapid"

	"gecgithub01.walmart.com/auk000v/chronicle/store"
)

// adversarialJSONCorpusRaw is the committed regression fixture (issue #44): a
// small hand-picked set of tricky JSON bodies + empty-array variants, replayed
// deterministically by TestJSONDifferentialCorpus / the empty-array asymmetry
// test. See the file's _comment field for the framing cases it pins.
//
//go:embed testdata/json_corpus/adversarial.json
var adversarialJSONCorpusRaw []byte

type adversarialJSONCorpus struct {
	Comment          string   `json:"_comment"`
	Bodies           []string `json:"bodies"`
	EmptyArrayBodies []string `json:"emptyArrayBodies"`
}

func loadJSONCorpus(t *testing.T) adversarialJSONCorpus {
	t.Helper()
	var c adversarialJSONCorpus
	if err := json.Unmarshal(adversarialJSONCorpusRaw, &c); err != nil {
		t.Fatalf("decode adversarial JSON corpus: %v", err)
	}
	if len(c.Bodies) == 0 || len(c.EmptyArrayBodies) == 0 {
		t.Fatalf("adversarial JSON corpus is empty: %d bodies, %d empty-array bodies", len(c.Bodies), len(c.EmptyArrayBodies))
	}
	return c
}

// json_differential_test.go is the JSON-mode flatten-append differential
// (issue #44). It drives ADVERSARIAL application/json bodies through
// store.ProcessJSONAppend on BOTH the in-process store.MemoryStore (the ORACLE)
// and the live Redis backend (the SUBJECT) on the same path and asserts they
// agree, frame for frame, on the flattening + offset folding (INV-DIFF-02), the
// empty-array create-vs-append asymmetry, and per-element STRICT offset
// monotonicity (INV-OFF-06 in JSON mode).
//
// What is actually differential here. ProcessJSONAppend is the SHARED Go
// splitter: a top-level array is flattened exactly one level (each element one
// message, its bytes captured verbatim by encoding/json into a json.RawMessage,
// so inner whitespace survives but the element's own surrounding whitespace is
// trimmed), any other JSON value is a single message of the outer-trimmed bytes,
// and the tail folds by Add(len(elem)) per element. Both backends call it, but
// they then store and re-read those frames through entirely different machinery:
// the MemoryStore keeps them in a Go slice, while the Redis path encodes each
// frame as a ZSET-lex member (encodeFrame) written by append.lua's ZADD and
// re-decoded by read.lua's ZRANGEBYLEX (decodeFrames). This property pins that
// the lex round-trip preserves the exact element bytes AND the exact per-element
// offsets the shared splitter computed — i.e. the live Lua framing is observably
// equivalent to the oracle on every adversarial value.
//
// Clock determinism mirrors equivalence_test.go: both stores share ONE injected
// store.FakeClock anchored at the Unix epoch (UnixNano stays exact as a Lua
// double; see TestEquivalenceMemoryVsRedis). No expiry is configured here so the
// clock never actually moves — JSON framing is time-independent — but the seam
// keeps the two backends' now identical for free.
//
// Real-Redis-or-skip: newTestStore skips under -short and when Redis is
// unreachable, and runs in the test-integration CI target (./store/redis/), so
// this property runs in CI against containerized Redis.
//
// Failing seeds: a divergence shrinks to a minimal JSON value; commit the
// auto-written failfile under testdata/equivalence_seeds/ per that directory's
// README. A small hand-picked adversarial corpus is committed as
// adversarialJSONCorpus below and replayed deterministically by
// TestJSONDifferentialCorpus.

// jsonScalarGen draws an adversarial JSON SCALAR token as raw bytes: numbers
// (incl. negative, fractional, exponent, leading/trailing-zero-free forms),
// strings carrying bytes that stress framing (the '|' frame separator, quotes,
// backslashes, control escapes, multi-byte UTF-8, and inner whitespace that the
// element capture must preserve), and the bare literals. These are emitted as
// already-valid JSON text so they can be spliced into arrays/objects verbatim.
func jsonScalarGen() *rapid.Generator[string] {
	return rapid.OneOf(
		// Numbers, including forms whose byte length differs from their value's
		// "natural" rendering (so an offset that folded a re-serialized form
		// instead of the raw element would diverge).
		rapid.SampledFrom([]string{
			"0", "1", "-1", "42", "-0", "3.14", "-2.5e10", "1E+9",
			"0.0000001", "100000000000", "123456789",
		}),
		// Strings: the inner bytes are the framing stress. Drawn as a Go string
		// then marshaled so the result is always valid JSON, but the CONTENT is
		// adversarial (frame separator, quotes, backslashes, controls, UTF-8).
		rapid.Custom(func(t *rapid.T) string {
			raw := rapid.StringOf(rapid.OneOf(
				rapid.SampledFrom([]rune{'|', '"', '\\', '\n', '\t', '\x00', ' ', '/', ':', ',', '[', ']', '{', '}'}),
				rapid.RuneFrom([]rune{'a', 'Z', '€', '🙂', 'é'}),
			)).Draw(t, "strContent")
			b, err := json.Marshal(raw)
			if err != nil { // unreachable: any Go string marshals
				t.Fatalf("marshal string content: %v", err)
			}
			return string(b)
		}),
		rapid.SampledFrom([]string{"true", "false", "null"}),
	)
}

// jsonValueGen draws an adversarial JSON VALUE of bounded depth: a scalar, a
// (possibly empty, possibly heterogeneous) array, or an object, with whitespace
// sprinkled between structural tokens at every level. Depth is decremented so
// generation terminates; at depth 0 only scalars are drawn. The whitespace is
// the point: encoding/json trims an element's OWN surrounding whitespace when it
// captures the json.RawMessage but preserves whitespace INSIDE the element, so
// two values that differ only in internal spacing must frame to different byte
// lengths — and both backends must agree on which.
func jsonValueGen(depth int) *rapid.Generator[string] {
	if depth <= 0 {
		return jsonScalarGen()
	}
	return rapid.Custom(func(t *rapid.T) string {
		switch rapid.SampledFrom([]string{"scalar", "array", "object"}).Draw(t, "kind") {
		case "array":
			n := rapid.IntRange(0, 4).Draw(t, "arrLen")
			elems := make([]string, n)
			for i := range elems {
				elems[i] = jsonValueGen(depth-1).Draw(t, fmt.Sprintf("arrElem%d", i))
			}
			return "[" + joinWithWS(t, elems, "arr") + "]"
		case "object":
			n := rapid.IntRange(0, 3).Draw(t, "objLen")
			pairs := make([]string, n)
			for i := range pairs {
				key, err := json.Marshal(rapid.StringN(0, 4, 8).Draw(t, fmt.Sprintf("objKey%d", i)))
				if err != nil {
					t.Fatalf("marshal key: %v", err)
				}
				val := jsonValueGen(depth-1).Draw(t, fmt.Sprintf("objVal%d", i))
				pairs[i] = ws(t, "ok") + string(key) + ws(t, "oc") + ":" + ws(t, "ov") + val
			}
			return "{" + joinWithWS(t, pairs, "obj") + "}"
		default:
			return jsonScalarGen().Draw(t, "scalar")
		}
	})
}

// ws draws a short run of insignificant JSON whitespace (the four RFC 8259
// whitespace bytes), often empty. Sprinkled between structural tokens to stress
// the element-capture trimming.
func ws(t *rapid.T, label string) string {
	return rapid.StringOfN(rapid.SampledFrom([]rune{' ', '\t', '\n', '\r'}), 0, 3, -1).Draw(t, "ws_"+label)
}

// joinWithWS joins items with commas, surrounding each item and separator with
// generated whitespace so the container carries adversarial internal spacing
// while staying valid JSON.
func joinWithWS(t *rapid.T, items []string, label string) string {
	if len(items) == 0 {
		return ws(t, label+"_empty")
	}
	out := ws(t, label+"_lead")
	for i, it := range items {
		if i > 0 {
			out += "," + ws(t, label+"_sep")
		}
		out += it + ws(t, label+"_post")
	}
	return out
}

// jsonBodyGen draws a full adversarial request body: a JSON value of bounded
// depth, wrapped (≈half the time) in outer whitespace so the OUTER trim in
// ProcessJSONAppend is exercised independently of the inner spacing. Roughly
// half the bodies are top-level arrays (the flatten path); the rest are single
// values (scalar/object/whitespace-wrapped value).
func jsonBodyGen() *rapid.Generator[string] {
	return rapid.Custom(func(t *rapid.T) string {
		var body string
		if rapid.Bool().Draw(t, "topLevelArray") {
			n := rapid.IntRange(0, 5).Draw(t, "topArrLen")
			elems := make([]string, n)
			for i := range elems {
				elems[i] = jsonValueGen(3).Draw(t, fmt.Sprintf("topElem%d", i))
			}
			body = "[" + joinWithWS(t, elems, "top") + "]"
		} else {
			body = jsonValueGen(3).Draw(t, "topValue")
		}
		// Outer whitespace exercises the leading-byte detection + outer trim.
		return ws(t, "outerLead") + body + ws(t, "outerTrail")
	})
}

// expectedFrames is the independent oracle for the flatten contract: it
// reproduces ProcessJSONAppend's split (outer-trim, one-level array flatten,
// single-value otherwise) and the Add(len(elem)) tail fold, WITHOUT calling the
// production helper, so an agreement between it and BOTH backends is a real
// three-way check rather than a tautology. Returns the per-element (data,
// endOffset) pairs starting from base, or allowEmptyErr=true to signal the
// empty-array-on-append rejection.
type frame struct {
	data string
	end  store.Offset
}

func expectedFrames(t rapidT, body string, base store.Offset, allowEmpty bool) (frames []frame, emptyArrayRejected bool) {
	if !json.Valid([]byte(body)) {
		t.Fatalf("generator produced invalid JSON: %q", body)
	}
	trimmed := trimJSONWS(body)
	var elems [][]byte
	if len(trimmed) > 0 && trimmed[0] == '[' {
		var arr []json.RawMessage
		if err := json.Unmarshal([]byte(trimmed), &arr); err != nil {
			t.Fatalf("oracle unmarshal array: %v (body %q)", err, body)
		}
		if len(arr) == 0 {
			if !allowEmpty {
				return nil, true
			}
			return nil, false // empty array on create: zero frames
		}
		for _, e := range arr {
			elems = append(elems, []byte(e))
		}
	} else {
		elems = [][]byte{[]byte(trimmed)}
	}
	cur := base
	for _, e := range elems {
		cur = cur.Add(uint64(len(e)))
		frames = append(frames, frame{data: string(e), end: cur})
	}
	return frames, false
}

// trimJSONWS trims the four RFC 8259 insignificant-whitespace bytes from both
// ends, matching bytes.TrimSpace's effect on JSON outer whitespace as used by
// ProcessJSONAppend (bytes.TrimSpace trims a slightly wider unicode set, but JSON
// outer whitespace is only these four, and the generator emits only these four).
func trimJSONWS(s string) string {
	isWS := func(b byte) bool { return b == ' ' || b == '\t' || b == '\n' || b == '\r' }
	i, j := 0, len(s)
	for i < j && isWS(s[i]) {
		i++
	}
	for j > i && isWS(s[j-1]) {
		j--
	}
	return s[i:j]
}

// assertJSONAppendAgrees appends one adversarial body to both backends and
// asserts: same error class; on success the same AppendResult tail; the same
// per-element frames + offsets via Read from base; per-element STRICT offset
// monotonicity (INV-OFF-06); agreement with the independent expectedFrames
// oracle; and identical FormatResponse round-trips. base is the pre-append tail
// (offsets fold forward from there). It returns the new tail for chaining.
func assertJSONAppendAgrees(rt rapidT, oracle *store.MemoryStore, subject *Store, path string, body string, base store.Offset) store.Offset {
	want, emptyRejected := expectedFrames(rt, body, base, false)

	oRes, oErr := oracle.Append(path, []byte(body), store.AppendOptions{ContentType: "application/json"})
	sRes, sErr := subject.Append(path, []byte(body), store.AppendOptions{ContentType: "application/json"})

	// Error-presence and class must match across backends.
	if (oErr == nil) != (sErr == nil) {
		rt.Fatalf("Append error presence mismatch for %q: oracle=%v subject=%v", body, oErr, sErr)
	}
	if oErr != nil {
		if errors.Is(oErr, store.ErrEmptyJSONArray) != errors.Is(sErr, store.ErrEmptyJSONArray) {
			rt.Fatalf("Append empty-array class mismatch for %q: oracle=%v subject=%v", body, oErr, sErr)
		}
		if !emptyRejected {
			rt.Fatalf("Append unexpectedly errored for %q: oracle=%v subject=%v (oracle expected %d frames)", body, oErr, sErr, len(want))
		}
		if !errors.Is(oErr, store.ErrEmptyJSONArray) {
			rt.Fatalf("expected ErrEmptyJSONArray for empty-array append %q, got oracle=%v subject=%v", body, oErr, sErr)
		}
		return base // no write: tail unchanged
	}
	if emptyRejected {
		rt.Fatalf("oracle expected empty-array rejection for %q but both backends accepted it", body)
	}

	// Success: AppendResult tail agrees and equals the oracle's folded end.
	if !oRes.Offset.Equal(sRes.Offset) {
		rt.Fatalf("Append tail mismatch for %q: oracle=%v subject=%v", body, oRes.Offset, sRes.Offset)
	}
	wantTail := base
	if len(want) > 0 {
		wantTail = want[len(want)-1].end
	}
	if !oRes.Offset.Equal(wantTail) {
		rt.Fatalf("INV-DIFF-02: Append tail %v disagrees with independent oracle %v for %q", oRes.Offset, wantTail, body)
	}

	// Read back from base on both backends and diff frame-for-frame.
	oMsgs, _, oRErr := oracle.Read(path, base)
	sMsgs, _, sRErr := subject.Read(path, base)
	if oRErr != nil || sRErr != nil {
		rt.Fatalf("Read after append errored for %q: oracle=%v subject=%v", body, oRErr, sRErr)
	}
	if len(oMsgs) != len(want) || len(sMsgs) != len(want) {
		rt.Fatalf("frame count mismatch for %q: oracle=%d subject=%d expected=%d",
			body, len(oMsgs), len(sMsgs), len(want))
	}

	prev := base
	for i := range want {
		// Per-element bytes agree across backends and with the independent oracle.
		if string(oMsgs[i].Data) != want[i].data {
			rt.Fatalf("frame[%d] data mismatch for %q: oracle=%q expected=%q", i, body, oMsgs[i].Data, want[i].data)
		}
		if string(sMsgs[i].Data) != want[i].data {
			rt.Fatalf("INV-DIFF-02: frame[%d] data mismatch for %q: subject=%q expected=%q", i, body, sMsgs[i].Data, want[i].data)
		}
		// Per-element end offsets agree across backends and with the oracle.
		if !oMsgs[i].Offset.Equal(want[i].end) || !sMsgs[i].Offset.Equal(want[i].end) {
			rt.Fatalf("frame[%d] offset mismatch for %q: oracle=%v subject=%v expected=%v",
				i, body, oMsgs[i].Offset, sMsgs[i].Offset, want[i].end)
		}
		// INV-OFF-06: STRICT per-element monotonicity. Each flattened element
		// advances the byte offset; equal-or-decreasing offsets are a violation.
		// (A zero-length element is impossible: every JSON value is >= 1 byte.)
		if !prev.LessThan(oMsgs[i].Offset) {
			rt.Fatalf("INV-OFF-06: oracle offset not strictly increasing at frame[%d] for %q: prev=%v cur=%v",
				i, body, prev, oMsgs[i].Offset)
		}
		if !prev.LessThan(sMsgs[i].Offset) {
			rt.Fatalf("INV-OFF-06: subject offset not strictly increasing at frame[%d] for %q: prev=%v cur=%v",
				i, body, prev, sMsgs[i].Offset)
		}
		prev = oMsgs[i].Offset
	}

	// FormatResponse re-wraps the flattened frames into a JSON array; both
	// backends must produce byte-identical output.
	oOut, oFErr := oracle.FormatResponse(path, oMsgs)
	sOut, sFErr := subject.FormatResponse(path, sMsgs)
	if oFErr != nil || sFErr != nil {
		rt.Fatalf("FormatResponse errored for %q: oracle=%v subject=%v", body, oFErr, sFErr)
	}
	if string(oOut) != string(sOut) {
		rt.Fatalf("FormatResponse mismatch for %q: oracle=%q subject=%q", body, oOut, sOut)
	}
	if !json.Valid(oOut) {
		rt.Fatalf("FormatResponse produced invalid JSON for %q: %q", body, oOut)
	}

	return oRes.Offset
}

// TestJSONDifferentialFlattenAppend is the rapid property: for a sequence of
// adversarial JSON appends to one application/json stream, the MemoryStore
// oracle and live Redis agree on framing + offsets (INV-DIFF-02), per-element
// strict offset monotonicity holds (INV-OFF-06), and the empty-array
// create-vs-append asymmetry is observed identically. Skipped under -short / when
// Redis is unreachable (newTestStore).
func TestJSONDifferentialFlattenAppend(t *testing.T) {
	base := newTestStore(t)

	rapid.Check(t, func(rt *rapid.T) {
		clock := store.NewFakeClock(time.Unix(0, 0))
		oracle := store.NewMemoryStore(store.WithClock(clock))
		subject := New(base.client, Options{Clock: clock})
		path := newJSONPath()
		opts := store.CreateOptions{ContentType: "application/json"}
		if _, _, err := oracle.Create(path, opts); err != nil {
			rt.Fatalf("oracle create: %v", err)
		}
		if _, _, err := subject.Create(path, opts); err != nil {
			rt.Fatalf("subject create: %v", err)
		}

		tail := store.ZeroOffset
		n := rapid.IntRange(1, 6).Draw(rt, "numAppends")
		for i := 0; i < n; i++ {
			body := jsonBodyGen().Draw(rt, fmt.Sprintf("body%d", i))
			tail = assertJSONAppendAgrees(rt, oracle, subject, path, body, tail)
		}

		// Full re-read from zero agrees across the whole accumulated stream.
		assertJSONReadAgreesFromZero(rt, oracle, subject, path)
	})
}

// assertJSONReadAgreesFromZero reads the whole stream from ZeroOffset on both
// backends and asserts frame count, per-element bytes + offsets, and strict
// monotonicity over the entire accumulated stream.
func assertJSONReadAgreesFromZero(rt rapidT, oracle *store.MemoryStore, subject *Store, path string) {
	oMsgs, oUp, oErr := oracle.Read(path, store.ZeroOffset)
	sMsgs, sUp, sErr := subject.Read(path, store.ZeroOffset)
	if oErr != nil || sErr != nil {
		rt.Fatalf("full Read errored: oracle=%v subject=%v", oErr, sErr)
	}
	if oUp != sUp {
		rt.Fatalf("full Read upToDate mismatch: oracle=%v subject=%v", oUp, sUp)
	}
	if len(oMsgs) != len(sMsgs) {
		rt.Fatalf("full Read count mismatch: oracle=%d subject=%d", len(oMsgs), len(sMsgs))
	}
	prev := store.ZeroOffset
	for i := range oMsgs {
		if string(oMsgs[i].Data) != string(sMsgs[i].Data) {
			rt.Fatalf("full Read frame[%d] data mismatch: oracle=%q subject=%q", i, oMsgs[i].Data, sMsgs[i].Data)
		}
		if !oMsgs[i].Offset.Equal(sMsgs[i].Offset) {
			rt.Fatalf("full Read frame[%d] offset mismatch: oracle=%v subject=%v", i, oMsgs[i].Offset, sMsgs[i].Offset)
		}
		if !prev.LessThan(oMsgs[i].Offset) {
			rt.Fatalf("INV-OFF-06: full Read offset not strictly increasing at frame[%d]: prev=%v cur=%v", i, prev, oMsgs[i].Offset)
		}
		prev = oMsgs[i].Offset
	}
}

// TestJSONDifferentialEmptyArrayAsymmetry pins the empty-array create-vs-append
// rule from BOTH backends: [] is REJECTED on append (ErrEmptyJSONArray) but
// VALID on create (zero frames, tail stays zero). Whitespace-wrapped empty
// arrays must behave identically. Asserted directly (not via rapid) so the
// asymmetry is a readable, deterministic regression row.
func TestJSONDifferentialEmptyArrayAsymmetry(t *testing.T) {
	base := newTestStore(t)
	corpus := loadJSONCorpus(t)
	clock := store.NewFakeClock(time.Unix(0, 0))
	oracle := store.NewMemoryStore(store.WithClock(clock))
	subject := New(base.client, Options{Clock: clock})

	for _, empty := range corpus.EmptyArrayBodies {
		t.Run(fmt.Sprintf("create-allows %q", empty), func(t *testing.T) {
			path := newJSONPath()
			opts := store.CreateOptions{ContentType: "application/json", InitialData: []byte(empty)}
			oMeta, _, oErr := oracle.Create(path, opts)
			sMeta, _, sErr := subject.Create(path, opts)
			if oErr != nil || sErr != nil {
				t.Fatalf("create with empty-array InitialData rejected: oracle=%v subject=%v", oErr, sErr)
			}
			if !oMeta.CurrentOffset.IsZero() || !sMeta.CurrentOffset.IsZero() {
				t.Fatalf("empty-array create should leave tail at zero: oracle=%v subject=%v", oMeta.CurrentOffset, sMeta.CurrentOffset)
			}
		})

		t.Run(fmt.Sprintf("append-rejects %q", empty), func(t *testing.T) {
			path := newJSONPath()
			opts := store.CreateOptions{ContentType: "application/json"}
			if _, _, err := oracle.Create(path, opts); err != nil {
				t.Fatalf("oracle create: %v", err)
			}
			if _, _, err := subject.Create(path, opts); err != nil {
				t.Fatalf("subject create: %v", err)
			}
			_, oErr := oracle.Append(path, []byte(empty), store.AppendOptions{ContentType: "application/json"})
			_, sErr := subject.Append(path, []byte(empty), store.AppendOptions{ContentType: "application/json"})
			if !errors.Is(oErr, store.ErrEmptyJSONArray) {
				t.Fatalf("oracle: empty-array append %q should be ErrEmptyJSONArray, got %v", empty, oErr)
			}
			if !errors.Is(sErr, store.ErrEmptyJSONArray) {
				t.Fatalf("subject: empty-array append %q should be ErrEmptyJSONArray, got %v", empty, sErr)
			}
		})
	}
}

// newJSONPath returns a fresh unique stream path so JSON-differential streams
// never alias across the run (mirrors the equivalence harness's unique paths).
func newJSONPath() string {
	return testPath("jsondiff")
}

// rapidT is the subset of *rapid.T the assertion helpers use (Fatalf). Using an
// interface lets the corpus test drive the same helpers with a thin *testing.T
// adapter so the corpus and the property share one assertion path.
type rapidT interface {
	Fatalf(format string, args ...any)
}

// testingTAdapter lets a *testing.T satisfy rapidT so the committed corpus runs
// through the exact same assertion helpers as the rapid property.
type testingTAdapter struct{ t *testing.T }

func (a testingTAdapter) Fatalf(format string, args ...any) { a.t.Fatalf(format, args...) }

// TestJSONDifferentialCorpus replays the committed adversarial-JSON regression
// fixture (testdata/json_corpus/adversarial.json) through the SAME
// assertJSONAppendAgrees path the rapid property uses, so every hand-picked
// tricky body is pinned as a deterministic MemoryStore-vs-Redis agreement on
// framing + offsets (INV-DIFF-02), with per-element strict monotonicity
// (INV-OFF-06). Each body gets a fresh stream so a single body's framing is
// asserted in isolation; the rapid property covers the multi-append accumulation
// case. Skipped under -short / when Redis is unreachable (newTestStore).
func TestJSONDifferentialCorpus(t *testing.T) {
	base := newTestStore(t)
	corpus := loadJSONCorpus(t)

	for i, body := range corpus.Bodies {
		t.Run(fmt.Sprintf("body%02d", i), func(t *testing.T) {
			clock := store.NewFakeClock(time.Unix(0, 0))
			oracle := store.NewMemoryStore(store.WithClock(clock))
			subject := New(base.client, Options{Clock: clock})
			path := newJSONPath()
			opts := store.CreateOptions{ContentType: "application/json"}
			if _, _, err := oracle.Create(path, opts); err != nil {
				t.Fatalf("oracle create: %v", err)
			}
			if _, _, err := subject.Create(path, opts); err != nil {
				t.Fatalf("subject create: %v", err)
			}
			rt := testingTAdapter{t}
			tail := assertJSONAppendAgrees(rt, oracle, subject, path, body, store.ZeroOffset)
			// Append a second copy so accumulation (non-zero base) is exercised
			// for every corpus body too.
			assertJSONAppendAgrees(rt, oracle, subject, path, body, tail)
			assertJSONReadAgreesFromZero(rt, oracle, subject, path)
		})
	}
}
