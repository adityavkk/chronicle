package redis

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	goredis "github.com/redis/go-redis/v9"

	"pgregory.net/rapid"
)

// equivalence_fuzz_test.go wires the EXISTING MemoryStore-vs-Redis rapid state
// machine (chronicleModel / runEquivalenceModel in equivalence_test.go, issue
// #26) as a native Go coverage-guided fuzz target via rapid.MakeFuzz (issue
// #42). No new model, no new actions, no new invariants: the identical Check
// oracle (diff (result, error, tail, metadata) between the oracle and live
// Redis after every step) is the fuzz oracle, and the identical
// StateMachineActions drive it. The ONLY thing that changes versus
// TestEquivalenceMemoryVsRedis is the source of the bitstream rapid draws from:
//
//   - rapid.Check (the PR gate)     — a uniform PRNG bitstream.
//   - rapid.MakeFuzz (this target)  — Go's coverage-guided fuzz input bytes,
//     mutated toward inputs that reach new code, so the rare Lua branches that
//     uniform random under-samples get directed coverage. checkFuzz packs the
//     []byte input into the same uint64 bitstream rapid.Check feeds the engine,
//     so every fuzz input is a valid, replayable op sequence.
//
// The four rare branches this target steers toward (per research/03 Pitfalls
// #6 and INVARIANTS.md coverage-gaps) are:
//
//   - epoch-bump-at-nonzero-seq  -> store.ErrInvalidEpochSeq      (INV-PROD-08)
//   - gap-at-lastSeq+1           -> store.ErrProducerSeqGap        (INV-PROD-08)
//   - fork-sub-offset overshoot  -> store.ErrInvalidForkSubOffset  (INV-CFG-01)
//   - close-by-producer duplicate-> ProducerResultDuplicate close  (INV-FENCE-03)
//
// and, since the write fence (#183, INV-DIFF-02 rung 3b), the fence-rung
// outcomes the short uniform state-machine runs under-sample: the accepted
// fenced-class write (which binds its producer and fixes the class's last
// offset), the sealed / epoch / producer_required / bound refusals, the
// supersession seal at grant, and the idempotent redelivered seal. Every
// hand-named branch-* fixture in the committed corpus is pinned by
// TestFuzzStoreEquivalenceCorpusReachesBranches below, so the corpus cannot
// silently go vacuous.
//
// Regime split (issue #42 deliverable): the PR gate runs the fast rapid.Check
// property over the committed testdata/fuzz/ corpus (Go replays every corpus
// file deterministically on a plain `go test` with NO -fuzz flag — these are
// inherited as regression fixtures for free). The long coverage-guided run is
// the NIGHTLY job (.github/workflows/fuzz-nightly.yml) which runs
// `go test -fuzz=FuzzStoreEquivalence -fuzztime=<budget>` against containerized
// Redis and fails on any new crasher / divergence. A failure prints a minimal,
// replayable command sequence plus a deterministic seed (rapid's auto-shrink),
// and Go writes the crashing input under testdata/fuzz/FuzzStoreEquivalence/
// where it is committed as a permanent regression fixture. See
// testdata/fuzz/README.md for the persisted-seed-format decision (research/03
// open question #4).

// boundarySeedBytes encodes the differential_test.go / equivalence_test.go
// boundary (epoch, seq) table — the accept/reject ladder rungs (first-contact
// seq 0, first-contact gap, epoch bump at seq 0, epoch bump at seq>0, duplicate,
// in-order, gap, stale epoch) — into raw fuzz-input byte strings.
//
// rapid's fuzz bridge (checkFuzz) consumes the input 8 bytes at a time as
// little-endian uint64 draws, so seeding the corpus with byte strings whose
// uint64 words carry these boundary magnitudes plants the interesting values
// directly in the bitstream the coverage-guided mutator works outward from,
// rather than making it discover them by luck across the 2^64 draw space. The
// values are deliberately small and varied (and repeated, so a draw landing on
// any word still hits a boundary), matching the boundaryEpochSeq table the
// generator samples from. These are registered as in-memory f.Add seeds; the
// hand-crafted FILE corpus under testdata/fuzz/FuzzStoreEquivalence/ pins inputs
// that have been verified to reach each of the four named rare branches.
func boundarySeedBytes() [][]byte {
	// One word per boundary (epoch, seq) rung plus the small action-selector
	// anchors {0,1,2,3}; words are laid down repeatedly so a long generated op
	// sequence keeps drawing from the interesting region as it consumes input.
	words := make([]uint64, 0, 4+2*len(boundaryEpochSeq))
	words = append(words, 0, 1, 2, 3)
	for _, es := range boundaryEpochSeq {
		words = append(words, uint64(es[0]), uint64(es[1]))
	}

	seeds := make([][]byte, 0, len(words)+2)

	// A seed per single boundary word, padded to a few repeats so the first
	// several draws all land on it (action choice, then the boundary value).
	for _, w := range words {
		seeds = append(seeds, repeatWord(w, 6))
	}

	// Two longer mixed seeds: the whole table laid down in order, and its
	// reverse, so a multi-step op sequence walks the full accept/reject ladder.
	mixed := make([]byte, 0, len(words)*8)
	for _, w := range words {
		mixed = binary.LittleEndian.AppendUint64(mixed, w)
	}
	seeds = append(seeds, mixed)

	rev := make([]byte, 0, len(words)*8)
	for i := len(words) - 1; i >= 0; i-- {
		rev = binary.LittleEndian.AppendUint64(rev, words[i])
	}
	seeds = append(seeds, rev)

	return seeds
}

// repeatWord lays the same little-endian uint64 down n times.
func repeatWord(w uint64, n int) []byte {
	out := make([]byte, 0, n*8)
	for i := 0; i < n; i++ {
		out = binary.LittleEndian.AppendUint64(out, w)
	}
	return out
}

// FuzzStoreEquivalence is the coverage-guided fuzz target over the existing
// MemoryStore-vs-Redis state machine. It is the SAME property body
// (runEquivalenceModel) the PR-gate property runner drives, wrapped with
// rapid.MakeFuzz so Go's fuzzer can mutate the input bitstream toward the rare
// Lua branches uniform random under-samples (issue #42).
//
// Running modes:
//
//	go test -run=^$ -fuzz=FuzzStoreEquivalence -fuzztime=20s ./store/redis/
//	    coverage-guided fuzzing against live Redis (the nightly regime).
//	go test ./store/redis/
//	    replays every committed testdata/fuzz/FuzzStoreEquivalence/ corpus file
//	    deterministically as a regression fixture (NO -fuzz; the PR gate).
//
// Skips under -short and when Redis is unreachable (fuzzStore handles both),
// so the corpus replay is a no-op on a machine without Redis rather than a
// failure.
func FuzzStoreEquivalence(f *testing.F) {
	base := fuzzStore(f) // skips under -short / unreachable Redis; does NOT flush

	// Plant the boundary-table values directly in the seed corpus so
	// coverage-guided mutation starts from the interesting (epoch, seq) region.
	for _, seed := range boundarySeedBytes() {
		f.Add(seed)
	}

	f.Fuzz(rapid.MakeFuzz(func(t *rapid.T) {
		runEquivalenceModel(t, base)
	}))
}

// corpusBranches names the branch each hand-named corpus fixture under
// testdata/fuzz/FuzzStoreEquivalence/ was harvested to reach, as the
// fenceTally key its replay must record.
var corpusBranches = map[string]string{
	"branch-epoch-bump-at-nonzero-seq":   "epoch_seq",
	"branch-seq-gap-at-boundary":         "seq_gap",
	"branch-close-by-producer-duplicate": "close_duplicate",
	"branch-fork-suboffset-overshoot":    "fork_suboffset_overshoot",
	"branch-fence-accept":                "fence:accept",
	"branch-fence-marker":                "fence:marker",
	"branch-fence-sealed":                "fence:sealed",
	"branch-fence-epoch":                 "fence:epoch",
	"branch-fence-producer-required":     "fence:producer_required",
	"branch-fence-bound":                 "fence:bound",
	"branch-fence-superseded-grant":      "grant:superseded",
	"branch-fence-seal-already":          "seal:already",
}

// TestFuzzStoreEquivalenceCorpusReachesBranches replays every hand-named
// corpus fixture through the identical fuzz bridge with a tallying model and
// asserts it still reaches the branch it is named for. A generator change
// that redecodes the bitstream fails here loudly instead of silently turning
// the fixture into a plain regression input, so the corpus never goes
// vacuous; the fix is to re-harvest the named fixtures from a fuzz run.
func TestFuzzStoreEquivalenceCorpusReachesBranches(t *testing.T) {
	base := newTestStore(t)
	names := make([]string, 0, len(corpusBranches))
	for name := range corpusBranches {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			input := readFuzzCorpus(t, filepath.Join("testdata", "fuzz", "FuzzStoreEquivalence", name))
			tally := newFenceTally()
			// A replay that ends by exhausting its input mid-draw is reported
			// as a skip by the fuzz bridge; every step before that point has
			// run and tallied. The replay gets its own subtest so the skip
			// cannot swallow the assertion — which is the whole probe.
			if !t.Run("replay", func(t *testing.T) {
				rapid.MakeFuzz(func(t *rapid.T) {
					runEquivalenceModelWith(t, base, tally)
				})(t, input)
			}) {
				t.Fatal("replay failed")
			}
			tally.reached(t, corpusBranches[name])
		})
	}
}

// readFuzzCorpus decodes one `go test fuzz v1` corpus file holding a single
// []byte value: the raw bitstream the fuzz bridge feeds rapid.
func readFuzzCorpus(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read corpus fixture: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 2 || lines[0] != "go test fuzz v1" {
		t.Fatalf("corpus fixture %s is not a go test fuzz v1 single-value file", path)
	}
	lit := strings.TrimSuffix(strings.TrimPrefix(lines[1], "[]byte("), ")")
	s, err := strconv.Unquote(lit)
	if err != nil {
		t.Fatalf("corpus fixture %s: bad []byte literal: %v", path, err)
	}
	return []byte(s)
}

// fuzzWorkerOnce / fuzzClient / fuzzErr give the fuzz target its OWN one-time
// live-Redis setup, distinct from testStoreFor (newTestStore), because
// `go test -fuzz` runs many short-lived WORKER PROCESSES that all attach to the
// same live Redis DB. The integration setup FlushDB()s on first use — fine for a
// single test process, FATAL under fuzzing where one worker flushing the DB
// wipes the streams a concurrent worker is mid-comparison on (the FINDING this
// target surfaced: "oracle live, subject not found" on a no-TTL stream). So the
// fuzz setup deliberately does NOT flush; per-process keyspace isolation
// (eqWorkerTag) keeps workers from aliasing, and every path already carries the
// process-unique testRunStamp, so leftover keys never collide.
var (
	fuzzWorkerOnce sync.Once
	fuzzClient     *goredis.Client
	fuzzErr        error
)

// fuzzStore connects to live Redis WITHOUT flushing and tags this worker
// process's keyspace so concurrent fuzz workers stay isolated. It skips under
// -short and when Redis is unreachable, exactly like newTestStore.
func fuzzStore(f *testing.F) *Store {
	f.Helper()
	if testing.Short() {
		f.Skip("skipping live-Redis fuzz target in -short mode")
	}
	fuzzWorkerOnce.Do(func() {
		url := os.Getenv("REDIS_URL")
		if url == "" {
			url = "redis://localhost:6379/15"
		}
		opts, err := goredis.ParseURL(url)
		if err != nil {
			fuzzErr = err
			return
		}
		fuzzClient = goredis.NewClient(opts)
		if err := fuzzClient.Ping(context.Background()).Err(); err != nil {
			fuzzErr = fmt.Errorf("redis not reachable at %s: %w (run `docker compose up -d --wait redis`)", url, err)
			return
		}
		// Per-process keyspace tag: PID makes concurrent workers' paths disjoint
		// even if their testRunStamp collided. The leading 'w' keeps the path
		// segment a valid identifier and visibly distinct from the property
		// runner's untagged "/eq<stamp>/<n>" paths.
		eqWorkerTag = fmt.Sprintf("w%d", os.Getpid())
	})
	if fuzzErr != nil {
		f.Fatal(fuzzErr)
	}
	return New(fuzzClient, Options{})
}
