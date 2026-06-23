package redis

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
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
