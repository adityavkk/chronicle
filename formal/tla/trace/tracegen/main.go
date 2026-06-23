// Command tracegen converts a JSONL fence trace (produced by the subtrace seam,
// issue #39) into a generated TLA+ module TraceData.tla that Trace.tla reads as a
// constant. Each subscription's lifecycle is emitted as a separate TraceLog
// sequence (the spec models per-subscription fences as independent, README
// "the model never couples two subs"), so one TraceData module per sub keeps each
// TLC validation a single-subscription DFS.
//
// The numeric-wake mapping (research/01 grain note): the spec mints wake_id as a
// per-sub counter that moves in lockstep with the generation (a fresh wake on
// every rotate, reuse on coalesce). The real wake_id is an opaque string, but the
// fence only ever compares (gen, wake) for EQUALITY, so we map each distinct real
// wake to the model generation that was current when it became the fence wake.
// That preserves every fence decision the trace observed (a FENCED ack still has
// reqWake != cur.wake), without inventing a numeric meaning the protocol lacks.
//
// Usage:
//
//	tracegen -in trace.jsonl -outdir formal/tla/trace -prefix T39
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type rec struct {
	Seq       int    `json:"seq"`
	Sub       string `json:"sub"`
	Op        string `json:"op"`
	LuaStatus string `json:"luaStatus"`
	Args      struct {
		Worker   string `json:"worker"`
		ReqGen   int64  `json:"reqGen"`
		ReqWake  string `json:"reqWake"`
		TokenGen int64  `json:"tokenGen"`
		Done     bool   `json:"done"`
		ArmLease bool   `json:"armLease"`
		WakeID   string `json:"wakeId"`
	} `json:"args"`
	PreState  state `json:"preState"`
	PostState state `json:"postState"`
}

type state struct {
	Exists       bool   `json:"exists"`
	Phase        string `json:"phase"`
	Generation   int64  `json:"generation"`
	WakeID       string `json:"wakeId"`
	LeaseUntilNs int64  `json:"leaseUntilNs"`
	Holder       bool   `json:"holder"`
	HolderWorker string `json:"holderWorker"`
	WakeSentNs   int64  `json:"wakeSentNs"`
	Dispatch     string `json:"dispatch"`
}

func main() {
	in := flag.String("in", "trace.jsonl", "JSONL trace input")
	outdir := flag.String("outdir", ".", "output dir for TraceData_*.tla")
	prefix := flag.String("prefix", "T39", "module name prefix")
	flag.Parse()

	bySub, order, err := load(*in)
	if err != nil {
		fmt.Fprintln(os.Stderr, "tracegen:", err)
		os.Exit(1)
	}
	if err := os.MkdirAll(*outdir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "tracegen:", err)
		os.Exit(1)
	}
	// Each scenario lives in its own subdirectory as module `TraceData` (TLA+
	// requires filename == module name, and Trace.tla EXTENDS TraceData), so the
	// runner points TLC at <outdir>/<scenario>/ with a copy of Trace.tla + the cfg.
	var scenarios []string
	for _, sub := range order {
		scen := sanitize(lastSeg(sub))
		recs := bySub[sub]
		body, maxGen, maxWorkers := emit("TraceData", recs)
		dir := filepath.Join(*outdir, scen)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			fmt.Fprintln(os.Stderr, "tracegen:", err)
			os.Exit(1)
		}
		path := filepath.Join(dir, "TraceData.tla")
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "tracegen:", err)
			os.Exit(1)
		}
		scenarios = append(scenarios, scen)
		fmt.Printf("wrote %s (%d lines, maxGen=%d, workers=%d)\n", path, len(recs), maxGen, maxWorkers)
	}
	idx := strings.Join(scenarios, "\n") + "\n"
	_ = os.WriteFile(filepath.Join(*outdir, *prefix+"_INDEX.txt"), []byte(idx), 0o644)
}

func load(path string) (map[string][]rec, []string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	bySub := map[string][]rec{}
	var order []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var r rec
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			return nil, nil, fmt.Errorf("line %q: %w", line, err)
		}
		if _, ok := bySub[r.Sub]; !ok {
			order = append(order, r.Sub)
		}
		bySub[r.Sub] = append(bySub[r.Sub], r)
	}
	return bySub, order, sc.Err()
}

// wakeNum maps a real wake_id string to the model generation that was current
// when it became the fence wake. The fence only compares (gen,wake) for equality,
// so this preserves every fence decision while giving the spec a Nat-valued wake.
// A request wake that never matched the current fence (a stale token) maps to a
// distinct sentinel below current gen so Fenced() fires exactly as observed.
func buildWakeMap(recs []rec) (map[string]int64, int64) {
	wm := map[string]int64{}
	var maxGen int64
	for _, r := range recs {
		if r.PostState.Generation > maxGen {
			maxGen = r.PostState.Generation
		}
		// The post-state wake is, by construction, current at the post generation.
		if r.PostState.Exists && r.PostState.WakeID != "" {
			if _, ok := wm[r.PostState.WakeID]; !ok {
				wm[r.PostState.WakeID] = r.PostState.Generation
			}
		}
		if r.PreState.Exists && r.PreState.WakeID != "" {
			if _, ok := wm[r.PreState.WakeID]; !ok {
				wm[r.PreState.WakeID] = r.PreState.Generation
			}
		}
	}
	return wm, maxGen
}

func emit(name string, recs []rec) (string, int64, int) {
	wm, maxGen := buildWakeMap(recs)
	// Canonicalize workers to w1..wN in first-seen order.
	wmap := map[string]int{}
	var wseen []string
	for _, r := range recs {
		if w := r.Args.Worker; w != "" {
			if _, ok := wmap[w]; !ok {
				wmap[w] = len(wseen) + 1
				wseen = append(wseen, w)
			}
		}
	}
	if len(wmap) == 0 {
		// Even worker-less traces (arm/expire only) need at least one worker in scope.
		wmap["_"] = 1
		wseen = []string{"_"}
	}
	worker := func(w string) string {
		if w == "" {
			return "NoWorker"
		}
		return fmt.Sprintf("\"w%d\"", wmap[w])
	}
	// ack/release carry no worker on the wire (they are fenced by token, not by
	// id), but the spec performs an ack BY the worker holding the matching token.
	// Attribute each ack/release to the worker whose most recent CLAIMED grant
	// minted the (gen) the request carries; an unmatched request (a deposed/stale
	// token — a FENCED ack) is left worker-less, which is exactly the spec's
	// no-op (no worker holds an ack-acceptable token).
	claimedGen := map[int64]string{} // gen -> worker who was granted it
	ackWorker := func(r rec) string {
		if r.Op != "ack" && r.Op != "release" {
			return r.Args.Worker
		}
		if w, ok := claimedGen[r.Args.TokenGen]; ok {
			return w
		}
		return ""
	}
	wakeOf := func(s string) int64 {
		if s == "" {
			return 0
		}
		if g, ok := wm[s]; ok {
			return g
		}
		return 0
	}

	var b strings.Builder
	fmt.Fprintf(&b, "---- MODULE %s ----\n", name)
	fmt.Fprintf(&b, "(* GENERATED by tracegen (issue #39) — do not edit by hand. *)\n")
	fmt.Fprintf(&b, "(* One subscription's fence trace, mapped to the SubscriptionFence model. *)\n")
	fmt.Fprintf(&b, "EXTENDS Naturals, Sequences\n\n")
	fmt.Fprintf(&b, "(* The worker-less sentinel for ack/release lines whose token holder is gone\n")
	fmt.Fprintf(&b, "   (a deposed/stale-token FENCED op). Matches SubscriptionFence's NoWorker. *)\n")
	fmt.Fprintf(&b, "NoWorker == \"none\"\n\n")
	fmt.Fprintf(&b, "(* The traced workers, canonicalized to w1..wN. *)\n")
	var wsyms []string
	for i := range wseen {
		wsyms = append(wsyms, fmt.Sprintf("\"w%d\"", i+1))
	}
	fmt.Fprintf(&b, "TraceWorkers == { %s }\n\n", strings.Join(wsyms, ", "))
	fmt.Fprintf(&b, "TraceMaxGen == %d\n\n", maxGen)
	fmt.Fprintf(&b, "(* The recorded fence linearization points, in order. Each record is one\n")
	fmt.Fprintf(&b, "   single-slot Lua commit = one spec action; wake is the model generation\n")
	fmt.Fprintf(&b, "   that minted the real wake_id (equality-preserving). *)\n")
	fmt.Fprintf(&b, "TraceLog == <<\n")
	for i, r := range recs {
		comma := ","
		if i == len(recs)-1 {
			comma = ""
		}
		done := "FALSE"
		if r.Args.Done {
			done = "TRUE"
		}
		armLease := "FALSE"
		if r.Args.ArmLease {
			armLease = "TRUE"
		}
		dispatch := "pullwake"
		if r.PostState.Dispatch == "webhook" || r.PreState.Dispatch == "webhook" {
			dispatch = "webhook"
		}
		// Record the worker granted this generation, for later ack/release attribution.
		if r.Op == "claim" && r.LuaStatus == "CLAIMED" && r.Args.Worker != "" {
			claimedGen[r.PostState.Generation] = r.Args.Worker
		}
		fmt.Fprintf(&b,
			"  [ op |-> \"%s\", status |-> \"%s\", worker |-> %s,\n"+
				"    reqGen |-> %d, tokGen |-> %d, reqWake |-> %d, done |-> %s, armLease |-> %s, dispatch |-> \"%s\",\n"+
				"    preGen |-> %d, postGen |-> %d, preWake |-> %d, postWake |-> %d,\n"+
				"    prePhase |-> \"%s\", postPhase |-> \"%s\", preLease |-> %d, postLease |-> %d ]%s\n",
			r.Op, r.LuaStatus, worker(ackWorker(r)),
			r.Args.ReqGen, r.Args.TokenGen, wakeOf(r.Args.ReqWake), done, armLease, dispatch,
			r.PreState.Generation, r.PostState.Generation, wakeOf(r.PreState.WakeID), wakeOf(r.PostState.WakeID),
			phase(r.PreState), phase(r.PostState), boolToInt(r.PreState.LeaseUntilNs != 0), boolToInt(r.PostState.LeaseUntilNs != 0),
			comma,
		)
	}
	fmt.Fprintf(&b, ">>\n")
	fmt.Fprintf(&b, "====\n")
	return b.String(), maxGen, len(wseen)
}

func phase(s state) string {
	if !s.Exists {
		return "nosub"
	}
	if s.Phase == "" {
		return "idle"
	}
	return s.Phase
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func lastSeg(s string) string {
	parts := strings.Split(s, "-")
	return parts[len(parts)-1]
}

func sanitize(s string) string {
	var b strings.Builder
	for _, c := range s {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			b.WriteRune(c)
		} else {
			b.WriteRune('_')
		}
	}
	out := b.String()
	if out == "" {
		out = "x"
	}
	return out
}

var _ = sort.Strings
