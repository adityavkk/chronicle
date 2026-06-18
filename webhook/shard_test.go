package webhook

import "testing"

func TestNewShardCount(t *testing.T) {
	for _, g := range []int{1, 2, 16, 256} {
		c, err := NewShardCount(g)
		if err != nil {
			t.Fatalf("NewShardCount(%d) error: %v", g, err)
		}
		if c.Value() != g {
			t.Errorf("Value()=%d, want %d", c.Value(), g)
		}
	}
	for _, g := range []int{0, -1, -16} {
		if _, err := NewShardCount(g); err == nil {
			t.Errorf("NewShardCount(%d) should error", g)
		}
	}
}

func TestShardIndexInRangeAndStable(t *testing.T) {
	const g = 16
	// In range, and deterministic (the client and chronicle must agree).
	for i := 0; i < 1000; i++ {
		e := "entity-" + itoa(i)
		idx := ShardIndex(e, g)
		if idx < 0 || idx >= g {
			t.Fatalf("ShardIndex(%q,%d)=%d out of range", e, g, idx)
		}
		if again := ShardIndex(e, g); again != idx {
			t.Fatalf("ShardIndex not deterministic for %q: %d != %d", e, idx, again)
		}
	}
	// G<=1 always shard 0 (the single per-type lease).
	if ShardIndex("anything", 1) != 0 || ShardIndex("x", 0) != 0 {
		t.Fatal("G<=1 must always map to shard 0")
	}
}

func TestShardIndexSpread(t *testing.T) {
	// The fix relies on roughly uniform hashing; assert no shard is starved or
	// overloaded across many entities.
	const g, n = 16, 8000
	counts := make([]int, g)
	for i := 0; i < n; i++ {
		counts[ShardIndex("e-"+itoa(i), g)]++
	}
	exp := n / g
	for s, c := range counts {
		if c < exp/2 || c > exp*2 {
			t.Errorf("shard %d got %d (expected ~%d) — badly skewed hash", s, c, exp)
		}
	}
}

func TestShardCountShard(t *testing.T) {
	c := MustShardCount(16)
	k := c.Shard("agent-handler", "entity-42")
	if k.SubID != "agent-handler" {
		t.Errorf("SubID=%q", k.SubID)
	}
	if k.Index != ShardIndex("entity-42", 16) {
		t.Errorf("Index=%d, want %d", k.Index, ShardIndex("entity-42", 16))
	}
}

func TestShardCountShardAt(t *testing.T) {
	c := MustShardCount(16)
	if _, err := c.ShardAt("s", 0); err != nil {
		t.Errorf("ShardAt 0 should be valid: %v", err)
	}
	if k, err := c.ShardAt("s", 15); err != nil || k.Index != 15 {
		t.Errorf("ShardAt 15 = %+v, %v", k, err)
	}
	for _, bad := range []int{-1, 16, 99} {
		if _, err := c.ShardAt("s", bad); err == nil {
			t.Errorf("ShardAt(%d) should error for G=16", bad)
		}
	}
}

func TestMustShardCountPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("MustShardCount(0) should panic")
		}
	}()
	MustShardCount(0)
}

// itoa avoids strconv in this test file's tight loops; small non-negative ints.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}
