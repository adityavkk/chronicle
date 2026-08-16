package webhook

import (
	"fmt"
	"io/fs"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func TestScriptReplyDecodersRejectMalformedReplies(t *testing.T) {
	cases := []struct {
		name   string
		decode func(scriptReply) (any, error)
		raw    []any
	}{
		{
			name: "arm nil generation",
			decode: func(r scriptReply) (any, error) {
				return decodeArmWakeReply(r)
			},
			raw: []any{"ARMED", nil, "wake-1"},
		},
		{
			name: "claim g>0 grant nil generation",
			decode: func(r scriptReply) (any, error) {
				return decodeClaimReply(r)
			},
			raw: []any{"CLAIMED", nil, "wake-1", "worker-1"},
		},
		{
			name: "claim g>0 busy nil generation",
			decode: func(r scriptReply) (any, error) {
				return decodeClaimReply(r)
			},
			raw: []any{"BUSY", nil, "", "worker-1"},
		},
		{
			name: "unknown status",
			decode: func(r scriptReply) (any, error) {
				return decodeArmWakeReply(r)
			},
			raw: []any{"ARMDD", "1", "wake-1"},
		},
		{
			name: "wrong armed arity",
			decode: func(r scriptReply) (any, error) {
				return decodeArmWakeReply(r)
			},
			raw: []any{"ARMED", "1"},
		},
		{
			name: "bad generation int",
			decode: func(r scriptReply) (any, error) {
				return decodeArmWakeReply(r)
			},
			raw: []any{"ARMED", "not-an-int", "wake-1"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := decodeScriptReply("test", tc.raw)
			if err == nil {
				_, err = tc.decode(r)
			}
			if err == nil {
				t.Fatal("malformed reply decoded without error")
			}
		})
	}
}

func TestScriptReplyDecodersKeepValidPayloads(t *testing.T) {
	r, err := decodeArmWakeReply(scriptReply{"ARMED", "42", "wake-1"})
	if err != nil {
		t.Fatalf("decode armed: %v", err)
	}
	armed, ok := r.(armWakeArmed)
	if !ok || armed.Generation != 42 || armed.WakeID != "wake-1" {
		t.Fatalf("decode armed = %#v", r)
	}

	c, err := decodeClaimReply(scriptReply{"CLAIMED", "7", "wake-2", "worker-1"})
	if err != nil {
		t.Fatalf("decode claim: %v", err)
	}
	claimed, ok := c.(claimClaimed)
	if !ok || claimed.Generation != 7 || claimed.WakeID != "wake-2" || claimed.Holder != "worker-1" {
		t.Fatalf("decode claim = %#v", c)
	}
	forbidden, err := decodeClaimReply(scriptReply{"FORBIDDEN"})
	if err != nil {
		t.Fatalf("decode forbidden claim: %v", err)
	}
	if _, ok := forbidden.(claimForbidden); !ok {
		t.Fatalf("decode forbidden claim = %#v", forbidden)
	}
}

func TestScriptABIRejectsWrongCallArity(t *testing.T) {
	validArmKeys := armWakeKeyVec{Sub: "sub", LeaseZSet: "lease", DueZSet: "due"}
	if err := armWakeScript.abi.validateCall(validArmKeys, []any{"id", "1700000000", "1000", "1", "wake", "replica", "1"}); err != nil {
		t.Fatalf("valid arm_wake call rejected: %v", err)
	}
	if err := armWakeScript.abi.validateCall(armWakeKeyVec{Sub: "sub", LeaseZSet: "lease"}, []any{"id", "1700000000", "1000", "1", "wake", "replica", "1"}); err == nil {
		t.Fatal("arm_wake with missing due key accepted")
	}
	if err := armWakeScript.abi.validateCall(validArmKeys, []any{"id", "1700000000"}); err == nil {
		t.Fatal("arm_wake with too few ARGV accepted")
	}
	ackKeys := ackKeyVec{ShardState: "shard", Links: "links", LeaseZSet: "lease", RetryZSet: "retry", DueZSet: "due", SubConfig: "sub"}
	if err := ackScript.abi.validateCall(ackKeys, []any{"member", "1", "wake", "1", "1", "1700000000", "1000", "1", "path-only", "replica", "1"}); err == nil {
		t.Fatal("ack with incomplete variadic ack pair accepted")
	}
	claimKeys := claimKeyVec{
		SubConfig: "sub", ShardState: "shard", LeaseZSet: "lease",
		IncarnationCounter: "inc", ShardRegistry: "registry", Links: "links",
	}
	validClaimArgs := []any{
		"member", "worker", "1700000000", "1000", "wake", "0",
		"owner", "incarnation", "cfg-hash", "1", "events/a",
	}
	if err := claimScript.abi.validateCall(claimKeys, validClaimArgs); err != nil {
		t.Fatalf("valid claim call rejected: %v", err)
	}
	if err := claimScript.abi.validateCall(claimKeys, validClaimArgs[:10]); err == nil {
		t.Fatal("claim with missing expected path accepted")
	}
}

func TestAllProductionLuaScriptsAreRegistered(t *testing.T) {
	registered := map[string]struct{}{}
	for _, reg := range registeredScripts {
		registered[reg.abi.File] = struct{}{}
	}
	if err := fs.WalkDir(scriptFS, "scripts", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".lua") {
			return err
		}
		name := strings.TrimPrefix(path, "scripts/")
		switch name {
		case "common.lua", "probe_predicates.lua":
			return nil
		}
		if _, ok := registered[name]; !ok {
			t.Fatalf("%s is not in the typed script registry", name)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRegisteredScriptABIMatchesLuaReferences(t *testing.T) {
	for _, reg := range registeredScripts {
		abi := reg.abi
		t.Run(abi.Name, func(t *testing.T) {
			bodyBytes, err := scriptFS.ReadFile("scripts/" + abi.File)
			if err != nil {
				t.Fatal(err)
			}
			body := string(bodyBytes)

			if got, want := keyRoleAliases(body), maxKeyRoles(abi.Keys); !reflect.DeepEqual(got, want) {
				t.Fatalf("Lua key role aliases = %v, registry roles = %v", got, want)
			}
			if got, want := argAliases(body), declaredArgAliases(abi.Args); !reflect.DeepEqual(got, want) {
				t.Fatalf("Lua argv aliases = %v, registry args = %v", got, want)
			}
			assertVariadicLayout(t, body, abi.Args)
			if got := maxIndexedReference(body, "ARGV"); got > maxDeclaredArgReference(abi.Args) {
				t.Fatalf("ARGV max reference = %d exceeds registry declaration %d", got, maxDeclaredArgReference(abi.Args))
			}

			gotStatuses := returnedStatusSet(body)
			wantStatuses := variantStatusSet(reg.variants)
			if !reflect.DeepEqual(gotStatuses, wantStatuses) {
				t.Fatalf("returned statuses = %v, declared statuses = %v", gotStatuses, wantStatuses)
			}
			if len(wantStatuses) > 0 {
				gotShapes := returnedPayloadShapes(body)
				wantShapes := variantPayloadShapes(reg.variants)
				if !reflect.DeepEqual(gotShapes, wantShapes) {
					t.Fatalf("returned payload shapes = %v, declared shapes = %v", gotShapes, wantShapes)
				}
			}
		})
	}
}

func TestVariadicLayoutRejectsStrideAndTrailingDrift(t *testing.T) {
	bodyBytes, err := scriptFS.ReadFile("scripts/ack.lua")
	if err != nil {
		t.Fatal(err)
	}
	body := string(bodyBytes)
	schema := ackScript.abi.Args
	if err := checkVariadicLayout(body, schema); err != nil {
		t.Fatalf("ack layout should be valid: %v", err)
	}
	badStride := strings.Replace(body, "i = i + 2", "i = i + 3", 1)
	if err := checkVariadicLayout(badStride, schema); err == nil {
		t.Fatal("ack layout with i = i + 3 passed")
	}
	badTrailing := strings.Replace(body,
		"owner_fenced(k_slot, ARGV[#ARGV - 1], ARGV[#ARGV])",
		"owner_fenced(k_slot, ARGV[#ARGV], ARGV[#ARGV - 1])", 1)
	if err := checkVariadicLayout(badTrailing, schema); err == nil {
		t.Fatal("ack layout with swapped trailing owner args passed")
	}
}

func maxKeyRoles(schemas []scriptKeySchema) []scriptKeyRole {
	var out []scriptKeyRole
	for _, schema := range schemas {
		if len(schema.Roles) > len(out) {
			out = schema.Roles
		}
	}
	return out
}

func keyRoleAliases(lua string) []scriptKeyRole {
	re := regexp.MustCompile(`(?m)^local k_([a-z0-9_]+) = KEYS\[(\d+)\]$`)
	matches := re.FindAllStringSubmatch(lua, -1)
	out := make([]scriptKeyRole, len(matches))
	for _, m := range matches {
		n, err := strconv.Atoi(m[2])
		if err != nil {
			testPanic(err)
		}
		if n < 1 || n > len(out) {
			testPanic(fmt.Errorf("key alias index %d outside 1..%d", n, len(out)))
		}
		out[n-1] = scriptKeyRole(m[1])
	}
	return out
}

func declaredArgAliases(schema scriptArgSchema) []string {
	out := make([]string, len(schema.Args))
	for i, arg := range schema.Args {
		out[i] = arg.Name
	}
	return out
}

func argAliases(lua string) []string {
	re := regexp.MustCompile(`(?m)^local a_([a-z0-9_]+) = ARGV\[(\d+)\]$`)
	matches := re.FindAllStringSubmatch(lua, -1)
	out := make([]string, len(matches))
	for _, m := range matches {
		n, err := strconv.Atoi(m[2])
		if err != nil {
			testPanic(err)
		}
		if n < 1 || n > len(out) {
			testPanic(fmt.Errorf("argv alias index %d outside 1..%d", n, len(out)))
		}
		out[n-1] = m[1]
	}
	return out
}

func assertVariadicLayout(t *testing.T, lua string, schema scriptArgSchema) {
	t.Helper()
	if err := checkVariadicLayout(lua, schema); err != nil {
		t.Fatal(err)
	}
}

func checkVariadicLayout(lua string, schema scriptArgSchema) error {
	if schema.Variadic == nil {
		return nil
	}
	baseRe := regexp.MustCompile(`(?m)^local i = (\d+)$`)
	m := baseRe.FindStringSubmatch(lua)
	if len(m) != 2 {
		return fmt.Errorf("variadic script has no checked base index")
	}
	base, err := strconv.Atoi(m[1])
	if err != nil {
		return err
	}
	if want := schema.Variadic.AfterFixed + 1; base != want {
		return fmt.Errorf("variadic base = %d, want %d", base, want)
	}

	strideRe := regexp.MustCompile(`(?m)^\s*i = i \+ (\d+)$`)
	strides := strideRe.FindAllStringSubmatch(lua, -1)
	if len(strides) != 1 {
		return fmt.Errorf("variadic script has %d loop strides, want 1", len(strides))
	}
	stride, err := strconv.Atoi(strides[0][1])
	if err != nil {
		return err
	}
	if want := len(schema.Variadic.Group); stride != want {
		return fmt.Errorf("variadic stride = %d, want %d", stride, want)
	}

	gotOffsets := variadicOffsets(lua)
	wantOffsets := make([]int, len(schema.Variadic.Group))
	for i := range wantOffsets {
		wantOffsets[i] = i
	}
	if !reflect.DeepEqual(gotOffsets, wantOffsets) {
		return fmt.Errorf("variadic ARGV offsets = %v, want %v", gotOffsets, wantOffsets)
	}

	gotTrailing, err := trailingNamesFromOffsets(trailingARGVOffsets(lua), schema)
	if err != nil {
		return err
	}
	if wantTrailing := trailingArgNames(schema); !reflect.DeepEqual(gotTrailing, wantTrailing) {
		return fmt.Errorf("trailing #ARGV args = %v, want %v", gotTrailing, wantTrailing)
	}
	return nil
}

func variadicOffsets(lua string) []int {
	re := regexp.MustCompile(`ARGV\[i(?: \+ (\d+))?\]`)
	seen := map[int]struct{}{}
	for _, m := range re.FindAllStringSubmatch(lua, -1) {
		off := 0
		if m[1] != "" {
			var err error
			off, err = strconv.Atoi(m[1])
			if err != nil {
				testPanic(err)
			}
		}
		seen[off] = struct{}{}
	}
	out := make([]int, 0, len(seen))
	for off := range seen {
		out = append(out, off)
	}
	slices.Sort(out)
	return out
}

func trailingARGVOffsets(lua string) []int {
	re := regexp.MustCompile(`ARGV\[#ARGV(?: - (\d+))?\]`)
	var out []int
	for _, m := range re.FindAllStringSubmatch(lua, -1) {
		if m[1] == "" {
			out = append(out, 0)
			continue
		}
		n, err := strconv.Atoi(m[1])
		if err != nil {
			testPanic(err)
		}
		out = append(out, -n)
	}
	return out
}

func trailingNamesFromOffsets(offsets []int, schema scriptArgSchema) ([]string, error) {
	if schema.Variadic == nil {
		return nil, nil
	}
	out := make([]string, len(offsets))
	n := len(schema.Variadic.Trailing)
	for i, off := range offsets {
		idx := n - 1 + off
		if idx < 0 || idx >= n {
			return nil, fmt.Errorf("trailing #ARGV offset %d outside %d trailing args", off, n)
		}
		out[i] = schema.Variadic.Trailing[idx].Name
	}
	return out, nil
}

func trailingArgNames(schema scriptArgSchema) []string {
	if schema.Variadic == nil {
		return nil
	}
	out := make([]string, len(schema.Variadic.Trailing))
	for i, arg := range schema.Variadic.Trailing {
		out[i] = arg.Name
	}
	return out
}

func maxDeclaredArgReference(schema scriptArgSchema) int {
	if schema.Variadic == nil {
		return len(schema.Args)
	}
	return schema.MinArgs
}

func maxIndexedReference(lua, name string) int {
	re := regexp.MustCompile(regexp.QuoteMeta(name) + `\[(\d+)\]`)
	max := 0
	for _, m := range re.FindAllStringSubmatch(lua, -1) {
		n, err := strconv.Atoi(m[1])
		if err != nil {
			testPanic(err)
		}
		if n > max {
			max = n
		}
	}
	return max
}

func returnedStatusSet(lua string) map[string]struct{} {
	out := map[string]struct{}{}
	returnRe := regexp.MustCompile(`return\s*\{([^\n}]*)\}`)
	quoteRe := regexp.MustCompile(`'([^']*)'`)
	for _, ret := range returnRe.FindAllStringSubmatch(lua, -1) {
		first := ret[1]
		if i := strings.Index(first, ","); i >= 0 {
			first = first[:i]
		}
		for _, q := range quoteRe.FindAllStringSubmatch(first, -1) {
			status := q[1]
			if status == "" {
				continue
			}
			out[status] = struct{}{}
		}
	}
	return out
}

func returnedPayloadShapes(lua string) map[string][]replyFieldKind {
	out := map[string][]replyFieldKind{}
	returnRe := regexp.MustCompile(`return\s*\{([^\n}]*)\}`)
	quoteRe := regexp.MustCompile(`'([^']*)'`)
	for _, ret := range returnRe.FindAllStringSubmatch(lua, -1) {
		parts := splitLuaReturn(ret[1])
		if len(parts) == 0 {
			continue
		}
		statuses := quoteRe.FindAllStringSubmatch(parts[0], -1)
		if len(statuses) == 0 {
			continue
		}
		shape := make([]replyFieldKind, 0, len(parts)-1)
		for _, part := range parts[1:] {
			shape = append(shape, luaReplyFieldKind(part))
		}
		for _, st := range statuses {
			out[st[1]] = shape
		}
	}
	return out
}

func splitLuaReturn(s string) []string {
	var out []string
	depth := 0
	start := 0
	inQuote := false
	for i, r := range s {
		switch r {
		case '\'':
			inQuote = !inQuote
		case '(':
			if !inQuote {
				depth++
			}
		case ')':
			if !inQuote && depth > 0 {
				depth--
			}
		case ',':
			if !inQuote && depth == 0 {
				out = append(out, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	out = append(out, strings.TrimSpace(s[start:]))
	return out
}

func luaReplyFieldKind(expr string) replyFieldKind {
	expr = strings.TrimSpace(expr)
	if strings.HasPrefix(expr, "tonumber(") {
		return replyInteger
	}
	if strings.Contains(expr, "lease_expiry_ns") || strings.Contains(expr, "until_ns") || strings.Contains(expr, "first") {
		return replyNS
	}
	if strings.Contains(expr, "gen") || strings.Contains(expr, "generation") || strings.Contains(expr, "retry_count") || strings.Contains(expr, "owner_epoch") || strings.Contains(expr, "epoch") {
		return replyInteger
	}
	return replyString
}

func variantPayloadShapes(variants []replyVariant) map[string][]replyFieldKind {
	out := map[string][]replyFieldKind{}
	for _, v := range variants {
		if strings.HasPrefix(v.Status, "<") {
			continue
		}
		out[v.Status] = append([]replyFieldKind(nil), v.Fields...)
		if out[v.Status] == nil {
			out[v.Status] = []replyFieldKind{}
		}
	}
	return out
}

func variantStatusSet(variants []replyVariant) map[string]struct{} {
	out := map[string]struct{}{}
	for _, v := range variants {
		if strings.HasPrefix(v.Status, "<") {
			continue
		}
		out[v.Status] = struct{}{}
	}
	return out
}

func testPanic(err error) {
	if err != nil {
		panic(err)
	}
}
