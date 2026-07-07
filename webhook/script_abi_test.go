package webhook

import (
	"io/fs"
	"reflect"
	"regexp"
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
}

func TestScriptABIRejectsWrongCallArity(t *testing.T) {
	if err := armWakeScript.abi.validateCall([]string{"sub", "lease", "due"}, []any{"id", "1700000000", "1000", "1", "wake", "replica", "1"}); err != nil {
		t.Fatalf("valid arm_wake call rejected: %v", err)
	}
	if err := armWakeScript.abi.validateCall([]string{"sub", "lease"}, []any{"id", "1700000000", "1000", "1", "wake", "replica", "1"}); err == nil {
		t.Fatal("arm_wake with too few KEYS accepted")
	}
	if err := armWakeScript.abi.validateCall([]string{"sub", "lease", "due"}, []any{"id", "1700000000"}); err == nil {
		t.Fatal("arm_wake with too few ARGV accepted")
	}
	if err := ackScript.abi.validateCall([]string{"shard", "links", "lease", "retry", "due", "sub"}, []any{"member", "1", "wake", "1", "1", "1700000000", "1000", "1", "path-only", "replica", "1"}); err == nil {
		t.Fatal("ack with incomplete variadic ack pair accepted")
	}
}

func TestAllProductionLuaScriptsAreRegistered(t *testing.T) {
	registered := map[string]struct{}{}
	for _, abi := range registeredScripts {
		registered[abi.File] = struct{}{}
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
	for _, abi := range registeredScripts {
		t.Run(abi.Name, func(t *testing.T) {
			bodyBytes, err := scriptFS.ReadFile("scripts/" + abi.File)
			if err != nil {
				t.Fatal(err)
			}
			body := string(bodyBytes)

			if got, want := maxIndexedReference(body, "KEYS"), maxKeyArity(abi.Keys); got != want {
				t.Fatalf("KEYS max reference = %d, registry max arity = %d", got, want)
			}
			if got := maxIndexedReference(body, "ARGV"); got > maxDeclaredArgReference(abi.Args) {
				t.Fatalf("ARGV max reference = %d exceeds registry declaration %d", got, maxDeclaredArgReference(abi.Args))
			}

			gotStatuses := returnedStatusSet(body)
			wantStatuses := setOf(abi.Statuses)
			if !reflect.DeepEqual(gotStatuses, wantStatuses) {
				t.Fatalf("returned statuses = %v, registry statuses = %v", gotStatuses, wantStatuses)
			}
		})
	}
}

func maxKeyArity(schemas []scriptKeySchema) int {
	max := 0
	for _, schema := range schemas {
		if len(schema.Roles) > max {
			max = len(schema.Roles)
		}
	}
	return max
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

func setOf(values []string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, v := range values {
		out[v] = struct{}{}
	}
	return out
}

func testPanic(err error) {
	if err != nil {
		panic(err)
	}
}
