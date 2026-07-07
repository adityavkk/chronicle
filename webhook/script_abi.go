package webhook

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// scriptABI is the Go-owned Lua ABI. Lua comments are documentation only; this
// registry is the contract the Redis boundary validates before and after EVAL.
type scriptABI struct {
	Name     string
	File     string
	Keys     []scriptKeySchema
	Args     scriptArgSchema
	Statuses []string
}

type scriptKeySchema struct {
	Roles []scriptKeyRole
}

type scriptKeyRole string

type scriptArgSchema struct {
	Args       []scriptArg
	Variadic   *scriptVariadicArgs
	MinArgs    int
	CustomHint string
}

type scriptArg struct {
	Name string
	Kind scriptArgKind
}

type scriptVariadicArgs struct {
	AfterFixed int
	Group      []scriptArg
	Trailing   []scriptArg
}

type scriptArgKind string

const (
	argString   scriptArgKind = "string"
	argInt      scriptArgKind = "int"
	argUnixNS   scriptArgKind = "unix_ns"
	argBool01   scriptArgKind = "bool01"
	argDuration scriptArgKind = "duration"
)

func keys(roles ...scriptKeyRole) scriptKeySchema { return scriptKeySchema{Roles: roles} }

func exactArgs(args ...scriptArg) scriptArgSchema { return scriptArgSchema{Args: args} }

func variadicArgs(fixed []scriptArg, group []scriptArg, trailing []scriptArg) scriptArgSchema {
	return scriptArgSchema{
		Args:     fixed,
		MinArgs:  len(fixed) + len(trailing),
		Variadic: &scriptVariadicArgs{AfterFixed: len(fixed), Group: group, Trailing: trailing},
	}
}

func arg(name string, kind scriptArgKind) scriptArg { return scriptArg{Name: name, Kind: kind} }

// typedScript couples a redis.Script with its ABI and sole raw-reply decoder.
type typedScript[R any] struct {
	abi    scriptABI
	script *redis.Script
	decode func(scriptReply) (R, error)
}

func newTypedScript[R any](abi scriptABI, decode func(scriptReply) (R, error)) typedScript[R] {
	return typedScript[R]{abi: abi, script: loadScript(abi.File), decode: decode}
}

func (s typedScript[R]) run(ctx context.Context, c redis.Scripter, keys []string, args ...any) (R, error) {
	var zero R
	if err := s.abi.validateCall(keys, args); err != nil {
		return zero, err
	}
	raw, err := s.script.Run(ctx, c, keys, args...).Result()
	if err != nil {
		return zero, err
	}
	reply, err := decodeScriptReply(s.abi.Name, raw)
	if err != nil {
		return zero, err
	}
	out, err := s.decode(reply)
	if err != nil {
		return zero, fmt.Errorf("%s: %w", s.abi.Name, err)
	}
	return out, nil
}

func (a scriptABI) validateCall(keys []string, args []any) error {
	keyOK := false
	for _, schema := range a.Keys {
		if len(keys) == len(schema.Roles) {
			keyOK = true
			break
		}
	}
	if !keyOK {
		return fmt.Errorf("%s: wrong key arity %d", a.Name, len(keys))
	}
	if err := a.Args.validate(a.Name, args); err != nil {
		return err
	}
	return nil
}

func (s scriptArgSchema) validate(name string, args []any) error {
	if s.Variadic == nil {
		if len(args) != len(s.Args) {
			return fmt.Errorf("%s: wrong argv arity %d", name, len(args))
		}
		return validateArgKinds(name, s.Args, args)
	}
	if len(args) < s.MinArgs {
		return fmt.Errorf("%s: wrong argv arity %d", name, len(args))
	}
	middle := len(args) - s.Variadic.AfterFixed - len(s.Variadic.Trailing)
	if middle < 0 || len(s.Variadic.Group) == 0 || middle%len(s.Variadic.Group) != 0 {
		return fmt.Errorf("%s: argv arity %d does not match %s", name, len(args), s.CustomHint)
	}
	if err := validateArgKinds(name, s.Args, args[:s.Variadic.AfterFixed]); err != nil {
		return err
	}
	groups := middle / len(s.Variadic.Group)
	if s.Variadic.AfterFixed > 0 && s.Args[s.Variadic.AfterFixed-1].Kind == argInt {
		if n, err := parseArgInt(args[s.Variadic.AfterFixed-1]); err == nil && int(n) != groups {
			return fmt.Errorf("%s: %s=%d but %d variadic groups", name, s.Args[s.Variadic.AfterFixed-1].Name, n, groups)
		}
	}
	pos := s.Variadic.AfterFixed
	for i := 0; i < groups; i++ {
		if err := validateArgKinds(name, s.Variadic.Group, args[pos:pos+len(s.Variadic.Group)]); err != nil {
			return err
		}
		pos += len(s.Variadic.Group)
	}
	if err := validateArgKinds(name, s.Variadic.Trailing, args[pos:]); err != nil {
		return err
	}
	return nil
}

func validateArgKinds(script string, schema []scriptArg, args []any) error {
	for i, spec := range schema {
		if err := validateArgKind(spec.Kind, args[i]); err != nil {
			return fmt.Errorf("%s: argv %s: %w", script, spec.Name, err)
		}
	}
	return nil
}

func validateArgKind(kind scriptArgKind, v any) error {
	if v == nil {
		return fmt.Errorf("nil")
	}
	switch kind {
	case argString:
		if _, ok := v.(string); !ok {
			return fmt.Errorf("expected string, got %T", v)
		}
		return nil
	case argInt, argUnixNS, argDuration:
		_, err := parseArgInt(v)
		return err
	case argBool01:
		n, err := parseArgInt(v)
		if err != nil {
			return err
		}
		if n != 0 && n != 1 {
			return fmt.Errorf("expected 0 or 1, got %d", n)
		}
		return nil
	default:
		return fmt.Errorf("unknown kind %q", kind)
	}
}

func parseArgInt(v any) (int64, error) {
	switch x := v.(type) {
	case int:
		return int64(x), nil
	case int64:
		return x, nil
	case string:
		n, err := strconv.ParseInt(x, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse int %q: %w", x, err)
		}
		return n, nil
	default:
		return 0, fmt.Errorf("expected int, got %T", v)
	}
}

type scriptReply []any

func decodeScriptReply(name string, raw any) (scriptReply, error) {
	arr, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("%s: unexpected script reply %T", name, raw)
	}
	for i, e := range arr {
		if e == nil {
			return nil, fmt.Errorf("%s: nil reply element %d", name, i)
		}
		switch e.(type) {
		case string, int64, int:
		default:
			return nil, fmt.Errorf("%s: unexpected reply element %d: %T", name, i, e)
		}
	}
	return scriptReply(arr), nil
}

func (r scriptReply) wantArity(n int) error {
	if len(r) != n {
		return fmt.Errorf("wrong reply arity %d, want %d", len(r), n)
	}
	return nil
}

func (r scriptReply) stringAt(i int) (string, error) {
	if i < 0 || i >= len(r) {
		return "", fmt.Errorf("missing field %d", i)
	}
	s, ok := r[i].(string)
	if !ok {
		return "", fmt.Errorf("field %d: expected string, got %T", i, r[i])
	}
	return s, nil
}

func (r scriptReply) int64At(i int) (int64, error) {
	if i < 0 || i >= len(r) {
		return 0, fmt.Errorf("missing integer field %d", i)
	}
	switch v := r[i].(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case string:
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("field %d: parse int %q: %w", i, v, err)
		}
		return n, nil
	default:
		return 0, fmt.Errorf("field %d: expected integer, got %T", i, r[i])
	}
}

func (r scriptReply) intAt(i int) (int, error) {
	n, err := r.int64At(i)
	if err != nil {
		return 0, err
	}
	return int(n), nil
}

func (r scriptReply) nsAt(i int) (int64, error) {
	if i < 0 || i >= len(r) {
		return 0, fmt.Errorf("missing timestamp field %d", i)
	}
	switch v := r[i].(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case string:
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n, nil
		}
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, fmt.Errorf("field %d: parse timestamp %q: %w", i, v, err)
		}
		return int64(f), nil
	default:
		return 0, fmt.Errorf("field %d: expected timestamp, got %T", i, r[i])
	}
}

func decodeStatus(r scriptReply, statuses ...string) (string, error) {
	if len(r) == 0 {
		return "", fmt.Errorf("empty reply")
	}
	st, err := r.stringAt(0)
	if err != nil {
		return "", err
	}
	for _, allowed := range statuses {
		if st == allowed {
			return st, nil
		}
	}
	return "", fmt.Errorf("unexpected status %q", st)
}
