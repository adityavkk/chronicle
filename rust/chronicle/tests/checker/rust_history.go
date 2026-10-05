package main

// This file adapts the Rust history generator's JSONL directly to the
// offline checker's Porcupine seam. It intentionally models only stream
// create/append/read/delete; cluster placement and the HTTP transport are not
// part of this offline check.

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/anishathalye/porcupine"
)

type rustProducer struct {
	ID         string
	Epoch, Seq uint64
}
type rustConfig struct {
	ContentType string
	ExpiresMS   *uint64
}
type rustInput struct {
	tenant, path string
	kind         string
	data         []byte
	incarnation  uint64
	producer     *rustProducer
	close        bool
	expected     *uint64
	config       rustConfig
}
type rustOutput struct {
	unknown          bool
	legacy           bool
	error            string
	end, incarnation uint64
	duplicate        bool
	data             []byte
	closed           bool
}
type rustProducerState struct {
	epoch, seq uint64
	results    map[uint64]uint64
}
type rustStream struct {
	exists, deleted, closed bool
	incarnation             uint64
	config                  rustConfig
	data                    []byte
	producers               map[string]rustProducerState
}

func (s rustStream) clone() rustStream {
	n := s
	n.data = append([]byte(nil), s.data...)
	n.producers = make(map[string]rustProducerState, len(s.producers))
	for k, p := range s.producers {
		q := p
		q.results = make(map[uint64]uint64, len(p.results))
		for seq, end := range p.results {
			q.results[seq] = end
		}
		n.producers[k] = q
	}
	return n
}

func rustModel() porcupine.Model {
	nm := porcupine.NondeterministicModel{
		Partition: func(h []porcupine.Operation) [][]porcupine.Operation {
			m := map[string][]porcupine.Operation{}
			order := []string{}
			for _, op := range h {
				in := op.Input.(rustInput)
				k := in.tenant + "\x00" + in.path
				if _, ok := m[k]; !ok {
					order = append(order, k)
				}
				m[k] = append(m[k], op)
			}
			out := make([][]porcupine.Operation, 0, len(order))
			for _, k := range order {
				out = append(out, m[k])
			}
			return out
		},
		Init:  func() []interface{} { return []interface{}{rustStream{producers: map[string]rustProducerState{}}} },
		Step:  rustStep,
		Equal: func(a, b interface{}) bool { return reflect.DeepEqual(a, b) },
		DescribeOperation: func(i, o interface{}) string {
			in := i.(rustInput)
			out := o.(rustOutput)
			return fmt.Sprintf("%s %s/%s => err=%s end=%d inc=%d unknown=%v", in.kind, in.tenant, in.path, out.error, out.end, out.incarnation, out.unknown)
		},
		DescribeState: func(s interface{}) string {
			x := s.(rustStream)
			return fmt.Sprintf("inc=%d exists=%v deleted=%v closed=%v bytes=%q", x.incarnation, x.exists, x.deleted, x.closed, x.data)
		},
	}
	return nm.ToModel()
}

func rustStep(state, input, output interface{}) []interface{} {
	s, in, out := state.(rustStream), input.(rustInput), output.(rustOutput)
	if out.unknown {
		if in.kind == "read" {
			return []interface{}{s}
		}
		// A transport-unknown mutation may have taken effect or not.  Its
		// operation interval is extended to history end by the parser.
		return append(rustApply(s, in), s)
	}
	if in.kind == "read" {
		if (!s.exists || s.deleted) && out.error == "Missing" && out.end == 0 && out.incarnation == 0 && len(out.data) == 0 {
			return []interface{}{s}
		}
		if out.error != "" {
			return nil
		}
		if !s.exists || s.deleted || out.incarnation != s.incarnation || out.end != uint64(len(s.data)) || out.closed != s.closed || !bytes.Equal(out.data, s.data) {
			return nil
		}
		return []interface{}{s}
	}
	return rustApplyObserved(s, in, out)
}

func rustApply(s rustStream, in rustInput) []interface{} {
	// Unknown branches only retain successful effects; application rejections
	// are represented by the unchanged branch.
	switch in.kind {
	case "create":
		req := uint64(1)
		if in.expected != nil {
			req = *in.expected
		}
		next := uint64(1)
		if s.exists {
			next = s.incarnation + 1
		}
		if s.exists && !s.deleted {
			return nil
		}
		if req != next {
			return nil
		}
		n := rustStream{exists: true, incarnation: next, config: in.config, data: append([]byte(nil), in.data...), closed: in.close, producers: map[string]rustProducerState{}}
		return []interface{}{n}
	case "append":
		if !s.exists || s.deleted || s.incarnation != in.incarnation || s.closed {
			return nil
		}
		n := s.clone()
		if p := in.producer; p != nil {
			old, has := s.producers[p.ID]
			if has && p.Epoch < old.epoch {
				return nil
			}
			if has && p.Epoch == old.epoch && p.Seq <= old.seq {
				return nil
			}
			if (has && p.Epoch == old.epoch && old.seq+1 != p.Seq) || (has && p.Epoch > old.epoch && p.Seq != 0) || (!has && p.Seq != 0) {
				return nil
			}
			results := map[uint64]uint64{}
			if has && old.epoch == p.Epoch {
				for k, v := range old.results {
					results[k] = v
				}
			}
			end := uint64(len(s.data) + len(in.data))
			results[p.Seq] = end
			n.producers[p.ID] = rustProducerState{p.Epoch, p.Seq, results}
		}
		n.data = append(n.data, in.data...)
		n.closed = in.close
		return []interface{}{n}
	case "delete":
		if !s.exists || s.deleted || s.incarnation != in.incarnation {
			return nil
		}
		n := s.clone()
		n.deleted = true
		n.data = nil
		n.producers = map[string]rustProducerState{}
		return []interface{}{n}
	}
	return nil
}

func rustApplyObserved(s rustStream, in rustInput, out rustOutput) []interface{} {
	err := func(want string) []interface{} {
		if out.error == want && out.end == 0 && out.incarnation == 0 && !out.duplicate {
			return []interface{}{s}
		}
		return nil
	}
	if in.kind == "create" {
		req := uint64(1)
		if in.expected != nil {
			req = *in.expected
		}
		if s.exists && !s.deleted {
			if s.incarnation != req {
				return err("StaleIncarnation")
			}
			if !reflect.DeepEqual(s.config, in.config) {
				return err("ConfigConflict")
			}
			if out.error == "" && (out.duplicate || out.legacy) && out.end == uint64(len(s.data)) && out.incarnation == s.incarnation {
				return []interface{}{s}
			}
			return nil
		}
		if (s.exists && req != s.incarnation+1) || (!s.exists && req != 1) {
			return err("StaleIncarnation")
		}
	}
	if in.kind == "append" {
		if !s.exists || s.deleted {
			return err("Missing")
		}
		if s.incarnation != in.incarnation {
			return err("StaleIncarnation")
		}
		if p := in.producer; p != nil {
			if old, ok := s.producers[p.ID]; ok {
				if p.Epoch < old.epoch {
					return err("EpochFenced")
				}
				if p.Epoch == old.epoch && p.Seq <= old.seq {
					end, found := old.results[p.Seq]
					if !found {
						return err("SequenceGap")
					}
					if out.error == "" && (out.duplicate || out.legacy) && out.end == end && out.incarnation == s.incarnation {
						return []interface{}{s}
					}
					return nil
				}
				if (p.Epoch == old.epoch && old.seq+1 != p.Seq) || (p.Epoch > old.epoch && p.Seq != 0) {
					return err("SequenceGap")
				}
			} else if p.Seq != 0 {
				return err("SequenceGap")
			}
		}
		if s.closed {
			return err("Closed")
		}
	}
	if in.kind == "delete" {
		if !s.exists || s.deleted {
			return err("Missing")
		}
		if s.incarnation != in.incarnation {
			return err("StaleIncarnation")
		}
	}
	if out.error != "" || out.duplicate {
		return nil
	}
	ns := rustApply(s, in)
	if len(ns) != 1 {
		return nil
	}
	n := ns[0].(rustStream)
	wantEnd := uint64(len(n.data))
	if in.kind == "delete" {
		wantEnd = 0
	}
	if out.end != wantEnd || out.incarnation != n.incarnation {
		return nil
	}
	return ns
}

type rustLine struct {
	Schema  int             `json:"schema"`
	Type    string          `json:"type"`
	F       string          `json:"f"`
	Process string          `json:"process"`
	ID      string          `json:"id"`
	Time    int64           `json:"time_ns"`
	Value   json.RawMessage `json:"value"`
}
type rustV1Meta struct {
	Tenant     string `json:"tenant"`
	Path       string `json:"path"`
	Seed       int    `json:"seed"`
	RecordSize int    `json:"record_size"`
}

// Schema 2 is the lifecycle-capable form. Every invoke carries tenant/path and
// operation fields directly: create uses data, content_type, expires_ms,
// expected_incarnation, and close; append uses data, incarnation, close and an
// optional producer plus epoch/seq; delete uses incarnation; read has no other
// input. Completions use end, incarnation, duplicate, error, plus read data and
// closed. Unknown fields, unsupported functions, and absent tenant/path are
// rejected rather than being interpreted as v1 events.
type rustValue struct {
	Tenant              string   `json:"tenant"`
	Path                string   `json:"path"`
	Data                string   `json:"data"`
	Record              string   `json:"record"`
	Error               string   `json:"error"`
	ContentType         string   `json:"content_type"`
	Records             []string `json:"records"`
	Status              int      `json:"status"`
	End                 uint64   `json:"end"`
	Incarnation         uint64   `json:"incarnation"`
	Epoch               uint64   `json:"epoch"`
	Seq                 uint64   `json:"seq"`
	Producer            string   `json:"producer"`
	Close               bool     `json:"close"`
	Closed              bool     `json:"closed"`
	Duplicate           bool     `json:"duplicate"`
	ExpectedIncarnation *uint64  `json:"expected_incarnation"`
	ExpiresMS           *uint64  `json:"expires_ms"`
	NextOffset          string   `json:"stream-next-offset"`
	StreamIncarnation   string   `json:"stream-incarnation"`
}
type rustPending struct {
	line rustLine
	in   rustInput
}

func parseRustHistory(r io.Reader) ([]porcupine.Operation, error) {
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 64<<10), 16<<20)
	var lines []rustLine
	var meta rustV1Meta
	var end int64
	for n := 1; s.Scan(); n++ {
		var l rustLine
		if err := strictValue(s.Bytes(), &l); err != nil {
			return nil, fmt.Errorf("line %d: %w", n, err)
		}
		if l.Schema != 1 && l.Schema != 2 {
			return nil, fmt.Errorf("line %d: unsupported schema %d", n, l.Schema)
		}
		if l.Type != "info" && l.F != "nemesis" && (l.ID == "" || l.Process == "") {
			return nil, fmt.Errorf("line %d: missing operation ID/process", n)
		}
		if l.Time < 0 || l.Time == int64(^uint64(0)>>1) {
			return nil, fmt.Errorf("line %d: invalid monotonic timestamp", n)
		}
		if l.Time >= end {
			end = l.Time + 1
		}
		if l.Schema == 1 && l.Type == "info" && l.F == "run" {
			if err := json.Unmarshal(l.Value, &meta); err != nil {
				return nil, fmt.Errorf("line %d run: %w", n, err)
			}
		}
		lines = append(lines, l)
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	if len(lines) == 0 {
		return nil, fmt.Errorf("empty history")
	}
	records := map[string][]byte{}
	for _, l := range lines {
		if l.Schema == 1 && l.Type == "info" && l.F == "record" {
			var v rustValue
			if json.Unmarshal(l.Value, &v) != nil || v.Record == "" {
				return nil, fmt.Errorf("record %q missing record", l.ID)
			}
			records[l.ID] = []byte(v.Record)
		}
	}
	pending := map[string][]rustPending{}
	clients := map[string]int{}
	var ops []porcupine.Operation
	for _, l := range lines {
		if l.Type == "info" {
			continue
		}
		key := l.Process + "\x00" + l.ID + "\x00" + l.F
		if l.F == "nemesis" || l.F == "stale-read" {
			continue
		}
		if l.Type == "invoke" {
			in, err := rustInputFor(l, meta, records)
			if err != nil {
				return nil, err
			}
			pending[key] = append(pending[key], rustPending{line: l, in: in})
			continue
		}
		q := pending[key]
		if len(q) == 0 {
			return nil, fmt.Errorf("completion without invoke: %s/%s/%s", l.Process, l.ID, l.F)
		}
		p := q[0]
		pending[key] = q[1:]
		if l.Time < p.line.Time {
			return nil, fmt.Errorf("completion precedes invocation: %s", key)
		}
		out, err := rustOutputFor(l, p.in)
		if err != nil {
			return nil, err
		}
		cid, ok := clients[l.Process]
		if !ok {
			cid = len(clients)
			clients[l.Process] = cid
		}
		ret := l.Time
		if out.unknown {
			ret = end
		}
		ops = append(ops, porcupine.Operation{ClientId: cid, Input: p.in, Output: out, Call: p.line.Time, Return: ret})
	}
	for k, q := range pending {
		if len(q) > 0 {
			return nil, fmt.Errorf("uncompleted invocation %s", k)
		}
	}
	if len(ops) == 0 {
		return nil, fmt.Errorf("history contains no checked operations")
	}
	return ops, nil
}

func rustInputFor(l rustLine, m rustV1Meta, records map[string][]byte) (rustInput, error) {
	in := rustInput{tenant: m.Tenant, path: m.Path, kind: l.F, incarnation: 1, config: rustConfig{ContentType: "application/octet-stream"}}
	if l.Schema == 1 {
		switch l.F {
		case "create":
		case "append", "append-retry":
			in.kind = "append"
			in.data = records[l.ID]
			if len(in.data) != m.RecordSize || m.RecordSize <= 0 {
				return in, fmt.Errorf("v1 append %q missing fixed-width record", l.ID)
			}
			parts := strings.Split(l.ID, "-")
			if len(parts) != 2 {
				return in, fmt.Errorf("bad v1 append id %q", l.ID)
			}
			seq, e := strconv.ParseUint(parts[1], 10, 64)
			if e != nil {
				return in, e
			}
			in.producer = &rustProducer{ID: fmt.Sprintf("history-%d-%s", m.Seed, strings.TrimPrefix(parts[0], "p")), Epoch: 0, Seq: seq}
		case "read":
		default:
			return in, fmt.Errorf("unsupported v1 function %q", l.F)
		}
		if in.tenant == "" || in.path == "" {
			return in, fmt.Errorf("v1 missing run tenant/path")
		}
		return in, nil
	}
	var v rustValue
	if err := strictValue(l.Value, &v); err != nil {
		return in, fmt.Errorf("v2 invoke %s: %w", l.ID, err)
	}
	in.tenant, in.path, in.data = v.Tenant, v.Path, []byte(v.Data)
	in.incarnation = v.Incarnation
	in.close = v.Close
	in.expected = v.ExpectedIncarnation
	in.config = rustConfig{v.ContentType, v.ExpiresMS}
	if v.Producer != "" {
		in.producer = &rustProducer{v.Producer, v.Epoch, v.Seq}
	}
	if in.tenant == "" || in.path == "" {
		return in, fmt.Errorf("v2 invoke missing tenant/path")
	}
	if in.kind != "create" && in.kind != "append" && in.kind != "read" && in.kind != "delete" {
		return in, fmt.Errorf("unsupported v2 function %q", in.kind)
	}
	return in, nil
}

func rustOutputFor(l rustLine, in rustInput) (rustOutput, error) {
	if l.Type == "unknown" {
		return rustOutput{unknown: true, legacy: l.Schema == 1}, nil
	}
	if l.Type != "ok" && l.Type != "fail" {
		return rustOutput{}, fmt.Errorf("unsupported completion type %q", l.Type)
	}
	var v rustValue
	if l.Schema == 2 {
		if err := strictValue(l.Value, &v); err != nil {
			return rustOutput{}, err
		}
		if err := validateRustV2Output(l, in); err != nil {
			return rustOutput{}, err
		}
	} else if err := json.Unmarshal(l.Value, &v); err != nil {
		return rustOutput{}, err
	}
	o := rustOutput{error: v.Error, duplicate: v.Duplicate, closed: v.Closed, legacy: l.Schema == 1}
	if l.Schema == 1 {
		var err error
		o.end, err = parseV1Offset(v.NextOffset)
		if err != nil && l.Type == "ok" {
			return o, err
		}
		if v.StreamIncarnation != "" {
			o.incarnation, err = strconv.ParseUint(v.StreamIncarnation, 10, 64)
			if err != nil {
				return o, err
			}
		}
		if in.kind == "read" {
			o.data = []byte(strings.Join(v.Records, ""))
		}
		if l.Type == "fail" {
			if v.Status == 409 && in.kind == "append" {
				o.error = "SequenceGap"
			} else {
				return o, fmt.Errorf("unsupported v1 application failure %s status %d", l.ID, v.Status)
			}
		}
	} else {
		o.end = v.End
		o.incarnation = v.Incarnation
		o.data = []byte(v.Data)
	}
	return o, nil
}

func validateRustV2Output(l rustLine, in rustInput) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(l.Value, &fields); err != nil {
		return err
	}
	allowed := map[string]bool{"error": true}
	if l.Type == "ok" {
		allowed = map[string]bool{"end": true, "incarnation": true}
		if in.kind == "read" {
			allowed["data"] = true
			allowed["closed"] = true
		} else {
			allowed["duplicate"] = true
		}
	}
	for field := range fields {
		if !allowed[field] {
			return fmt.Errorf("v2 %s %s has invalid output field %q", l.Type, l.ID, field)
		}
	}
	_, hasError := fields["error"]
	if l.Type == "fail" && (!hasError || strings.TrimSpace(stringValue(fields["error"])) == "") {
		return fmt.Errorf("v2 fail %s missing error", l.ID)
	}
	return nil
}

func stringValue(raw json.RawMessage) string {
	var value string
	_ = json.Unmarshal(raw, &value)
	return value
}

func strictValue(raw json.RawMessage, v interface{}) error {
	if len(raw) == 0 {
		return fmt.Errorf("missing value")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	var extra interface{}
	if err := d.Decode(&extra); err != io.EOF {
		return fmt.Errorf("expected one JSON object")
	}
	return nil
}

func parseV1Offset(s string) (uint64, error) {
	parts := strings.Split(s, "_")
	if len(parts) != 2 || len(parts[1]) != 16 {
		return 0, fmt.Errorf("bad stream-next-offset %q", s)
	}
	return strconv.ParseUint(parts[1], 10, 64)
}

func checkRustHistory(r io.Reader, timeout time.Duration) (porcupine.CheckResult, error) {
	ops, err := parseRustHistory(r)
	if err != nil {
		return porcupine.Illegal, err
	}
	// Cheap necessary condition before the exponential search: a successful
	// append must be retained by every strict read invoked after its response.
	// This is also what makes the retained missing-ack regression fail quickly.
	for _, write := range ops {
		win, wout := write.Input.(rustInput), write.Output.(rustOutput)
		if win.kind != "append" || !wout.legacy || wout.unknown || wout.error != "" {
			continue
		}
		for _, read := range ops {
			rin, rout := read.Input.(rustInput), read.Output.(rustOutput)
			if rin.kind == "read" && rout.legacy && win.tenant == rin.tenant && win.path == rin.path && wout.incarnation == rout.incarnation && !rout.unknown && rout.error == "" && write.Return < read.Call && !bytes.Contains(rout.data, win.data) {
				return porcupine.Illegal, nil
			}
		}
	}
	if timeout <= 0 {
		return porcupine.Unknown, nil
	}
	result, _ := porcupine.CheckOperationsVerbose(rustModel(), ops, timeout)
	return result, nil
}
