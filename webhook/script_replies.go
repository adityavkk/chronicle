package webhook

import "fmt"

type createSubReply interface {
	createSubReply()
	status() string
}
type (
	createSubCreated  struct{ Owner string }
	createSubMatched  struct{ Owner string }
	createSubConflict struct{ Owner string }
)

func (createSubCreated) createSubReply()  {}
func (createSubMatched) createSubReply()  {}
func (createSubConflict) createSubReply() {}
func (createSubCreated) status() string   { return "CREATED" }
func (createSubMatched) status() string   { return "MATCHED" }
func (createSubConflict) status() string  { return "CONFLICT" }

var createSubReplyVariants = []replyVariant{
	{Status: "CREATED", Fields: []replyFieldKind{replyString}},
	{Status: "MATCHED", Fields: []replyFieldKind{replyString}},
	{Status: "CONFLICT", Fields: []replyFieldKind{replyString}},
}

var createSubDecoder = scriptDecoder[createSubReply]{Variants: createSubReplyVariants, Decode: decodeCreateSubReply}

func decodeCreateSubReply(r scriptReply) (createSubReply, error) {
	st, err := decodeStatus(r, createSubReplyVariants)
	if err != nil {
		return nil, err
	}
	if err := r.wantArity(2); err != nil {
		return nil, err
	}
	owner, err := r.stringAt(1)
	if err != nil {
		return nil, err
	}
	switch st {
	case "CREATED":
		return createSubCreated{Owner: owner}, nil
	case "MATCHED":
		return createSubMatched{Owner: owner}, nil
	case "CONFLICT":
		return createSubConflict{Owner: owner}, nil
	default:
		return nil, fmt.Errorf("unhandled create_sub status %q", st)
	}
}

type armWakeReply interface {
	armWakeReply()
	status() string
}
type armWakeArmed struct {
	Generation int64
	WakeID     string
}
type armWakeBusy struct {
	Generation int64
	WakeID     string
}
type (
	armWakeNoSub  struct{}
	armWakeFenced struct{}
)

func (armWakeArmed) armWakeReply()   {}
func (armWakeBusy) armWakeReply()    {}
func (armWakeNoSub) armWakeReply()   {}
func (armWakeFenced) armWakeReply()  {}
func (armWakeArmed) status() string  { return "ARMED" }
func (armWakeBusy) status() string   { return "BUSY" }
func (armWakeNoSub) status() string  { return "NOSUB" }
func (armWakeFenced) status() string { return "FENCED" }

var armWakeReplyVariants = []replyVariant{
	{Status: "ARMED", Fields: []replyFieldKind{replyInteger, replyString}},
	{Status: "BUSY", Fields: []replyFieldKind{replyInteger, replyString}},
	{Status: "NOSUB"},
	{Status: "FENCED"},
}

var armWakeDecoder = scriptDecoder[armWakeReply]{Variants: armWakeReplyVariants, Decode: decodeArmWakeReply}

func decodeArmWakeReply(r scriptReply) (armWakeReply, error) {
	st, err := decodeStatus(r, armWakeReplyVariants)
	if err != nil {
		return nil, err
	}
	switch st {
	case "ARMED":
		if err := r.wantArity(3); err != nil {
			return nil, err
		}
		gen, err := r.int64At(1)
		if err != nil {
			return nil, err
		}
		wake, err := r.stringAt(2)
		if err != nil {
			return nil, err
		}
		return armWakeArmed{Generation: gen, WakeID: wake}, nil
	case "BUSY":
		if err := r.wantArity(3); err != nil {
			return nil, err
		}
		gen, err := r.int64At(1)
		if err != nil {
			return nil, err
		}
		wake, err := r.stringAt(2)
		if err != nil {
			return nil, err
		}
		return armWakeBusy{Generation: gen, WakeID: wake}, nil
	case "NOSUB":
		if err := r.wantArity(1); err != nil {
			return nil, err
		}
		return armWakeNoSub{}, nil
	case "FENCED":
		if err := r.wantArity(1); err != nil {
			return nil, err
		}
		return armWakeFenced{}, nil
	default:
		return nil, fmt.Errorf("unhandled arm_wake status %q", st)
	}
}

type claimReply interface {
	claimReply()
	status() string
}
type claimClaimed struct {
	Generation     int64
	WakeID, Holder string
}
type claimBusy struct {
	Generation int64
	Holder     string
}
type (
	claimNoSub     struct{}
	claimForbidden struct{}
)

func (claimClaimed) claimReply()      {}
func (claimBusy) claimReply()         {}
func (claimNoSub) claimReply()        {}
func (claimForbidden) claimReply()    {}
func (claimClaimed) status() string   { return "CLAIMED" }
func (claimBusy) status() string      { return "BUSY" }
func (claimNoSub) status() string     { return "NOSUB" }
func (claimForbidden) status() string { return "FORBIDDEN" }

var claimReplyVariants = []replyVariant{
	{Status: "CLAIMED", Fields: []replyFieldKind{replyInteger, replyString, replyString}},
	{Status: "BUSY", Fields: []replyFieldKind{replyInteger, replyString, replyString}},
	{Status: "NOSUB"},
	{Status: "FORBIDDEN"},
}

var claimDecoder = scriptDecoder[claimReply]{Variants: claimReplyVariants, Decode: decodeClaimReply}

func decodeClaimReply(r scriptReply) (claimReply, error) {
	st, err := decodeStatus(r, claimReplyVariants)
	if err != nil {
		return nil, err
	}
	switch st {
	case "CLAIMED":
		if err := r.wantArity(4); err != nil {
			return nil, err
		}
		gen, err := r.int64At(1)
		if err != nil {
			return nil, err
		}
		wake, err := r.stringAt(2)
		if err != nil {
			return nil, err
		}
		holder, err := r.stringAt(3)
		if err != nil {
			return nil, err
		}
		return claimClaimed{Generation: gen, WakeID: wake, Holder: holder}, nil
	case "BUSY":
		if err := r.wantArity(4); err != nil {
			return nil, err
		}
		gen, err := r.int64At(1)
		if err != nil {
			return nil, err
		}
		// Field 2 is wake_id in the ABI and is intentionally ignored by today's API.
		if _, err := r.stringAt(2); err != nil {
			return nil, err
		}
		holder, err := r.stringAt(3)
		if err != nil {
			return nil, err
		}
		return claimBusy{Generation: gen, Holder: holder}, nil
	case "NOSUB":
		if err := r.wantArity(1); err != nil {
			return nil, err
		}
		return claimNoSub{}, nil
	case "FORBIDDEN":
		if err := r.wantArity(1); err != nil {
			return nil, err
		}
		return claimForbidden{}, nil
	default:
		return nil, fmt.Errorf("unhandled claim status %q", st)
	}
}

type scheduleRetryReply interface {
	scheduleRetryReply()
	status() string
}
type scheduleRetryOK struct {
	RetryCount  int
	FirstFailNs int64
}
type (
	scheduleRetryNoSub  struct{}
	scheduleRetryStale  struct{}
	scheduleRetryFenced struct{}
)

func (scheduleRetryOK) scheduleRetryReply()     {}
func (scheduleRetryNoSub) scheduleRetryReply()  {}
func (scheduleRetryStale) scheduleRetryReply()  {}
func (scheduleRetryFenced) scheduleRetryReply() {}
func (scheduleRetryOK) status() string          { return "OK" }
func (scheduleRetryNoSub) status() string       { return "NOSUB" }
func (scheduleRetryStale) status() string       { return "STALE" }
func (scheduleRetryFenced) status() string      { return "FENCED" }

var scheduleRetryReplyVariants = []replyVariant{
	{Status: "OK", Fields: []replyFieldKind{replyInteger, replyNS}},
	{Status: "NOSUB"},
	{Status: "STALE"},
	{Status: "FENCED"},
}

var scheduleRetryDecoder = scriptDecoder[scheduleRetryReply]{Variants: scheduleRetryReplyVariants, Decode: decodeScheduleRetryReply}

func decodeScheduleRetryReply(r scriptReply) (scheduleRetryReply, error) {
	st, err := decodeStatus(r, scheduleRetryReplyVariants)
	if err != nil {
		return nil, err
	}
	switch st {
	case "OK":
		if err := r.wantArity(3); err != nil {
			return nil, err
		}
		count, err := r.intAt(1)
		if err != nil {
			return nil, err
		}
		first, err := r.int64At(2)
		if err != nil {
			return nil, err
		}
		return scheduleRetryOK{RetryCount: count, FirstFailNs: first}, nil
	case "NOSUB":
		if err := r.wantArity(1); err != nil {
			return nil, err
		}
		return scheduleRetryNoSub{}, nil
	case "STALE":
		if err := r.wantArity(1); err != nil {
			return nil, err
		}
		return scheduleRetryStale{}, nil
	case "FENCED":
		if err := r.wantArity(1); err != nil {
			return nil, err
		}
		return scheduleRetryFenced{}, nil
	default:
		return nil, fmt.Errorf("unhandled schedule_retry status %q", st)
	}
}

type stringListReply []string

var stringListDecoder = scriptDecoder[stringListReply]{Decode: decodeStringListReply}

func decodeStringListReply(r scriptReply) (stringListReply, error) {
	out := make([]string, len(r))
	for i := range r {
		s, err := r.stringAt(i)
		if err != nil {
			return nil, err
		}
		out[i] = s
	}
	return stringListReply(out), nil
}

type keyMaterialReply struct{ Kid, Material string }

var keyMaterialDecoder = scriptDecoder[keyMaterialReply]{
	Variants: []replyVariant{{Status: "<key-material>", Fields: []replyFieldKind{replyString, replyString}}},
	Decode:   decodeKeyMaterialReply,
}

func decodeKeyMaterialReply(r scriptReply) (keyMaterialReply, error) {
	if err := r.wantArity(2); err != nil {
		return keyMaterialReply{}, err
	}
	kid, err := r.stringAt(0)
	if err != nil {
		return keyMaterialReply{}, err
	}
	material, err := r.stringAt(1)
	if err != nil {
		return keyMaterialReply{}, err
	}
	return keyMaterialReply{Kid: kid, Material: material}, nil
}

type rotateKeyReply interface {
	rotateKeyReply()
	status() string
}
type (
	rotateKeyRotated  struct{ NewKid string }
	rotateKeyConflict struct{ CurrentKid string }
)

func (rotateKeyRotated) rotateKeyReply()  {}
func (rotateKeyConflict) rotateKeyReply() {}
func (rotateKeyRotated) status() string   { return "rotated" }
func (rotateKeyConflict) status() string  { return "conflict" }

var rotateKeyReplyVariants = []replyVariant{
	{Status: "rotated", Fields: []replyFieldKind{replyString}},
	{Status: "conflict", Fields: []replyFieldKind{replyString}},
}

var rotateKeyDecoder = scriptDecoder[rotateKeyReply]{Variants: rotateKeyReplyVariants, Decode: decodeRotateKeyReply}

func decodeRotateKeyReply(r scriptReply) (rotateKeyReply, error) {
	st, err := decodeStatus(r, rotateKeyReplyVariants)
	if err != nil {
		return nil, err
	}
	if err := r.wantArity(2); err != nil {
		return nil, err
	}
	kid, err := r.stringAt(1)
	if err != nil {
		return nil, err
	}
	switch st {
	case "rotated":
		return rotateKeyRotated{NewKid: kid}, nil
	case "conflict":
		return rotateKeyConflict{CurrentKid: kid}, nil
	default:
		return nil, fmt.Errorf("unhandled rotate_key status %q", st)
	}
}
