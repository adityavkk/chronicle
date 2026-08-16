package webhook

import "fmt"

func decodeUnitStatus(r scriptReply, variants []replyVariant) (string, error) {
	st, err := decodeStatus(r, variants)
	if err != nil {
		return "", err
	}
	if err := r.wantArity(1); err != nil {
		return "", err
	}
	return st, nil
}

type linkStreamReply interface {
	linkStreamReply()
	status() string
}
type (
	linkStreamLinked    struct{}
	linkStreamUpgraded  struct{}
	linkStreamExists    struct{}
	linkStreamNoSub     struct{}
	linkStreamForbidden struct{}
)

func (linkStreamLinked) linkStreamReply()    {}
func (linkStreamUpgraded) linkStreamReply()  {}
func (linkStreamExists) linkStreamReply()    {}
func (linkStreamNoSub) linkStreamReply()     {}
func (linkStreamForbidden) linkStreamReply() {}
func (linkStreamLinked) status() string      { return "LINKED" }
func (linkStreamUpgraded) status() string    { return "UPGRADED" }
func (linkStreamExists) status() string      { return "EXISTS" }
func (linkStreamNoSub) status() string       { return "NOSUB" }
func (linkStreamForbidden) status() string   { return "FORBIDDEN" }

var (
	linkStreamReplyVariants = []replyVariant{{Status: "LINKED"}, {Status: "UPGRADED"}, {Status: "EXISTS"}, {Status: "NOSUB"}, {Status: "FORBIDDEN"}}
	linkStreamDecoder       = scriptDecoder[linkStreamReply]{Variants: linkStreamReplyVariants, Decode: decodeLinkStreamReply}
)

func decodeLinkStreamReply(r scriptReply) (linkStreamReply, error) {
	st, err := decodeUnitStatus(r, linkStreamReplyVariants)
	if err != nil {
		return nil, err
	}
	switch st {
	case "LINKED":
		return linkStreamLinked{}, nil
	case "UPGRADED":
		return linkStreamUpgraded{}, nil
	case "EXISTS":
		return linkStreamExists{}, nil
	case "NOSUB":
		return linkStreamNoSub{}, nil
	case "FORBIDDEN":
		return linkStreamForbidden{}, nil
	default:
		return nil, fmt.Errorf("unhandled link_stream status %q", st)
	}
}

type unlinkStreamReply interface {
	unlinkStreamReply()
	status() string
}
type (
	unlinkStreamRemoved   struct{}
	unlinkStreamGlob      struct{}
	unlinkStreamGone      struct{}
	unlinkStreamNoSub     struct{}
	unlinkStreamForbidden struct{}
)

func (unlinkStreamRemoved) unlinkStreamReply()   {}
func (unlinkStreamGlob) unlinkStreamReply()      {}
func (unlinkStreamGone) unlinkStreamReply()      {}
func (unlinkStreamNoSub) unlinkStreamReply()     {}
func (unlinkStreamForbidden) unlinkStreamReply() {}
func (unlinkStreamRemoved) status() string       { return "REMOVED" }
func (unlinkStreamGlob) status() string          { return "GLOB" }
func (unlinkStreamGone) status() string          { return "GONE" }
func (unlinkStreamNoSub) status() string         { return "NOSUB" }
func (unlinkStreamForbidden) status() string     { return "FORBIDDEN" }

var (
	unlinkStreamReplyVariants = []replyVariant{{Status: "REMOVED"}, {Status: "GLOB"}, {Status: "GONE"}, {Status: "NOSUB"}, {Status: "FORBIDDEN"}}
	unlinkStreamDecoder       = scriptDecoder[unlinkStreamReply]{Variants: unlinkStreamReplyVariants, Decode: decodeUnlinkStreamReply}
)

func decodeUnlinkStreamReply(r scriptReply) (unlinkStreamReply, error) {
	st, err := decodeUnitStatus(r, unlinkStreamReplyVariants)
	if err != nil {
		return nil, err
	}
	switch st {
	case "REMOVED":
		return unlinkStreamRemoved{}, nil
	case "GLOB":
		return unlinkStreamGlob{}, nil
	case "GONE":
		return unlinkStreamGone{}, nil
	case "NOSUB":
		return unlinkStreamNoSub{}, nil
	case "FORBIDDEN":
		return unlinkStreamForbidden{}, nil
	default:
		return nil, fmt.Errorf("unhandled unlink_stream status %q", st)
	}
}

type writeFenceReply interface {
	writeFenceReply()
	status() string
}
type (
	writeFenceOK     struct{}
	writeFenceFenced struct{}
	writeFenceNoSub  struct{}
)

func (writeFenceOK) writeFenceReply()     {}
func (writeFenceFenced) writeFenceReply() {}
func (writeFenceNoSub) writeFenceReply()  {}
func (writeFenceOK) status() string       { return "OK" }
func (writeFenceFenced) status() string   { return "FENCED" }
func (writeFenceNoSub) status() string    { return "NOSUB" }

var (
	writeFenceReplyVariants = []replyVariant{{Status: "OK"}, {Status: "FENCED"}, {Status: "NOSUB"}}
	writeFenceDecoder       = scriptDecoder[writeFenceReply]{Variants: writeFenceReplyVariants, Decode: decodeWriteFenceReply}
)

func decodeWriteFenceReply(r scriptReply) (writeFenceReply, error) {
	st, err := decodeUnitStatus(r, writeFenceReplyVariants)
	if err != nil {
		return nil, err
	}
	switch st {
	case "OK":
		return writeFenceOK{}, nil
	case "FENCED":
		return writeFenceFenced{}, nil
	case "NOSUB":
		return writeFenceNoSub{}, nil
	default:
		return nil, fmt.Errorf("unhandled check_write_fence status %q", st)
	}
}

type ackReply interface {
	ackReply()
	status() string
}
type (
	ackOK     struct{}
	ackFenced struct{}
	ackNoSub  struct{}
)

func (ackOK) ackReply()          {}
func (ackFenced) ackReply()      {}
func (ackNoSub) ackReply()       {}
func (ackOK) status() string     { return "OK" }
func (ackFenced) status() string { return "FENCED" }
func (ackNoSub) status() string  { return "NOSUB" }

var (
	ackReplyVariants = []replyVariant{{Status: "OK"}, {Status: "FENCED"}, {Status: "NOSUB"}}
	ackDecoder       = scriptDecoder[ackReply]{Variants: ackReplyVariants, Decode: decodeAckReply}
)

func decodeAckReply(r scriptReply) (ackReply, error) {
	st, err := decodeUnitStatus(r, ackReplyVariants)
	if err != nil {
		return nil, err
	}
	switch st {
	case "OK":
		return ackOK{}, nil
	case "FENCED":
		return ackFenced{}, nil
	case "NOSUB":
		return ackNoSub{}, nil
	default:
		return nil, fmt.Errorf("unhandled ack status %q", st)
	}
}

type releaseReply interface {
	releaseReply()
	status() string
}
type (
	releaseOK     struct{}
	releaseFenced struct{}
	releaseNoSub  struct{}
)

func (releaseOK) releaseReply()      {}
func (releaseFenced) releaseReply()  {}
func (releaseNoSub) releaseReply()   {}
func (releaseOK) status() string     { return "OK" }
func (releaseFenced) status() string { return "FENCED" }
func (releaseNoSub) status() string  { return "NOSUB" }

var (
	releaseReplyVariants = []replyVariant{{Status: "OK"}, {Status: "FENCED"}, {Status: "NOSUB"}}
	releaseDecoder       = scriptDecoder[releaseReply]{Variants: releaseReplyVariants, Decode: decodeReleaseReply}
)

func decodeReleaseReply(r scriptReply) (releaseReply, error) {
	st, err := decodeUnitStatus(r, releaseReplyVariants)
	if err != nil {
		return nil, err
	}
	switch st {
	case "OK":
		return releaseOK{}, nil
	case "FENCED":
		return releaseFenced{}, nil
	case "NOSUB":
		return releaseNoSub{}, nil
	default:
		return nil, fmt.Errorf("unhandled release status %q", st)
	}
}

type expireLeaseReply interface {
	expireLeaseReply()
	status() string
}
type (
	expireLeaseExpired struct{}
	expireLeaseActive  struct{}
	expireLeaseNoSub   struct{}
	expireLeaseFenced  struct{}
)

func (expireLeaseExpired) expireLeaseReply() {}
func (expireLeaseActive) expireLeaseReply()  {}
func (expireLeaseNoSub) expireLeaseReply()   {}
func (expireLeaseFenced) expireLeaseReply()  {}
func (expireLeaseExpired) status() string    { return "EXPIRED" }
func (expireLeaseActive) status() string     { return "ACTIVE" }
func (expireLeaseNoSub) status() string      { return "NOSUB" }
func (expireLeaseFenced) status() string     { return "FENCED" }

var (
	expireLeaseReplyVariants = []replyVariant{{Status: "EXPIRED"}, {Status: "ACTIVE"}, {Status: "NOSUB"}, {Status: "FENCED"}}
	expireLeaseDecoder       = scriptDecoder[expireLeaseReply]{Variants: expireLeaseReplyVariants, Decode: decodeExpireLeaseReply}
)

func decodeExpireLeaseReply(r scriptReply) (expireLeaseReply, error) {
	st, err := decodeUnitStatus(r, expireLeaseReplyVariants)
	if err != nil {
		return nil, err
	}
	switch st {
	case "EXPIRED":
		return expireLeaseExpired{}, nil
	case "ACTIVE":
		return expireLeaseActive{}, nil
	case "NOSUB":
		return expireLeaseNoSub{}, nil
	case "FENCED":
		return expireLeaseFenced{}, nil
	default:
		return nil, fmt.Errorf("unhandled expire_lease status %q", st)
	}
}

type restoreLeaseReply interface {
	restoreLeaseReply()
	status() string
}
type (
	restoreLeaseRestored struct{}
	restoreLeaseIntact   struct{}
	restoreLeaseNoSub    struct{}
)

func (restoreLeaseRestored) restoreLeaseReply() {}
func (restoreLeaseIntact) restoreLeaseReply()   {}
func (restoreLeaseNoSub) restoreLeaseReply()    {}
func (restoreLeaseRestored) status() string     { return "RESTORED" }
func (restoreLeaseIntact) status() string       { return "INTACT" }
func (restoreLeaseNoSub) status() string        { return "NOSUB" }

var (
	restoreLeaseReplyVariants = []replyVariant{{Status: "RESTORED"}, {Status: "INTACT"}, {Status: "NOSUB"}}
	restoreLeaseDecoder       = scriptDecoder[restoreLeaseReply]{Variants: restoreLeaseReplyVariants, Decode: decodeRestoreLeaseReply}
)

func decodeRestoreLeaseReply(r scriptReply) (restoreLeaseReply, error) {
	st, err := decodeUnitStatus(r, restoreLeaseReplyVariants)
	if err != nil {
		return nil, err
	}
	switch st {
	case "RESTORED":
		return restoreLeaseRestored{}, nil
	case "INTACT":
		return restoreLeaseIntact{}, nil
	case "NOSUB":
		return restoreLeaseNoSub{}, nil
	default:
		return nil, fmt.Errorf("unhandled restore_lease status %q", st)
	}
}

type recordSuccessReply interface {
	recordSuccessReply()
	status() string
}
type (
	recordSuccessOK     struct{}
	recordSuccessStale  struct{}
	recordSuccessFenced struct{}
	recordSuccessNoSub  struct{}
)

func (recordSuccessOK) recordSuccessReply()     {}
func (recordSuccessStale) recordSuccessReply()  {}
func (recordSuccessFenced) recordSuccessReply() {}
func (recordSuccessNoSub) recordSuccessReply()  {}
func (recordSuccessOK) status() string          { return "OK" }
func (recordSuccessStale) status() string       { return "STALE" }
func (recordSuccessFenced) status() string      { return "FENCED" }
func (recordSuccessNoSub) status() string       { return "NOSUB" }

var (
	recordSuccessReplyVariants = []replyVariant{{Status: "OK"}, {Status: "STALE"}, {Status: "FENCED"}, {Status: "NOSUB"}}
	recordSuccessDecoder       = scriptDecoder[recordSuccessReply]{Variants: recordSuccessReplyVariants, Decode: decodeRecordSuccessReply}
)

func decodeRecordSuccessReply(r scriptReply) (recordSuccessReply, error) {
	st, err := decodeUnitStatus(r, recordSuccessReplyVariants)
	if err != nil {
		return nil, err
	}
	switch st {
	case "OK":
		return recordSuccessOK{}, nil
	case "STALE":
		return recordSuccessStale{}, nil
	case "FENCED":
		return recordSuccessFenced{}, nil
	case "NOSUB":
		return recordSuccessNoSub{}, nil
	default:
		return nil, fmt.Errorf("unhandled record_success status %q", st)
	}
}

type recordWakeSentReply interface {
	recordWakeSentReply()
	status() string
}
type (
	recordWakeSentOK    struct{}
	recordWakeSentStale struct{}
	recordWakeSentNoSub struct{}
)

func (recordWakeSentOK) recordWakeSentReply()    {}
func (recordWakeSentStale) recordWakeSentReply() {}
func (recordWakeSentNoSub) recordWakeSentReply() {}
func (recordWakeSentOK) status() string          { return "OK" }
func (recordWakeSentStale) status() string       { return "STALE" }
func (recordWakeSentNoSub) status() string       { return "NOSUB" }

var (
	recordWakeSentReplyVariants = []replyVariant{{Status: "OK"}, {Status: "STALE"}, {Status: "NOSUB"}}
	recordWakeSentDecoder       = scriptDecoder[recordWakeSentReply]{Variants: recordWakeSentReplyVariants, Decode: decodeRecordWakeSentReply}
)

func decodeRecordWakeSentReply(r scriptReply) (recordWakeSentReply, error) {
	st, err := decodeUnitStatus(r, recordWakeSentReplyVariants)
	if err != nil {
		return nil, err
	}
	switch st {
	case "OK":
		return recordWakeSentOK{}, nil
	case "STALE":
		return recordWakeSentStale{}, nil
	case "NOSUB":
		return recordWakeSentNoSub{}, nil
	default:
		return nil, fmt.Errorf("unhandled record_wake_sent status %q", st)
	}
}

type deleteSubReply interface {
	deleteSubReply()
	status() string
}
type (
	deleteSubOK        struct{}
	deleteSubNoSub     struct{}
	deleteSubForbidden struct{}
)

func (deleteSubOK) deleteSubReply()        {}
func (deleteSubNoSub) deleteSubReply()     {}
func (deleteSubForbidden) deleteSubReply() {}
func (deleteSubOK) status() string         { return "OK" }
func (deleteSubNoSub) status() string      { return "NOSUB" }
func (deleteSubForbidden) status() string  { return "FORBIDDEN" }

var (
	deleteSubReplyVariants = []replyVariant{{Status: "OK"}, {Status: "NOSUB"}, {Status: "FORBIDDEN"}}
	deleteSubDecoder       = scriptDecoder[deleteSubReply]{Variants: deleteSubReplyVariants, Decode: decodeDeleteSubReply}
)

func decodeDeleteSubReply(r scriptReply) (deleteSubReply, error) {
	st, err := decodeUnitStatus(r, deleteSubReplyVariants)
	if err != nil {
		return nil, err
	}
	switch st {
	case "OK":
		return deleteSubOK{}, nil
	case "NOSUB":
		return deleteSubNoSub{}, nil
	case "FORBIDDEN":
		return deleteSubForbidden{}, nil
	default:
		return nil, fmt.Errorf("unhandled delete_sub status %q", st)
	}
}
