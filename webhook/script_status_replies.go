package webhook

import "fmt"

func decodeUnitStatus(r scriptReply, statuses ...string) (string, error) {
	st, err := decodeStatus(r, statuses...)
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
	linkStreamLinked   struct{}
	linkStreamUpgraded struct{}
	linkStreamExists   struct{}
)

func (linkStreamLinked) linkStreamReply()   {}
func (linkStreamUpgraded) linkStreamReply() {}
func (linkStreamExists) linkStreamReply()   {}
func (linkStreamLinked) status() string     { return "LINKED" }
func (linkStreamUpgraded) status() string   { return "UPGRADED" }
func (linkStreamExists) status() string     { return "EXISTS" }
func decodeLinkStreamReply(r scriptReply) (linkStreamReply, error) {
	st, err := decodeUnitStatus(r, "LINKED", "UPGRADED", "EXISTS")
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
	default:
		return nil, fmt.Errorf("unhandled link_stream status %q", st)
	}
}

type unlinkStreamReply interface {
	unlinkStreamReply()
	status() string
}
type (
	unlinkStreamRemoved struct{}
	unlinkStreamGlob    struct{}
	unlinkStreamGone    struct{}
)

func (unlinkStreamRemoved) unlinkStreamReply() {}
func (unlinkStreamGlob) unlinkStreamReply()    {}
func (unlinkStreamGone) unlinkStreamReply()    {}
func (unlinkStreamRemoved) status() string     { return "REMOVED" }
func (unlinkStreamGlob) status() string        { return "GLOB" }
func (unlinkStreamGone) status() string        { return "GONE" }
func decodeUnlinkStreamReply(r scriptReply) (unlinkStreamReply, error) {
	st, err := decodeUnitStatus(r, "REMOVED", "GLOB", "GONE")
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
	default:
		return nil, fmt.Errorf("unhandled unlink_stream status %q", st)
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
func decodeAckReply(r scriptReply) (ackReply, error) {
	st, err := decodeUnitStatus(r, "OK", "FENCED", "NOSUB")
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
func decodeReleaseReply(r scriptReply) (releaseReply, error) {
	st, err := decodeUnitStatus(r, "OK", "FENCED", "NOSUB")
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
func decodeExpireLeaseReply(r scriptReply) (expireLeaseReply, error) {
	st, err := decodeUnitStatus(r, "EXPIRED", "ACTIVE", "NOSUB", "FENCED")
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
func decodeRestoreLeaseReply(r scriptReply) (restoreLeaseReply, error) {
	st, err := decodeUnitStatus(r, "RESTORED", "INTACT", "NOSUB")
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
func decodeRecordSuccessReply(r scriptReply) (recordSuccessReply, error) {
	st, err := decodeUnitStatus(r, "OK", "STALE", "FENCED", "NOSUB")
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
func decodeRecordWakeSentReply(r scriptReply) (recordWakeSentReply, error) {
	st, err := decodeUnitStatus(r, "OK", "STALE", "NOSUB")
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
type deleteSubOK struct{}

func (deleteSubOK) deleteSubReply() {}
func (deleteSubOK) status() string  { return "OK" }
func decodeDeleteSubReply(r scriptReply) (deleteSubReply, error) {
	if _, err := decodeUnitStatus(r, "OK"); err != nil {
		return nil, err
	}
	return deleteSubOK{}, nil
}
