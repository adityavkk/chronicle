package webhook

import "fmt"

type slotClaimReply interface {
	slotClaimReply()
	status() string
	toSlotClaim() SlotClaim
}
type slotClaimClaimed struct {
	Owner    ReplicaID
	Epoch    OwnerEpoch
	ExpiryNs int64
}
type slotClaimRenewed struct {
	Owner    ReplicaID
	Epoch    OwnerEpoch
	ExpiryNs int64
}
type slotClaimBusy struct {
	Owner    ReplicaID
	Epoch    OwnerEpoch
	ExpiryNs int64
}

func (slotClaimClaimed) slotClaimReply() {}
func (slotClaimRenewed) slotClaimReply() {}
func (slotClaimBusy) slotClaimReply()    {}
func (slotClaimClaimed) status() string  { return "CLAIMED" }
func (slotClaimRenewed) status() string  { return "RENEWED" }
func (slotClaimBusy) status() string     { return "BUSY" }
func (r slotClaimClaimed) toSlotClaim() SlotClaim {
	return SlotClaim{Status: SlotClaimed, Owner: r.Owner, Epoch: r.Epoch, ExpiryNs: r.ExpiryNs}
}

func (r slotClaimRenewed) toSlotClaim() SlotClaim {
	return SlotClaim{Status: SlotRenewed, Owner: r.Owner, Epoch: r.Epoch, ExpiryNs: r.ExpiryNs}
}

func (r slotClaimBusy) toSlotClaim() SlotClaim {
	return SlotClaim{Status: SlotBusy, Owner: r.Owner, Epoch: r.Epoch, ExpiryNs: r.ExpiryNs}
}

func decodeSlotClaimReply(r scriptReply) (slotClaimReply, error) {
	st, err := decodeStatus(r, "CLAIMED", "RENEWED", "BUSY")
	if err != nil {
		return nil, err
	}
	if err := r.wantArity(4); err != nil {
		return nil, err
	}
	owner, err := r.stringAt(1)
	if err != nil {
		return nil, err
	}
	epoch, err := r.int64At(2)
	if err != nil {
		return nil, err
	}
	expiry, err := r.nsAt(3)
	if err != nil {
		return nil, err
	}
	base := struct {
		Owner    ReplicaID
		Epoch    OwnerEpoch
		ExpiryNs int64
	}{ReplicaID{id: owner}, OwnerEpoch{e: epoch}, expiry}
	switch st {
	case "CLAIMED":
		return slotClaimClaimed(base), nil
	case "RENEWED":
		return slotClaimRenewed(base), nil
	case "BUSY":
		return slotClaimBusy(base), nil
	default:
		return nil, fmt.Errorf("unhandled claim_shard status %q", st)
	}
}

type reserveLegacySlotReply interface {
	reserveLegacySlotReply()
	status() string
	toSlotClaim() SlotClaim
}
type reserveLegacySlotReserved struct {
	Owner    ReplicaID
	Epoch    OwnerEpoch
	ExpiryNs int64
}
type reserveLegacySlotBusy struct {
	Owner    ReplicaID
	Epoch    OwnerEpoch
	ExpiryNs int64
}

func (reserveLegacySlotReserved) reserveLegacySlotReply() {}
func (reserveLegacySlotBusy) reserveLegacySlotReply()     {}
func (reserveLegacySlotReserved) status() string          { return "RESERVED" }
func (reserveLegacySlotBusy) status() string              { return "BUSY" }
func (r reserveLegacySlotReserved) toSlotClaim() SlotClaim {
	return SlotClaim{Status: SlotRenewed, Owner: r.Owner, Epoch: r.Epoch, ExpiryNs: r.ExpiryNs}
}

func (r reserveLegacySlotBusy) toSlotClaim() SlotClaim {
	return SlotClaim{Status: SlotBusy, Owner: r.Owner, Epoch: r.Epoch, ExpiryNs: r.ExpiryNs}
}

func decodeReserveLegacySlotReply(r scriptReply) (reserveLegacySlotReply, error) {
	st, err := decodeStatus(r, "RESERVED", "BUSY")
	if err != nil {
		return nil, err
	}
	if err := r.wantArity(4); err != nil {
		return nil, err
	}
	owner, err := r.stringAt(1)
	if err != nil {
		return nil, err
	}
	epoch, err := r.int64At(2)
	if err != nil {
		return nil, err
	}
	expiry, err := r.nsAt(3)
	if err != nil {
		return nil, err
	}
	base := struct {
		Owner    ReplicaID
		Epoch    OwnerEpoch
		ExpiryNs int64
	}{ReplicaID{id: owner}, OwnerEpoch{e: epoch}, expiry}
	switch st {
	case "RESERVED":
		return reserveLegacySlotReserved(base), nil
	case "BUSY":
		return reserveLegacySlotBusy(base), nil
	default:
		return nil, fmt.Errorf("unhandled reserve_legacy_slot status %q", st)
	}
}

type ownerCheckReply interface {
	ownerCheckReply()
	status() string
	toOwnerCheck() OwnerCheck
}
type (
	ownerCheckOwner   struct{}
	ownerCheckFenced  struct{}
	ownerCheckUnowned struct{}
)

func (ownerCheckOwner) ownerCheckReply()           {}
func (ownerCheckFenced) ownerCheckReply()          {}
func (ownerCheckUnowned) ownerCheckReply()         {}
func (ownerCheckOwner) status() string             { return "OWNER" }
func (ownerCheckFenced) status() string            { return "FENCED" }
func (ownerCheckUnowned) status() string           { return "UNOWNED" }
func (ownerCheckOwner) toOwnerCheck() OwnerCheck   { return OwnerCheckOwner }
func (ownerCheckFenced) toOwnerCheck() OwnerCheck  { return OwnerCheckFenced }
func (ownerCheckUnowned) toOwnerCheck() OwnerCheck { return OwnerCheckUnowned }

func decodeOwnerCheckReply(r scriptReply) (ownerCheckReply, error) {
	st, err := decodeStatus(r, "OWNER", "FENCED", "UNOWNED")
	if err != nil {
		return nil, err
	}
	if err := r.wantArity(1); err != nil {
		return nil, err
	}
	switch st {
	case "OWNER":
		return ownerCheckOwner{}, nil
	case "FENCED":
		return ownerCheckFenced{}, nil
	case "UNOWNED":
		return ownerCheckUnowned{}, nil
	default:
		return nil, fmt.Errorf("unhandled check_owner status %q", st)
	}
}
