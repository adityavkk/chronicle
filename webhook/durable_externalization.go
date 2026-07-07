package webhook

import (
	"context"

	"gecgithub01.walmart.com/auk000v/chronicle/internal/durable"
)

// DurableExternalization is the webhook outbox contract: durable marker,
// external action, idempotence key, and recovery scanner.
type DurableExternalization = durable.Externalization

const (
	// MarkerWakeIntentDurable is the durable wake intent minted by arm_wake.
	MarkerWakeIntentDurable durable.Marker = "webhook:wake-intent"
	// MarkerPullWakeEmitUnstamped is a pull-wake with no durable emit stamp.
	MarkerPullWakeEmitUnstamped durable.Marker = "webhook:pull-wake-emit-unstamped"
	// MarkerPullWakeEmitStamped is a stamped pull-wake that may need duplicate reemit.
	MarkerPullWakeEmitStamped durable.Marker = "webhook:pull-wake-emit-stamped"
	// MarkerWebhookDeliveryLease is a webhook delivery lease or retry marker.
	MarkerWebhookDeliveryLease durable.Marker = "webhook:delivery-lease"
	// MarkerClaimGrantDurable is a durable pull-wake claim grant.
	MarkerClaimGrantDurable durable.Marker = "webhook:claim-grant"
	// MarkerSlotOwnershipGrant is a durable slot-owner lease grant.
	MarkerSlotOwnershipGrant durable.Marker = "webhook:slot-ownership-grant"

	// ActionDispatchWake dispatches a webhook or pull-wake notification.
	ActionDispatchWake durable.Action = "dispatch wake"
	// ActionAppendPullWakeEvent appends a wake event to a wake stream.
	ActionAppendPullWakeEvent durable.Action = "append pull-wake event"
	// ActionRedeliverWebhook sends or re-sends a webhook notification.
	ActionRedeliverWebhook durable.Action = "redeliver webhook"
	// ActionReturnClaimToWorker returns a durable claim to an external worker.
	ActionReturnClaimToWorker durable.Action = "return claim to worker"
	// ActionRunOwnedBackgroundWork runs work under an owner epoch.
	ActionRunOwnedBackgroundWork durable.Action = "run owned background work"

	// KeySubscriptionGenerationWake is the subscription wake fence.
	KeySubscriptionGenerationWake durable.IdempotenceKey = "subscription_id + generation + wake_id"
	// KeyClaimShardGenerationWake is the pull-wake claim fence.
	KeyClaimShardGenerationWake durable.IdempotenceKey = "subscription_id + shard + generation + wake_id"
	// KeySlotOwnerEpoch is the slot-owner fence.
	KeySlotOwnerEpoch durable.IdempotenceKey = "slot_key + owner_epoch"
)

var (
	wakeIntentExternalization = durable.NewExternalization(
		MarkerWakeIntentDurable,
		ActionDispatchWake,
		KeySubscriptionGenerationWake,
		durable.NewScanner("sweep/due scan owed wake intents", MarkerWakeIntentDurable, ActionDispatchWake, scanWakeIntents),
	)
	pullWakeUnstampedExternalization = durable.NewExternalization(
		MarkerPullWakeEmitUnstamped,
		ActionAppendPullWakeEvent,
		KeySubscriptionGenerationWake,
		durable.NewScanner("ReemitUnstampedPullWakes scan", MarkerPullWakeEmitUnstamped, ActionAppendPullWakeEvent, scanUnstampedPullWakeEmits),
	)
	pullWakeStampedExternalization = durable.NewExternalization(
		MarkerPullWakeEmitStamped,
		ActionAppendPullWakeEvent,
		KeySubscriptionGenerationWake,
		durable.NewScanner("ReemitStalePullWakes scan", MarkerPullWakeEmitStamped, ActionAppendPullWakeEvent, scanStalePullWakeEmits),
	)
	webhookDeliveryExternalization = durable.NewExternalization(
		MarkerWebhookDeliveryLease,
		ActionRedeliverWebhook,
		KeySubscriptionGenerationWake,
		durable.NewScanner("retry/lease delivery scan", MarkerWebhookDeliveryLease, ActionRedeliverWebhook, scanWebhookDeliveries),
	)
	claimGrantExternalization = durable.NewExternalization(
		MarkerClaimGrantDurable,
		ActionReturnClaimToWorker,
		KeyClaimShardGenerationWake,
		durable.NewScanner("claim lease takeover scan", MarkerClaimGrantDurable, ActionReturnClaimToWorker, scanClaimGrants),
	)
	slotOwnershipExternalization = durable.NewExternalization(
		MarkerSlotOwnershipGrant,
		ActionRunOwnedBackgroundWork,
		KeySlotOwnerEpoch,
		durable.NewScanner("slot owner work scan", MarkerSlotOwnershipGrant, ActionRunOwnedBackgroundWork, scanSlotOwnership),
	)
)

var webhookDurableExternalizations = []DurableExternalization{
	wakeIntentExternalization,
	pullWakeUnstampedExternalization,
	pullWakeStampedExternalization,
	webhookDeliveryExternalization,
	claimGrantExternalization,
	slotOwnershipExternalization,
}

func scanWakeIntents(_ context.Context, rt durable.ScanRuntime) error {
	if err := rt.QueryMarker(MarkerWakeIntentDurable); err != nil {
		return err
	}
	return rt.RedriveAction(ActionDispatchWake)
}

func scanUnstampedPullWakeEmits(_ context.Context, rt durable.ScanRuntime) error {
	if err := rt.QueryMarker(MarkerPullWakeEmitUnstamped); err != nil {
		return err
	}
	return rt.RedriveAction(ActionAppendPullWakeEvent)
}

func scanStalePullWakeEmits(_ context.Context, rt durable.ScanRuntime) error {
	if err := rt.QueryMarker(MarkerPullWakeEmitStamped); err != nil {
		return err
	}
	return rt.RedriveAction(ActionAppendPullWakeEvent)
}

func scanWebhookDeliveries(_ context.Context, rt durable.ScanRuntime) error {
	if err := rt.QueryMarker(MarkerWebhookDeliveryLease); err != nil {
		return err
	}
	return rt.RedriveAction(ActionRedeliverWebhook)
}

func scanClaimGrants(_ context.Context, rt durable.ScanRuntime) error {
	if err := rt.QueryMarker(MarkerClaimGrantDurable); err != nil {
		return err
	}
	return rt.RedriveAction(ActionReturnClaimToWorker)
}

func scanSlotOwnership(_ context.Context, rt durable.ScanRuntime) error {
	if err := rt.QueryMarker(MarkerSlotOwnershipGrant); err != nil {
		return err
	}
	return rt.RedriveAction(ActionRunOwnedBackgroundWork)
}

func durableWakeIntent() DurableExternalization { return wakeIntentExternalization }

func durablePullWakeUnstampedEmit() DurableExternalization { return pullWakeUnstampedExternalization }

func durablePullWakeStampedEmit() DurableExternalization { return pullWakeStampedExternalization }

func durableWebhookDelivery() DurableExternalization { return webhookDeliveryExternalization }

func durableClaimGrant() DurableExternalization { return claimGrantExternalization }

func durableSlotOwnership() DurableExternalization { return slotOwnershipExternalization }

// DurableExternalizations returns the crash-boundary outbox declarations for the
// webhook control plane. Each declaration names the durable marker, external
// action, idempotence key, and recovery scanner for a side effect that crosses a
// crash boundary.
func DurableExternalizations() []DurableExternalization {
	out := make([]DurableExternalization, len(webhookDurableExternalizations))
	copy(out, webhookDurableExternalizations)
	return out
}
