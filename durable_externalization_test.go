package chronicle_test

import (
	"context"
	"testing"

	"gecgithub01.walmart.com/auk000v/chronicle/internal/durable"
	"gecgithub01.walmart.com/auk000v/chronicle/store"
	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

func TestDurableExternalizationChecklist(t *testing.T) {
	registries := map[string]int{
		"webhook": 0,
		"store":   0,
	}
	seen := map[string]struct{}{}
	for _, e := range webhook.DurableExternalizations() {
		registries["webhook"]++
		assertDurableExternalization(t, "webhook", e.Marker(), e.Action(), e.IdempotenceKey(), e.Scanner(), e.Complete())
		seen[string(e.Marker())] = struct{}{}
	}
	for _, e := range store.DurableExternalizations() {
		registries["store"]++
		assertDurableExternalization(t, "store", e.Marker(), e.Action(), e.IdempotenceKey(), e.Scanner(), e.Complete())
		seen[string(e.Marker())] = struct{}{}
	}
	for name, count := range registries {
		if count == 0 {
			t.Fatalf("%s registered no durable externalizations", name)
		}
	}
	wantMarkers := []string{
		string(webhook.MarkerWakeIntentDurable),
		string(webhook.MarkerPullWakeEmitUnstamped),
		string(webhook.MarkerPullWakeEmitStamped),
		string(webhook.MarkerWebhookDeliveryLease),
		string(webhook.MarkerClaimGrantDurable),
		string(webhook.MarkerSlotOwnershipGrant),
		string(store.MarkerProducerSequence),
	}
	for _, marker := range wantMarkers {
		if _, ok := seen[marker]; !ok {
			t.Fatalf("durable marker %s is not registered", marker)
		}
	}
}

func assertDurableExternalization(t *testing.T, registry string, marker durable.Marker, action durable.Action, key durable.IdempotenceKey, scanner durable.Scanner, complete bool) {
	t.Helper()
	if !complete {
		t.Fatalf("%s durable externalization marker=%v action=%v is incomplete", registry, marker, action)
	}
	if marker == "" || action == "" || key == "" {
		t.Fatalf("%s durable externalization has empty marker/action/key: %v/%v/%v", registry, marker, action, key)
	}
	if !scanner.NonVacuous() || scanner.MarkerQueried() != marker || scanner.ActionRedriven() != action {
		t.Fatalf("%s durable externalization marker=%v has vacuous scanner", registry, marker)
	}
}

func TestDurableExternalizationRejectsVacuousScanner(t *testing.T) {
	if (durable.Scanner{}).NonVacuous() {
		t.Fatal("zero scanner must be vacuous")
	}
	scanner := durable.NewScanner("query-only", "marker", "action", func(_ context.Context, rt durable.ScanRuntime) error {
		return rt.QueryMarker("marker")
	})
	if scanner.NonVacuous() {
		t.Fatal("scanner that never re-drives must be vacuous")
	}
}
