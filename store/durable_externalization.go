package store

import (
	"context"

	"gecgithub01.walmart.com/auk000v/chronicle/internal/durable"
)

const (
	// MarkerProducerSequence is the durable producer state used for replay detection.
	MarkerProducerSequence durable.Marker = "store:producer-sequence"
	// ActionReplayProducer classifies a retried producer append as accept or duplicate.
	ActionReplayProducer durable.Action = "replay producer append"
	// KeyProducerEpochSeq is the producer idempotence fence.
	KeyProducerEpochSeq durable.IdempotenceKey = "stream_path + producer_id + epoch + seq"
)

var producerReplayExternalization = durable.NewExternalization(
	MarkerProducerSequence,
	ActionReplayProducer,
	KeyProducerEpochSeq,
	durable.NewScanner("ValidateProducer duplicate replay scan", MarkerProducerSequence, ActionReplayProducer, scanProducerReplay),
)

var storeDurableExternalizations = []durable.Externalization{producerReplayExternalization}

func scanProducerReplay(_ context.Context, rt durable.ScanRuntime) error {
	if err := rt.QueryMarker(MarkerProducerSequence); err != nil {
		return err
	}
	return rt.RedriveAction(ActionReplayProducer)
}

func durableProducerReplay() durable.Externalization { return producerReplayExternalization }

// DurableExternalizations returns stream-store crash-boundary outbox declarations.
func DurableExternalizations() []durable.Externalization {
	out := make([]durable.Externalization, len(storeDurableExternalizations))
	copy(out, storeDurableExternalizations)
	return out
}
