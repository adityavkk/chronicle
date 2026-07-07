package store

import "gecgithub01.walmart.com/auk000v/chronicle/internal/durable"

const (
	// MarkerProducerSequence is the durable producer state used for replay detection.
	MarkerProducerSequence durable.Marker = "store:producer-sequence"
	// ActionReplayProducer classifies a retried producer append as accept or duplicate.
	ActionReplayProducer durable.Action = "replay producer append"
	// KeyProducerEpochSeq is the producer idempotence fence.
	KeyProducerEpochSeq durable.IdempotenceKey = "stream_path + producer_id + epoch + seq"
)

var storeDurableExternalizations = []durable.Externalization{
	durable.NewExternalization(
		MarkerProducerSequence,
		ActionReplayProducer,
		KeyProducerEpochSeq,
		durable.NewScanner("ValidateProducer scans producer state and classifies duplicate replay", MarkerProducerSequence, ActionReplayProducer),
	),
}

// DurableExternalizations returns stream-store crash-boundary outbox declarations.
func DurableExternalizations() []durable.Externalization {
	out := make([]durable.Externalization, len(storeDurableExternalizations))
	copy(out, storeDurableExternalizations)
	return out
}
