// Ported from the Durable Streams reference Caddy plugin
// (packages/caddy-plugin/store/memory_store.go validateProducer @ 82f9963),
// lifted into a pure function: the clock is injected so the function is
// deterministic. It is the single source of truth for idempotent-producer
// semantics (PROTOCOL.md §5.2.1) — the Redis Lua script mirrors it, and the
// tests in producer_test.go hold the two implementations to the same table.
package store

// ValidateProducer applies the idempotent-producer state machine to one
// request. state is the producer's current state, or nil on first contact.
// nowUnix stamps NewState.LastUpdated.
//
// The returned AppendResult carries the response-shaping fields for every
// outcome (CurrentEpoch on ErrStaleEpoch, Expected/ReceivedSeq on
// ErrProducerSeqGap, LastSeq on success and duplicates). The returned
// *ProducerState is non-nil only when the caller must persist new state.
func ValidateProducer(state *ProducerState, epoch, seq, nowUnix int64) (AppendResult, *ProducerState, error) {
	// No existing state - accept as new producer
	if state == nil {
		if seq != 0 {
			// First message from producer must be seq=0
			return AppendResult{
				ProducerResult: ProducerResultNone,
				ExpectedSeq:    0,
				ReceivedSeq:    seq,
			}, nil, ErrProducerSeqGap
		}
		newState := &ProducerState{
			Epoch:       epoch,
			LastSeq:     0,
			LastUpdated: nowUnix,
		}
		return AppendResult{
			ProducerResult: ProducerResultAccepted,
			LastSeq:        0,
		}, newState, nil
	}

	// Epoch validation (client-declared, server-validated)
	if epoch < state.Epoch {
		// Stale epoch - zombie fencing
		return AppendResult{
			ProducerResult: ProducerResultNone,
			CurrentEpoch:   state.Epoch,
		}, nil, ErrStaleEpoch
	}

	if epoch > state.Epoch {
		// New epoch - must start at seq=0
		if seq != 0 {
			return AppendResult{
				ProducerResult: ProducerResultNone,
			}, nil, ErrInvalidEpochSeq
		}
		newState := &ProducerState{
			Epoch:       epoch,
			LastSeq:     0,
			LastUpdated: nowUnix,
		}
		return AppendResult{
			ProducerResult: ProducerResultAccepted,
			LastSeq:        0,
		}, newState, nil
	}

	// Same epoch - sequence validation
	if seq <= state.LastSeq {
		// Duplicate - idempotent success
		return AppendResult{
			ProducerResult: ProducerResultDuplicate,
			LastSeq:        state.LastSeq,
		}, nil, nil
	}

	if seq == state.LastSeq+1 {
		newState := &ProducerState{
			Epoch:       epoch,
			LastSeq:     seq,
			LastUpdated: nowUnix,
		}
		return AppendResult{
			ProducerResult: ProducerResultAccepted,
			LastSeq:        seq,
		}, newState, nil
	}

	// seq > lastSeq + 1 - gap detected
	return AppendResult{
		ProducerResult: ProducerResultNone,
		ExpectedSeq:    state.LastSeq + 1,
		ReceivedSeq:    seq,
	}, nil, ErrProducerSeqGap
}
