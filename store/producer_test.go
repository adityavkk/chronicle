package store

import (
	"errors"
	"testing"
)

// The table below transcribes PROTOCOL.md §5.2.1's validation pseudocode plus
// the reference implementation's first-contact behavior. The Redis Lua script
// is held to the same table by the integration tests.
func TestValidateProducer(t *testing.T) {
	const now = int64(1700000000)

	st := func(epoch, lastSeq int64) *ProducerState {
		return &ProducerState{Epoch: epoch, LastSeq: lastSeq, LastUpdated: now - 100}
	}

	tests := []struct {
		name       string
		state      *ProducerState
		epoch, seq int64

		wantErr      error
		wantResult   ProducerResult
		wantNewState *ProducerState
		wantCurEpoch int64
		wantExpected int64
		wantReceived int64
		wantLastSeq  int64
	}{
		{
			name:  "new producer seq 0 accepted",
			state: nil, epoch: 0, seq: 0,
			wantResult:   ProducerResultAccepted,
			wantNewState: &ProducerState{Epoch: 0, LastSeq: 0, LastUpdated: now},
		},
		{
			name:  "new producer any epoch accepted at seq 0",
			state: nil, epoch: 7, seq: 0,
			wantResult:   ProducerResultAccepted,
			wantNewState: &ProducerState{Epoch: 7, LastSeq: 0, LastUpdated: now},
		},
		{
			name:  "new producer nonzero seq is a gap with expected 0",
			state: nil, epoch: 0, seq: 3,
			wantErr: ErrProducerSeqGap, wantExpected: 0, wantReceived: 3,
		},
		{
			name:  "stale epoch fenced",
			state: st(5, 9), epoch: 4, seq: 0,
			wantErr: ErrStaleEpoch, wantCurEpoch: 5,
		},
		{
			name:  "epoch bump must start at seq 0",
			state: st(5, 9), epoch: 6, seq: 1,
			wantErr: ErrInvalidEpochSeq,
		},
		{
			name:  "epoch bump at seq 0 accepted, lastSeq resets",
			state: st(5, 9), epoch: 6, seq: 0,
			wantResult:   ProducerResultAccepted,
			wantNewState: &ProducerState{Epoch: 6, LastSeq: 0, LastUpdated: now},
		},
		{
			name:  "duplicate seq returns highest accepted seq",
			state: st(2, 4), epoch: 2, seq: 4,
			wantResult: ProducerResultDuplicate, wantLastSeq: 4,
		},
		{
			name:  "old duplicate seq still reports highest accepted seq",
			state: st(2, 4), epoch: 2, seq: 1,
			wantResult: ProducerResultDuplicate, wantLastSeq: 4,
		},
		{
			name:  "next seq accepted",
			state: st(2, 4), epoch: 2, seq: 5,
			wantResult:   ProducerResultAccepted,
			wantNewState: &ProducerState{Epoch: 2, LastSeq: 5, LastUpdated: now},
			wantLastSeq:  5,
		},
		{
			name:  "seq gap rejected with expected and received",
			state: st(2, 4), epoch: 2, seq: 7,
			wantErr: ErrProducerSeqGap, wantExpected: 5, wantReceived: 7,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, newState, err := ValidateProducer(tt.state, tt.epoch, tt.seq, now)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if result.ProducerResult != tt.wantResult {
				t.Errorf("ProducerResult = %v, want %v", result.ProducerResult, tt.wantResult)
			}
			if result.CurrentEpoch != tt.wantCurEpoch {
				t.Errorf("CurrentEpoch = %d, want %d", result.CurrentEpoch, tt.wantCurEpoch)
			}
			if result.ExpectedSeq != tt.wantExpected {
				t.Errorf("ExpectedSeq = %d, want %d", result.ExpectedSeq, tt.wantExpected)
			}
			if result.ReceivedSeq != tt.wantReceived {
				t.Errorf("ReceivedSeq = %d, want %d", result.ReceivedSeq, tt.wantReceived)
			}
			if result.LastSeq != tt.wantLastSeq {
				t.Errorf("LastSeq = %d, want %d", result.LastSeq, tt.wantLastSeq)
			}
			switch {
			case tt.wantNewState == nil && newState != nil:
				t.Errorf("newState = %+v, want nil", newState)
			case tt.wantNewState != nil && newState == nil:
				t.Errorf("newState = nil, want %+v", tt.wantNewState)
			case tt.wantNewState != nil && *newState != *tt.wantNewState:
				t.Errorf("newState = %+v, want %+v", newState, tt.wantNewState)
			}
		})
	}
}
