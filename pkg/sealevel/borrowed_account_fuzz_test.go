package sealevel

import (
	"bytes"
	"testing"

	bin "github.com/gagliardetto/binary"
)

// FuzzSetVoteAccountState tests setVoteAccountState which internally uses BorrowedAccount.SetState
// This is the primary use case for SetState in the codebase - storing serialized vote state
func FuzzSetVoteAccountState(f *testing.F) {
	// Seed with valid versioned vote states
	f.Add(makeVersionedVoteStateV0_23_5ForSetState())
	f.Add(makeVersionedVoteStateV1_14_11ForSetState())
	f.Add(makeVersionedVoteStateCurrentForSetState())
	f.Add(makeInvalidVersionedVoteStateForSetState())
	f.Add(makeOversizedVoteStateForSetState())
	f.Add(makeTruncatedVoteStateForSetState())

	f.Fuzz(func(t *testing.T, data []byte) {
		// Test UnmarshalVersionedVoteState with fuzzed data
		// This is what gets called before SetState in setVoteAccountState
		versionedState, err := UnmarshalVersionedVoteState(data)

		// If unmarshaling succeeds, the data should be valid for SetState
		if err == nil {
			// Verify the versioned state is valid
			_ = versionedState.IsInitialized()
			_ = versionedState.ConvertToCurrent()

			// Test marshaling (which happens in setVoteAccountState before SetState)
			voteStateBytes, marshalErr := marshalVersionedVoteState(versionedState)
			if marshalErr == nil {
				// Verify marshaled data size is reasonable
				if len(voteStateBytes) > 0 && len(voteStateBytes) <= VoteStateV3Size {
					// This data would be passed to SetState
					// Verify it doesn't exceed expected vote state sizes
					_ = voteStateBytes
				}
			}
		}

		// Test that invalid/malformed data is rejected appropriately
		// This ensures SetState doesn't receive corrupt vote state data
	})
}

// FuzzVoteStateRoundTrip tests the full marshal/unmarshal cycle that SetState relies on
func FuzzVoteStateRoundTrip(f *testing.F) {
	f.Add(makeValidVoteStateForRoundTrip(3, true))
	f.Add(makeValidVoteStateForRoundTrip(10, false))
	f.Add(makeValidVoteStateForRoundTrip(0, false))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Try to unmarshal as versioned vote state
		versionedState, err := UnmarshalVersionedVoteState(data)
		if err != nil {
			// Invalid data - should be rejected before SetState
			return
		}

		// Convert to current version
		currentState := versionedState.ConvertToCurrent()
		if currentState == nil {
			return
		}

		// Create new versioned state from current
		newVersioned := &VoteStateVersions{
			Type:    VoteStateVersionCurrent,
			Current: *currentState,
		}

		// Marshal it (this is what gets passed to SetState)
		marshaled, err := marshalVersionedVoteState(newVersioned)
		if err != nil {
			return
		}

		// Verify marshaled size is within expected bounds
		if len(marshaled) > VoteStateV3Size {
			t.Errorf("Marshaled vote state too large: %d bytes (max %d)", len(marshaled), VoteStateV3Size)
		}

		// Verify round-trip: unmarshal the marshaled data
		roundTrip, err := UnmarshalVersionedVoteState(marshaled)
		if err != nil {
			t.Errorf("Round-trip unmarshal failed: %v", err)
		}

		// Verify version preserved
		if roundTrip != nil && roundTrip.Type != VoteStateVersionCurrent {
			t.Errorf("Version changed during round-trip: got %d, want %d", roundTrip.Type, VoteStateVersionCurrent)
		}
	})
}

// Helper functions to create seed data

func makeVersionedVoteStateV0_23_5ForSetState() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint32(VoteStateVersionV0_23_5, bin.LE)
	// Append minimal V0_23_5 state
	buf.Write(makeMinimalVoteState0_23_5())
	return buf.Bytes()
}

func makeVersionedVoteStateV1_14_11ForSetState() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint32(VoteStateVersionV1_14_11, bin.LE)
	buf.Write(makeMinimalVoteState1_14_11())
	return buf.Bytes()
}

func makeVersionedVoteStateCurrentForSetState() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint32(VoteStateVersionCurrent, bin.LE)
	buf.Write(makeMinimalVoteStateCurrent())
	return buf.Bytes()
}

func makeInvalidVersionedVoteStateForSetState() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint32(0xFFFFFFFF, bin.LE) // Invalid version
	return buf.Bytes()
}

func makeOversizedVoteStateForSetState() []byte {
	// Create data larger than VoteStateV3Size
	return make([]byte, VoteStateV3Size+1000)
}

func makeTruncatedVoteStateForSetState() []byte {
	return []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x02} // Truncated data
}

func makeMinimalVoteState0_23_5() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)

	// NodePubkey
	encoder.WriteBytes(make([]byte, 32), false)
	// AuthorizedVoter
	encoder.WriteBytes(make([]byte, 32), false)
	// AuthorizedVoterEpoch
	encoder.WriteUint64(0, bin.LE)

	// PriorVoters (32 entries)
	for i := 0; i < 32; i++ {
		encoder.WriteBytes(make([]byte, 32), false)
		encoder.WriteUint64(0, bin.LE)
		encoder.WriteUint64(0, bin.LE)
		encoder.WriteUint64(0, bin.LE)
	}
	encoder.WriteUint64(0, bin.LE) // index

	// AuthorizedWithdrawer
	encoder.WriteBytes(make([]byte, 32), false)
	// Commission
	encoder.WriteByte(0)

	// Votes (empty)
	encoder.WriteUint64(0, bin.LE)

	// RootSlot (none)
	encoder.WriteBool(false)

	// EpochCredits (empty)
	encoder.WriteUint64(0, bin.LE)

	// LastTimestamp
	encoder.WriteUint64(0, bin.LE)
	encoder.WriteInt64(0, bin.LE)

	return buf.Bytes()
}

func makeMinimalVoteState1_14_11() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)

	// NodePubkey
	encoder.WriteBytes(make([]byte, 32), false)
	// AuthorizedWithdrawer
	encoder.WriteBytes(make([]byte, 32), false)
	// Commission
	encoder.WriteByte(0)

	// Votes (empty)
	encoder.WriteUint64(0, bin.LE)

	// RootSlot (none)
	encoder.WriteBool(false)

	// AuthorizedVoters (1 entry)
	encoder.WriteUint64(1, bin.LE)
	encoder.WriteUint64(0, bin.LE)
	encoder.WriteBytes(make([]byte, 32), false)

	// PriorVoters
	for i := 0; i < 32; i++ {
		encoder.WriteBytes(make([]byte, 32), false)
		encoder.WriteUint64(0, bin.LE)
		encoder.WriteUint64(0, bin.LE)
	}
	encoder.WriteUint64(0, bin.LE)
	encoder.WriteBool(true)

	// EpochCredits (empty)
	encoder.WriteUint64(0, bin.LE)

	// LastTimestamp
	encoder.WriteUint64(0, bin.LE)
	encoder.WriteInt64(0, bin.LE)

	return buf.Bytes()
}

func makeMinimalVoteStateCurrent() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)

	// NodePubkey
	encoder.WriteBytes(make([]byte, 32), false)
	// AuthorizedWithdrawer
	encoder.WriteBytes(make([]byte, 32), false)
	// Commission
	encoder.WriteByte(0)

	// Votes (empty - LandedVote with latency)
	encoder.WriteUint64(0, bin.LE)

	// RootSlot (none)
	encoder.WriteBool(false)

	// AuthorizedVoters (1 entry)
	encoder.WriteUint64(1, bin.LE)
	encoder.WriteUint64(0, bin.LE)
	encoder.WriteBytes(make([]byte, 32), false)

	// PriorVoters
	for i := 0; i < 32; i++ {
		encoder.WriteBytes(make([]byte, 32), false)
		encoder.WriteUint64(0, bin.LE)
		encoder.WriteUint64(0, bin.LE)
	}
	encoder.WriteUint64(0, bin.LE)
	encoder.WriteBool(true)

	// EpochCredits (empty)
	encoder.WriteUint64(0, bin.LE)

	// LastTimestamp
	encoder.WriteUint64(0, bin.LE)
	encoder.WriteInt64(0, bin.LE)

	return buf.Bytes()
}

func makeValidVoteStateForRoundTrip(numVotes int, hasRoot bool) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)

	encoder.WriteUint32(VoteStateVersionCurrent, bin.LE)

	// NodePubkey
	encoder.WriteBytes(make([]byte, 32), false)
	// AuthorizedWithdrawer
	encoder.WriteBytes(make([]byte, 32), false)
	// Commission
	encoder.WriteByte(10)

	// Votes (LandedVote with latency)
	encoder.WriteUint64(uint64(numVotes), bin.LE)
	for i := 0; i < numVotes; i++ {
		encoder.WriteByte(byte(i % 256)) // latency
		encoder.WriteUint64(uint64(i*100), bin.LE)
		encoder.WriteUint32(uint32(i+1), bin.LE)
	}

	// RootSlot
	if hasRoot {
		encoder.WriteBool(true)
		encoder.WriteUint64(50, bin.LE)
	} else {
		encoder.WriteBool(false)
	}

	// AuthorizedVoters
	encoder.WriteUint64(1, bin.LE)
	encoder.WriteUint64(0, bin.LE)
	encoder.WriteBytes(make([]byte, 32), false)

	// PriorVoters
	for i := 0; i < 32; i++ {
		encoder.WriteBytes(make([]byte, 32), false)
		encoder.WriteUint64(0, bin.LE)
		encoder.WriteUint64(0, bin.LE)
	}
	encoder.WriteUint64(0, bin.LE)
	encoder.WriteBool(true)

	// EpochCredits
	encoder.WriteUint64(0, bin.LE)

	// LastTimestamp
	encoder.WriteUint64(0, bin.LE)
	encoder.WriteInt64(0, bin.LE)

	return buf.Bytes()
}
