package sealevel

import (
	"bytes"
	"math"
	"testing"

	bin "github.com/gagliardetto/binary"
)

// FuzzVoteLockout tests vote lockout serialization/deserialization
func FuzzVoteLockout(f *testing.F) {
	f.Add(makeValidVoteLockout(100, 5))
	f.Add(makeValidVoteLockout(0, 0))
	f.Add(makeValidVoteLockout(math.MaxUint64, math.MaxUint32))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var lockout VoteLockout
		err := lockout.UnmarshalWithDecoder(decoder)
		if err == nil {
			// Test round-trip
			buf := new(bytes.Buffer)
			encoder := bin.NewBinEncoder(buf)
			_ = lockout.MarshalWithEncoder(encoder)

			// Test lockout calculations
			_ = lockout.Lockout()
			_ = lockout.LastLockedOutSlot()
			_ = lockout.IsLockedOutAtSlot(lockout.Slot + 100)

			// Test increment
			lockout.IncreaseConfirmationCount(1)
		}
	})
}

// FuzzLandedVote tests landed vote serialization/deserialization
func FuzzLandedVote(f *testing.F) {
	f.Add(makeValidLandedVote(10, 100, 5))
	f.Add(makeValidLandedVote(0, 0, 0))
	f.Add(makeValidLandedVote(255, math.MaxUint64, math.MaxUint32))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var landedVote LandedVote
		err := landedVote.UnmarshalWithDecoder(decoder)
		if err == nil {
			// Test round-trip
			buf := new(bytes.Buffer)
			encoder := bin.NewBinEncoder(buf)
			_ = landedVote.MarshalWithEncoder(encoder)
		}
	})
}

// FuzzEpochCredits tests epoch credits serialization/deserialization
func FuzzEpochCredits(f *testing.F) {
	f.Add(makeValidEpochCredits(0, 0, 0))
	f.Add(makeValidEpochCredits(100, 1000, 500))
	f.Add(makeValidEpochCredits(math.MaxUint64, math.MaxUint64, math.MaxUint64))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var epochCredits EpochCredits
		err := epochCredits.UnmarshalWithDecoder(decoder)
		if err == nil {
			// Test round-trip
			buf := new(bytes.Buffer)
			encoder := bin.NewBinEncoder(buf)
			_ = epochCredits.MarshalWithEncoder(encoder)
		}
	})
}

// FuzzBlockTimestamp tests block timestamp serialization/deserialization
func FuzzBlockTimestamp(f *testing.F) {
	f.Add(makeValidBlockTimestamp(0, 0))
	f.Add(makeValidBlockTimestamp(1000, 1234567890))
	f.Add(makeValidBlockTimestamp(math.MaxUint64, math.MaxInt64))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var blockTs BlockTimestamp
		err := blockTs.UnmarshalWithDecoder(decoder)
		if err == nil {
			// Test round-trip
			buf := new(bytes.Buffer)
			encoder := bin.NewBinEncoder(buf)
			_ = blockTs.MarshalWithEncoder(encoder)
		}
	})
}

// FuzzPriorVoter tests prior voter serialization/deserialization
func FuzzPriorVoter(f *testing.F) {
	f.Add(makeValidPriorVoter(0, 10, 100, true))
	f.Add(makeValidPriorVoter(100, 200, 1000, false))
	f.Add(makeInvalidPriorVoter())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var priorVoter PriorVoter

		// Test both versions
		err := priorVoter.UnmarshalWithDecoder(decoder, true)
		if err == nil {
			buf := new(bytes.Buffer)
			encoder := bin.NewBinEncoder(buf)
			_ = priorVoter.MarshalWithEncoder(encoder, true)
		}

		// Reset and test v1.14.11 version
		decoder = bin.NewBinDecoder(data)
		err = priorVoter.UnmarshalWithDecoder(decoder, false)
		if err == nil {
			buf := new(bytes.Buffer)
			encoder := bin.NewBinEncoder(buf)
			_ = priorVoter.MarshalWithEncoder(encoder, false)
		}
	})
}

// FuzzPriorVoters tests prior voters circular buffer
func FuzzPriorVoters(f *testing.F) {
	f.Add(makeValidPriorVoters(5, false))
	f.Add(makeValidPriorVoters(32, true))
	f.Add(makeInvalidPriorVoters())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var priorVoters PriorVoters
		err := priorVoters.UnmarshalWithDecoder(decoder)
		if err == nil {
			// Test round-trip
			buf := new(bytes.Buffer)
			encoder := bin.NewBinEncoder(buf)
			err = priorVoters.MarshalWithEncoder(encoder)
			if err == nil {
				// Test accessor methods
				_ = priorVoters.Last()

				// Test append
				newPrior := PriorVoter{
					EpochStart: 100,
					EpochEnd:   200,
					Slot:       1000,
				}
				priorVoters.Append(newPrior)
			}
		}
	})
}

// FuzzAuthorizedVoters tests authorized voters B-tree structure
func FuzzAuthorizedVoters(f *testing.F) {
	f.Add(makeValidAuthorizedVoters(1))
	f.Add(makeValidAuthorizedVoters(10))
	f.Add(makeValidAuthorizedVoters(100))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var authVoters AuthorizedVoters
		err := authVoters.UnmarshalWithDecoder(decoder)
		if err == nil {
			// Test round-trip
			buf := new(bytes.Buffer)
			encoder := bin.NewBinEncoder(buf)
			err = authVoters.MarshalWithEncoder(encoder)
			if err == nil {
				// Test lookup operations
				_, _, _ = authVoters.GetOrCalculateAuthorizedVoterForEpoch(50)
				_, _ = authVoters.GetAndCacheAuthorizedVoterForEpoch(75)

				// Test purge - now returns (bool, error)
				_, _ = authVoters.PurgeAuthorizedVoters(100)
			}
		}
	})
}

// FuzzVoteState0_23_5 tests legacy vote state format
func FuzzVoteState0_23_5(f *testing.F) {
	f.Add(makeValidVoteState0_23_5(3, true))
	f.Add(makeValidVoteState0_23_5(10, false))
	f.Add(makeInvalidVoteState0_23_5())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var voteState VoteState0_23_5
		err := voteState.UnmarshalWithDecoder(decoder)
		if err == nil {
			// Test round-trip
			buf := new(bytes.Buffer)
			encoder := bin.NewBinEncoder(buf)
			_ = voteState.MarshalWithEncoder(encoder)
		}
	})
}

// FuzzVoteState1_14_11 tests intermediate vote state format
func FuzzVoteState1_14_11(f *testing.F) {
	f.Add(makeValidVoteState1_14_11(3, true))
	f.Add(makeValidVoteState1_14_11(10, false))
	f.Add(makeInvalidVoteState1_14_11())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var voteState VoteState1_14_11
		err := voteState.UnmarshalWithDecoder(decoder)
		if err == nil {
			// Test round-trip
			buf := new(bytes.Buffer)
			encoder := bin.NewBinEncoder(buf)
			_ = voteState.MarshalWithEncoder(encoder)
		}
	})
}

// FuzzVoteState tests current vote state format
func FuzzVoteState(f *testing.F) {
	f.Add(makeValidVoteState(3, true))
	f.Add(makeValidVoteState(10, false))
	f.Add(makeInvalidVoteState())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var voteState VoteState
		err := voteState.UnmarshalWithDecoder(decoder)
		if err == nil {
			// Test round-trip
			buf := new(bytes.Buffer)
			encoder := bin.NewBinEncoder(buf)
			err = voteState.MarshalWithEncoder(encoder)
			if err == nil {
				// Test accessor methods
				_ = voteState.Credits()
				_, _ = voteState.GetAndUpdateAuthorizedVoter(100)
			}
		}
	})
}

// FuzzVoteStateVersions tests versioned vote state container
func FuzzVoteStateVersions(f *testing.F) {
	f.Add(makeVersionedVoteStateV0_23_5())
	f.Add(makeVersionedVoteStateV1_14_11())
	f.Add(makeVersionedVoteStateCurrent())
	f.Add(makeInvalidVersionedVoteState())

	f.Fuzz(func(t *testing.T, data []byte) {
		// Test unmarshal
		versionedState, err := UnmarshalVersionedVoteState(data)
		if err == nil {
			// Test version detection
			_ = versionedState.IsInitialized()

			// Test conversion to current
			_ = versionedState.ConvertToCurrent()

			// Note: MarshalVersionedVoteState doesn't exist, but we can test individual versions
			if versionedState.Type == VoteStateVersionV0_23_5 {
				buf := new(bytes.Buffer)
				encoder := bin.NewBinEncoder(buf)
				_ = versionedState.V0_23_5.MarshalWithEncoder(encoder)
			}
		}
	})
}

// Helper functions to create seed data

func makeValidVoteLockout(slot uint64, confirmationCount uint32) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint64(slot, bin.LE)
	encoder.WriteUint32(confirmationCount, bin.LE)
	return buf.Bytes()
}

func makeValidLandedVote(latency byte, slot uint64, confirmationCount uint32) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteByte(latency)
	encoder.WriteUint64(slot, bin.LE)
	encoder.WriteUint32(confirmationCount, bin.LE)
	return buf.Bytes()
}

func makeValidEpochCredits(epoch, credits, prevCredits uint64) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint64(epoch, bin.LE)
	encoder.WriteUint64(credits, bin.LE)
	encoder.WriteUint64(prevCredits, bin.LE)
	return buf.Bytes()
}

func makeValidBlockTimestamp(slot uint64, timestamp int64) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint64(slot, bin.LE)
	encoder.WriteInt64(timestamp, bin.LE)
	return buf.Bytes()
}

func makeValidPriorVoter(epochStart, epochEnd, slot uint64, isVersion0_23_5 bool) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteBytes(make([]byte, 32), false)
	encoder.WriteUint64(epochStart, bin.LE)
	encoder.WriteUint64(epochEnd, bin.LE)
	if isVersion0_23_5 {
		encoder.WriteUint64(slot, bin.LE)
	}
	return buf.Bytes()
}

func makeInvalidPriorVoter() []byte {
	return []byte{1, 2, 3}
}

func makeValidPriorVoters(filledEntries int, isEmpty bool) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)

	// Write 32 prior voter entries
	for i := 0; i < 32; i++ {
		encoder.WriteBytes(make([]byte, 32), false)
		encoder.WriteUint64(uint64(i*10), bin.LE)
		encoder.WriteUint64(uint64(i*10+10), bin.LE)
	}

	// Index
	if filledEntries > 0 {
		encoder.WriteUint64(uint64(filledEntries-1), bin.LE)
	} else {
		encoder.WriteUint64(0, bin.LE)
	}

	// IsEmpty
	encoder.WriteBool(isEmpty)

	return buf.Bytes()
}

func makeInvalidPriorVoters() []byte {
	// Not enough data for 32 entries
	return make([]byte, 100)
}

func makeValidAuthorizedVoters(numVoters int) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)

	encoder.WriteUint64(uint64(numVoters), bin.LE)
	for i := 0; i < numVoters; i++ {
		encoder.WriteUint64(uint64(i*10), bin.LE)   // epoch
		encoder.WriteBytes(make([]byte, 32), false) // pubkey
	}

	return buf.Bytes()
}

func makeValidVoteState0_23_5(numVotes int, hasRoot bool) []byte {
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
	encoder.WriteByte(10)

	// Votes
	encoder.WriteUint64(uint64(numVotes), bin.LE)
	for i := 0; i < numVotes; i++ {
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

	// EpochCredits
	encoder.WriteUint64(0, bin.LE)

	// LastTimestamp
	encoder.WriteUint64(0, bin.LE)
	encoder.WriteInt64(0, bin.LE)

	return buf.Bytes()
}

func makeInvalidVoteState0_23_5() []byte {
	// Truncated state
	return make([]byte, 100)
}

func makeValidVoteState1_14_11(numVotes int, hasRoot bool) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)

	// NodePubkey
	encoder.WriteBytes(make([]byte, 32), false)
	// AuthorizedWithdrawer
	encoder.WriteBytes(make([]byte, 32), false)
	// Commission
	encoder.WriteByte(10)

	// Votes
	encoder.WriteUint64(uint64(numVotes), bin.LE)
	for i := 0; i < numVotes; i++ {
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

func makeInvalidVoteState1_14_11() []byte {
	// Truncated state
	return make([]byte, 100)
}

func makeValidVoteState(numVotes int, hasRoot bool) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)

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

func makeInvalidVoteState() []byte {
	// Truncated state
	return make([]byte, 100)
}

func makeVersionedVoteStateV0_23_5() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint32(VoteStateVersionV0_23_5, bin.LE)
	// Append minimal V0_23_5 state
	buf.Write(makeValidVoteState0_23_5(0, false))
	return buf.Bytes()
}

func makeVersionedVoteStateV1_14_11() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint32(VoteStateVersionV1_14_11, bin.LE)
	// Append minimal V1_14_11 state
	buf.Write(makeValidVoteState1_14_11(0, false))
	return buf.Bytes()
}

func makeVersionedVoteStateCurrent() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint32(VoteStateVersionCurrent, bin.LE)
	// Append minimal current state
	buf.Write(makeValidVoteState(0, false))
	return buf.Bytes()
}

func makeInvalidVersionedVoteState() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	// Invalid version
	encoder.WriteUint32(0xFFFFFFFF, bin.LE)
	return buf.Bytes()
}

// ============================================================================
// ROUND-TRIP FUZZ TESTS
// These test marshal/unmarshal cycles to ensure data integrity
// ============================================================================

// FuzzVoteLockoutRoundTrip tests VoteLockout marshal/unmarshal round-trip
func FuzzVoteLockoutRoundTrip(f *testing.F) {
	f.Add(makeValidVoteLockout(100, 5))
	f.Add(makeValidVoteLockout(0, 0))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var lockout VoteLockout
		err := lockout.UnmarshalWithDecoder(decoder)
		if err != nil {
			return
		}

		// Marshal
		buf := new(bytes.Buffer)
		encoder := bin.NewBinEncoder(buf)
		err = lockout.MarshalWithEncoder(encoder)
		if err != nil {
			t.Errorf("Marshal failed: %v", err)
			return
		}

		// Unmarshal again
		decoder2 := bin.NewBinDecoder(buf.Bytes())
		var lockout2 VoteLockout
		err = lockout2.UnmarshalWithDecoder(decoder2)
		if err != nil {
			t.Errorf("Round-trip unmarshal failed: %v", err)
			return
		}

		// Verify equality
		if lockout.Slot != lockout2.Slot || lockout.ConfirmationCount != lockout2.ConfirmationCount {
			t.Errorf("Round-trip data mismatch: got {%d, %d}, want {%d, %d}",
				lockout2.Slot, lockout2.ConfirmationCount, lockout.Slot, lockout.ConfirmationCount)
		}
	})
}

// FuzzVoteStateVersionsRoundTrip tests VoteStateVersions marshal/unmarshal round-trip
func FuzzVoteStateVersionsRoundTrip(f *testing.F) {
	f.Add(makeVersionedVoteStateV0_23_5())
	f.Add(makeVersionedVoteStateV1_14_11())
	f.Add(makeVersionedVoteStateCurrent())

	f.Fuzz(func(t *testing.T, data []byte) {
		// Unmarshal
		versionedState, err := UnmarshalVersionedVoteState(data)
		if err != nil {
			return
		}

		// Marshal
		buf := new(bytes.Buffer)
		encoder := bin.NewBinEncoder(buf)
		err = versionedState.MarshalWithEncoder(encoder)
		if err != nil {
			t.Errorf("Marshal failed: %v", err)
			return
		}

		// Unmarshal again
		versionedState2, err := UnmarshalVersionedVoteState(buf.Bytes())
		if err != nil {
			t.Errorf("Round-trip unmarshal failed: %v", err)
			return
		}

		// Verify version preserved
		if versionedState.Type != versionedState2.Type {
			t.Errorf("Round-trip version mismatch: got %d, want %d", versionedState2.Type, versionedState.Type)
		}

		// Verify initialized state preserved
		if versionedState.IsInitialized() != versionedState2.IsInitialized() {
			t.Errorf("Round-trip initialization state mismatch")
		}
	})
}

// FuzzLandedVoteRoundTrip tests LandedVote marshal/unmarshal round-trip
func FuzzLandedVoteRoundTrip(f *testing.F) {
	f.Add(makeValidLandedVote(50, 500, 5))
	f.Add(makeValidLandedVote(0, 0, 0))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var landedVote LandedVote
		err := landedVote.UnmarshalWithDecoder(decoder)
		if err != nil {
			return
		}

		// Marshal
		buf := new(bytes.Buffer)
		encoder := bin.NewBinEncoder(buf)
		err = landedVote.MarshalWithEncoder(encoder)
		if err != nil {
			t.Errorf("Marshal failed: %v", err)
			return
		}

		// Unmarshal again
		decoder2 := bin.NewBinDecoder(buf.Bytes())
		var landedVote2 LandedVote
		err = landedVote2.UnmarshalWithDecoder(decoder2)
		if err != nil {
			t.Errorf("Round-trip unmarshal failed: %v", err)
			return
		}

		// Verify equality
		if landedVote.Latency != landedVote2.Latency || landedVote.Lockout.Slot != landedVote2.Lockout.Slot {
			t.Errorf("Round-trip data mismatch")
		}
	})
}

// FuzzEpochCreditsRoundTrip tests EpochCredits marshal/unmarshal round-trip
func FuzzEpochCreditsRoundTrip(f *testing.F) {
	f.Add(makeValidEpochCredits(10, 5000, 4500))
	f.Add(makeValidEpochCredits(0, 0, 0))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var epochCredits EpochCredits
		err := epochCredits.UnmarshalWithDecoder(decoder)
		if err != nil {
			return
		}

		// Marshal
		buf := new(bytes.Buffer)
		encoder := bin.NewBinEncoder(buf)
		err = epochCredits.MarshalWithEncoder(encoder)
		if err != nil {
			t.Errorf("Marshal failed: %v", err)
			return
		}

		// Unmarshal again
		decoder2 := bin.NewBinDecoder(buf.Bytes())
		var epochCredits2 EpochCredits
		err = epochCredits2.UnmarshalWithDecoder(decoder2)
		if err != nil {
			t.Errorf("Round-trip unmarshal failed: %v", err)
			return
		}

		// Verify equality
		if epochCredits.Epoch != epochCredits2.Epoch ||
			epochCredits.Credits != epochCredits2.Credits ||
			epochCredits.PrevCredits != epochCredits2.PrevCredits {
			t.Errorf("Round-trip data mismatch")
		}
	})
}

// FuzzBlockTimestampRoundTrip tests BlockTimestamp marshal/unmarshal round-trip
func FuzzBlockTimestampRoundTrip(f *testing.F) {
	f.Add(makeValidBlockTimestamp(1000, 1609459200))
	f.Add(makeValidBlockTimestamp(0, 0))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var blockTimestamp BlockTimestamp
		err := blockTimestamp.UnmarshalWithDecoder(decoder)
		if err != nil {
			return
		}

		// Marshal
		buf := new(bytes.Buffer)
		encoder := bin.NewBinEncoder(buf)
		err = blockTimestamp.MarshalWithEncoder(encoder)
		if err != nil {
			t.Errorf("Marshal failed: %v", err)
			return
		}

		// Unmarshal again
		decoder2 := bin.NewBinDecoder(buf.Bytes())
		var blockTimestamp2 BlockTimestamp
		err = blockTimestamp2.UnmarshalWithDecoder(decoder2)
		if err != nil {
			t.Errorf("Round-trip unmarshal failed: %v", err)
			return
		}

		// Verify equality
		if blockTimestamp.Slot != blockTimestamp2.Slot || blockTimestamp.Timestamp != blockTimestamp2.Timestamp {
			t.Errorf("Round-trip data mismatch")
		}
	})
}

// FuzzPriorVoter0_23_5RoundTrip tests PriorVoter (v0.23.5) marshal/unmarshal round-trip
func FuzzPriorVoter0_23_5RoundTrip(f *testing.F) {
	f.Add(makeValidPriorVoter(100, 200, 150, true))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var priorVoter PriorVoter
		err := priorVoter.UnmarshalWithDecoder(decoder, true) // v0.23.5
		if err != nil {
			return
		}

		// Marshal
		buf := new(bytes.Buffer)
		encoder := bin.NewBinEncoder(buf)
		err = priorVoter.MarshalWithEncoder(encoder, true) // v0.23.5
		if err != nil {
			t.Errorf("Marshal failed: %v", err)
			return
		}

		// Unmarshal again
		decoder2 := bin.NewBinDecoder(buf.Bytes())
		var priorVoter2 PriorVoter
		err = priorVoter2.UnmarshalWithDecoder(decoder2, true) // v0.23.5
		if err != nil {
			t.Errorf("Round-trip unmarshal failed: %v", err)
			return
		}

		// Verify equality
		if priorVoter.Pubkey != priorVoter2.Pubkey ||
			priorVoter.EpochStart != priorVoter2.EpochStart ||
			priorVoter.EpochEnd != priorVoter2.EpochEnd ||
			priorVoter.Slot != priorVoter2.Slot {
			t.Errorf("Round-trip data mismatch")
		}
	})
}

// FuzzPriorVoter1_14_11RoundTrip tests PriorVoter (v1.14.11) marshal/unmarshal round-trip
func FuzzPriorVoter1_14_11RoundTrip(f *testing.F) {
	f.Add(makeValidPriorVoter(100, 200, 150, false))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var priorVoter PriorVoter
		err := priorVoter.UnmarshalWithDecoder(decoder, false) // v1.14.11
		if err != nil {
			return
		}

		// Marshal
		buf := new(bytes.Buffer)
		encoder := bin.NewBinEncoder(buf)
		err = priorVoter.MarshalWithEncoder(encoder, false) // v1.14.11
		if err != nil {
			t.Errorf("Marshal failed: %v", err)
			return
		}

		// Unmarshal again
		decoder2 := bin.NewBinDecoder(buf.Bytes())
		var priorVoter2 PriorVoter
		err = priorVoter2.UnmarshalWithDecoder(decoder2, false) // v1.14.11
		if err != nil {
			t.Errorf("Round-trip unmarshal failed: %v", err)
			return
		}

		// Verify equality
		if priorVoter.Pubkey != priorVoter2.Pubkey ||
			priorVoter.EpochStart != priorVoter2.EpochStart ||
			priorVoter.EpochEnd != priorVoter2.EpochEnd ||
			priorVoter.Slot != priorVoter2.Slot {
			t.Errorf("Round-trip data mismatch")
		}
	})
}

// FuzzPriorVoters0_23_5RoundTrip tests PriorVoters0_23_5 marshal/unmarshal round-trip
func FuzzPriorVoters0_23_5RoundTrip(f *testing.F) {
	f.Add(makeValidPriorVoters(2, false))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var priorVoters PriorVoters0_23_5
		err := priorVoters.UnmarshalWithDecoder(decoder)
		if err != nil {
			return
		}

		// Marshal
		buf := new(bytes.Buffer)
		encoder := bin.NewBinEncoder(buf)
		err = priorVoters.MarshalWithEncoder(encoder)
		if err != nil {
			t.Errorf("Marshal failed: %v", err)
			return
		}

		// Unmarshal again
		decoder2 := bin.NewBinDecoder(buf.Bytes())
		var priorVoters2 PriorVoters0_23_5
		err = priorVoters2.UnmarshalWithDecoder(decoder2)
		if err != nil {
			t.Errorf("Round-trip unmarshal failed: %v", err)
			return
		}

		// Verify buffer index preserved
		if priorVoters.Index != priorVoters2.Index {
			t.Errorf("Round-trip index mismatch: got %d, want %d",
				priorVoters2.Index, priorVoters.Index)
		}
	})
}

// FuzzPriorVotersRoundTrip tests PriorVoters marshal/unmarshal round-trip
func FuzzPriorVotersRoundTrip(f *testing.F) {
	f.Add(makeValidPriorVoters(2, false))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var priorVoters PriorVoters
		err := priorVoters.UnmarshalWithDecoder(decoder)
		if err != nil {
			return
		}

		// Marshal
		buf := new(bytes.Buffer)
		encoder := bin.NewBinEncoder(buf)
		err = priorVoters.MarshalWithEncoder(encoder)
		if err != nil {
			t.Errorf("Marshal failed: %v", err)
			return
		}

		// Unmarshal again
		decoder2 := bin.NewBinDecoder(buf.Bytes())
		var priorVoters2 PriorVoters
		err = priorVoters2.UnmarshalWithDecoder(decoder2)
		if err != nil {
			t.Errorf("Round-trip unmarshal failed: %v", err)
			return
		}

		// Verify basic structure preserved
		if priorVoters.IsEmpty != priorVoters2.IsEmpty {
			t.Errorf("Round-trip IsEmpty mismatch")
		}
	})
}

// FuzzAuthorizedVoterRoundTrip tests AuthorizedVoter marshal/unmarshal round-trip
func FuzzAuthorizedVoterRoundTrip(f *testing.F) {
	// Create a simple authorized voter seed data
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint64(100, bin.LE)            // epoch
	encoder.WriteBytes(make([]byte, 32), false) // pubkey
	f.Add(buf.Bytes())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var authVoter AuthorizedVoter
		err := authVoter.UnmarshalWithDecoder(decoder)
		if err != nil {
			return
		}

		// Marshal
		buf := new(bytes.Buffer)
		encoder := bin.NewBinEncoder(buf)
		err = authVoter.MarshalWithEncoder(encoder)
		if err != nil {
			t.Errorf("Marshal failed: %v", err)
			return
		}

		// Unmarshal again
		decoder2 := bin.NewBinDecoder(buf.Bytes())
		var authVoter2 AuthorizedVoter
		err = authVoter2.UnmarshalWithDecoder(decoder2)
		if err != nil {
			t.Errorf("Round-trip unmarshal failed: %v", err)
			return
		}

		// Verify equality
		if authVoter.Epoch != authVoter2.Epoch || authVoter.Pubkey != authVoter2.Pubkey {
			t.Errorf("Round-trip data mismatch")
		}
	})
}

// FuzzAuthorizedVotersRoundTrip tests AuthorizedVoters marshal/unmarshal round-trip
func FuzzAuthorizedVotersRoundTrip(f *testing.F) {
	f.Add(makeValidAuthorizedVoters(1))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var authVoters AuthorizedVoters
		err := authVoters.UnmarshalWithDecoder(decoder)
		if err != nil {
			return
		}

		// Marshal
		buf := new(bytes.Buffer)
		encoder := bin.NewBinEncoder(buf)
		err = authVoters.MarshalWithEncoder(encoder)
		if err != nil {
			t.Errorf("Marshal failed: %v", err)
			return
		}

		// Unmarshal again
		decoder2 := bin.NewBinDecoder(buf.Bytes())
		var authVoters2 AuthorizedVoters
		err = authVoters2.UnmarshalWithDecoder(decoder2)
		if err != nil {
			t.Errorf("Round-trip unmarshal failed: %v", err)
			return
		}

		// Verify basic structure preserved (check if both are empty or both are non-empty)
		isEmpty1 := authVoters.AuthorizedVoters.Len() == 0
		isEmpty2 := authVoters2.AuthorizedVoters.Len() == 0
		if isEmpty1 != isEmpty2 {
			t.Errorf("Round-trip empty state mismatch")
		}
	})
}

// FuzzVoteState0_23_5RoundTrip tests VoteState0_23_5 marshal/unmarshal round-trip
func FuzzVoteState0_23_5RoundTrip(f *testing.F) {
	f.Add(makeValidVoteState0_23_5(5, true))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var voteState VoteState0_23_5
		err := voteState.UnmarshalWithDecoder(decoder)
		if err != nil {
			return
		}

		// Marshal
		buf := new(bytes.Buffer)
		encoder := bin.NewBinEncoder(buf)
		err = voteState.MarshalWithEncoder(encoder)
		if err != nil {
			t.Errorf("Marshal failed: %v", err)
			return
		}

		// Unmarshal again
		decoder2 := bin.NewBinDecoder(buf.Bytes())
		var voteState2 VoteState0_23_5
		err = voteState2.UnmarshalWithDecoder(decoder2)
		if err != nil {
			t.Errorf("Round-trip unmarshal failed: %v", err)
			return
		}

		// Verify key fields
		if voteState.NodePubkey != voteState2.NodePubkey ||
			voteState.AuthorizedVoter != voteState2.AuthorizedVoter ||
			voteState.Commission != voteState2.Commission {
			t.Errorf("Round-trip data mismatch")
		}
	})
}

// FuzzVoteState1_14_11RoundTrip tests VoteState1_14_11 marshal/unmarshal round-trip
func FuzzVoteState1_14_11RoundTrip(f *testing.F) {
	f.Add(makeValidVoteState1_14_11(5, true))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var voteState VoteState1_14_11
		err := voteState.UnmarshalWithDecoder(decoder)
		if err != nil {
			return
		}

		// Marshal
		buf := new(bytes.Buffer)
		encoder := bin.NewBinEncoder(buf)
		err = voteState.MarshalWithEncoder(encoder)
		if err != nil {
			t.Errorf("Marshal failed: %v", err)
			return
		}

		// Unmarshal again
		decoder2 := bin.NewBinDecoder(buf.Bytes())
		var voteState2 VoteState1_14_11
		err = voteState2.UnmarshalWithDecoder(decoder2)
		if err != nil {
			t.Errorf("Round-trip unmarshal failed: %v", err)
			return
		}

		// Verify key fields
		if voteState.NodePubkey != voteState2.NodePubkey ||
			voteState.Commission != voteState2.Commission {
			t.Errorf("Round-trip data mismatch")
		}
	})
}

// FuzzVoteStateCurrentRoundTrip tests VoteState (current) marshal/unmarshal round-trip
func FuzzVoteStateCurrentRoundTrip(f *testing.F) {
	f.Add(makeValidVoteState(5, true))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var voteState VoteState
		err := voteState.UnmarshalWithDecoder(decoder)
		if err != nil {
			return
		}

		// Marshal
		buf := new(bytes.Buffer)
		encoder := bin.NewBinEncoder(buf)
		err = voteState.MarshalWithEncoder(encoder)
		if err != nil {
			t.Errorf("Marshal failed: %v", err)
			return
		}

		// Unmarshal again
		decoder2 := bin.NewBinDecoder(buf.Bytes())
		var voteState2 VoteState
		err = voteState2.UnmarshalWithDecoder(decoder2)
		if err != nil {
			t.Errorf("Round-trip unmarshal failed: %v", err)
			return
		}

		// Verify key fields
		if voteState.NodePubkey != voteState2.NodePubkey ||
			voteState.Commission != voteState2.Commission {
			t.Errorf("Round-trip data mismatch")
		}
	})
}
