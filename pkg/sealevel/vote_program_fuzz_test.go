package sealevel

import (
	"bytes"
	"encoding/binary"
	"testing"

	bin "github.com/gagliardetto/binary"
)

// FuzzVoteInstrVoteInit tests vote initialization instruction deserialization
func FuzzVoteInstrVoteInit(f *testing.F) {
	f.Add(makeValidVoteInitInstr())
	f.Add(makeInvalidVoteInitInstr())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var voteInit VoteInstrVoteInit
		_ = voteInit.UnmarshalWithDecoder(decoder)
	})
}

// FuzzVoteInstrVoteAuthorize tests authorization instruction deserialization
func FuzzVoteInstrVoteAuthorize(f *testing.F) {
	f.Add(makeValidVoteAuthorizeInstr(VoteAuthorizeTypeVoter))
	f.Add(makeValidVoteAuthorizeInstr(VoteAuthorizeTypeWithdrawer))
	f.Add(makeInvalidVoteAuthorizeInstr())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var voteAuth VoteInstrVoteAuthorize
		_ = voteAuth.UnmarshalWithDecoder(decoder)
	})
}

// FuzzVoteInstrVote tests vote instruction deserialization
func FuzzVoteInstrVote(f *testing.F) {
	f.Add(makeValidVoteInstr([]uint64{1, 2, 3}, true))
	f.Add(makeValidVoteInstr([]uint64{100, 200, 300}, false))
	f.Add(makeInvalidVoteInstr())
	f.Add(makeOversizedVoteInstr())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var vote VoteInstrVote
		_ = vote.UnmarshalWithDecoder(decoder)
	})
}

// FuzzVoteInstrWithdraw tests withdraw instruction deserialization
func FuzzVoteInstrWithdraw(f *testing.F) {
	f.Add(makeValidWithdrawInstr(0))
	f.Add(makeValidWithdrawInstr(1000000))
	f.Add(makeValidWithdrawInstr(^uint64(0)))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var withdraw VoteInstrWithdraw
		_ = withdraw.UnmarshalWithDecoder(decoder)
	})
}

// FuzzVoteInstrUpdateCommission tests commission update instruction deserialization
func FuzzVoteInstrUpdateCommission(f *testing.F) {
	f.Add(makeValidUpdateCommissionInstr(0))
	f.Add(makeValidUpdateCommissionInstr(50))
	f.Add(makeValidUpdateCommissionInstr(100))
	f.Add(makeValidUpdateCommissionInstr(255))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var updateComm VoteInstrUpdateCommission
		_ = updateComm.UnmarshalWithDecoder(decoder)
	})
}

// FuzzVoteInstrVoteSwitch tests vote switch instruction deserialization
func FuzzVoteInstrVoteSwitch(f *testing.F) {
	f.Add(makeValidVoteSwitchInstr([]uint64{1, 2, 3}))
	f.Add(makeInvalidVoteSwitchInstr())
	f.Add(makeOversizedVoteSwitchInstr())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var voteSwitch VoteInstrVoteSwitch
		_ = voteSwitch.UnmarshalWithDecoder(decoder)
	})
}

// FuzzVoteInstrUpdateVoteState tests vote state update instruction deserialization
func FuzzVoteInstrUpdateVoteState(f *testing.F) {
	f.Add(makeValidUpdateVoteStateInstr(3, true, true))
	f.Add(makeValidUpdateVoteStateInstr(10, false, false))
	f.Add(makeInvalidUpdateVoteStateInstr())
	f.Add(makeOversizedUpdateVoteStateInstr())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var updateVoteState VoteInstrUpdateVoteState
		_ = updateVoteState.UnmarshalWithDecoder(decoder)
	})
}

// FuzzVoteInstrAuthorizeWithSeed tests authorize with seed instruction deserialization
func FuzzVoteInstrAuthorizeWithSeed(f *testing.F) {
	f.Add(makeValidAuthorizeWithSeedInstr("test_seed"))
	f.Add(makeValidAuthorizeWithSeedInstr(""))
	f.Add(makeInvalidAuthorizeWithSeedInstr())
	f.Add(makeInvalidUTF8AuthorizeWithSeedInstr())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var authWithSeed VoteInstrAuthorizeWithSeed
		_ = authWithSeed.UnmarshalWithDecoder(decoder)
	})
}

// FuzzLockoutOffset tests lockout offset deserialization
func FuzzLockoutOffset(f *testing.F) {
	f.Add(makeValidLockoutOffset(0, 1))
	f.Add(makeValidLockoutOffset(100, 5))
	f.Add(makeValidLockoutOffset(^uint64(0), 32))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var lockoutOffset LockoutOffset
		_ = lockoutOffset.UnmarshalWithDecoder(decoder)
	})
}

// FuzzCompactUpdateVoteState tests compact vote state deserialization
func FuzzCompactUpdateVoteState(f *testing.F) {
	f.Add(makeValidCompactUpdateVoteState(3, true))
	f.Add(makeValidCompactUpdateVoteState(10, false))
	f.Add(makeInvalidCompactUpdateVoteState())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var compactUpdate CompactUpdateVoteState
		_ = compactUpdate.UnmarshalWithDecoder(decoder)
	})
}

// FuzzVoteInstrTowerSync tests tower sync instruction deserialization
func FuzzVoteInstrTowerSync(f *testing.F) {
	f.Add(makeValidTowerSyncInstr(5, true))
	f.Add(makeValidTowerSyncInstr(10, false))
	f.Add(makeInvalidTowerSyncInstr())

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var towerSync VoteInstrTowerSync
		_ = towerSync.UnmarshalWithDecoder(decoder)
	})
}

// FuzzIsCommissionUpdateAllowed tests commission update timing validation
func FuzzIsCommissionUpdateAllowed(f *testing.F) {
	f.Add(uint64(0), uint64(0), uint64(1000), uint64(100))
	f.Add(uint64(500), uint64(0), uint64(1000), uint64(100))
	f.Add(uint64(999), uint64(0), uint64(1000), uint64(100))

	f.Fuzz(func(t *testing.T, slot, firstNormalSlot, slotsPerEpoch uint64, warmup uint64) {
		if slotsPerEpoch == 0 {
			slotsPerEpoch = 1 // Avoid division by zero
		}

		epochSchedule := SysvarEpochSchedule{
			SlotsPerEpoch:   slotsPerEpoch,
			FirstNormalSlot: firstNormalSlot,
		}

		_ = isCommissionUpdateAllowed(slot, epochSchedule)
	})
}

// Helper functions to create seed data

func makeValidVoteInitInstr() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)

	// NodePubkey
	encoder.WriteBytes(make([]byte, 32), false)
	// AuthorizedVoter
	encoder.WriteBytes(make([]byte, 32), false)
	// AuthorizedWithdrawer
	encoder.WriteBytes(make([]byte, 32), false)
	// Commission
	encoder.WriteByte(10)

	return buf.Bytes()
}

func makeInvalidVoteInitInstr() []byte {
	// Truncated instruction
	return []byte{1, 2, 3, 4}
}

func makeValidVoteAuthorizeInstr(authType uint32) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)

	encoder.WriteBytes(make([]byte, 32), false)
	encoder.WriteUint32(authType, bin.LE)

	return buf.Bytes()
}

func makeInvalidVoteAuthorizeInstr() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)

	encoder.WriteBytes(make([]byte, 32), false)
	// Invalid authorization type
	encoder.WriteUint32(0xFFFFFFFF, bin.LE)

	return buf.Bytes()
}

func makeValidVoteInstr(slots []uint64, hasTimestamp bool) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)

	encoder.WriteUint64(uint64(len(slots)), bin.LE)
	for _, slot := range slots {
		encoder.WriteUint64(slot, bin.LE)
	}
	encoder.WriteBytes(make([]byte, 32), false) // hash

	if hasTimestamp {
		encoder.WriteBool(true)
		encoder.WriteInt64(1234567890, bin.LE)
	} else {
		encoder.WriteBool(false)
	}

	return buf.Bytes()
}

func makeInvalidVoteInstr() []byte {
	// Invalid slot count
	return []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
}

func makeOversizedVoteInstr() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)

	// Make instruction larger than 1232 bytes
	encoder.WriteUint64(200, bin.LE) // too many slots
	for i := 0; i < 200; i++ {
		encoder.WriteUint64(uint64(i), bin.LE)
	}

	return buf.Bytes()
}

func makeValidWithdrawInstr(lamports uint64) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteUint64(lamports, bin.LE)
	return buf.Bytes()
}

func makeValidUpdateCommissionInstr(commission byte) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)
	encoder.WriteByte(commission)
	return buf.Bytes()
}

func makeValidVoteSwitchInstr(slots []uint64) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)

	// Vote part
	encoder.WriteUint64(uint64(len(slots)), bin.LE)
	for _, slot := range slots {
		encoder.WriteUint64(slot, bin.LE)
	}
	encoder.WriteBytes(make([]byte, 32), false) // vote hash
	encoder.WriteBool(false)                    // no timestamp

	// Switch hash
	encoder.WriteBytes(make([]byte, 32), false)

	return buf.Bytes()
}

func makeInvalidVoteSwitchInstr() []byte {
	// Truncated instruction
	return []byte{1, 2, 3}
}

func makeOversizedVoteSwitchInstr() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)

	// Make instruction larger than 1232 bytes
	encoder.WriteUint64(200, bin.LE) // too many slots
	for i := 0; i < 200; i++ {
		encoder.WriteUint64(uint64(i), bin.LE)
	}
	encoder.WriteBytes(make([]byte, 32), false)
	encoder.WriteBool(false)
	encoder.WriteBytes(make([]byte, 32), false)

	return buf.Bytes()
}

func makeValidUpdateVoteStateInstr(numLockouts int, hasRoot bool, hasTimestamp bool) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)

	encoder.WriteUint64(uint64(numLockouts), bin.LE)
	for i := 0; i < numLockouts; i++ {
		encoder.WriteUint64(uint64(i*100), bin.LE) // slot
		encoder.WriteUint32(uint32(i+1), bin.LE)   // confirmation_count
	}

	if hasRoot {
		encoder.WriteBool(true)
		encoder.WriteUint64(50, bin.LE)
	} else {
		encoder.WriteBool(false)
	}

	encoder.WriteBytes(make([]byte, 32), false) // hash

	if hasTimestamp {
		encoder.WriteBool(true)
		encoder.WriteInt64(1234567890, bin.LE)
	} else {
		encoder.WriteBool(false)
	}

	return buf.Bytes()
}

func makeInvalidUpdateVoteStateInstr() []byte {
	// Invalid lockout count
	return []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
}

func makeOversizedUpdateVoteStateInstr() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)

	// Too many lockouts (would cause overflow with * 12 check)
	encoder.WriteUint64(^uint64(0)/12+1, bin.LE)

	return buf.Bytes()
}

func makeValidAuthorizeWithSeedInstr(seed string) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)

	encoder.WriteUint32(VoteAuthorizeTypeVoter, bin.LE)
	encoder.WriteBytes(make([]byte, 32), false) // derived key owner
	encoder.WriteRustString(seed)
	encoder.WriteBytes(make([]byte, 32), false) // new authority

	return buf.Bytes()
}

func makeInvalidAuthorizeWithSeedInstr() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)

	encoder.WriteUint32(VoteAuthorizeTypeVoter, bin.LE)
	encoder.WriteBytes(make([]byte, 32), false) // derived key owner
	// Invalid string length
	encoder.WriteUint64(0xFFFFFFFFFFFFFFFF, bin.LE)

	return buf.Bytes()
}

func makeInvalidUTF8AuthorizeWithSeedInstr() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)

	encoder.WriteUint32(VoteAuthorizeTypeVoter, bin.LE)
	encoder.WriteBytes(make([]byte, 32), false) // derived key owner
	// Invalid UTF-8 string
	encoder.WriteUint64(3, bin.LE)
	encoder.WriteBytes([]byte{0xFF, 0xFE, 0xFD}, false)

	return buf.Bytes()
}

func makeValidLockoutOffset(offset uint64, confirmationCount byte) []byte {
	buf := new(bytes.Buffer)

	// Write as varint manually
	varIntBuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(varIntBuf, offset)
	buf.Write(varIntBuf[:n])
	buf.WriteByte(confirmationCount)

	return buf.Bytes()
}

func makeValidCompactUpdateVoteState(numOffsets int, hasTimestamp bool) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)

	encoder.WriteUint64(100, bin.LE) // root

	encoder.WriteCompactU16(numOffsets)
	for i := 0; i < numOffsets; i++ {
		// Write varint manually
		varIntBuf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(varIntBuf, uint64(i+1))
		buf.Write(varIntBuf[:n])
		encoder.WriteByte(byte(i + 1))
	}

	encoder.WriteBytes(make([]byte, 32), false) // hash

	if hasTimestamp {
		encoder.WriteBool(true)
		encoder.WriteInt64(1234567890, bin.LE)
	} else {
		encoder.WriteBool(false)
	}

	return buf.Bytes()
}

func makeInvalidCompactUpdateVoteState() []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)

	encoder.WriteUint64(100, bin.LE) // root
	// Invalid compact u16
	encoder.WriteUint16(0xFFFF, bin.LE)

	return buf.Bytes()
}

func makeValidTowerSyncInstr(numOffsets int, hasTimestamp bool) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBinEncoder(buf)

	encoder.WriteUint64(100, bin.LE) // root

	encoder.WriteCompactU16(numOffsets)
	for i := 0; i < numOffsets; i++ {
		// Write varint manually
		varIntBuf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(varIntBuf, uint64(i+1))
		buf.Write(varIntBuf[:n])
		encoder.WriteByte(byte(i + 1))
	}

	encoder.WriteBytes(make([]byte, 32), false) // hash

	if hasTimestamp {
		encoder.WriteBool(true)
		encoder.WriteInt64(1234567890, bin.LE)
	} else {
		encoder.WriteBool(false)
	}

	encoder.WriteBytes(make([]byte, 32), false) // block_id

	return buf.Bytes()
}

func makeInvalidTowerSyncInstr() []byte {
	// Truncated tower sync
	return []byte{1, 2, 3, 4}
}

// ============================================================================
// ROUND-TRIP FUZZ TESTS
// These test marshal/unmarshal cycles to ensure data integrity
// ============================================================================

// FuzzVoteInstrVoteInitRoundTrip tests VoteInstrVoteInit marshal/unmarshal round-trip
func FuzzVoteInstrVoteInitRoundTrip(f *testing.F) {
	f.Add(makeValidVoteInitInstr())

	f.Fuzz(func(t *testing.T, data []byte) {
		// Unmarshal
		decoder := bin.NewBinDecoder(data)
		var voteInit VoteInstrVoteInit
		err := voteInit.UnmarshalWithDecoder(decoder)
		if err != nil {
			return // Invalid data
		}

		// Marshal back - Note: VoteInstrVoteInit doesn't have MarshalWithEncoder
		// So we manually encode it
		buf := new(bytes.Buffer)
		encoder := bin.NewBinEncoder(buf)
		encoder.WriteBytes(voteInit.NodePubkey[:], false)
		encoder.WriteBytes(voteInit.AuthorizedVoter[:], false)
		encoder.WriteBytes(voteInit.AuthorizedWithdrawer[:], false)
		encoder.WriteByte(voteInit.Commission)

		marshaled := buf.Bytes()

		// Unmarshal again
		decoder2 := bin.NewBinDecoder(marshaled)
		var voteInit2 VoteInstrVoteInit
		err = voteInit2.UnmarshalWithDecoder(decoder2)
		if err != nil {
			t.Errorf("Round-trip unmarshal failed: %v", err)
			return
		}

		// Verify equality
		if voteInit.NodePubkey != voteInit2.NodePubkey ||
			voteInit.AuthorizedVoter != voteInit2.AuthorizedVoter ||
			voteInit.AuthorizedWithdrawer != voteInit2.AuthorizedWithdrawer ||
			voteInit.Commission != voteInit2.Commission {
			t.Errorf("Round-trip data mismatch")
		}
	})
}

// FuzzVoteInstrVoteAuthorizeRoundTrip tests VoteInstrVoteAuthorize marshal/unmarshal round-trip
func FuzzVoteInstrVoteAuthorizeRoundTrip(f *testing.F) {
	f.Add(makeValidVoteAuthorizeInstr(VoteAuthorizeTypeVoter))
	f.Add(makeValidVoteAuthorizeInstr(VoteAuthorizeTypeWithdrawer))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var voteAuth VoteInstrVoteAuthorize
		err := voteAuth.UnmarshalWithDecoder(decoder)
		if err != nil {
			return
		}

		// Marshal
		buf := new(bytes.Buffer)
		encoder := bin.NewBinEncoder(buf)
		encoder.WriteBytes(voteAuth.Pubkey[:], false)
		encoder.WriteUint32(voteAuth.VoteAuthorize, bin.LE)

		// Unmarshal again
		decoder2 := bin.NewBinDecoder(buf.Bytes())
		var voteAuth2 VoteInstrVoteAuthorize
		err = voteAuth2.UnmarshalWithDecoder(decoder2)
		if err != nil {
			t.Errorf("Round-trip unmarshal failed: %v", err)
			return
		}

		// Verify equality
		if voteAuth.Pubkey != voteAuth2.Pubkey || voteAuth.VoteAuthorize != voteAuth2.VoteAuthorize {
			t.Errorf("Round-trip data mismatch")
		}
	})
}

// FuzzVoteInstrWithdrawRoundTrip tests VoteInstrWithdraw marshal/unmarshal round-trip
func FuzzVoteInstrWithdrawRoundTrip(f *testing.F) {
	f.Add(makeValidWithdrawInstr(0))
	f.Add(makeValidWithdrawInstr(1000000))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var withdraw VoteInstrWithdraw
		err := withdraw.UnmarshalWithDecoder(decoder)
		if err != nil {
			return
		}

		// Marshal
		buf := new(bytes.Buffer)
		encoder := bin.NewBinEncoder(buf)
		encoder.WriteUint64(withdraw.Lamports, bin.LE)

		// Unmarshal again
		decoder2 := bin.NewBinDecoder(buf.Bytes())
		var withdraw2 VoteInstrWithdraw
		err = withdraw2.UnmarshalWithDecoder(decoder2)
		if err != nil {
			t.Errorf("Round-trip unmarshal failed: %v", err)
			return
		}

		// Verify equality
		if withdraw.Lamports != withdraw2.Lamports {
			t.Errorf("Round-trip data mismatch: got %d, want %d", withdraw2.Lamports, withdraw.Lamports)
		}
	})
}

// FuzzVoteInstrUpdateCommissionRoundTrip tests VoteInstrUpdateCommission marshal/unmarshal round-trip
func FuzzVoteInstrUpdateCommissionRoundTrip(f *testing.F) {
	f.Add(makeValidUpdateCommissionInstr(0))
	f.Add(makeValidUpdateCommissionInstr(100))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var updateComm VoteInstrUpdateCommission
		err := updateComm.UnmarshalWithDecoder(decoder)
		if err != nil {
			return
		}

		// Marshal
		buf := new(bytes.Buffer)
		encoder := bin.NewBinEncoder(buf)
		encoder.WriteByte(updateComm.Commission)

		// Unmarshal again
		decoder2 := bin.NewBinDecoder(buf.Bytes())
		var updateComm2 VoteInstrUpdateCommission
		err = updateComm2.UnmarshalWithDecoder(decoder2)
		if err != nil {
			t.Errorf("Round-trip unmarshal failed: %v", err)
			return
		}

		// Verify equality
		if updateComm.Commission != updateComm2.Commission {
			t.Errorf("Round-trip data mismatch: got %d, want %d", updateComm2.Commission, updateComm.Commission)
		}
	})
}

// FuzzLockoutOffsetRoundTrip tests LockoutOffset marshal/unmarshal round-trip
func FuzzLockoutOffsetRoundTrip(f *testing.F) {
	f.Add(makeValidLockoutOffset(0, 1))
	f.Add(makeValidLockoutOffset(100, 32))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBinDecoder(data)
		var lockoutOffset LockoutOffset
		err := lockoutOffset.UnmarshalWithDecoder(decoder)
		if err != nil {
			return
		}

		// Marshal
		buf := new(bytes.Buffer)
		varIntBuf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(varIntBuf, lockoutOffset.Offset)
		buf.Write(varIntBuf[:n])
		buf.WriteByte(lockoutOffset.ConfirmationCount)

		// Unmarshal again
		decoder2 := bin.NewBinDecoder(buf.Bytes())
		var lockoutOffset2 LockoutOffset
		err = lockoutOffset2.UnmarshalWithDecoder(decoder2)
		if err != nil {
			t.Errorf("Round-trip unmarshal failed: %v", err)
			return
		}

		// Verify equality
		if lockoutOffset.Offset != lockoutOffset2.Offset || lockoutOffset.ConfirmationCount != lockoutOffset2.ConfirmationCount {
			t.Errorf("Round-trip data mismatch")
		}
	})
}
