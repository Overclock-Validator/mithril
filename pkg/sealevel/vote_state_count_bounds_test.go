package sealevel

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func le64(v uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v)
	return b
}

// patchCount marshals a state that contains a single element whose first field
// is the distinctive value marker, then overwrites the u64 element count that
// precedes it (and preceding prefixBytes, e.g. a landed vote's latency byte)
// with count.
func patchCount(t *testing.T, state *VoteStateVersions, marker uint64, prefixBytes int, count uint64) []byte {
	t.Helper()
	data, err := MarshalVersionedVoteState(state)
	require.NoError(t, err)
	data = bytes.Clone(data)
	idx := bytes.Index(data, le64(marker))
	require.GreaterOrEqual(t, idx, 8+prefixBytes, "marker not found")
	require.Equal(t, 1, bytes.Count(data, le64(marker)), "marker must be unique")
	copy(data[idx-prefixBytes-8:], le64(count))
	return data
}

func TestUnmarshalVoteStateRejectsOversizedCounts(t *testing.T) {
	const slotMarker = 0xAABBCCDDEEFF0011
	const epochMarker = 0x1122334455667788

	type tc struct {
		name   string
		state  func() *VoteStateVersions
		marker uint64
		prefix int
	}

	votes := func(version uint32) *VoteStateVersions {
		s := &VoteStateVersions{Type: version}
		switch version {
		case VoteStateVersionV0_23_5:
			s.V0_23_5.Votes.PushBack(VoteLockout{Slot: slotMarker, ConfirmationCount: 1})
		case VoteStateVersionV1_14_11:
			s.V1_14_11.Votes.PushBack(VoteLockout{Slot: slotMarker, ConfirmationCount: 1})
		case VoteStateVersionCurrent:
			s.Current.Votes.PushBack(LandedVote{Latency: 1, Lockout: VoteLockout{Slot: slotMarker, ConfirmationCount: 1}})
		case VoteStateVersionV4:
			s.V4.Votes.PushBack(LandedVote{Latency: 1, Lockout: VoteLockout{Slot: slotMarker, ConfirmationCount: 1}})
		}
		return s
	}
	credits := func(version uint32) *VoteStateVersions {
		s := &VoteStateVersions{Type: version}
		ec := []EpochCredits{{Epoch: epochMarker, Credits: 2, PrevCredits: 1}}
		switch version {
		case VoteStateVersionCurrent:
			s.Current.EpochCredits = ec
		case VoteStateVersionV4:
			s.V4.EpochCredits = ec
		}
		return s
	}

	cases := []tc{
		{"lockouts/v0_23_5", func() *VoteStateVersions { return votes(VoteStateVersionV0_23_5) }, slotMarker, 0},
		{"lockouts/v1_14_11", func() *VoteStateVersions { return votes(VoteStateVersionV1_14_11) }, slotMarker, 0},
		{"lockouts/current", func() *VoteStateVersions { return votes(VoteStateVersionCurrent) }, slotMarker, 1},
		{"lockouts/v4", func() *VoteStateVersions { return votes(VoteStateVersionV4) }, slotMarker, 1},
		{"epoch_credits/current", func() *VoteStateVersions { return credits(VoteStateVersionCurrent) }, epochMarker, 0},
		{"epoch_credits/v4", func() *VoteStateVersions { return credits(VoteStateVersionV4) }, epochMarker, 0},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Counts that would panic in makeslice (2^62), overflow int (2^63, 2^64-1),
			// or simply dwarf the remaining bytes.
			for _, count := range []uint64{1 << 62, 1 << 63, ^uint64(0), 1 << 20} {
				data := patchCount(t, c.state(), c.marker, c.prefix, count)
				require.NotPanics(t, func() {
					_, err := UnmarshalVersionedVoteState(data)
					require.ErrorIs(t, err, InstrErrInvalidAccountData, "count=%d", count)
				}, "count=%d", count)
			}

			// An honest count still decodes.
			data := patchCount(t, c.state(), c.marker, c.prefix, 1)
			_, err := UnmarshalVersionedVoteState(data)
			require.NoError(t, err)
		})
	}
}
