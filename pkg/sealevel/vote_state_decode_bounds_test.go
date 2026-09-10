package sealevel

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func requireInvalidVoteStateWithoutPanic(t *testing.T, data []byte) {
	t.Helper()
	require.NotPanics(t, func() {
		_, err := UnmarshalVersionedVoteState(data)
		require.Error(t, err)
	})
}

func TestVoteStateDecoderBoundsAttackerControlledCollections(t *testing.T) {
	for _, versioned := range []*VoteStateVersions{
		{Type: VoteStateVersionCurrent, Current: VoteState{}},
		{Type: VoteStateVersionV4, V4: VoteState4{}},
	} {
		data, err := MarshalVersionedVoteState(versioned)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(data), 24)
		// A zero-entry state ends with epoch-credit count followed by the
		// 16-byte BlockTimestamp. This used to int-convert and Grow(MaxUint64).
		binary.LittleEndian.PutUint64(data[len(data)-24:len(data)-16], math.MaxUint64)
		requireInvalidVoteStateWithoutPanic(t, data)
	}

	current, err := MarshalVersionedVoteState(&VoteStateVersions{
		Type: VoteStateVersionCurrent,
	})
	require.NoError(t, err)
	// Version tag + node + withdrawer + commission precede the lockout count.
	const currentLockoutCountOffset = 4 + 32 + 32 + 1
	binary.LittleEndian.PutUint64(
		current[currentLockoutCountOffset:currentLockoutCountOffset+8],
		math.MaxUint64,
	)
	requireInvalidVoteStateWithoutPanic(t, current)

	current, err = MarshalVersionedVoteState(&VoteStateVersions{
		Type: VoteStateVersionCurrent,
	})
	require.NoError(t, err)
	// With zero lockouts and no root, the authorized-voter count immediately
	// follows the lockout count and one-byte Option tag.
	const currentAuthorizedVoterCountOffset = currentLockoutCountOffset + 8 + 1
	binary.LittleEndian.PutUint64(
		current[currentAuthorizedVoterCountOffset:currentAuthorizedVoterCountOffset+8],
		math.MaxUint64,
	)
	requireInvalidVoteStateWithoutPanic(t, current)
}
