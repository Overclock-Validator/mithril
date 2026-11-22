package sealevel

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// markerSlot identifies the single lockout in each fixture so the test can
// find the lockout count that precedes it.
const markerSlot = 0x1122334455667788

var allVoteStateVersions = []uint32{
	VoteStateVersionV0_23_5, VoteStateVersionV1_14_11, VoteStateVersionCurrent, VoteStateVersionV4,
}

// voteStateWithLockoutCount marshals a vote state of the given version with one
// lockout, then rewrites its lockout count to count.
func voteStateWithLockoutCount(t *testing.T, version uint32, count uint64) []byte {
	t.Helper()
	lockout := VoteLockout{Slot: markerSlot, ConfirmationCount: 1}
	versioned := &VoteStateVersions{Type: version}
	countBack := 8 // the count sits right before the lockout's slot
	switch version {
	case VoteStateVersionV0_23_5:
		versioned.V0_23_5.Votes.PushBack(lockout)
	case VoteStateVersionV1_14_11:
		versioned.V1_14_11.Votes.PushBack(lockout)
	case VoteStateVersionCurrent:
		versioned.Current.Votes.PushBack(LandedVote{Lockout: lockout})
		countBack++ // and the landed vote's latency byte
	case VoteStateVersionV4:
		versioned.V4.Votes.PushBack(LandedVote{Lockout: lockout})
		countBack++
	}
	data, err := MarshalVersionedVoteState(versioned)
	require.NoError(t, err)
	at := bytes.Index(data, binary.LittleEndian.AppendUint64(nil, markerSlot))
	require.Positive(t, at)
	at -= countBack
	require.Equal(t, uint64(1), binary.LittleEndian.Uint64(data[at:]), "located the lockout count")
	binary.LittleEndian.PutUint64(data[at:], count)
	return data
}

// An account's lockout count is untrusted. Counts around 2^62 used to reach
// deque.SetBaseCap unchecked: 2^62 panicked in makeslice and 2^62+1 looped
// forever. Agave reserves at most MAX_LOCKOUT_HISTORY and fails at the end of
// the data; every version must do the same.
func TestVoteStateDecodeRejectsHugeLockoutCount(t *testing.T) {
	counts := []uint64{MaxLockoutHistory + 1, 1 << 34, 1 << 62, 1<<62 + 1, math.MaxInt64, math.MaxUint64}
	for _, version := range allVoteStateVersions {
		for _, count := range counts {
			t.Run(fmt.Sprintf("v%d/%#x", version, count), func(t *testing.T) {
				data := voteStateWithLockoutCount(t, version, count)
				done := make(chan error, 1)
				go func() {
					defer func() {
						if r := recover(); r != nil {
							done <- fmt.Errorf("panic: %v", r)
						}
					}()
					if _, err := UnmarshalVersionedVoteState(data); err == nil {
						done <- fmt.Errorf("decoded %d lockouts from one", count)
						return
					}
					done <- nil
				}()
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(5 * time.Second):
					t.Fatal("decode did not return")
				}
			})
		}
	}
}

func TestVoteStateDecodeKeepsValidLockouts(t *testing.T) {
	for _, version := range allVoteStateVersions {
		data := voteStateWithLockoutCount(t, version, 1)
		decoded, err := UnmarshalVersionedVoteState(data)
		require.NoError(t, err, "version %d", version)
		reencoded, err := MarshalVersionedVoteState(decoded)
		require.NoError(t, err)
		require.Equal(t, data, reencoded, "version %d round trips", version)
	}
}
