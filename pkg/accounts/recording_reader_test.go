package accounts

import (
	"errors"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func recordingFixture(t *testing.T) (*OverlayAccounts, [32]byte, [32]byte, [32]byte) {
	t.Helper()
	inParent, inDelta, absent := [32]byte{1}, [32]byte{2}, [32]byte{3}
	parent := NewMemAccountsWithLen(2)
	require.NoError(t, parent.SetAccountWithoutLock(solana.PublicKey(inParent), &Account{Key: inParent, Lamports: 10}))
	require.NoError(t, parent.SetAccountWithoutLock(solana.PublicKey(inDelta), &Account{Key: inDelta, Lamports: 20}))
	overlay := NewOverlayAccounts(parent)
	require.NoError(t, overlay.SetAccount(&inDelta, &Account{Key: inDelta, Lamports: 21}))
	return overlay, inParent, inDelta, absent
}

// A read is served exactly as the overlay serves it, and recorded with the
// version it came from.
func TestRecordingReaderReadsAsOverlay(t *testing.T) {
	overlay, inParent, inDelta, absent := recordingFixture(t)
	r := NewRecordingReader(overlay)
	for _, key := range [][32]byte{inParent, inDelta, absent} {
		want, wantErr := overlay.GetAccount(&key)
		got, gotErr := r.GetAccount(&key)
		require.Same(t, want, got)
		require.Equal(t, wantErr, gotErr)
	}
	reads := r.Reads()
	require.Len(t, reads, 3)
	require.False(t, reads[0].InDelta)
	require.True(t, reads[1].InDelta)
	deltaObject, _ := overlay.DeltaEntry(inDelta)
	require.Same(t, deltaObject, reads[1].Version)
	require.False(t, reads[2].InDelta)
	require.True(t, r.Unchanged(nil))
	require.False(t, r.Wrote())
}

// A store to any key that was read, whether it was in the delta, only in the
// parent, or absent, is seen; a store to a key that was not read is not.
func TestRecordingReaderSeesStoresToReadKeys(t *testing.T) {
	for name, pick := range map[string]int{"parent": 0, "delta": 1, "absent": 2} {
		t.Run(name, func(t *testing.T) {
			overlay, inParent, inDelta, absent := recordingFixture(t)
			keys := [][32]byte{inParent, inDelta, absent}
			r := NewRecordingReader(overlay)
			for i := range keys {
				_, _ = r.GetAccount(&keys[i])
			}
			unread := [32]byte{9}
			require.NoError(t, overlay.SetAccount(&unread, &Account{Key: unread, Lamports: 1}))
			require.True(t, r.Unchanged(nil), "a store to a key never read changes nothing that was read")
			key := keys[pick]
			before, _ := overlay.GetAccount(&key)
			same := &Account{Key: key}
			if before != nil {
				same = before.Clone()
			}
			require.NoError(t, overlay.SetAccount(&key, same))
			require.False(t, r.Unchanged(nil), "a store is a new version even with identical content")
			require.True(t, r.Unchanged(func(k [32]byte) bool { return k == key }), "a skipped key is left to the caller")
		})
	}
}

// A key read twice returns the first read's value even if the overlay moved in
// between, so an execution never sees two values for one key; the move is still
// reported.
func TestRecordingReaderRepeatReadIsStable(t *testing.T) {
	overlay, inParent, inDelta, _ := recordingFixture(t)
	r := NewRecordingReader(overlay)
	firstParent, _ := r.GetAccount(&inParent)
	firstDelta, _ := r.GetAccount(&inDelta)
	require.NoError(t, overlay.SetAccount(&inParent, &Account{Key: inParent, Lamports: 11}))
	require.NoError(t, overlay.SetAccount(&inDelta, &Account{Key: inDelta, Lamports: 22}))
	againParent, _ := r.GetAccountWithoutLock(solana.PublicKey(inParent))
	againDelta, _ := r.GetAccount(&inDelta)
	require.Same(t, firstParent, againParent)
	require.Same(t, firstDelta, againDelta)
	require.Len(t, r.Reads(), 2)
	require.False(t, r.Unchanged(nil))
}

// Every write is refused and remembered, and reaches neither the overlay nor
// its parent.
func TestRecordingReaderRefusesWrites(t *testing.T) {
	overlay, inParent, _, _ := recordingFixture(t)
	for name, write := range map[string]func(r *RecordingReader) error{
		"SetAccount": func(r *RecordingReader) error { return r.SetAccount(&inParent, &Account{Key: inParent, Lamports: 99}) },
		"SetAccountWithoutLock": func(r *RecordingReader) error {
			return r.SetAccountWithoutLock(solana.PublicKey(inParent), &Account{Key: inParent, Lamports: 99})
		},
		"AllAccounts": func(r *RecordingReader) error {
			if r.AllAccounts() != nil {
				return nil
			}
			return ErrSealedAccounts
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := NewRecordingReader(overlay)
			require.False(t, r.Wrote())
			require.True(t, errors.Is(write(r), ErrSealedAccounts))
			require.True(t, r.Wrote())
			got, err := overlay.GetAccount(&inParent)
			require.NoError(t, err)
			require.EqualValues(t, 10, got.Lamports)
			_, present := overlay.DeltaEntry(inParent)
			require.False(t, present)
		})
	}
}

// Many keys switch the lookup to a map without changing what is recorded.
func TestRecordingReaderManyKeys(t *testing.T) {
	overlay, _, _, _ := recordingFixture(t)
	r := NewRecordingReader(overlay)
	for round := 0; round < 2; round++ {
		for i := 0; i < 3*recordingReaderIndexAt; i++ {
			key := [32]byte{0xAA, byte(i)}
			_, _ = r.GetAccount(&key)
		}
	}
	require.Len(t, r.Reads(), 3*recordingReaderIndexAt)
	require.True(t, r.Unchanged(nil))
	key := [32]byte{0xAA, 5}
	require.NoError(t, overlay.SetAccount(&key, &Account{Key: key, Lamports: 1}))
	require.False(t, r.Unchanged(nil))
}

var _ Accounts = (*RecordingReader)(nil)

// A read outside the given keys is counted even when one of those keys was
// never read, which a difference of lengths would hide.
func TestRecordingReaderReadsOutside(t *testing.T) {
	overlay, inParent, inDelta, absent := recordingFixture(t)
	r := NewRecordingReader(overlay)
	captured := [][32]byte{inParent, inDelta}
	require.Zero(t, r.ReadsOutside(captured))
	_, _ = r.GetAccount(&inParent)
	require.Zero(t, r.ReadsOutside(captured))
	_, _ = r.GetAccount(&absent)
	require.Equal(t, 1, r.ReadsOutside(captured), "two reads against two captured keys, one of them outside")
	_, _ = r.GetAccount(&absent)
	require.Equal(t, 1, r.ReadsOutside(captured), "a key read twice is one read")
}
