package bankhash

import (
	"bytes"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
)

// Large accounts have their old and new values hashed on separate goroutines.
// The delta must stay byte-for-byte equal to the single-threaded reference for
// every kind of change, at and around the size where the split begins.
func TestCalculateDeltaLtHashLargeAccountsMatchLegacyReference(t *testing.T) {
	ctx, modified := buildLtHashDifferentialFixture(t, 65)
	parent := ctx.ParentAccts.(accounts.MemAccounts)
	sizes := []int{ltHashLargeAccountBytes/2 - 1, ltHashLargeAccountBytes / 2, ltHashLargeAccountBytes/2 + 1, ltHashLargeAccountBytes, 3 * ltHashLargeAccountBytes}
	split := 0
	for i, size := range sizes {
		for variant := range 4 {
			key := ltHashFixtureKey(10_000 + i*10 + variant)
			oldAcct := ltHashFixtureAccount(key, i+1, uint64(i+7), 0)
			oldAcct.Data = bytes.Repeat([]byte{byte(i + 1), byte(variant + 3)}, size/2)
			newAcct := oldAcct.Clone()
			switch variant {
			case 0: // one byte of a large account changes
				newAcct.Data[len(newAcct.Data)-1] ^= 0xff
			case 1: // the account grows
				newAcct.Data = append(newAcct.Data, bytes.Repeat([]byte{0x5a}, size/3)...)
			case 2: // created large
				oldAcct.Lamports = 0
			case 3: // deleted
				newAcct.Lamports = 0
			}
			if oldAcct.Lamports != 0 && newAcct.Lamports != 0 && len(oldAcct.Data)+len(newAcct.Data) >= ltHashLargeAccountBytes {
				split++
			}
			if err := parent.SetAccountWithoutLock(key, oldAcct); err != nil {
				t.Fatal(err)
			}
			modified = append(modified, newAcct)
		}
	}
	if split == 0 {
		t.Fatal("fixture has no account large enough to split")
	}

	want := legacyCalculateDeltaLtHash(ctx, modified)
	for _, splitLarge := range []bool{true, false} {
		previous := ltHashSplitLarge
		ltHashSplitLarge = splitLarge
		got := calculateDeltaLtHash(ctx, modified)
		ltHashSplitLarge = previous
		if !bytes.Equal(got.Hash(), want.Hash()) {
			t.Fatalf("delta differs from the legacy reference with large-account split %t", splitLarge)
		}
	}
}
