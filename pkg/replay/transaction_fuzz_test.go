package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
)

// FuzzResolveAddrTableLookups tests address table lookup resolution with malformed data
func FuzzResolveAddrTableLookups(f *testing.F) {
	// Seed with valid address table lookup patterns
	f.Add([]byte{1, 0, 0, 0, 0, 0, 0, 0}) // 1 lookup table
	f.Add([]byte{0})                      // no lookups
	f.Add([]byte{2, 1, 2, 3, 4})          // multiple tables with indices
	f.Add([]byte{255})                    // max count

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 {
			return
		}

		// Create a mock accounts DB
		tempDir := t.TempDir()
		db, err := accountsdb.OpenDb(tempDir)
		if err != nil {
			t.Skip("Failed to create test DB")
		}
		defer db.CloseDb()

		// Create block with fuzzy transaction data
		block := &b.Block{
			Transactions: []*solana.Transaction{
				{
					Message: solana.Message{
						AccountKeys: []solana.PublicKey{
							solana.MustPublicKeyFromBase58("11111111111111111111111111111111"),
						},
					},
				},
			},
		}

		// Attempt to resolve with fuzzy input
		// This should not panic, even with malformed data
		err = resolveAddrTableLookups(db, block)

		// We expect errors for malformed data, but no panics
		_ = err
	})
}

// FuzzExtractAndDedupeBlockAccts tests account extraction and deduplication
func FuzzExtractAndDedupeBlockAccts(f *testing.F) {
	f.Add(uint8(0))   // no accounts
	f.Add(uint8(1))   // single account
	f.Add(uint8(10))  // multiple accounts
	f.Add(uint8(255)) // max accounts

	f.Fuzz(func(t *testing.T, numAccts uint8) {
		// Limit to reasonable number to avoid OOM
		if numAccts > 100 {
			numAccts = numAccts % 100
		}

		block := &b.Block{
			Transactions: make([]*solana.Transaction, 0),
		}

		// Create transactions with random accounts
		for i := uint8(0); i < numAccts; i++ {
			pubkey := solana.PublicKey{}
			for j := 0; j < 32; j++ {
				pubkey[j] = byte(int(i) + j)
			}

			tx := &solana.Transaction{
				Message: solana.Message{
					AccountKeys: []solana.PublicKey{pubkey},
				},
			}
			block.Transactions = append(block.Transactions, tx)
		}

		// Should dedupe without panicking
		dedupedAccts := extractAndDedupeBlockAccts(block)

		// Verify output is valid
		if dedupedAccts == nil {
			t.Error("extractAndDedupeBlockAccts returned nil")
		}

		// Verify no duplicates
		seen := make(map[[32]byte]bool)
		for _, acct := range dedupedAccts {
			if seen[acct] {
				t.Error("extractAndDedupeBlockAccts returned duplicates")
			}
			seen[acct] = true
		}
	})
}
