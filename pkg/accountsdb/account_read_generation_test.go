package accountsdb

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestAccountReadsRetainRetirementMarkersThroughVerification(t *testing.T) {
	for _, mode := range []string{"exact batch", "exact single", "point read", "batch read", "canceled batch", "fold"} {
		t.Run(mode, func(t *testing.T) {
			base := foldAcct(1, 10, nil)
			fixture := newProductionAccountsDBFixtureWithConfig(t, []*accounts.Account{base}, func(config *ProductionAccountIndexConfig) {
				config.ShardCount = 1
				config.SealKeys = 100
				config.RebaseKeys = 100
			})
			db := openProductionAccountsDBFixture(t, fixture)
			index := db.ProductionIndex
			releaseRebase := make(chan struct{})
			rebaseEntered := make(chan struct{})
			var releaseOnce, enteredOnce sync.Once
			index.mutable.stateMu.Lock()
			realRebase := index.mutable.config.Callbacks.RequestRebase
			index.mutable.config.Callbacks.RequestRebase = func(ctx context.Context, request ShardedMutableRebaseRequest) error {
				enteredOnce.Do(func() { close(rebaseEntered) })
				select {
				case <-releaseRebase:
				case <-ctx.Done():
					return ctx.Err()
				}
				return realRebase(ctx, request)
			}
			index.mutable.stateMu.Unlock()
			t.Cleanup(func() {
				releaseOnce.Do(func() { close(releaseRebase) })
				db.afterAccountIndexLookup = nil
				require.NoError(t, db.CloseDb())
			})

			// Find an absent key whose fingerprint points at the bootstrap file.
			var missing solana.PublicKey
			candidateFound := false
			for nonce := uint64(1); nonce < 1<<20; nonce++ {
				binary.LittleEndian.PutUint64(missing[8:16], nonce)
				_, source, found, err := index.LookupCandidate(missing)
				require.NoError(t, err)
				if found && source == accountIndexSourceBase {
					candidateFound = true
					break
				}
			}
			require.True(t, candidateFound, "no base fingerprint collision found")

			// Delete and compact the bootstrap account using the real durable
			// paths. Its old base candidate now requires a retirement marker.
			recoverProductionAccountsDBFixture(t, db)
			_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 110, Delta: []*accounts.Account{foldAcct(1, 0, nil)}}}, 110, nil, nil)
			require.NoError(t, err)
			_, err = db.CommitBatch(nil, 120, nil, nil)
			require.NoError(t, err)
			compacted, err := db.CompactOnce(CompactionConfig{RewindHorizonBatches: 1})
			require.NoError(t, err)
			require.Equal(t, 1, compacted.FilesDeleted)
			require.NoFileExists(t, fixture.basePath)
			require.True(t, index.IsRetired(fixture.baseSlot, fixture.baseFileID))

			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			sealDone := make(chan error, 1)
			go func() { sealDone <- index.ForceSeal(ctx) }()
			select {
			case <-rebaseEntered:
			case <-ctx.Done():
				t.Fatal("rebase did not start")
			}
			view, err := index.view.Acquire()
			require.NoError(t, err)
			oldGeneration := view.generation
			require.NoError(t, view.Close())

			readCtx, cancelRead := context.WithCancel(ctx)
			defer cancelRead()
			markerHeldDuringVerification := false
			db.afterAccountIndexLookup = func() {
				releaseOnce.Do(func() { close(releaseRebase) })
				select {
				case err := <-sealDone:
					require.NoError(t, err, "rebase must publish while the reader remains pinned")
				case <-ctx.Done():
					t.Fatal("rebase blocked on the account read")
				}
				// Drain asynchronous cleanup when the lookup has dropped its pin.
				// This forces the failing interleaving without depending on sleeps;
				// with a retained pin, cleanup must wait until the read returns.
				if oldGeneration.refs.Load() == 0 {
					require.Eventually(t, func() bool {
						return !index.IsRetired(fixture.baseSlot, fixture.baseFileID)
					}, 5*time.Second, time.Millisecond)
				}
				markerHeldDuringVerification = index.IsRetired(fixture.baseSlot, fixture.baseFileID)
				if mode == "canceled batch" {
					cancelRead()
				}
			}

			switch mode {
			case "exact batch":
				_, found, err := db.lookupExactAccountIndexEntries([]solana.PublicKey{missing})
				require.NoError(t, err)
				require.Equal(t, []bool{false}, found)
			case "exact single":
				_, _, found, err := db.lookupExactAccountIndexEntry(missing)
				require.NoError(t, err)
				require.False(t, found)
			case "point read":
				_, err := db.readIndexedAccount(missing)
				require.ErrorIs(t, err, ErrNoAccount)
			case "batch read", "canceled batch":
				got, _, err := db.GetAccountsBatchSharedWithStats(readCtx, 120, []solana.PublicKey{missing})
				if mode == "canceled batch" {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.NoError(t, err)
					require.Len(t, got, 1)
					require.Equal(t, missing, got[0].Key)
					require.Zero(t, got[0].Lamports)
				}
			case "fold":
				created := foldAcct(9, 20, nil)
				created.Key = missing
				_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 130, Delta: []*accounts.Account{created}}}, 130, nil, nil)
				require.NoError(t, err, "a base false positive must not abort the fold's undo lookup")
			}
			db.afterAccountIndexLookup = nil
			require.True(t, markerHeldDuringVerification, "read released its generation before verification")
			require.Eventually(t, func() bool {
				return !index.IsRetired(fixture.baseSlot, fixture.baseFileID)
			}, 5*time.Second, time.Millisecond, "read leaked its generation pin")
		})
	}
}
