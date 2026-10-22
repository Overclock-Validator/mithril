package rpcserver

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/transactionhistory"
	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Like pauseHistoryBankhashRead, this callback reaches actual cold SST reads.
// Only uncached point data-block loads count, not footer/index/filter reads.
// The one-SST fixture calibrates two real lookups before arming the callback.
type historyReselectionFS struct {
	vfs.FS
	onRead func(int) error
	reads  int
}

type historyReselectionFile struct {
	vfs.File
	fs *historyReselectionFS
}

func (fs *historyReselectionFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	file, err := fs.FS.Open(name, opts...)
	if err == nil && filepath.Ext(name) == ".sst" {
		return &historyReselectionFile{File: file, fs: fs}, nil
	}
	return file, err
}

func (file *historyReselectionFile) ReadAt(data []byte, offset int64) (int, error) {
	var pcs [64]uintptr
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs[:])])
	lookup, dataBlock := false, false
	for {
		frame, more := frames.Next()
		lookup = lookup || strings.HasSuffix(frame.Function, ".(*AccountsDb).GetBankHashForSlot")
		dataBlock = dataBlock || strings.HasSuffix(frame.Function, ".(*singleLevelIterator).loadBlock")
		if !more {
			break
		}
	}
	if lookup && dataBlock {
		file.fs.reads++
		if file.fs.onRead != nil {
			if err := file.fs.onRead(file.fs.reads); err != nil {
				return 0, err
			}
		}
	}
	return file.File.ReadAt(data, offset)
}

func historyReselectionHook(t *testing.T, server *RpcServer, hook func(int) error) *historyReselectionFS {
	t.Helper()
	require.NotNil(t, hook)
	require.NoError(t, server.acctsDb.BankHashStore.Flush())
	require.NoError(t, server.acctsDb.BankHashStore.Close())
	directory := filepath.Join(filepath.Dir(server.acctsDb.AcctsDir), "bankhash_db")
	tables, err := filepath.Glob(filepath.Join(directory, "*.sst"))
	require.NoError(t, err)
	require.Len(t, tables, 1, "the logical-read calibration requires one small bankhash SST")
	fs := &historyReselectionFS{FS: vfs.Default, onRead: func(int) error { return nil }}
	cache := pebble.NewCache(0)
	defer cache.Unref()
	db, err := pebble.Open(directory,
		&pebble.Options{FS: fs, Cache: cache, DisableAutomaticCompactions: true})
	require.NoError(t, err)
	server.acctsDb.BankHashStore = db
	bank, durable := server.rootedBank.Load(), server.acctsDb.DurableThrough()
	version, stable := server.acctsDb.CommittedAccountVersion()
	require.True(t, stable)
	for call := 1; call <= 2; call++ {
		hash, err := server.acctsDb.GetBankHashForSlot(20)
		require.NoError(t, err)
		require.Len(t, hash, 32)
		require.Equal(t, call, fs.reads, "one uncached data-block load per actual point lookup")
	}
	current, stable := server.acctsDb.CommittedAccountVersion()
	require.True(t, stable)
	require.Equal(t, version, current)
	require.Equal(t, durable, server.acctsDb.DurableThrough())
	require.Same(t, bank, server.rootedBank.Load())
	fs.reads, fs.onRead = 0, hook
	return fs
}

func historyReselectionCommit(server *RpcServer, slot uint64) error {
	_, err := server.acctsDb.CommitBatch([]accounts.SlotDelta{{Slot: slot,
		Delta: []*accounts.Account{{Key: solana.PublicKey{201}, Lamports: slot}}}}, slot, nil, nil)
	return err
}

func TestHistoryReselectionBoundAndExclusions(t *testing.T) {
	for _, method := range []string{"block", "transaction", "status"} {
		for _, change := range []string{"once", "twice", "cancel", "pending", "replace", "bankhash", "header", "storage", "admission", "initial-pending", "initial-nil", "initial-unstable", "cache-change"} {
			if change == "cache-change" && method != "status" {
				continue
			}
			t.Run(method+"/"+change, func(t *testing.T) {
				server, block, _, _ := signatureStatusFixture(t)
				if change == "bankhash" {
					wrong := solana.Hash{99}
					require.NoError(t, server.acctsDb.StoreBankHashForSlot(20, wrong[:]))
				}
				if change == "header" {
					block.ParentSlot = 11
				}
				require.NoError(t, server.transactionHistory.WriteBlock(block))
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				fs := historyReselectionHook(t, server, func(read int) error {
					if change == "storage" {
						return errors.New("cold bankhash read failed")
					}
					if read > 1 && change != "twice" {
						return nil
					}
					if err := historyReselectionCommit(server, 20+uint64(read)*10); err != nil {
						return err
					}
					switch change {
					case "cancel":
						cancel()
					case "pending":
						server.SetHistoryRecoveryPending(true)
					case "replace":
						if err := server.RecordBlockHistory(&b.Block{Slot: 20, ParentSlot: 10, Blockhash: solana.Hash{99}, LastBlockhash: solana.Hash{10}}); err != nil {
							return err
						}
						if err := server.PrepareBlockHistory(20); err != nil {
							return err
						}
						return server.SetRootedBlockHistorySlot(20)
					case "cache-change":
						view, err := server.resultCache.Lookup(nil)
						if err != nil {
							return err
						}
						return server.resultCache.Publish(txstatus.ResultBank{ID: txstatus.ResultBankID{Slot: 21, Blockhash: solana.Hash{21}}, Parent: view.Tip, Complete: true})
					}
					return nil
				})
				if change == "admission" {
					server.transactionHistoryReads <- struct{}{}
					server.transactionHistoryReads <- struct{}{}
				}
				switch change {
				case "initial-pending":
					server.SetHistoryRecoveryPending(true)
				case "initial-nil":
					server.rootedBank.Store(nil)
				case "initial-unstable":
					// Reuse the real partial-rewind refusal fixture, without a
					// publication hook or alteration of the account version.
					path := filepath.Join(server.acctsDb.AcctsDir, "rewound")
					require.NoError(t, os.WriteFile(path, nil, 0o600))
					t.Cleanup(func() { require.NoError(t, os.Remove(path)) })
					_, err := server.acctsDb.RewindToBatchBoundary(10)
					require.Error(t, err)
					_, stable := server.acctsDb.CommittedAccountVersion()
					require.False(t, stable)
				}
				var err error
				switch method {
				case "block":
					var got transactionhistory.Block
					_, got, err = server.readTransactionHistoryBlock(ctx, 20)
					if err == nil {
						require.Equal(t, block, got)
					}
				case "transaction":
					var got transactionhistory.Record
					var found bool
					_, got, found, err = server.readTransactionHistoryTransaction(ctx, block.Records[0].FirstSignature)
					if err == nil {
						require.True(t, found)
						require.Equal(t, block.Records[0], got)
					}
				case "status":
					var view txstatus.ResultView
					view, err = server.readSignatureStatuses(ctx, []solana.Signature{block.Records[0].FirstSignature}, true)
					if err == nil {
						require.Len(t, view.Results, 1)
						if change == "replace" {
							// The first pass did not observe the replacement. A
							// completely fresh selection sees the canonical miss.
							require.Equal(t, txstatus.CachedResult{}, view.Results[0])
							latest, lookupErr := server.resultCache.Lookup(nil)
							require.NoError(t, lookupErr)
							require.Equal(t, latest.Generation, view.Generation)
							require.Equal(t, latest.Root, view.Root)
							require.Equal(t, latest.Tip, view.Tip)
						} else {
							require.True(t, view.Results[0].Found)
							require.Equal(t, uint64(20), view.Results[0].Slot)
						}
					} else {
						require.Equal(t, txstatus.ResultView{}, view)
					}
				}
				if change == "once" || (method == "status" && change == "cache-change") {
					require.NoError(t, err)
					require.Equal(t, 2, fs.reads, "one fresh selection after the changed view")
				} else if method == "status" && change == "replace" {
					require.NoError(t, err)
					require.Equal(t, 1, fs.reads, "the fresh canonical miss does not read an orphan bankhash")
				} else {
					if change == "cancel" {
						require.ErrorIs(t, err, context.Canceled)
					} else {
						require.ErrorIs(t, err, transactionhistory.ErrUnavailable)
					}
					want := 1
					if change == "twice" {
						want = 2
						if method != "status" {
							require.EqualError(t, err, transactionhistory.ErrUnavailable.Error())
						}
					} else if change == "admission" {
						want = 0
						<-server.transactionHistoryReads
						<-server.transactionHistoryReads
					} else if strings.HasPrefix(change, "initial-") || (method == "status" && change == "header") {
						want = 0
					}
					require.Equal(t, want, fs.reads, "non-coherence refusals must not restart")
				}
				require.Zero(t, len(server.transactionHistoryReads))
			})
		}
	}
}

func TestHistoryReselectionStatusBudget(t *testing.T) {
	for _, change := range []string{"stable", "changed", "cache-changed"} {
		t.Run(change, func(t *testing.T) {
			server, block, _, _ := signatureStatusFixture(t)
			metadata, err := transactionhistory.UnmarshalMetadata(block.Records[0].Metadata)
			require.NoError(t, err)
			metadata.InnerInstructions = []transactionhistory.InnerInstructionGroup{{Index: 0}}
			for range 16 {
				metadata.InnerInstructions[0].Instructions = append(metadata.InnerInstructions[0].Instructions,
					transactionhistory.InnerInstruction{ProgramIDIndex: 1, Data: bytes.Repeat([]byte{1}, 10<<10)})
			}
			block.Records[0].Metadata, err = metadata.MarshalBinary()
			require.NoError(t, err)
			require.NoError(t, server.transactionHistory.WriteBlock(block))
			fs := historyReselectionHook(t, server, func(read int) error {
				if read == 1 {
					if change == "changed" {
						return historyReselectionCommit(server, 30)
					}
					if change == "cache-changed" {
						view, err := server.resultCache.Lookup(nil)
						if err != nil {
							return err
						}
						return server.resultCache.Publish(txstatus.ResultBank{
							ID:     txstatus.ResultBankID{Slot: 21, Blockhash: solana.Hash{21}},
							Parent: view.Tip, Complete: true})
					}
				}
				return nil
			})
			signatures := make([]solana.Signature, 24)
			for i := range signatures {
				signatures[i] = block.Records[0].FirstSignature
			}
			view, err := server.readSignatureStatuses(t.Context(), signatures, true)
			if change != "stable" {
				require.ErrorIs(t, err, transactionhistory.ErrLimit, "the second attempt must not reset the shared budget")
				require.Equal(t, txstatus.ResultView{}, view, "partially selected statuses must not escape")
				require.Greater(t, fs.reads, len(signatures), "the first attempt completed before reselection exhausted the remaining budget")
			} else {
				require.NoError(t, err)
				require.Len(t, view.Results, len(signatures))
				for _, result := range view.Results {
					require.True(t, result.Found && result.Finalized)
				}
			}
			require.Zero(t, len(server.transactionHistoryReads))
		})
	}
	joined := errors.Join(errHistoryViewChanged, errors.New("snapshot close failed"))
	require.ErrorIs(t, joined, errHistoryViewChanged)
	require.NotEqual(t, errHistoryViewChanged, joined, "only the pure sentinel permits another attempt")
}
