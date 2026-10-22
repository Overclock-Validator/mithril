package rpcserver

import (
	"errors"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/transactionhistory"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// The pause is in an actual cold Pebble SST read, not a replacement RPC reader.
type historyReadPauseFS struct {
	vfs.FS
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

type historyReadPauseFile struct {
	vfs.File
	fs *historyReadPauseFS
}

func (fs *historyReadPauseFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	file, err := fs.FS.Open(name, opts...)
	if err == nil && filepath.Ext(name) == ".sst" {
		return &historyReadPauseFile{File: file, fs: fs}, nil
	}
	return file, err
}

func (file *historyReadPauseFile) ReadAt(data []byte, offset int64) (int, error) {
	// Pebble can read SST metadata on a background goroutine. Delay only the
	// data lookup reached by this request, not a concurrent engine task.
	var pcs [64]uintptr
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs[:])])
	lookup := false
	for {
		frame, more := frames.Next()
		lookup = lookup || strings.HasSuffix(frame.Function, ".(*AccountsDb).GetBankHashForSlot")
		if !more {
			break
		}
	}
	if lookup && file.fs.armed.CompareAndSwap(true, false) {
		close(file.fs.entered)
		<-file.fs.release
	}
	return file.File.ReadAt(data, offset)
}

func (fs *historyReadPauseFS) resume() { fs.once.Do(func() { close(fs.release) }) }

func pauseHistoryBankhashRead(t *testing.T, server *RpcServer) *historyReadPauseFS {
	t.Helper()
	require.NoError(t, server.acctsDb.BankHashStore.Flush())
	require.NoError(t, server.acctsDb.BankHashStore.Close())
	fs := &historyReadPauseFS{FS: vfs.Default, entered: make(chan struct{}), release: make(chan struct{})}
	cache := pebble.NewCache(0)
	defer cache.Unref()
	db, err := pebble.Open(filepath.Join(filepath.Dir(server.acctsDb.AcctsDir), "bankhash_db"),
		&pebble.Options{FS: fs, Cache: cache, DisableAutomaticCompactions: true})
	require.NoError(t, err)
	server.acctsDb.BankHashStore = db
	t.Cleanup(fs.resume)
	fs.armed.Store(true)
	return fs
}

func TestTransactionHistoryReadDoesNotBlockRecovery(t *testing.T) {
	for _, method := range []string{"block", "transaction"} {
		for _, change := range []string{"unchanged", "recovery-cycle", "recovery", "fold", "rewind", "summary-replaced", "summary-pruned", "summary-ambiguous"} {
			if method == "block" && change == "summary-ambiguous" {
				continue
			}
			t.Run(method+"/"+change, func(t *testing.T) {
				server, want := transactionHistoryReadFixture(t)
				require.NoError(t, server.transactionHistory.WriteBlock(want))
				if change == "summary-ambiguous" {
					earlier := want
					earlier.ID = transactionhistory.BlockID{Slot: 10, Blockhash: [32]byte{10}}
					earlier.ParentSlot, earlier.PreviousBlockhash = 0, [32]byte{}
					earlier.Bankhash = want.ParentBankhash
					require.NoError(t, server.transactionHistory.WriteBlock(earlier))
				}
				fs := pauseHistoryBankhashRead(t, server)
				readDone := make(chan error, 1)
				go func() {
					if method == "block" {
						_, got, err := server.readTransactionHistoryBlock(t.Context(), want.ID.Slot)
						if err == nil && got.ID != want.ID {
							err = errors.New("history block identity changed")
						}
						readDone <- err
						return
					}
					_, got, found, err := server.readTransactionHistoryTransaction(t.Context(), want.Records[0].FirstSignature)
					if err == nil && (!found || got.FirstSignature != want.Records[0].FirstSignature) {
						err = errors.New("history transaction identity changed")
					}
					readDone <- err
				}()
				select {
				case <-fs.entered:
				case <-time.After(5 * time.Second):
					t.Fatal("history lookup did not reach the cold bankhash read")
				}
				changeDone := make(chan error, 1)
				go func() {
					var err error
					switch change {
					case "unchanged":
						server.SetHistoryRecoveryPending(false)
					case "recovery-cycle":
						server.SetHistoryRecoveryPending(true)
						server.SetHistoryRecoveryPending(false)
					case "recovery":
						server.SetHistoryRecoveryPending(true)
					case "fold":
						_, err = server.acctsDb.CommitBatch([]accounts.SlotDelta{{Slot: 30, Delta: []*accounts.Account{{Key: solana.PublicKey{201}, Lamports: 30}}}}, 30, nil, nil)
						server.SetRootedBankState(30, 8, 1)
					case "rewind":
						server.SetHistoryRecoveryPending(true)
						_, err = server.acctsDb.RewindToBatchBoundary(10)
						if err == nil {
							err = server.RewindBlockHistory(10)
						}
						server.SetRootedBankState(10, 6, 0)
						server.SetHistoryRecoveryPending(false)
					case "summary-replaced", "summary-ambiguous":
						changed := &b.Block{Slot: 20, ParentSlot: 10, Blockhash: solana.Hash{21}, LastBlockhash: solana.Hash{10}}
						if change == "summary-ambiguous" {
							changed = &b.Block{Slot: 10, Blockhash: solana.Hash{10}}
						}
						err = server.RecordBlockHistory(changed)
						if err == nil {
							err = server.PrepareBlockHistory(20)
						}
						if err == nil {
							err = server.SetRootedBlockHistorySlot(20)
						}
					case "summary-pruned":
						err = server.RewindBlockHistory(10)
					}
					changeDone <- err
				}()
				select {
				case err := <-changeDone:
					require.NoError(t, err)
				case <-time.After(time.Second):
					t.Error("recovery or publication waited for the optional bankhash read")
					fs.resume()
					require.NoError(t, <-changeDone)
				}
				fs.resume()
				select {
				case err := <-readDone:
					if change == "unchanged" || change == "recovery-cycle" || change == "fold" {
						require.NoError(t, err)
					} else {
						require.ErrorIs(t, err, transactionhistory.ErrUnavailable)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("history lookup did not finish after the read resumed")
				}
				require.Zero(t, len(server.transactionHistoryReads))
			})
		}
	}
}
