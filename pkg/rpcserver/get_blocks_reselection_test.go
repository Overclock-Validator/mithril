package rpcserver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestGetBlocksHTTPReselectsForwardPublication(t *testing.T) {
	server, original := getBlocksFixture(t)
	original.Close()
	initial := server.rootedBank.Load()
	var publishErr error
	published := false
	finished := make(chan *blocksReadContext, 1)
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Admission is the first Err call. Starting at one runs the existing
		// third-call hook at Store.Blocks entry, after selection but before mu.
		ctx := &blocksReadContext{Context: r.Context(), checks: 1, action: func() {
			if server.rootedBank.Load() != initial {
				t.Error("forward publication hook ran after the initial bank changed")
			}
			publishErr = server.RecordBlockHistory(&b.Block{Slot: 13, ParentSlot: 12, Blockhash: solana.Hash{13}})
			if publishErr == nil {
				publishErr = server.PrepareBlockHistory(13)
			}
			if publishErr == nil {
				_, publishErr = server.acctsDb.CommitBatch([]accounts.SlotDelta{{
					Slot: 13, Delta: []*accounts.Account{{Key: solana.PublicKey{205}, Lamports: 13}},
				}}, 13, nil, nil)
			}
			if publishErr == nil {
				publishErr = server.SetRootedBlockHistorySlot(13)
			}
			if publishErr == nil {
				server.SetRootedBankState(13, 3, 0)
				published = true
			}
		}}
		server.ServeHTTP(w, r.WithContext(ctx))
		finished <- ctx
	}))
	t.Cleanup(endpoint.Close)
	endpoint.Client().Timeout = 5 * time.Second
	body := getBlockHistoryHTTPCall(t, endpoint, "getBlocks", `[10,13]`)
	ctx := <-finished
	require.NoError(t, publishErr)
	require.True(t, published)
	require.NotContains(t, body, "error")
	require.JSONEq(t, `[10,12,13]`, string(body["result"]), "fresh selection must recompute the root-clamped end")
	require.Equal(t, 8, ctx.checks, "one admission, one discarded read and one complete fresh range")
}

// These callbacks run on the HTTP handler's goroutine. Err calls 3 and 7
// reach the first slot probe of each three-slot attempt. Done reaches the
// publication wait only after the first read has completed five Err calls.
type blocksReselectionContext struct {
	*blocksReadContext
	onSecondRead func()
	onWait       func()
	waitChecks   int
}

func (ctx *blocksReselectionContext) Err() error {
	err := ctx.blocksReadContext.Err()
	if ctx.checks == 7 && ctx.onSecondRead != nil {
		action := ctx.onSecondRead
		ctx.onSecondRead = nil
		action()
	}
	return err
}

func (ctx *blocksReselectionContext) Done() <-chan struct{} {
	if ctx.checks == 5 && ctx.onWait != nil {
		action := ctx.onWait
		ctx.onWait = nil
		ctx.waitChecks = ctx.checks
		action()
	}
	return ctx.blocksReadContext.Context.Done()
}

func TestGetBlocksHTTPReselectionStaysBounded(t *testing.T) {
	server, original := getBlocksFixture(t)
	original.Close()
	changes, waits := 0, 0
	finished := make(chan *blocksReselectionContext, 1)
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		change := func() {
			changes++
			server.SetRootedBankState(12, 2, 0)
		}
		ctx := &blocksReselectionContext{
			blocksReadContext: &blocksReadContext{Context: r.Context(), action: change},
			onSecondRead:      change,
			onWait:            func() { waits++ },
		}
		server.ServeHTTP(w, r.WithContext(ctx))
		finished <- ctx
	}))
	t.Cleanup(endpoint.Close)
	endpoint.Client().Timeout = 5 * time.Second
	body := getBlockHistoryHTTPCall(t, endpoint, "getBlocks", `[10,12]`)
	ctx := <-finished
	requireGetBlockHistoryError(t, body, -32004, "Block not available for slot 10")
	require.Equal(t, 2, changes)
	require.Equal(t, 1, waits)
	require.Equal(t, 9, ctx.checks, "the second invalidation must not admit a third selection")
}

func TestGetBlocksHTTPDoesNotReselectBackwardRoot(t *testing.T) {
	server, original := getBlocksFixture(t)
	original.Close()
	finished := make(chan *blocksReadContext, 1)
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := &blocksReadContext{Context: r.Context(), action: func() {
			server.SetRootedBankState(10, 1, 0)
		}}
		server.ServeHTTP(w, r.WithContext(ctx))
		finished <- ctx
	}))
	t.Cleanup(endpoint.Close)
	endpoint.Client().Timeout = 5 * time.Second
	body := getBlockHistoryHTTPCall(t, endpoint, "getBlocks", `[10,12]`)
	ctx := <-finished
	requireGetBlockHistoryError(t, body, -32004, "Block not available for slot 10")
	require.Equal(t, 5, ctx.checks, "a root regression must not be treated as a forward publication")
}

func TestGetBlocksHTTPReselectionWaitCancellationAndRecovery(t *testing.T) {
	for _, cancelWait := range []bool{true, false} {
		name := "recovery"
		if cancelWait {
			name = "cancellation"
		}
		t.Run(name, func(t *testing.T) {
			server, original := getBlocksFixture(t)
			original.Close()
			locked := false
			finished := make(chan *blocksReselectionContext, 1)
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				base, cancel := context.WithCancel(r.Context())
				defer cancel()
				ctx := &blocksReselectionContext{
					blocksReadContext: &blocksReadContext{Context: base, action: func() {
						server.SetRootedBankState(12, 2, 0)
					}},
					onWait: func() {
						if cancelWait {
							cancel()
							return
						}
						locked = server.historyRecoveryMu.TryLock()
						if locked {
							server.historyRecoveryMu.Unlock()
							server.SetHistoryRecoveryPending(true)
						}
					},
				}
				server.ServeHTTP(w, r.WithContext(ctx))
				finished <- ctx
			}))
			t.Cleanup(endpoint.Close)
			endpoint.Client().Timeout = 5 * time.Second
			body := getBlockHistoryHTTPCall(t, endpoint, "getBlocks", `[10,12]`)
			ctx := <-finished
			require.Equal(t, 5, ctx.waitChecks, "the action must run at the publication wait, not before selection")
			require.NotContains(t, body, "result")
			if cancelWait {
				require.Contains(t, string(body["error"]), context.Canceled.Error())
				require.LessOrEqual(t, ctx.checks, 6, "cancellation must stop before a fresh slot scan")
			} else {
				require.True(t, locked, "the recovery writer must be able to acquire the gate during the wait")
				require.True(t, server.historyRecoveryPending)
				requireGetBlockHistoryError(t, body, -32004, "Block not available for slot 10")
				require.Equal(t, 5, ctx.checks, "recovery must refuse before the second range scan")
			}
		})
	}
}
