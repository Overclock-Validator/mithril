package rpcserver

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type blocksCommitBarrier struct{ entered, release chan struct{} }

func (barrier *blocksCommitBarrier) AccountsChanged(uint64, []*accounts.Account, bool) {
	close(barrier.entered)
	<-barrier.release
}

// Done is reached by the existing publication wait after query admission.
type blocksReadinessContext struct {
	context.Context
	owner  *historyResponseOwner
	onWait func()
	onRead func()
	waits  int
	checks int
}

func (ctx *blocksReadinessContext) Err() error {
	ctx.checks++
	if ctx.checks == 3 && ctx.onRead != nil {
		ctx.onRead()
	}
	return ctx.Context.Err()
}

func (ctx *blocksReadinessContext) Done() <-chan struct{} {
	if ctx.owner.acquired {
		ctx.waits++
		if ctx.onWait != nil {
			action := ctx.onWait
			ctx.onWait = nil
			action()
		}
	}
	return ctx.Context.Done()
}

type blocksReadinessResult struct {
	body []byte
	err  error
}

func blocksReadinessRequest(t *testing.T, server *RpcServer, params string, onWait func(*historyResponseOwner, context.CancelFunc), onRead ...func()) (<-chan blocksReadinessResult, <-chan [3]int) {
	t.Helper()
	finished := make(chan [3]int, 1)
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		base, cancel := context.WithCancel(r.Context())
		defer cancel()
		owner := newResponseOwner(base, w, server.queryResponses, time.Now().Add(historyResponseTimeout))
		ctx := &blocksReadinessContext{Context: context.WithValue(base, queryResponseKey{}, owner), owner: owner}
		ctx.onWait = func() { onWait(owner, cancel) }
		if len(onRead) != 0 {
			ctx.onRead = onRead[0]
		}
		server.rpcService.ServeHTTP(w, r.WithContext(ctx))
		if err := owner.release(); err != nil {
			t.Errorf("release query response: %v", err)
		}
		finished <- [3]int{owner.calls, ctx.waits, ctx.checks}
	}))
	t.Cleanup(endpoint.Close)
	result := make(chan blocksReadinessResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.URL, strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"getBlocks","params":`+params+`}`))
		if err != nil {
			result <- blocksReadinessResult{err: err}
			return
		}
		response, err := endpoint.Client().Do(request)
		if err != nil {
			result <- blocksReadinessResult{err: err}
			return
		}
		body, err := io.ReadAll(response.Body)
		result <- blocksReadinessResult{body: body, err: errors.Join(err, response.Body.Close())}
	}()
	return result, finished
}

func requireBlocksReadinessResult(t *testing.T, result blocksReadinessResult, want string) {
	t.Helper()
	require.NoError(t, result.err)
	var body map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(result.body, &body))
	require.NotContains(t, body, "error", "%s", result.body)
	require.JSONEq(t, want, string(body["result"]))
}

func prepareBlocksReadinessTip(t *testing.T, server *RpcServer) {
	t.Helper()
	require.NoError(t, server.RecordBlockHistory(&b.Block{Slot: 13, ParentSlot: 12, Blockhash: solana.Hash{13}}))
	require.NoError(t, server.PrepareBlockHistory(13))
}

func TestGetBlocksWaitsForInitialAccountPublication(t *testing.T) {
	server, original := getBlocksFixture(t)
	original.Close()
	prepareBlocksReadinessTip(t, server)
	barrier := &blocksCommitBarrier{entered: make(chan struct{}), release: make(chan struct{})}
	unobserve := server.acctsDb.ObserveAccountState(barrier)
	defer unobserve()
	var release sync.Once
	unblock := func() { release.Do(func() { close(barrier.release) }) }
	defer unblock()
	committed := make(chan error, 1)
	go func() {
		_, err := server.acctsDb.CommitBatch([]accounts.SlotDelta{{Slot: 13}}, 13, nil, nil)
		if err == nil {
			err = server.SetRootedBlockHistorySlot(13)
			server.SetRootedBankState(13, 3, 0)
		}
		committed <- err
	}()
	<-barrier.entered
	version, stable := server.acctsDb.CommittedAccountVersion()
	require.False(t, stable)
	require.Equal(t, uint64(1), version&1, "the real CommitBatch must still own its publication fence")
	waited := make(chan struct{})
	result, finished := blocksReadinessRequest(t, server, `[10,13,{"minContextSlot":13}]`, func(*historyResponseOwner, context.CancelFunc) { close(waited) })
	var early *blocksReadinessResult
	select {
	case <-waited:
		select {
		case value := <-result:
			early = &value
			t.Error("request returned while the actual account commit remained odd")
		case <-time.After(20 * time.Millisecond):
		}
	case value := <-result:
		early = &value
		t.Error("request refused before waiting for the active CommitBatch")
	}
	unblock()
	require.NoError(t, <-committed)
	if early == nil {
		value := <-result
		early = &value
	}
	requireBlocksReadinessResult(t, *early, `[10,12,13]`)
	counts := <-finished
	require.Equal(t, 1, counts[0], "publication waits must not consume another query call")
	require.Greater(t, counts[1], 1, "one 1ms retry cannot cover this 20ms real commit")
	require.Zero(t, len(server.queryResponses))
}

func TestGetBlocksWaitsForInitialForwardPublication(t *testing.T) {
	for _, historyAhead := range []bool{false, true} {
		name := "durable ahead"
		if historyAhead {
			name = "history ahead"
		}
		t.Run(name, func(t *testing.T) {
			server, original := getBlocksFixture(t)
			original.Close()
			prepareBlocksReadinessTip(t, server)
			_, err := server.acctsDb.CommitBatch([]accounts.SlotDelta{{Slot: 13}}, 13, nil, nil)
			require.NoError(t, err)
			if historyAhead {
				require.NoError(t, server.SetRootedBlockHistorySlot(13))
			}
			waited := make(chan struct{})
			result, finished := blocksReadinessRequest(t, server, `[10,13,{"minContextSlot":13}]`, func(*historyResponseOwner, context.CancelFunc) { close(waited) })
			var early *blocksReadinessResult
			select {
			case <-waited:
				select {
				case value := <-result:
					early = &value
					t.Error("request returned before the scalar bank publication")
				case <-time.After(20 * time.Millisecond):
				}
			case value := <-result:
				early = &value
				t.Error("request refused the known forward publication gap")
			}
			require.NoError(t, server.SetRootedBlockHistorySlot(13))
			server.SetRootedBankState(13, 3, 0)
			if early == nil {
				value := <-result
				early = &value
			}
			requireBlocksReadinessResult(t, *early, `[10,12,13]`)
			counts := <-finished
			require.Equal(t, 1, counts[0])
			require.Greater(t, counts[1], 1)
		})
	}
}

func TestGetBlocksInitialWaitSafety(t *testing.T) {
	for _, action := range []string{"owner deadline", "cancel", "recovery pending", "completed rewind"} {
		t.Run(action, func(t *testing.T) {
			server, original := getBlocksFixture(t)
			original.Close()
			prepareBlocksReadinessTip(t, server)
			_, err := server.acctsDb.CommitBatch([]accounts.SlotDelta{{Slot: 13}}, 13, nil, nil)
			require.NoError(t, err)
			require.NoError(t, server.SetRootedBlockHistorySlot(13))
			locked := false
			var rewindErr error
			started := time.Now()
			result, finished := blocksReadinessRequest(t, server, `[10,13]`, func(owner *historyResponseOwner, cancel context.CancelFunc) {
				locked = server.historyRecoveryMu.TryLock()
				if !locked {
					return
				}
				server.historyRecoveryMu.Unlock()
				switch action {
				case "owner deadline":
					// Keep the context and network deadline live: only the admitted
					// owner's absolute deadline limits this otherwise permanent gap.
					owner.deadline = time.Now().Add(10 * time.Millisecond)
				case "cancel":
					cancel()
				case "recovery pending":
					server.SetHistoryRecoveryPending(true)
				case "completed rewind":
					server.SetHistoryRecoveryPending(true)
					_, rewindErr = server.acctsDb.RewindToBatchBoundary(12)
					if rewindErr == nil {
						rewindErr = server.RewindBlockHistory(12)
					}
					server.SetRootedBankState(12, 2, 0)
					server.SetHistoryRecoveryPending(false)
				}
			})
			value := <-result
			require.NoError(t, value.err)
			require.NoError(t, rewindErr)
			require.True(t, locked, "the publication wait must release the recovery read lock")
			require.Less(t, time.Since(started), time.Second)
			require.NotContains(t, string(value.body), `"result"`)
			switch action {
			case "owner deadline":
				require.Contains(t, string(value.body), context.DeadlineExceeded.Error())
			case "cancel":
				require.Contains(t, string(value.body), context.Canceled.Error())
			default:
				require.Contains(t, string(value.body), `"code":-32004`)
			}
			counts := <-finished
			require.Equal(t, 1, counts[0])
			require.Positive(t, counts[1])
			require.Zero(t, len(server.queryResponses))
		})
	}
}

func TestGetBlocksWaitsForPostReadAccountPublication(t *testing.T) {
	server, original := getBlocksFixture(t)
	original.Close()
	prepareBlocksReadinessTip(t, server)
	barrier := &blocksCommitBarrier{entered: make(chan struct{}), release: make(chan struct{})}
	unobserve := server.acctsDb.ObserveAccountState(barrier)
	defer unobserve()
	var release sync.Once
	unblock := func() { release.Do(func() { close(barrier.release) }) }
	defer unblock()
	committed, waited := make(chan error, 1), make(chan struct{})
	result, finished := blocksReadinessRequest(t, server, `[10,13]`, func(*historyResponseOwner, context.CancelFunc) { close(waited) }, func() {
		go func() {
			_, err := server.acctsDb.CommitBatch([]accounts.SlotDelta{{Slot: 13}}, 13, nil, nil)
			if err == nil {
				err = server.SetRootedBlockHistorySlot(13)
				server.SetRootedBankState(13, 3, 0)
			}
			committed <- err
		}()
		<-barrier.entered
	})
	var early *blocksReadinessResult
	select {
	case <-waited:
		select {
		case value := <-result:
			early = &value
			t.Error("post-read odd publication did not wait for commit completion")
		case <-time.After(20 * time.Millisecond):
		}
	case value := <-result:
		early = &value
		t.Error("post-read active publication was refused without reselection")
	}
	unblock()
	require.NoError(t, <-committed)
	if early == nil {
		value := <-result
		early = &value
	}
	requireBlocksReadinessResult(t, *early, `[10,12,13]`)
	counts := <-finished
	require.Equal(t, 1, counts[0])
	require.Greater(t, counts[1], 1)
	require.Equal(t, 10, counts[2], "one discarded three-slot scan and one recomputed four-slot scan only")
}

func TestGetBlocksInitialWaitRejectsUnsafeHistory(t *testing.T) {
	for _, change := range []string{"backward history", "uncommitted history", "snapshot history ahead"} {
		t.Run(change, func(t *testing.T) {
			server, original := getBlocksFixture(t)
			original.Close()
			if change == "backward history" {
				require.NoError(t, server.RewindBlockHistory(10))
			} else {
				require.NoError(t, server.SetRootedBlockHistorySlot(13))
				if change == "snapshot history ahead" {
					server.acctsDb = newRPCAccountsDB(t)
				}
			}
			result, finished := blocksReadinessRequest(t, server, `[10,12]`, func(owner *historyResponseOwner, _ context.CancelFunc) {
				owner.deadline = time.Now().Add(10 * time.Millisecond)
			})
			value := <-result
			require.NoError(t, value.err)
			require.NotContains(t, string(value.body), `"result"`)
			require.Contains(t, string(value.body), `"code":-32004`)
			counts := <-finished
			require.Equal(t, [3]int{1, 0, 1}, counts, "unsafe history must refuse without a wait or slot scan")
		})
	}
}
