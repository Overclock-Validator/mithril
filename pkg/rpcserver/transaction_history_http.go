package rpcserver

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/transactionhistory"
	"github.com/filecoin-project/go-jsonrpc"
)

const historyResponseTimeout = 30 * time.Second

type historyResponseKey struct{}
type queryResponseKey struct{}

var errQueryResponseUnavailable = errors.New("query response unavailable")

// historyResponseOwner belongs to one HTTP request. The pinned JSON-RPC server
// executes HTTP batch members sequentially, including serialization and writes.
type historyResponseOwner struct {
	slots      chan struct{}
	controller *http.ResponseController
	deadline   time.Time
	calls      int
	acquired   bool
}

func historyResponseContext(ctx context.Context, w http.ResponseWriter, slots chan struct{}) (context.Context, *historyResponseOwner) {
	owner := newResponseOwner(ctx, w, slots, time.Now().Add(historyResponseTimeout))
	if owner == nil {
		return ctx, nil
	}
	return context.WithValue(ctx, historyResponseKey{}, owner), owner
}

func newResponseOwner(ctx context.Context, w http.ResponseWriter, slots chan struct{}, deadline time.Time) *historyResponseOwner {
	if slots == nil {
		return nil
	}
	if earlier, ok := ctx.Deadline(); ok && earlier.Before(deadline) {
		deadline = earlier
	}
	return &historyResponseOwner{slots: slots, controller: http.NewResponseController(w), deadline: deadline}
}

// acquireHistoryResponse admits at most two history payload calls per HTTP
// request. Later batch members are unavailable; metadata-only methods bypass it.
// A successful permit lasts until the outer HTTP handler finishes its writes.
func acquireHistoryResponse(ctx context.Context) error {
	owner, _ := ctx.Value(historyResponseKey{}).(*historyResponseOwner)
	return owner.acquire(ctx, transactionhistory.ErrUnavailable)
}

// acquireQueryResponse limits signature-status payloads to two concurrent HTTP
// responses and two calls per request.
func acquireQueryResponse(ctx context.Context) error {
	owner, _ := ctx.Value(queryResponseKey{}).(*historyResponseOwner)
	return owner.acquire(ctx, errQueryResponseUnavailable)
}

func (owner *historyResponseOwner) acquire(ctx context.Context, unavailable error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if jsonrpc.GetConnectionType(ctx) != jsonrpc.ConnectionTypeHTTP {
		return unavailable
	}
	if owner == nil || owner.calls >= 2 {
		return unavailable
	}
	owner.calls++
	if !time.Now().Before(owner.deadline) {
		return context.DeadlineExceeded
	}
	if owner.acquired {
		return nil
	}
	select {
	case owner.slots <- struct{}{}:
	default:
		return unavailable
	}
	if err := owner.controller.SetWriteDeadline(owner.deadline); err != nil {
		<-owner.slots
		return errors.Join(unavailable, err)
	}
	owner.acquired = true
	return nil
}

// release flushes the final HTTP buffer before returning the permit. It runs
// only after JSON-RPC serialization, never when a request is merely canceled.
// Clearing the deadline here preserves healthy keepalive connections; batch
// members cannot renew their fixed request deadline.
func (owner *historyResponseOwner) release() error {
	return finalizeResponseOwners(owner)
}

// finalizeResponseOwners keeps every acquired permit through one final flush
// and deadline clear. Mixed owners share the same HTTP writer and deadline.
func finalizeResponseOwners(owners ...*historyResponseOwner) error {
	var controller *http.ResponseController
	for _, owner := range owners {
		if owner != nil && owner.acquired {
			controller = owner.controller
			break
		}
	}
	if controller == nil {
		return nil
	}
	defer func() {
		for _, owner := range owners {
			if owner != nil && owner.acquired {
				owner.acquired = false
				<-owner.slots
			}
		}
	}()
	return errors.Join(controller.Flush(), controller.SetWriteDeadline(time.Time{}))
}
