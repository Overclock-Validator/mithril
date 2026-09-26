package turbine

import "context"

// acquireRequest keeps the total request/job bounds unchanged while reserving
// one permit for completion or full-block recovery. Waiting completions win the
// next available permit over prefetch; completions are otherwise equal priority.
// This is not replay-head scheduling: unfinished prefetch already admitted for
// the head keeps its existing rolling job window, and future-slot completions
// also use the reservation. No worker is reserved and no admitted job is evicted.
//
// Prefetch can wait while completions remain queued. It is speculative work and
// resumes when the completion backlog drains. Cancellation/close wake waiters
// without admitting a request; accepted requests retain the full join contract.
func (v *transactionVerifier) acquireRequest(ctx context.Context, prefetch bool) error {
	v.mu.Lock()
	waitingCompletion := false
	defer func() {
		if waitingCompletion {
			v.completionWaiters--
			v.wakeAdmissionLocked()
		}
		v.mu.Unlock()
	}()
	for {
		if v.closed {
			return errTransactionVerifierClosed
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(v.requests) < cap(v.requests) &&
			(!prefetch || (v.prefetchRequests < cap(v.requests)-1 && v.completionWaiters == 0)) {
			v.requests <- struct{}{}
			if prefetch {
				v.prefetchRequests++
			}
			// Add under the same lock as close, so closeAndWait cannot finish
			// while an accepted request has yet to start its goroutine.
			v.request.Add(1)
			return nil
		}
		if !prefetch && !waitingCompletion {
			v.completionWaiters++
			waitingCompletion = true
		}
		if v.admissionChanged == nil {
			v.admissionChanged = make(chan struct{})
		}
		changed := v.admissionChanged
		v.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
		case <-v.stopped:
		}
		v.mu.Lock()
	}
}

func (v *transactionVerifier) releaseRequest(prefetch bool) {
	v.mu.Lock()
	<-v.requests
	if prefetch {
		v.prefetchRequests--
	}
	v.wakeAdmissionLocked()
	v.mu.Unlock()
}

func (v *transactionVerifier) wakeAdmissionLocked() {
	if v.admissionChanged != nil {
		close(v.admissionChanged)
		v.admissionChanged = nil
	}
}
