package node

import "sync"

// joinedStopper turns a service's stop signal + done channel into one
// idempotent barrier. It is used for the leader loop because finishing an
// active bank after the signal can still read AccountsDB.
type joinedStopper struct {
	signal func()
	done   <-chan struct{}
	once   sync.Once
}

func newJoinedStopper(signal func(), done <-chan struct{}) *joinedStopper {
	return &joinedStopper{signal: signal, done: done}
}

func (stopper *joinedStopper) Stop() {
	if stopper == nil {
		return
	}
	stopper.once.Do(func() {
		if stopper.signal != nil {
			stopper.signal()
		}
	})
	if stopper.done != nil {
		<-stopper.done
	}
}
