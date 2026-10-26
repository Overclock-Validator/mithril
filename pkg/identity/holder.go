package identity

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/gagliardetto/solana-go"
)

// Updater adopts next before the holder publishes it, mirroring Agave's
// NotifyKeyUpdate: it returns once next is in effect for its subsystem, and an
// error aborts the swap. Updaters are also called with the previous keypair to
// undo a swap that a later updater rejected, so they must accept either key.
// An updater must not call Swap.
type Updater func(next *Keypair) error

// Listener observes a swap after the holder has published next.
type Listener func(prev, next *Keypair)

// Holder owns the current identity. Readers load it with Current at use time
// and must not cache anything derived from it unless they refresh that value
// through an Updater or Listener.
type Holder struct {
	current atomic.Pointer[Keypair]

	// swapMu serializes swaps so updaters see one transition at a time.
	swapMu sync.Mutex

	hooksMu   sync.Mutex
	nextHook  uint64
	updaters  []namedUpdater
	listeners []registeredListener
}

type namedUpdater struct {
	id   uint64
	name string
	fn   Updater
}

type registeredListener struct {
	id uint64
	fn Listener
}

// NewHolder holds a copy of a 64-byte Solana keypair.
func NewHolder(key ed25519.PrivateKey) (*Holder, error) {
	kp, err := NewKeypair(key)
	if err != nil {
		return nil, err
	}
	return newHolder(kp), nil
}

// NewEphemeralHolder holds a fresh random identity, for nodes started without
// a configured identity keypair.
func NewEphemeralHolder() (*Holder, error) {
	kp, err := GenerateKeypair()
	if err != nil {
		return nil, err
	}
	return newHolder(kp), nil
}

// HolderOrEphemeral returns h, or a new ephemeral holder when h is nil. It
// serves components that run with a throwaway identity when none is given.
func HolderOrEphemeral(h *Holder) (*Holder, error) {
	if h != nil {
		return h, nil
	}
	return NewEphemeralHolder()
}

func newHolder(kp *Keypair) *Holder {
	h := &Holder{}
	h.current.Store(kp)
	return h
}

func (h *Holder) Current() *Keypair {
	return h.current.Load()
}

func (h *Holder) PublicKey() solana.PublicKey {
	return h.current.Load().PublicKey()
}

// RegisterUpdater adds fn to the updaters every later swap runs, in
// registration order. The returned function removes it; a swap already in
// progress may still call it once.
func (h *Holder) RegisterUpdater(name string, fn Updater) (unregister func()) {
	h.hooksMu.Lock()
	defer h.hooksMu.Unlock()
	h.nextHook++
	id := h.nextHook
	h.updaters = append(h.updaters, namedUpdater{id: id, name: name, fn: fn})
	return func() {
		h.hooksMu.Lock()
		defer h.hooksMu.Unlock()
		for i, u := range h.updaters {
			if u.id == id {
				h.updaters = append(h.updaters[:i:i], h.updaters[i+1:]...)
				return
			}
		}
	}
}

// OnSwap adds fn to the listeners every later successful swap notifies.
func (h *Holder) OnSwap(fn Listener) (unsubscribe func()) {
	h.hooksMu.Lock()
	defer h.hooksMu.Unlock()
	h.nextHook++
	id := h.nextHook
	h.listeners = append(h.listeners, registeredListener{id: id, fn: fn})
	return func() {
		h.hooksMu.Lock()
		defer h.hooksMu.Unlock()
		for i, l := range h.listeners {
			if l.id == id {
				h.listeners = append(h.listeners[:i:i], h.listeners[i+1:]...)
				return
			}
		}
	}
}

// Swap makes key the current identity. Every updater adopts it first; if one
// fails, the updaters that already ran are given the previous keypair again
// and the holder keeps it. Swapping to the current pubkey is a no-op.
func (h *Holder) Swap(key ed25519.PrivateKey) (prev *Keypair, err error) {
	next, err := NewKeypair(key)
	if err != nil {
		return nil, err
	}
	h.swapMu.Lock()
	defer h.swapMu.Unlock()
	prev = h.current.Load()
	if prev.PublicKey() == next.PublicKey() {
		return prev, nil
	}

	// Updaters block until their subsystem adopts the key, so run them from a
	// copy rather than under hooksMu.
	h.hooksMu.Lock()
	updaters := append([]namedUpdater(nil), h.updaters...)
	h.hooksMu.Unlock()
	for i, u := range updaters {
		if err := u.fn(next); err != nil {
			err = fmt.Errorf("apply identity %s to %s: %w", next.PublicKey(), u.name, err)
			for j := i - 1; j >= 0; j-- {
				if rerr := updaters[j].fn(prev); rerr != nil {
					err = errors.Join(err, fmt.Errorf("restore identity %s to %s: %w", prev.PublicKey(), updaters[j].name, rerr))
				}
			}
			return prev, err
		}
	}
	h.current.Store(next)

	h.hooksMu.Lock()
	listeners := append([]registeredListener(nil), h.listeners...)
	h.hooksMu.Unlock()
	for _, l := range listeners {
		l.fn(prev, next)
	}
	return prev, nil
}
