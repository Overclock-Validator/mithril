package identity

import "sync/atomic"

// Derived caches a value computed from an identity, such as a TLS
// certificate. Its owner registers Refresh as an Updater, so the value for the
// incoming identity is built before the swap commits and a failure aborts it.
type Derived[T any] struct {
	derive func(*Keypair) (T, error)
	value  atomic.Pointer[derivedValue[T]]
}

type derivedValue[T any] struct {
	keypair *Keypair
	value   T
}

// NewDerived derives the value for the holder's current identity.
func NewDerived[T any](h *Holder, derive func(*Keypair) (T, error)) (*Derived[T], error) {
	d := &Derived[T]{derive: derive}
	if err := d.Refresh(h.Current()); err != nil {
		return nil, err
	}
	return d, nil
}

// Refresh replaces the cached value with one derived from kp.
func (d *Derived[T]) Refresh(kp *Keypair) error {
	value, err := d.derive(kp)
	if err != nil {
		return err
	}
	d.value.Store(&derivedValue[T]{keypair: kp, value: value})
	return nil
}

// Get returns the most recently refreshed value.
func (d *Derived[T]) Get() T {
	return d.value.Load().value
}

// Keypair returns the identity the cached value was derived from.
func (d *Derived[T]) Keypair() *Keypair {
	return d.value.Load().keypair
}
