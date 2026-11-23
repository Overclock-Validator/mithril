package dashboard

import "time"

func sameStatusBinding(a, b *Snapshot) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	sameOptional := func(x, y *string) bool {
		return (x == nil && y == nil) || (x != nil && y != nil && *x == *y)
	}
	return a.RunID == b.RunID && a.StartedAt.Equal(b.StartedAt) &&
		sameOptional(a.GenesisHash, b.GenesisHash) && sameOptional(a.IdentityKey, b.IdentityKey)
}

// Both bounds matter: a small wall-clock step may expire status before its
// monotonic deadline without exceeding the reader's clock-skew tolerance.
func statusLeaseContinuous(monotonicRemaining, wallRemaining time.Duration) bool {
	return monotonicRemaining > 0 && wallRemaining > 0
}
