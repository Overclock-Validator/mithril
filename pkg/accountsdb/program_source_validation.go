package accountsdb

import (
	"bytes"
	"weak"

	"github.com/Overclock-Validator/mithril/pkg/features"
)

// Keep only the last successfully checked immutable byte range. A weak pointer
// identifies the allocation without pinning a potentially large old account.
// The descriptor itself is immutable and atomically replaced by concurrent VMs.
type immutableProgramSource struct {
	start weak.Pointer[byte]
}

// MatchesImmutableSource may only be used for an explicitly immutable source:
// neither the caller nor another account owner may mutate its backing array.
// Ordinary transaction-local/writable sources must use MatchesSource instead.
// Loader metadata must still be checked against the current bank on every call.
func (entry *ProgramCacheEntry) MatchesImmutableSource(source []byte, f *features.Features) bool {
	if entry == nil || !entry.sourceBound || !entry.sourceFeaturesMatch(f) {
		return false
	}
	if len(source) != len(entry.sourceBytes) {
		return false
	}
	if len(source) == 0 {
		return true
	}
	// The caller already holds a live reference. Weak identities distinguish
	// both the allocation and byte offset, even after an old allocation is freed.
	// Converting the memo back to a strong pointer can park during GC.
	start := weak.Make(&source[0])
	if checked := entry.validatedSource.Load(); checked != nil && checked.start == start {
		return true
	}
	if !bytes.Equal(entry.sourceBytes, source) {
		return false
	}
	entry.validatedSource.Store(&immutableProgramSource{start: start})
	return true
}
