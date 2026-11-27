package accountsdb

import (
	"bytes"
	"weak"

	"github.com/Overclock-Validator/mithril/pkg/features"
)

// Keep the last few successfully checked immutable byte ranges, newest first.
// A weak pointer identifies the allocation without pinning a potentially large
// old account. The descriptor itself is immutable and atomically replaced by
// concurrent VMs.
//
// More than one is kept because more than one reader can hold its own copy of
// the same program at once (the node's replay, a replay from a checkpoint, a
// simulation). With a single slot they would evict each other and compare the
// whole program on every invocation.
type immutableProgramSource struct {
	starts [immutableProgramSources]weak.Pointer[byte]
	count  int
}

const immutableProgramSources = 4

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
	checked := entry.validatedSource.Load()
	if checked != nil {
		for i := 0; i < checked.count; i++ {
			if checked.starts[i] == start {
				return true
			}
		}
	}
	if !bytes.Equal(entry.sourceBytes, source) {
		return false
	}
	// A lost race between two validators drops one identity, which only
	// costs that reader one more comparison.
	next := &immutableProgramSource{count: 1}
	next.starts[0] = start
	if checked != nil {
		next.count += copy(next.starts[1:], checked.starts[:checked.count])
	}
	entry.validatedSource.Store(next)
	return true
}
