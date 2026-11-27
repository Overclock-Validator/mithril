package accountsdb

import (
	"bytes"
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/features"
)

func TestImmutableProgramSourceValidation(t *testing.T) {
	f := features.NewFeaturesDefault()
	source := bytes.Repeat([]byte{17}, 128<<10)
	entry := &ProgramCacheEntry{}
	if entry.MatchesImmutableSource(source, f) {
		t.Fatal("unbound source matched")
	}
	entry.BindSource(source, f)
	for range 3 {
		if !entry.MatchesImmutableSource(source, f) {
			t.Fatal("immutable source missed")
		}
	}
	first := entry.validatedSource.Load()
	if first == nil {
		t.Fatal("validation was not memoized")
	}
	if !entry.MatchesImmutableSource(source, f) || first != entry.validatedSource.Load() {
		t.Fatal("warm validation allocated another descriptor")
	}
	changed := bytes.Clone(source)
	changed[len(changed)-1]++
	if entry.MatchesImmutableSource(changed, f) {
		t.Fatal("same-length fork matched")
	}
	if entry.MatchesImmutableSource(source[:len(source)-1], f) {
		t.Fatal("different range matched")
	}
	differentFeatures := f.Clone()
	differentFeatures.EnableFeature(features.VirtualAddressSpaceAdjustments, 0)
	if entry.MatchesImmutableSource(source, differentFeatures) {
		t.Fatal("different features matched")
	}
	replacement := bytes.Clone(source)
	if !entry.MatchesImmutableSource(replacement, f) {
		t.Fatal("identical replacement missed")
	}
	if first == entry.validatedSource.Load() {
		t.Fatal("replacement was not validated")
	}
	// Models an expired weak reference without depending on GC scheduling.
	entry.validatedSource.Store(&immutableProgramSource{})
	if !entry.MatchesImmutableSource(source, f) {
		t.Fatal("expired memo prevented full validation")
	}
	if entry.MatchesImmutableSource(nil, f) {
		t.Fatal("empty source matched nonempty program")
	}
	// The ordinary path must never trust an immutable-source memo for mutable data.
	source[0]++
	if entry.MatchesSource(source, f) {
		t.Fatal("mutable source bypassed comparison")
	}
	entry.BindSource(nil, f)
	if entry.validatedSource.Load() != nil || !entry.MatchesImmutableSource(nil, f) {
		t.Fatal("rebinding retained old validation")
	}
}

func TestImmutableProgramSourceConcurrentVersions(t *testing.T) {
	f := features.NewFeaturesDefault()
	source := bytes.Repeat([]byte{23}, 8192)
	entry := &ProgramCacheEntry{}
	entry.BindSource(source, f)
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			version := bytes.Clone(source)
			want := worker%2 == 0
			if !want {
				version[len(version)-1]++
			}
			for range 100 {
				if got := entry.MatchesImmutableSource(version, f); got != want {
					t.Errorf("fork validation: got %v want %v", got, want)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// A large backing allocation avoids the special tiny-allocation behavior of
// weak pointers. The helper returns no strong reference to the borrowed bytes.
func sourceWithOnlyWeakReference(t *testing.T) *ProgramCacheEntry {
	source := bytes.Repeat([]byte{31}, 128<<10)
	entry := &ProgramCacheEntry{}
	f := features.NewFeaturesDefault()
	entry.BindSource(source, f)
	if !entry.MatchesImmutableSource(source, f) {
		t.Fatal("initial source validation failed")
	}
	return entry
}

func TestImmutableProgramSourceDoesNotRetainAccount(t *testing.T) {
	entry := sourceWithOnlyWeakReference(t)
	runtime.GC()
	runtime.GC()
	if entry.validatedSource.Load().starts[0].Value() != nil {
		t.Fatal("validation memo retained borrowed source")
	}
	runtime.KeepAlive(entry)
}

func BenchmarkProgramSourceValidation(b *testing.B) {
	for _, size := range []int{32 << 10, 256 << 10, 2 << 20} {
		for _, immutable := range []bool{false, true} {
			b.Run(fmt.Sprintf("bytes_%d/immutable_%v", size, immutable), func(b *testing.B) {
				source := bytes.Repeat([]byte{7}, size)
				f := features.NewFeaturesDefault()
				f.EnableFeature(features.VirtualAddressSpaceAdjustments, 0)
				entry := &ProgramCacheEntry{}
				entry.BindSource(source, f)
				entry.MatchesImmutableSource(source, f)
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					var matches bool
					if immutable {
						matches = entry.MatchesImmutableSource(source, f)
					} else {
						matches = entry.MatchesSource(source, f)
					}
					if !matches {
						b.Fatal("source did not match")
					}
				}
				runtime.KeepAlive(source)
			})
		}
	}
}

func TestImmutableSourceMemoTracksFeatureMutationAndRebind(t *testing.T) {
	source := []byte{1, 2, 3}
	bank := features.NewFeaturesDefault()
	entry := &ProgramCacheEntry{}
	entry.BindSource(source, bank)
	for range 2 {
		if !entry.MatchesImmutableSource(source, bank) {
			t.Fatal("initial match")
		}
	}
	if entry.matchedFeatures.Load() == nil || entry.validatedSource.Load() == nil {
		t.Fatal("both memos must be populated")
	}
	bank.EnableFeature(features.Alpenglow, 10)
	if entry.MatchesImmutableSource(source, bank) {
		t.Fatal("byte identity bypassed changed features")
	}
	entry.BindSource(source, bank)
	if entry.matchedFeatures.Load() != nil || entry.validatedSource.Load() != nil {
		t.Fatal("rebind retained memo")
	}
	if !entry.MatchesImmutableSource(source, bank) {
		t.Fatal("rebound source")
	}
	bank.DisableFeature(features.Alpenglow)
	if entry.MatchesImmutableSource(source, bank) {
		t.Fatal("disable bypassed feature validation")
	}
}

func TestImmutableProgramSourceDistinguishesRangesWithinAllocation(t *testing.T) {
	data := bytes.Repeat([]byte{4}, 513)
	data[512] = 5
	f := features.NewFeaturesDefault()
	entry := new(ProgramCacheEntry)
	entry.BindSource(data[:512], f)
	if !entry.MatchesImmutableSource(data[:512], f) {
		t.Fatal("original byte range did not match")
	}
	runtime.GC()
	if entry.MatchesImmutableSource(data[1:], f) {
		t.Fatal("different offset in same allocation matched")
	}
	if entry.MatchesImmutableSource(data[:511], f) {
		t.Fatal("different length at same address matched")
	}
	if !entry.MatchesImmutableSource(data[:512], f) {
		t.Fatal("original range no longer matched")
	}
}

// Two readers holding their own copy of one program must not evict each
// other's checked identity: after each has been compared once, neither is
// compared again.
func TestImmutableProgramSourceRemembersSeveralReaders(t *testing.T) {
	first := bytes.Repeat([]byte{7}, 64<<10)
	second := bytes.Clone(first)
	entry := &ProgramCacheEntry{}
	f := features.NewFeaturesDefault()
	entry.BindSource(first, f)
	if !entry.MatchesImmutableSource(first, f) || !entry.MatchesImmutableSource(second, f) {
		t.Fatal("equal sources did not validate")
	}
	memo := entry.validatedSource.Load()
	for range 3 {
		if !entry.MatchesImmutableSource(first, f) || !entry.MatchesImmutableSource(second, f) {
			t.Fatal("a remembered source stopped validating")
		}
	}
	if memo != entry.validatedSource.Load() {
		t.Fatal("alternating readers replaced the memo, so each was compared again")
	}
	// A fifth distinct copy pushes out the oldest, and still validates.
	for range immutableProgramSources {
		if !entry.MatchesImmutableSource(bytes.Clone(first), f) {
			t.Fatal("a further copy did not validate")
		}
	}
	if got := entry.validatedSource.Load().count; got != immutableProgramSources {
		t.Fatalf("memo holds %d identities, want %d", got, immutableProgramSources)
	}
	if entry.MatchesImmutableSource(bytes.Repeat([]byte{8}, 64<<10), f) {
		t.Fatal("a different program validated")
	}
	runtime.KeepAlive(first)
	runtime.KeepAlive(second)
}
