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
	entry.validatedSource.Store(&immutableProgramSource{length: len(source)})
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
	if entry.validatedSource.Load().start.Value() != nil {
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
