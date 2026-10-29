package accountsdb

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"syscall"
	"testing"
)

// Literal pre-change ordering is an independent oracle for persisted cursors
// and for the retry class, whose scan age takes precedence over its filename.
func legacyCompactKey(c compactCandidate) string {
	if c.manifestPath != "" && c.allocated >= sparseCompactionMinSize {
		if c.alreadySparse {
			if c.allocated >= c.size/10 {
				return "1:" + c.name
			}
			return "2:" + c.name
		}
		return "0:" + c.name
	}
	return "2:" + c.name
}
func legacyCompactLess(a, b compactCandidate) bool {
	ak, bk := legacyCompactKey(a), legacyCompactKey(b)
	if ak[0] == '1' && bk[0] == '1' && a.scanOrder != b.scanOrder {
		return a.scanOrder < b.scanOrder
	}
	return ak < bk
}
func comparatorFixture(n int) []compactCandidate {
	r := rand.New(rand.NewSource(731))
	out := make([]compactCandidate, n)
	for i := range out {
		size := int64(128 << 20)
		allocations := []int64{0, sparseCompactionMinSize - 1, sparseCompactionMinSize, size/10 - 1, size / 10, size/10 + 1, size}
		out[i] = compactCandidate{name: fmt.Sprintf("%d.%d", 460700000+r.Intn(100000), r.Intn(10000000)), size: size, allocated: allocations[r.Intn(len(allocations))], alreadySparse: r.Intn(2) == 0, scanOrder: uint64(r.Intn(20)), fileId: uint64(i)}
		if r.Intn(4) != 0 {
			out[i].manifestPath = "manifest"
		}
	}
	return out
}
func TestCompactComparatorLegacyEquivalence(t *testing.T) {
	candidates := comparatorFixture(3000)
	// Include identical ordering keys with distinct identities, unusual filenames,
	// and long shared prefixes. sort.Slice must retain the same tie behavior.
	for _, name := range []string{"", "x", "x:y", "é", "00000000000000000000000000000000000000000000000000000000000000000000000001"} {
		for _, c := range candidates[:50] {
			c.name = name
			candidates = append(candidates, c)
		}
	}
	for _, c := range candidates {
		if got, want := compactCandidateKey(c), legacyCompactKey(c); got != want {
			t.Fatalf("key %q != %q", got, want)
		}
		if compactCandidateClass(c) != legacyCompactKey(c)[0] {
			t.Fatal("class mismatch")
		}
	}
	r := rand.New(rand.NewSource(509))
	for i := 0; i < 100000; i++ {
		a, b := candidates[r.Intn(len(candidates))], candidates[r.Intn(len(candidates))]
		if compactCandidateLess(a, b) != legacyCompactLess(a, b) {
			t.Fatalf("ordering mismatch: %+v %+v", a, b)
		}
	}
	old := append([]compactCandidate(nil), candidates...)
	fresh := append([]compactCandidate(nil), candidates...)
	sort.Slice(old, func(i, j int) bool { return legacyCompactLess(old[i], old[j]) })
	sort.Slice(fresh, func(i, j int) bool { return compactCandidateLess(fresh[i], fresh[j]) })
	if !reflect.DeepEqual(old, fresh) {
		t.Fatal("sorted candidate identity/order differs")
	}
	for _, class := range []byte{'0', '1', '2'} {
		var group []compactCandidate
		for _, c := range old {
			if legacyCompactKey(c)[0] == class {
				group = append(group, c)
			}
		}
		for _, cursor := range []string{"", string(class) + ":", string(class) + ":460750000.5000000", "z"} {
			// Production rotates only filename-ordered classes 0 and 2.
			if class == '1' || len(group) == 0 {
				continue
			}
			i := sort.Search(len(group), func(i int) bool { return legacyCompactKey(group[i]) > cursor })
			want := append(append([]compactCandidate(nil), group[i:]...), group[:i]...)
			if !reflect.DeepEqual(rotateCompactCandidates(group, cursor), want) {
				t.Fatal("cursor rotation changed")
			}
		}
	}
}
func benchmarkCPUSeconds() float64 {
	var u syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &u); err != nil {
		panic(err)
	}
	return float64(u.Utime.Sec+u.Stime.Sec) + float64(u.Utime.Usec+u.Stime.Usec)/1e6
}
func BenchmarkCompactCandidateSort(b *testing.B) {
	for _, n := range []int{10000, 100000} {
		fixture := comparatorFixture(n)
		for _, legacy := range []bool{true, false} {
			name := "direct"
			if legacy {
				name = "legacy"
			}
			b.Run(fmt.Sprintf("%d/%s", n, name), func(b *testing.B) {
				scratch := make([]compactCandidate, n)
				b.ReportAllocs()
				b.ResetTimer()
				cpuStart := benchmarkCPUSeconds()
				for k := 0; k < b.N; k++ {
					copy(scratch, fixture)
					if legacy {
						sort.Slice(scratch, func(i, j int) bool { return legacyCompactLess(scratch[i], scratch[j]) })
					} else {
						sort.Slice(scratch, func(i, j int) bool { return compactCandidateLess(scratch[i], scratch[j]) })
					}
				}
				cpu := benchmarkCPUSeconds() - cpuStart
				b.StopTimer()
				b.ReportMetric(cpu*1e9/float64(b.N), "cpu-ns/op")
			})
		}
	}
}
