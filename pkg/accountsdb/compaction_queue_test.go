package accountsdb

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// Exact old implementation, including cursor rotation and alternation. Tests
// exhaust the queue, rather than just comparing its first budget-sized prefix.
func sortedCompactionQueue(candidates []compactCandidate, sparseCursor, slowCursor string, next, slowFirst bool) []compactCandidate {
	sort.Slice(candidates, func(i, j int) bool { return compactCandidateLess(candidates[i], candidates[j]) })
	freshEnd := sort.Search(len(candidates), func(i int) bool { return compactCandidateClass(candidates[i]) >= '1' })
	sparseEnd := sort.Search(len(candidates), func(i int) bool { return compactCandidateClass(candidates[i]) >= '2' })
	fresh := rotateCompactCandidates(candidates[:freshEnd], sparseCursor)
	retry := append([]compactCandidate(nil), candidates[freshEnd:sparseEnd]...)
	slow := rotateCompactCandidates(candidates[sparseEnd:], slowCursor)
	out := make([]compactCandidate, 0, len(candidates))
	for len(fresh) > 0 || len(retry) > 0 {
		if len(retry) > 0 && (next || len(fresh) == 0) {
			out = append(out, retry[0])
			retry = retry[1:]
			next = false
		} else {
			out = append(out, fresh[0])
			fresh = fresh[1:]
			next = true
		}
	}
	if slowFirst {
		return append(slow, out...)
	}
	return append(out, slow...)
}
func TestCompactionQueueMatchesSortedOrder(t *testing.T) {
	rng := rand.New(rand.NewSource(59))
	cursors := []string{"", "0", "0:", "0:0", "0:999999999999", "1:foo", "2:", "2:999999999999", "z"}
	for round := 0; round < 400; round++ {
		n := rng.Intn(2000)
		if round < 3 {
			n = round
		}
		items := comparatorFixture(n)
		// Actual filenames are unique, even if slots and retry ages coincide.
		for i := range items {
			items[i].name = fmt.Sprintf("%d.%d", rng.Intn(100), i)
		}
		sc, bc := cursors[rng.Intn(len(cursors))], cursors[rng.Intn(len(cursors))]
		if n > 0 && round%2 == 0 {
			sc = compactCandidateKey(items[rng.Intn(n)])
			bc = compactCandidateKey(items[rng.Intn(n)])
		}
		next, slowFirst := round%2 == 0, round%8 == 0
		want := sortedCompactionQueue(append([]compactCandidate(nil), items...), sc, bc, next, slowFirst)
		q := newCompactCandidateQueue(items, sc, bc, next, slowFirst)
		got := make([]compactCandidate, 0, n)
		for {
			c, ok := q.next()
			if !ok {
				break
			}
			got = append(got, c)
		}
		require.Equal(t, len(want), len(got), "round %d", round)
		for i := range want {
			require.Equal(t, want[i], got[i], "round %d index %d", round, i)
		}
		_, ok := q.next()
		require.False(t, ok)
	}
}
func BenchmarkCompactionQueue(b *testing.B) {
	for _, n := range []int{1000, 100000, 500000} {
		original := comparatorFixture(n)
		for _, method := range []string{"sort", "heap"} {
			b.Run(fmt.Sprintf("%d/%s", n, method), func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for pass := 0; pass < b.N; pass++ {
					items := append([]compactCandidate(nil), original...)
					if method == "sort" {
						q := sortedCompactionQueue(items, "0:460750000.0", "2:460750000.0", true, false)
						if len(q) != n {
							b.Fatal("length")
						}
					} else {
						q := newCompactCandidateQueue(items, "0:460750000.0", "2:460750000.0", true, false)
						for i := 0; i < 16; i++ {
							_, ok := q.next()
							if !ok {
								b.Fatal("length")
							}
						}
					}
				}
			})
		}
	}
}
