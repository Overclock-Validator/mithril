package turbine

import (
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"
)

// Run this identical public-API benchmark on both revisions. Each sample owns
// a fresh spool, so no completed-slot dedupe or full-queue dropping is timed.
// Close is untimed but drains the writer before the next sample. These ordinary
// filesystem measurements do not simulate rare storage stalls or whole replay.
func BenchmarkShredSpoolMarkComplete(b *testing.B) {
	root := b.TempDir()
	elapsed := make([]int64, 0, b.N)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s, err := OpenShredSpool(filepath.Join(root, strconv.Itoa(i)), 0)
		if err != nil {
			b.Fatal(err)
		}
		s.Append(100, []byte("packet"))
		b.StartTimer()
		start := time.Now()
		s.MarkComplete(100, 0, 1)
		duration := time.Since(start).Nanoseconds()
		b.StopTimer()
		elapsed = append(elapsed, duration)
		s.Close()
	}
	sort.Slice(elapsed, func(i, j int) bool { return elapsed[i] < elapsed[j] })
	b.ReportMetric(float64(elapsed[(len(elapsed)-1)/2]), "p50-ns")
	b.ReportMetric(float64(elapsed[(99*len(elapsed)+99)/100-1]), "p99-ns")
}
