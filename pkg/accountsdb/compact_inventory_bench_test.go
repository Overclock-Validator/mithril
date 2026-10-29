package accountsdb

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
)

// BenchmarkCompactOnceManyBootstrapFiles times one compaction cycle in a
// directory shaped like mainnet's: many bootstrap appendvecs plus a rewind
// horizon of fold manifests. MITHRIL_BENCH_BOOTSTRAP_FILES sets the file count
// (mainnet: about 480,000). Each cycle may delete up to 16 empty bootstrap
// files, identically with and without the inventory.
func BenchmarkCompactOnceManyBootstrapFiles(b *testing.B) {
	n := 2000
	if v, err := strconv.Atoi(os.Getenv("MITHRIL_BENCH_BOOTSTRAP_FILES")); err == nil && v > 0 {
		n = v
	}
	db, dir := newFoldTestDb(b)
	defer db.CloseDb()
	for i := 0; i < n; i++ {
		name := filepath.Join(db.AcctsDir, fmt.Sprintf("%d.%d", 1000+i/8, i+1))
		if err := os.WriteFile(name, nil, 0o644); err != nil {
			b.Fatal(err)
		}
	}
	var high [8]byte
	binary.LittleEndian.PutUint64(high[:], uint64(n))
	if err := os.WriteFile(filepath.Join(dir, "bootstrap_high_file_id"), high[:], 0o644); err != nil {
		b.Fatal(err)
	}
	db.LargestFileId.Store(uint64(n))
	const horizon = 64
	for batch := 0; batch < horizon+2; batch++ {
		through := uint64(1_000_000 + batch*100)
		delta := make([]*accounts.Account, 0, 2000)
		for k := 0; k < 2000; k++ {
			acct := foldAcct(byte(k), through, []byte("v"))
			acct.Key[1], acct.Key[2] = byte(k>>8), byte(batch)
			delta = append(delta, acct)
		}
		if _, err := db.CommitBatch(foldDeltas(accounts.SlotDelta{Slot: through, Delta: delta}), through,
			map[uint64][32]byte{through: bh(through)}, []byte("ctx")); err != nil {
			b.Fatal(err)
		}
	}
	cfg := CompactionConfig{RewindHorizonBatches: horizon}
	if _, err := db.CompactOnce(cfg); err != nil { // warm: builds the inventory and pins
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.CompactOnce(cfg); err != nil {
			b.Fatal(err)
		}
	}
}
