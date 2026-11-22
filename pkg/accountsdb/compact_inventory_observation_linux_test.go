//go:build linux

package accountsdb

import (
	"os"
	"syscall"
	"testing"
)

func TestCompactionInventoryUsesLinuxBlockAccounting(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	commitTestBatch(t, db, 101, foldAcct(1, 1, []byte("small allocated file")))
	entries, err := os.ReadDir(db.AcctsDir)
	if err != nil {
		t.Fatal(err)
	}
	var files, logical, allocated uint64
	for _, entry := range entries {
		if _, _, ok := parseDataFileName(entry.Name()); !ok {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			t.Fatal(err)
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Blocks < 0 || info.Size() < 0 {
			t.Fatal("no independent Linux allocation witness")
		}
		files++
		logical += uint64(info.Size())
		allocated += uint64(stat.Blocks) * 512
	}
	stats, err := db.CompactOnce(CompactionConfig{RewindHorizonBatches: 64, ObserveInventory: true})
	if err != nil {
		t.Fatal(err)
	}
	o := stats.Inventory
	if files == 0 || !o.Known || !o.AllocatedBytesKnown || o.DataFiles != files || o.LogicalBytes != logical || o.AllocatedBytes != allocated {
		t.Fatalf("inventory differs from independent stat blocks: %+v; files=%d logical=%d allocated=%d", o, files, logical, allocated)
	}
}
