package accountsdb

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestPinScratchMatchesOwnedReads(t *testing.T) {
	var scratch pinManifestScratch
	dir := t.TempDir()
	path := filepath.Join(dir, "manifest")
	for _, n := range []int{5, 0, 23, 3} {
		m := SegmentManifest{Version: segManifestVersion, Kind: ManifestKindFold, BatchSeq: 1, ThroughSlot: 10, FileId: 42}
		for i := 0; i < n; i++ {
			m.Records = append(m.Records, ManifestRecord{Offset: uint64(i + 5), Pubkey: [32]byte{byte(i)}, PrevValid: i%2 == 0})
			m.Bankhashes = append(m.Bankhashes, SlotBankhash{Slot: uint64(i)})
			m.ResumeCtx = append(m.ResumeCtx, byte(i))
		}
		b := m.encode()
		if err := os.WriteFile(path, b, 0600); err != nil {
			t.Fatal(err)
		}
		owned, err := ReadSegmentManifest(path)
		if err != nil {
			t.Fatal(err)
		}
		var got []ManifestRecord
		err = scratch.visit(path, func(r ManifestRecord) error { got = append(got, r); return nil })
		if err != nil || !reflect.DeepEqual(owned.Records, append(make([]ManifestRecord, 0, n), got...)) {
			t.Fatalf("n=%d err=%v mismatch", n, err)
		}
		damaged := bytes.Clone(b)
		damaged[len(damaged)-1] ^= 1
		if err := os.WriteFile(path, damaged, 0600); err != nil {
			t.Fatal(err)
		}
		if err = scratch.visit(path, nil); !errors.Is(err, ErrTornManifest) {
			t.Fatal(err)
		}
	}
}
