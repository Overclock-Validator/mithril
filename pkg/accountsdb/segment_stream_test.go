package accountsdb

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func streamedManifestFixture(n int) *SegmentManifest {
	m := &SegmentManifest{Version: segManifestVersion, Kind: ManifestKindFold,
		BatchSeq: 9, FromSlot: 100, ThroughSlot: 200, FileId: 42, DataLen: 12345, DataCRC: 123,
		Bankhashes: []SlotBankhash{{Slot: 101, Bankhash: [32]byte{1}}, {Slot: 200, Bankhash: [32]byte{2}}},
		ResumeCtx:  make([]byte, 200_003), Records: make([]ManifestRecord, n)}
	for i := range m.Records {
		r := &m.Records[i]
		binary.LittleEndian.PutUint64(r.Pubkey[:8], uint64(i+1))
		r.Offset, r.OwnerSlot, r.PrevValid = uint64(i*128), uint64(100+i%100), i%3 != 0
		r.Prev = AccountIndexEntry{Slot: uint64(i), FileId: uint64(3 + i%7), Offset: uint64(i * 256)}
	}
	return m
}

func TestVisitSegmentManifestRecordsMatchesFullDecode(t *testing.T) {
	for _, n := range []int{0, 1, 3000} {
		m := streamedManifestFixture(n)
		path := filepath.Join(t.TempDir(), "manifest")
		require.NoError(t, os.WriteFile(path, m.encode(), 0600))
		full, err := ReadSegmentManifest(path)
		require.NoError(t, err)
		var got []ManifestRecord
		err = visitSegmentManifestRecords(path, func(r ManifestRecord) error { got = append(got, r); return nil })
		require.NoError(t, err)
		require.Equal(t, full.Records, append(make([]ManifestRecord, 0, n), got...))
		require.NoError(t, visitSegmentManifestRecords(path, nil))
	}
}

func TestVisitSegmentManifestRecordsRejectsMalformed(t *testing.T) {
	m := streamedManifestFixture(3)
	valid := m.encode()
	const prefixLen = 4 + 4 + 1 + 5*8 + 4
	ctxLenAt := prefixLen + 4 + len(m.Bankhashes)*40
	nRecordsAt := ctxLenAt + 4 + len(m.ResumeCtx)
	withCRC := func(body []byte) []byte {
		out := append([]byte(nil), body...)
		return binary.LittleEndian.AppendUint32(out, crc32.ChecksumIEEE(body))
	}
	cases := map[string][]byte{
		"empty":            nil,
		"short-prefix":     withCRC(valid[:12]),
		"truncated-record": withCRC(valid[:len(valid)-5]),
		"trailing-body":    withCRC(append(append([]byte(nil), valid[:len(valid)-4]...), 9)),
		"truncated-crc":    valid[:len(valid)-1],
		"appended":         append(append([]byte(nil), valid...), 9),
	}
	for name, offset := range map[string]int{"magic": 0, "version": 4, "banks": prefixLen, "context": ctxLenAt, "records": nRecordsAt} {
		data := append([]byte(nil), valid[:len(valid)-4]...)
		value := ^uint32(0)
		if name == "banks" {
			value = uint32(len(m.Bankhashes) + 1)
		}
		binary.LittleEndian.PutUint32(data[offset:], value)
		cases[name] = withCRC(data)
	}
	badCRC := append([]byte(nil), valid...)
	badCRC[len(badCRC)-1] ^= 1
	cases["bad-crc"] = badCRC
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "manifest")
			require.NoError(t, os.WriteFile(path, data, 0600))
			_, err := ReadSegmentManifest(path)
			require.ErrorIs(t, err, ErrTornManifest)
			require.ErrorIs(t, visitSegmentManifestRecords(path, func(ManifestRecord) error { return nil }), ErrTornManifest)
		})
	}
}

func TestVisitSegmentManifestRecordsVisitorErrorAndGrowth(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest")
	m := streamedManifestFixture(2)
	require.NoError(t, os.WriteFile(path, m.encode(), 0600))
	want := errors.New("conflicting undo pointer")
	require.ErrorIs(t, visitSegmentManifestRecords(path, func(ManifestRecord) error { return want }), want)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	require.NoError(t, err)
	defer f.Close()
	err = visitSegmentManifestRecords(path, func(ManifestRecord) error {
		_, err := f.Write([]byte{1})
		return err
	})
	require.ErrorIs(t, err, ErrTornManifest)
}

func TestCompactionStreamedPinsRejectConflictingUndoReferences(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	commitTestBatch(t, db, 110, foldAcct(1, 1, []byte("first")))
	result := commitTestBatch(t, db, 120, foldAcct(1, 2, []byte("second")))
	cfg := CompactionConfig{RewindHorizonBatches: 64}
	_, err := db.CompactOnce(cfg)
	require.NoError(t, err)
	require.NotNil(t, db.compactPinCache)
	path := segmentManifestPath(db.AcctsDir, 120, result.FileId)
	m, err := ReadSegmentManifest(path)
	require.NoError(t, err)
	require.True(t, m.Records[0].PrevValid)
	conflict := m.Records[0]
	conflict.Pubkey[0] ^= 1
	m.Records = append(m.Records, conflict)
	require.NoError(t, WriteSegmentManifest(db.AcctsDir, m))
	_, err = db.CompactOnce(cfg)
	require.ErrorContains(t, err, "conflicting undo pointers")
	require.Nil(t, db.compactPinCache, "tentative streamed pins must never be published")
}

func BenchmarkManifestPinRecords(b *testing.B) {
	m := streamedManifestFixture(40_000)
	path := filepath.Join(b.TempDir(), "manifest")
	require.NoError(b, os.WriteFile(path, m.encode(), 0600))
	for _, stream := range []bool{false, true} {
		name := "full"
		if stream {
			name = "stream"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var sum uint64
				visit := func(r ManifestRecord) error {
					if r.PrevValid {
						sum += r.Prev.Offset
					}
					return nil
				}
				if stream {
					require.NoError(b, visitSegmentManifestRecords(path, visit))
				} else {
					full, err := ReadSegmentManifest(path)
					require.NoError(b, err)
					for _, r := range full.Records {
						_ = visit(r)
					}
				}
				if sum == 0 {
					b.Fatal("no records visited")
				}
			}
		})
	}
}
