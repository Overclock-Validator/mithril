package history

import (
	"encoding/binary"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// sigTruth is every indexed transaction by its full signature.
type sigTruth map[[64]byte][]TxLocation

func randomSigs(rng *rand.Rand, from, to uint64, truth sigTruth, shared [][64]byte) []SlotSignatures {
	var out []SlotSignatures
	for slot := from; slot <= to; slot++ {
		if rng.Intn(4) == 0 {
			continue // skipped slot
		}
		n := rng.Intn(40)
		ss := SlotSignatures{Slot: slot, Prefixes: make([]uint64, n)}
		for pos := range ss.Prefixes {
			var sig [64]byte
			if len(shared) > 0 && rng.Intn(8) == 0 {
				// Same first 8 bytes as another transaction, different signature.
				sig = shared[rng.Intn(len(shared))]
				binary.BigEndian.PutUint64(sig[8:], rng.Uint64())
			} else {
				rng.Read(sig[:])
			}
			ss.Prefixes[pos] = SignaturePrefix(sig[:])
			truth[sig] = append(truth[sig], TxLocation{Slot: slot, Position: uint32(pos)})
		}
		out = append(out, ss)
	}
	return out
}

// candidatesWant is every location whose signature shares sig's prefix.
func candidatesWant(truth sigTruth, sig [64]byte, from, to uint64) []TxLocation {
	var want []TxLocation
	for s, locs := range truth {
		if SignaturePrefix(s[:]) != SignaturePrefix(sig[:]) {
			continue
		}
		for _, l := range locs {
			if l.Slot >= from && l.Slot <= to {
				want = append(want, l)
			}
		}
	}
	sort.Slice(want, func(i, j int) bool {
		if want[i].Slot != want[j].Slot {
			return want[i].Slot < want[j].Slot
		}
		return want[i].Position < want[j].Position
	})
	return want
}

func checkSigs(t *testing.T, s *Store, truth sigTruth, from, to uint64) {
	t.Helper()
	n := 0
	for sig := range truth {
		got, _, _, _, err := s.TransactionsBySignature(sig[:])
		require.NoError(t, err)
		want := candidatesWant(truth, sig, from, to)
		if len(want) == 0 {
			require.Empty(t, got)
		} else {
			require.Equal(t, want, got)
		}
		if n++; n > 400 {
			break
		}
	}
	var missing [64]byte
	missing[0] = 0xff
	got, _, _, _, err := s.TransactionsBySignature(missing[:])
	require.NoError(t, err)
	require.Equal(t, candidatesWant(truth, missing, from, to), got)
}

func TestSignatureIndex(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{SegmentChunks: 3, KeepSegments: 100})
	require.NoError(t, err)
	rng := rand.New(rand.NewSource(3))
	truth := sigTruth{}
	var shared [][64]byte
	for i := 0; i < 20; i++ {
		var sig [64]byte
		rng.Read(sig[:])
		shared = append(shared, sig)
	}
	// 8 chunks of 10 slots: two closed segments of 3 chunks, an open one of 2.
	for c := uint64(0); c < 8; c++ {
		from, to := 100+10*c, 109+10*c
		require.NoError(t, s.AppendKeys(from, to, nil, &BankData{Signatures: randomSigs(rng, from, to, truth, shared)}))
	}
	from, through, ok := s.signatureCoverage()
	require.True(t, ok)
	require.Equal(t, uint64(100), from)
	require.Equal(t, uint64(179), through)
	checkSigs(t, s, truth, 100, 179)

	// Closed segments hold one merged file each; the open one, a file per chunk.
	for i, seg := range s.segments {
		if i < 2 {
			require.Len(t, seg.sigFiles, 1)
			require.Equal(t, sigMergedFileName(seg.from(), seg.to()), filepath.Base(seg.sigFiles[0].path))
			m, _ := filepath.Glob(filepath.Join(seg.dir, "sig-*"))
			require.Empty(t, m)
		} else {
			require.Len(t, seg.sigFiles, 2)
		}
	}

	// Reopened, the index reads the same.
	require.NoError(t, s.Close())
	s, err = Open(dir, Options{SegmentChunks: 3, KeepSegments: 100})
	require.NoError(t, err)
	checkSigs(t, s, truth, 100, 179)

	// Truncating inside a closed segment rewrites its merged file.
	require.NoError(t, s.TruncateFrom(115))
	_, through, ok = s.signatureCoverage()
	require.True(t, ok)
	require.Equal(t, uint64(109), through)
	checkSigs(t, s, truth, 100, 109)
	for sig, locs := range truth {
		var kept []TxLocation
		for _, l := range locs {
			if l.Slot <= 109 {
				kept = append(kept, l)
			}
		}
		if len(kept) == 0 {
			delete(truth, sig)
		} else {
			truth[sig] = kept
		}
	}
	// Recording continues, and the segment closes again with a new merge.
	for c := uint64(1); c < 5; c++ {
		from, to := 100+10*c, 109+10*c
		require.NoError(t, s.AppendKeys(from, to, nil, &BankData{Signatures: randomSigs(rng, from, to, truth, shared)}))
	}
	checkSigs(t, s, truth, 100, 149)
	require.NoError(t, s.Close())
	s, err = Open(dir, Options{SegmentChunks: 3, KeepSegments: 100})
	require.NoError(t, err)
	checkSigs(t, s, truth, 100, 149)
	require.Len(t, s.segments[0].sigFiles, 1)
	require.NoError(t, s.Close())
}

func TestSignatureCoverageGaps(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{SegmentChunks: 4, KeepSegments: 100})
	require.NoError(t, err)
	rng := rand.New(rand.NewSource(4))
	truth := sigTruth{}
	sigsFor := func(from, to uint64) *BankData {
		return &BankData{Signatures: randomSigs(rng, from, to, truth, nil)}
	}
	require.NoError(t, s.AppendKeys(100, 109, nil, sigsFor(100, 109)))
	require.NoError(t, s.AppendKeys(110, 119, nil, nil)) // not indexed
	require.NoError(t, s.AppendKeys(120, 129, nil, sigsFor(120, 129)))
	require.NoError(t, s.AppendKeys(130, 139, nil, sigsFor(130, 139)))
	from, through, ok := s.signatureCoverage()
	require.True(t, ok)
	require.Equal(t, []uint64{120, 139}, []uint64{from, through})
	// The segment is closed but not merged, since a chunk is missing.
	require.NoError(t, s.AppendKeys(140, 149, nil, nil))
	require.Len(t, s.segments[0].sigFiles, 3)
	_, _, ok = s.signatureCoverage()
	require.False(t, ok)
	// Older candidates are still found.
	checkSigs(t, s, truth, 100, 139)

	// A damaged chunk file is dropped at open; its chunk is no longer indexed.
	require.NoError(t, s.AppendKeys(150, 159, nil, sigsFor(150, 159)))
	require.NoError(t, s.AppendKeys(160, 169, nil, sigsFor(160, 169)))
	require.NoError(t, s.Close())
	p := filepath.Join(dir, segmentDirName(140), sigChunkFileName(160, 169))
	b, err := os.ReadFile(p)
	require.NoError(t, err)
	b[sigHeaderSize] ^= 1
	require.NoError(t, os.WriteFile(p, b, 0o644))
	// A block's damage is found when the block is read, not at open.
	s, err = Open(dir, Options{SegmentChunks: 4, KeepSegments: 100})
	require.NoError(t, err)
	var anyErr error
	for sig := range truth {
		if _, _, _, _, err := s.TransactionsBySignature(sig[:]); err != nil {
			anyErr = err
		}
	}
	require.ErrorIs(t, anyErr, errSigFile)
	require.NoError(t, s.Close())
	// A damaged trailer is found at open.
	b[sigHeaderSize] ^= 1
	b[len(b)-5] ^= 1
	require.NoError(t, os.WriteFile(p, b, 0o644))
	s, err = Open(dir, Options{SegmentChunks: 4, KeepSegments: 100})
	require.NoError(t, err)
	_, statErr := os.Stat(p)
	require.True(t, os.IsNotExist(statErr))
	_, _, ok = s.signatureCoverage()
	require.False(t, ok)
	require.NoError(t, s.Close())
}

func TestSignatureFileBlocks(t *testing.T) {
	// Many entries sharing prefixes across block boundaries.
	dir := t.TempDir()
	var slots []SlotSignatures
	truth := map[uint64][]TxLocation{}
	rng := rand.New(rand.NewSource(5))
	for slot := uint64(1000); slot < 1010; slot++ {
		ss := SlotSignatures{Slot: slot, Prefixes: make([]uint64, 700)}
		for pos := range ss.Prefixes {
			p := uint64(rng.Intn(50)) << 40 // 50 prefixes, ~140 entries each
			ss.Prefixes[pos] = p
			truth[p] = append(truth[p], TxLocation{Slot: slot, Position: uint32(pos)})
		}
		slots = append(slots, ss)
	}
	f, err := writeChunkSigs(filepath.Join(dir, "x.hsg"), 1000, 1009, slots)
	require.NoError(t, err)
	require.Greater(t, len(f.fences), 10)
	for p, want := range truth {
		got, err := f.lookup(p, nil)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	got, err := f.lookup(1<<62, nil)
	require.NoError(t, err)
	require.Empty(t, got)
	// Out-of-order slots are rejected.
	_, err = writeChunkSigs(filepath.Join(dir, "y.hsg"), 1000, 1009, []SlotSignatures{{Slot: 1003}, {Slot: 1002}})
	require.Error(t, err)
	_, err = writeChunkSigs(filepath.Join(dir, "y.hsg"), 1000, 1009, []SlotSignatures{{Slot: 1010}})
	require.Error(t, err)
}

// BenchmarkSignatureMerge merges a full segment at about 1,000 transactions a
// slot: 64 chunks of 128 slots.
func BenchmarkSignatureMerge(b *testing.B) {
	dir := b.TempDir()
	rng := rand.New(rand.NewSource(6))
	var files []*sigFile
	for c := uint64(0); c < 64; c++ {
		from, to := 128*c, 128*c+127
		var slots []SlotSignatures
		for slot := from; slot <= to; slot++ {
			ss := SlotSignatures{Slot: slot, Prefixes: make([]uint64, 1000)}
			for i := range ss.Prefixes {
				ss.Prefixes[i] = rng.Uint64()
			}
			slots = append(slots, ss)
		}
		f, err := writeChunkSigs(filepath.Join(dir, sigChunkFileName(from, to)), from, to, slots)
		require.NoError(b, err)
		files = append(files, f)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m, err := mergeSigFiles(filepath.Join(dir, sigMergedFileName(0, 8191)), 0, 8191, files)
		require.NoError(b, err)
		b.ReportMetric(float64(m.n), "entries")
		_ = os.Remove(m.path)
	}
}

// BenchmarkSignatureChunkWrite writes one 128-slot chunk's file.
func BenchmarkSignatureChunkWrite(b *testing.B) {
	dir := b.TempDir()
	rng := rand.New(rand.NewSource(7))
	var slots []SlotSignatures
	for slot := uint64(0); slot < 128; slot++ {
		ss := SlotSignatures{Slot: slot, Prefixes: make([]uint64, 1000)}
		for i := range ss.Prefixes {
			ss.Prefixes[i] = rng.Uint64()
		}
		slots = append(slots, ss)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := writeChunkSigs(filepath.Join(dir, "c.hsg"), 0, 127, slots)
		require.NoError(b, err)
	}
}
