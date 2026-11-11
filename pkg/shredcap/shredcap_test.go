package shredcap

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

// slotPackets generates a slot's Merkle data and coding shreds.
func slotPackets(t *testing.T, leader solana.PrivateKey, slot, parent uint64) [][]byte {
	t.Helper()
	gen := turbine.ShredGenerator{Slot: slot, ParentSlot: parent, Version: 1}
	packets, _, _, _, err := gen.MakeShredsFromData(leader, bytes.Repeat([]byte{byte(slot)}, 4000), true, solana.Hash{}, 0, 0)
	require.NoError(t, err)
	return packets
}

func readAll(t *testing.T, data []byte) []Record {
	t.Helper()
	rd, err := NewReader(bytes.NewReader(data))
	require.NoError(t, err)
	defer rd.Close()
	var out []Record
	for {
		rec, err := rd.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		require.NoError(t, err)
		rec.Shred = append([]byte(nil), rec.Shred...)
		out = append(out, rec)
	}
}

func TestReaderReadsWriterOutputPlainAndZstd(t *testing.T) {
	var plain bytes.Buffer
	w, err := NewWriter(&plain, "test")
	require.NoError(t, err)
	require.NoError(t, w.WriteShred([]byte("shred-one")))
	require.NoError(t, w.WriteBankHash(7, [32]byte{1, 2, 3}, 12))
	require.NoError(t, w.WriteRootSlot(7))
	require.NoError(t, w.Flush())

	var compressed bytes.Buffer
	enc, err := zstd.NewWriter(&compressed)
	require.NoError(t, err)
	_, err = enc.Write(plain.Bytes())
	require.NoError(t, err)
	require.NoError(t, enc.Close())

	for name, data := range map[string][]byte{"plain": plain.Bytes(), "zstd": compressed.Bytes()} {
		t.Run(name, func(t *testing.T) {
			recs := readAll(t, data)
			require.Len(t, recs, 3)
			require.Equal(t, KindShred, recs[0].Kind)
			require.Equal(t, []byte("shred-one"), recs[0].Shred)
			require.Equal(t, Record{Kind: KindBankHash, Slot: 7, BankHash: [32]byte{1, 2, 3}, DataShreds: 12}, recs[1])
			require.Equal(t, Record{Kind: KindRootSlot, Slot: 7}, recs[2])
		})
	}
}

// rawWriter builds pcapng blocks in either byte order, for cases the Writer
// never produces.
type rawWriter struct {
	buf   bytes.Buffer
	order interface {
		binary.ByteOrder
		binary.AppendByteOrder
	}
}

func (r *rawWriter) block(blockType uint32, body []byte) {
	for len(body)%4 != 0 {
		body = append(body, 0)
	}
	total := uint32(12 + len(body))
	r.buf.Write(r.order.AppendUint32(r.order.AppendUint32(nil, blockType), total))
	r.buf.Write(body)
	r.buf.Write(r.order.AppendUint32(nil, total))
}

func (r *rawWriter) section() {
	body := r.order.AppendUint32(nil, byteOrderMagic)
	body = r.order.AppendUint16(body, 1)
	body = r.order.AppendUint16(body, 0)
	r.block(blockSectionHeader, r.order.AppendUint64(body, ^uint64(0)))
}

func (r *rawWriter) iface(link uint16, name string) {
	body := r.order.AppendUint16(nil, link)
	body = r.order.AppendUint16(body, 0)
	body = r.order.AppendUint32(body, 0)
	if name != "" {
		body = r.order.AppendUint16(body, optIfName)
		body = r.order.AppendUint16(body, uint16(len(name)))
		body = append(body, name...)
		for len(body)%4 != 0 {
			body = append(body, 0)
		}
	}
	r.block(blockInterface, body)
}

func (r *rawWriter) packet(iface uint32, data []byte) {
	body := r.order.AppendUint32(nil, iface)
	body = r.order.AppendUint64(body, 0)
	body = r.order.AppendUint32(body, uint32(len(data)))
	body = r.order.AppendUint32(body, uint32(len(data)))
	r.block(blockEnhanced, append(body, data...))
}

func ipv4UDP(proto byte, payload []byte) []byte {
	pkt := make([]byte, 28, 28+len(payload))
	pkt[0], pkt[9] = 0x45, proto
	return append(pkt, payload...)
}

func TestReaderLinkTypesSectionsAndSkips(t *testing.T) {
	for _, order := range []interface {
		binary.ByteOrder
		binary.AppendByteOrder
	}{binary.LittleEndian, binary.BigEndian} {
		t.Run(fmt.Sprint(order), func(t *testing.T) {
			r := &rawWriter{order: order}
			r.section()
			r.iface(linkEthernet, "eth0")
			r.iface(9999, "unknown") // ignored link type
			r.iface(linkUser0, "")   // metadata by link type alone
			eth := append(make([]byte, 12), 0x08, 0x00)
			r.packet(0, append(eth, ipv4UDP(17, []byte("eth-shred"))...))
			r.packet(0, append(eth, ipv4UDP(6, []byte("tcp"))...)) // not UDP
			r.packet(1, ipv4UDP(17, []byte("ignored-link")))
			r.block(blockSimplePacket, []byte{0, 0, 0, 0}) // obsolete, skipped
			meta := binary.LittleEndian.AppendUint32(nil, recordRootSlotV0)
			r.packet(2, binary.LittleEndian.AppendUint64(meta, 42))
			r.packet(2, binary.LittleEndian.AppendUint32(nil, 99)) // unknown record type

			// A second section restarts interface numbering.
			r.section()
			r.iface(linkIPv4, "lo")
			r.packet(0, ipv4UDP(17, []byte("second-section")))

			recs := readAll(t, r.buf.Bytes())
			require.Len(t, recs, 3)
			require.Equal(t, []byte("eth-shred"), recs[0].Shred)
			require.Equal(t, Record{Kind: KindRootSlot, Slot: 42}, recs[1])
			require.Equal(t, []byte("second-section"), recs[2].Shred)
		})
	}
}

func TestReaderRejectsCorruptInput(t *testing.T) {
	_, err := NewReader(bytes.NewReader([]byte("not pcapng at all")))
	require.ErrorContains(t, err, "not a pcapng file")

	var good bytes.Buffer
	w, err := NewWriter(&good, "test")
	require.NoError(t, err)
	require.NoError(t, w.WriteShred([]byte("x")))
	require.NoError(t, w.Flush())
	data := good.Bytes()
	data[len(data)-1] ^= 0xFF // trailing length of the last block
	rd, err := NewReader(bytes.NewReader(data))
	require.NoError(t, err)
	_, err = rd.Next()
	require.ErrorContains(t, err, "trailing length")

	r := &rawWriter{order: binary.LittleEndian}
	r.section()
	r.packet(3, []byte("x"))
	rd, err = NewReader(bytes.NewReader(r.buf.Bytes()))
	require.NoError(t, err)
	_, err = rd.Next()
	require.ErrorContains(t, err, "undeclared interface")
}

// testCapture writes slots 100..102 (chain), 103 (a fork off 100) and 104
// (missing its last data shred), with duplicates and a repair nonce.
func testCapture(t *testing.T, roots bool) ([]byte, map[uint64][][]byte) {
	t.Helper()
	leader, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)
	parents := map[uint64]uint64{100: 99, 101: 100, 102: 101, 103: 100, 104: 102}
	packets := make(map[uint64][][]byte)
	var buf bytes.Buffer
	w, err := NewWriter(&buf, "test")
	require.NoError(t, err)
	for slot := uint64(100); slot <= 104; slot++ {
		packets[slot] = slotPackets(t, leader, slot, parents[slot])
		for i, p := range packets[slot] {
			shred, err := turbine.ParseShred(p)
			require.NoError(t, err)
			if slot == 104 && shred.Type == turbine.ShredTypeData && shred.LastInSlot() {
				continue
			}
			if i == 0 {
				// A repair response: the shred followed by a 4-byte nonce.
				require.NoError(t, w.WriteShred(append(append([]byte(nil), p...), 1, 2, 3, 4)))
			}
			require.NoError(t, w.WriteShred(p))
		}
		if slot <= 103 {
			require.NoError(t, w.WriteBankHash(slot, [32]byte{byte(slot)}, 0))
		}
		if roots && slot <= 102 {
			require.NoError(t, w.WriteRootSlot(slot))
		}
	}
	require.NoError(t, w.WriteShred([]byte("not a shred")))
	require.NoError(t, w.Flush())
	return buf.Bytes(), packets
}

func requireCorpus(t *testing.T, dir string, packets map[uint64][][]byte, slots []uint64) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, "shreds"))
	require.NoError(t, err)
	require.Len(t, entries, len(slots), "only chain slots are kept")
	for _, slot := range slots {
		got, err := turbine.ReadSpoolFile(filepath.Join(dir, "shreds", turbine.SpoolFileName(slot)))
		require.NoError(t, err)
		require.ElementsMatch(t, packets[slot], got, "slot %d holds each canonical shred once", slot)
	}
	idx, err := os.ReadFile(filepath.Join(dir, turbine.SpoolJournalName))
	require.NoError(t, err)
	require.Len(t, idx, 16*len(slots))
}

func TestImportCorpusUsesRootedChain(t *testing.T) {
	capture, packets := testCapture(t, true)
	dir := filepath.Join(t.TempDir(), "corpus")
	man, stats, err := ImportCorpus(bytes.NewReader(capture), dir, ImportOptions{Source: "test.pcapng"})
	require.NoError(t, err)

	require.Equal(t, []uint64{100, 101, 102}, man.Slots)
	require.Equal(t, uint64(99), man.Checkpoint)
	require.Equal(t, uint64(102), man.Head)
	require.Equal(t, solana.Hash([32]byte{101}).String(), man.BankHashes["101"])
	require.Len(t, man.BankHashes, 3)
	require.Equal(t, []uint64{103}, stats.OffChain)
	require.Equal(t, []uint64{104}, stats.Incomplete)
	require.Equal(t, 5, stats.Duplicates, "the nonce-carrying copy of each slot's first shred")
	require.Equal(t, 1, stats.Unparsed)
	require.Equal(t, 3, stats.Roots)
	require.Equal(t, []uint64{100, 101, 102}, stats.CountMismatch, "the test records a data shred count of 0")
	require.Empty(t, stats.ParentBreaks)
	requireCorpus(t, dir, packets, man.Slots)

	reread, err := ReadManifest(dir)
	require.NoError(t, err)
	require.Equal(t, man.Slots, reread.Slots)
	require.Nil(t, reread.TransactionCounts)

	_, _, err = ImportCorpus(bytes.NewReader(capture), dir, ImportOptions{})
	require.ErrorContains(t, err, "already exists")
}

func TestImportCorpusWithoutRootsFollowsParents(t *testing.T) {
	capture, packets := testCapture(t, false)
	dir := filepath.Join(t.TempDir(), "corpus")
	man, stats, err := ImportCorpus(bytes.NewReader(capture), dir, ImportOptions{})
	require.NoError(t, err)
	// 103 is the highest complete slot with a bank hash; its parent is 100.
	require.Equal(t, []uint64{100, 103}, man.Slots)
	require.Equal(t, []uint64{101, 102}, stats.OffChain)
	requireCorpus(t, dir, packets, man.Slots)
}

func TestImportCorpusSlotRange(t *testing.T) {
	capture, _ := testCapture(t, true)
	man, stats, err := ImportCorpus(bytes.NewReader(capture), filepath.Join(t.TempDir(), "c"), ImportOptions{FromSlot: 101, ToSlot: 102})
	require.NoError(t, err)
	require.Equal(t, []uint64{101, 102}, man.Slots)
	require.Equal(t, uint64(100), man.Checkpoint)
	require.Positive(t, stats.OutOfRange)
}

func TestExportThenImportRoundTrips(t *testing.T) {
	capture, packets := testCapture(t, true)
	first := filepath.Join(t.TempDir(), "first")
	man, _, err := ImportCorpus(bytes.NewReader(capture), first, ImportOptions{})
	require.NoError(t, err)

	var exported bytes.Buffer
	n, err := ExportCorpus(first, &exported, "test", ExportOptions{})
	require.NoError(t, err)
	require.Equal(t, 3, n)

	second := filepath.Join(t.TempDir(), "second")
	again, stats, err := ImportCorpus(bytes.NewReader(exported.Bytes()), second, ImportOptions{})
	require.NoError(t, err)
	require.Equal(t, man.Slots, again.Slots)
	require.Equal(t, man.BankHashes, again.BankHashes)
	require.Equal(t, man.Checkpoint, again.Checkpoint)
	require.Empty(t, stats.CountMismatch, "export records each slot's real data shred count")
	require.Zero(t, stats.Duplicates)
	require.Equal(t, []uint64{99}, stats.Incomplete, "the placeholder for the snapshot slot is not replayed")
	requireCorpus(t, second, dataOnly(t, packets), man.Slots)
}

func dataOnly(t *testing.T, packets map[uint64][][]byte) map[uint64][][]byte {
	t.Helper()
	out := make(map[uint64][][]byte)
	for slot, list := range packets {
		for _, p := range list {
			shred, err := turbine.ParseShred(p)
			require.NoError(t, err)
			if shred.Type == turbine.ShredTypeData {
				out[slot] = append(out[slot], p)
			}
		}
	}
	return out
}

// TestExportMeetsBacktestOrder checks the layout firedancer-dev backtest
// needs: a first shred at or below the snapshot slot, then per slot only data
// shreds in index order, its bank hash with that many data shreds, and a root.
func TestExportMeetsBacktestOrder(t *testing.T) {
	capture, _ := testCapture(t, true)
	corpus := filepath.Join(t.TempDir(), "corpus")
	_, _, err := ImportCorpus(bytes.NewReader(capture), corpus, ImportOptions{})
	require.NoError(t, err)

	for _, tc := range []struct {
		name     string
		snapshot uint64
		lead     bool
	}{
		{"checkpoint snapshot gets a placeholder", 0, true},
		{"older snapshot gets a placeholder at its slot", 50, true},
		{"snapshot inside the corpus needs none", 100, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			_, err := ExportCorpus(corpus, &buf, "test", ExportOptions{SnapshotSlot: tc.snapshot})
			require.NoError(t, err)
			recs := readAll(t, buf.Bytes())
			if tc.lead {
				lead, err := turbine.ParseShred(recs[0].Shred)
				require.NoError(t, err)
				want := tc.snapshot
				if want == 0 {
					want = 99
				}
				require.Equal(t, want, lead.Slot)
				require.Equal(t, turbine.ShredTypeData, lead.Type)
				recs = recs[1:]
			}
			slot, next, count := uint64(100), uint32(0), uint64(0)
			for _, rec := range recs {
				switch rec.Kind {
				case KindShred:
					shred, err := turbine.ParseShred(rec.Shred)
					require.NoError(t, err)
					require.Equal(t, turbine.ShredTypeData, shred.Type)
					require.Equal(t, slot, shred.Slot)
					require.Equal(t, next, shred.Index)
					next++
					count++
				case KindBankHash:
					require.Equal(t, slot, rec.Slot)
					require.Equal(t, count, rec.DataShreds)
				case KindRootSlot:
					require.Equal(t, slot, rec.Slot)
					slot, next, count = slot+1, 0, 0
				}
			}
			require.Equal(t, uint64(103), slot, "slots 100..102 were each closed by a root record")
		})
	}
}

func TestPlaceholderShredAtGenesis(t *testing.T) {
	packet, err := placeholderShred(0)
	require.NoError(t, err)
	shred, err := turbine.ParseShred(packet)
	require.NoError(t, err)
	require.Equal(t, uint64(0), shred.Slot)
	require.Equal(t, uint64(0), shred.ParentSlot(), "no underflow below slot 0")
	require.Equal(t, turbine.ShredTypeData, shred.Type)
}
