package accountsdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Batch fold segments are the wear-friendly durable commit unit: one sequential
// data file holding the union-deduped newest account versions across a K-slot
// batch, plus a manifest. The data file reuses the appendvec record encoding and
// the "<slot>.<fileId>" filename scheme (slot = ThroughSlot), so the existing
// read path serves bootstrap appendvecs and fold segments identically.
//
// The manifest is four things at once:
//   - the commit record: a durable manifest means the batch is decided; recovery
//     completes the index flip from it (the redo role)
//   - the index redo log: Records carry every index entry the batch installs,
//     which is what makes running the Pebble index without a WAL safe
//   - the undo pointer log: Prev fields name each key's index entry before this
//     batch overwrote it, enabling rewind to a batch boundary within the horizon
//   - the carrier of the batch's bankhashes and end-of-batch resume context, so
//     the durable watermark survives a hard crash without the state file (which
//     is only written on graceful shutdown)
const (
	segManifestMagic   = uint32(0x4d534547) // "MSEG"
	segManifestVersion = uint32(1)

	segManifestSuffix    = ".manifest"
	segManifestTmpSuffix = ".manifest.tmp"
	// Parked by rewind: ignored by recovery replay, readable by rewind-resume.
	segManifestRewoundSuffix = ".manifest.rewound"

	// ManifestKindFold marks a batch-fold commit (BatchSeq is meaningful and
	// contiguous). ManifestKindCompact marks a compaction output (BatchSeq 0);
	// its only job is proving the data file is not an orphan.
	ManifestKindFold    = uint8(1)
	ManifestKindCompact = uint8(2)
)

// ErrTornManifest reports a manifest that fails CRC or structural validation.
var ErrTornManifest = errors.New("accountsdb: torn or malformed segment manifest")

// SlotBankhash records one slot's bankhash inside a fold batch.
type SlotBankhash struct {
	Slot     uint64
	Bankhash [32]byte
}

// ManifestRecord describes one deduped account version in the segment and the
// index entry it replaced (the undo pointer).
type ManifestRecord struct {
	Pubkey    [32]byte
	Offset    uint64            // record offset within this segment's data file
	OwnerSlot uint64            // slot that produced this version (observability)
	PrevValid bool              // false => the key was absent from the index before this batch
	Prev      AccountIndexEntry // index entry before this batch overwrote it
}

// SegmentManifest is the durable commit record for one fold or compaction segment.
type SegmentManifest struct {
	Version     uint32
	Kind        uint8
	BatchSeq    uint64 // fold: contiguous and monotonic; compact: 0
	FromSlot    uint64 // exclusive lower bound of the batch
	ThroughSlot uint64 // inclusive; equals the data file's name slot
	FileId      uint64
	DataLen     uint64 // exact data file length
	DataCRC     uint32 // crc32 (IEEE) of the data file bytes
	Bankhashes  []SlotBankhash
	Records     []ManifestRecord
	ResumeCtx   []byte // opaque serialized state.ResumeContext at ThroughSlot
}

// ManifestHeader is the fixed-size prefix of a manifest, cheap to scan in bulk.
// CRC validation requires a full ReadSegmentManifest.
type ManifestHeader struct {
	Kind        uint8
	BatchSeq    uint64
	FromSlot    uint64
	ThroughSlot uint64
	FileId      uint64
	DataLen     uint64
	DataCRC     uint32
	Path        string
}

// SegmentDataName returns the data-file basename for a segment.
func SegmentDataName(throughSlot, fileId uint64) string {
	return fmt.Sprintf("%d.%d", throughSlot, fileId)
}

func segmentManifestPath(acctsDir string, throughSlot, fileId uint64) string {
	return filepath.Join(acctsDir, SegmentDataName(throughSlot, fileId)+segManifestSuffix)
}

const manifestRecordSize = 32 + 8 + 8 + 1 + 24

func (m *SegmentManifest) encode() []byte {
	var body bytes.Buffer
	body.Grow(64 + len(m.Bankhashes)*40 + len(m.ResumeCtx) + len(m.Records)*manifestRecordSize)
	var u64 [8]byte
	var u32 [4]byte
	put32 := func(v uint32) { binary.LittleEndian.PutUint32(u32[:], v); body.Write(u32[:]) }
	put64 := func(v uint64) { binary.LittleEndian.PutUint64(u64[:], v); body.Write(u64[:]) }

	put32(segManifestMagic)
	put32(segManifestVersion)
	body.WriteByte(m.Kind)
	put64(m.BatchSeq)
	put64(m.FromSlot)
	put64(m.ThroughSlot)
	put64(m.FileId)
	put64(m.DataLen)
	put32(m.DataCRC)

	put32(uint32(len(m.Bankhashes)))
	for _, bh := range m.Bankhashes {
		put64(bh.Slot)
		body.Write(bh.Bankhash[:])
	}

	put32(uint32(len(m.ResumeCtx)))
	body.Write(m.ResumeCtx)

	put64(uint64(len(m.Records)))
	var prevBuf [24]byte
	for i := range m.Records {
		r := &m.Records[i]
		body.Write(r.Pubkey[:])
		put64(r.Offset)
		put64(r.OwnerSlot)
		if r.PrevValid {
			body.WriteByte(1)
		} else {
			body.WriteByte(0)
		}
		r.Prev.Marshal(&prevBuf)
		body.Write(prevBuf[:])
	}

	binary.LittleEndian.PutUint32(u32[:], crc32.ChecksumIEEE(body.Bytes()))
	body.Write(u32[:])
	return body.Bytes()
}

// WriteSegmentManifest durably stages a manifest: tmp write + fsync + rename +
// dir fsync. Once the rename is durable, the batch commit is decided — recovery
// completes the index flip from the manifest.
func WriteSegmentManifest(acctsDir string, m *SegmentManifest) error {
	final := segmentManifestPath(acctsDir, m.ThroughSlot, m.FileId)
	tmp := final + ".tmp"
	data := m.encode()
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("accountsdb: create manifest tmp: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("accountsdb: write manifest: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("accountsdb: fsync manifest: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("accountsdb: close manifest: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("accountsdb: rename manifest: %w", err)
	}
	return fsyncDir(acctsDir)
}

type manifestDecoder struct {
	data []byte
	off  int
}

func (d *manifestDecoder) remain() int { return len(d.data) - d.off }
func (d *manifestDecoder) u8() (byte, error) {
	if d.remain() < 1 {
		return 0, ErrTornManifest
	}
	v := d.data[d.off]
	d.off++
	return v, nil
}
func (d *manifestDecoder) u32() (uint32, error) {
	if d.remain() < 4 {
		return 0, ErrTornManifest
	}
	v := binary.LittleEndian.Uint32(d.data[d.off:])
	d.off += 4
	return v, nil
}
func (d *manifestDecoder) u64() (uint64, error) {
	if d.remain() < 8 {
		return 0, ErrTornManifest
	}
	v := binary.LittleEndian.Uint64(d.data[d.off:])
	d.off += 8
	return v, nil
}
func (d *manifestDecoder) bytes(n int) ([]byte, error) {
	if n < 0 || d.remain() < n {
		return nil, ErrTornManifest
	}
	v := d.data[d.off : d.off+n]
	d.off += n
	return v, nil
}

func decodeManifestPrefix(d *manifestDecoder, m *SegmentManifest) error {
	magic, err := d.u32()
	if err != nil || magic != segManifestMagic {
		return ErrTornManifest
	}
	if m.Version, err = d.u32(); err != nil || m.Version != segManifestVersion {
		return ErrTornManifest
	}
	if m.Kind, err = d.u8(); err != nil {
		return err
	}
	if m.BatchSeq, err = d.u64(); err != nil {
		return err
	}
	if m.FromSlot, err = d.u64(); err != nil {
		return err
	}
	if m.ThroughSlot, err = d.u64(); err != nil {
		return err
	}
	if m.FileId, err = d.u64(); err != nil {
		return err
	}
	if m.DataLen, err = d.u64(); err != nil {
		return err
	}
	if m.DataCRC, err = d.u32(); err != nil {
		return err
	}
	return nil
}

// ReadSegmentManifest loads and fully validates a manifest (trailing CRC included).
func ReadSegmentManifest(path string) (*SegmentManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) < 4 {
		return nil, ErrTornManifest
	}
	body, tail := data[:len(data)-4], data[len(data)-4:]
	if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(tail) {
		return nil, ErrTornManifest
	}

	d := &manifestDecoder{data: body}
	m := &SegmentManifest{}
	if err := decodeManifestPrefix(d, m); err != nil {
		return nil, err
	}

	nBank, err := d.u32()
	if err != nil {
		return nil, err
	}
	m.Bankhashes = make([]SlotBankhash, 0, nBank)
	for i := uint32(0); i < nBank; i++ {
		var bh SlotBankhash
		if bh.Slot, err = d.u64(); err != nil {
			return nil, err
		}
		raw, err := d.bytes(32)
		if err != nil {
			return nil, err
		}
		copy(bh.Bankhash[:], raw)
		m.Bankhashes = append(m.Bankhashes, bh)
	}

	ctxLen, err := d.u32()
	if err != nil {
		return nil, err
	}
	ctx, err := d.bytes(int(ctxLen))
	if err != nil {
		return nil, err
	}
	m.ResumeCtx = append([]byte(nil), ctx...)

	nRec, err := d.u64()
	if err != nil {
		return nil, err
	}
	if nRec > uint64(d.remain())/manifestRecordSize+1 {
		return nil, ErrTornManifest
	}
	m.Records = make([]ManifestRecord, 0, nRec)
	for i := uint64(0); i < nRec; i++ {
		var r ManifestRecord
		pk, err := d.bytes(32)
		if err != nil {
			return nil, err
		}
		copy(r.Pubkey[:], pk)
		if r.Offset, err = d.u64(); err != nil {
			return nil, err
		}
		if r.OwnerSlot, err = d.u64(); err != nil {
			return nil, err
		}
		pv, err := d.u8()
		if err != nil {
			return nil, err
		}
		r.PrevValid = pv == 1
		prevRaw, err := d.bytes(24)
		if err != nil {
			return nil, err
		}
		r.Prev.Unmarshal((*[24]byte)(prevRaw))
		m.Records = append(m.Records, r)
	}
	if d.remain() != 0 {
		return nil, ErrTornManifest
	}
	return m, nil
}

// readManifestHeader parses only the fixed prefix — no CRC validation.
func readManifestHeader(path string) (ManifestHeader, error) {
	f, err := os.Open(path)
	if err != nil {
		return ManifestHeader{}, err
	}
	defer f.Close()
	var buf [64]byte
	n, err := f.Read(buf[:])
	if n < 61 { // magic+version+kind+5*u64+u32 = 4+4+1+40+12
		if err != nil {
			return ManifestHeader{}, ErrTornManifest
		}
		return ManifestHeader{}, ErrTornManifest
	}
	d := &manifestDecoder{data: buf[:n]}
	var m SegmentManifest
	if err := decodeManifestPrefix(d, &m); err != nil {
		return ManifestHeader{}, err
	}
	return ManifestHeader{
		Kind:        m.Kind,
		BatchSeq:    m.BatchSeq,
		FromSlot:    m.FromSlot,
		ThroughSlot: m.ThroughSlot,
		FileId:      m.FileId,
		DataLen:     m.DataLen,
		DataCRC:     m.DataCRC,
		Path:        path,
	}, nil
}

// ListFoldManifests scans acctsDir for fold-kind manifests (".manifest" only —
// ".rewound" and ".tmp" are ignored) and returns their headers ascending by
// BatchSeq. Headers that fail even prefix parsing are returned with BatchSeq 0
// and Kind 0 so the caller can quarantine by path.
func ListFoldManifests(acctsDir string) ([]ManifestHeader, error) {
	entries, err := os.ReadDir(acctsDir)
	if err != nil {
		return nil, err
	}
	var out []ManifestHeader
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, segManifestSuffix) || strings.HasSuffix(name, segManifestTmpSuffix) {
			continue
		}
		hdr, err := readManifestHeader(filepath.Join(acctsDir, name))
		if err != nil {
			out = append(out, ManifestHeader{Path: filepath.Join(acctsDir, name)})
			continue
		}
		if hdr.Kind != ManifestKindFold {
			continue
		}
		out = append(out, hdr)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BatchSeq < out[j].BatchSeq })
	return out, nil
}

// listAllManifestFileIds returns the fileId of every manifest-backed data file
// (fold and compact, including rewound manifests) for orphan classification.
func listAllManifestFileIds(acctsDir string) (map[uint64]struct{}, error) {
	entries, err := os.ReadDir(acctsDir)
	if err != nil {
		return nil, err
	}
	ids := make(map[uint64]struct{})
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			continue
		}
		if !strings.HasSuffix(name, segManifestSuffix) && !strings.HasSuffix(name, segManifestRewoundSuffix) {
			continue
		}
		if strings.HasSuffix(name, segManifestTmpSuffix) {
			continue
		}
		hdr, err := readManifestHeader(filepath.Join(acctsDir, name))
		if err != nil {
			continue
		}
		ids[hdr.FileId] = struct{}{}
	}
	return ids, nil
}

// parseDataFileName parses "<slot>.<fileId>" appendvec/segment basenames.
func parseDataFileName(name string) (slot uint64, fileId uint64, ok bool) {
	dot := strings.IndexByte(name, '.')
	if dot <= 0 || dot == len(name)-1 {
		return 0, 0, false
	}
	var err error
	if slot, err = strconv.ParseUint(name[:dot], 10, 64); err != nil {
		return 0, 0, false
	}
	if fileId, err = strconv.ParseUint(name[dot+1:], 10, 64); err != nil {
		return 0, 0, false
	}
	return slot, fileId, true
}

// crcOfFile streams a file through a crc32 (IEEE) digest and returns the
// checksum with the byte length, without loading the whole file into memory
// (fold segments can be large on a busy cluster).
func crcOfFile(path string) (uint32, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	h := crc32.NewIEEE()
	n, err := io.Copy(h, f)
	if err != nil {
		return 0, 0, err
	}
	return h.Sum32(), n, nil
}
