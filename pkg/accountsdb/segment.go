package accountsdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
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
//     allowing recovery to complete a journal publication after a crash
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

type segmentManifestRenamedError struct {
	err error
}

func (publication *segmentManifestRenamedError) Error() string {
	return fmt.Sprintf("accountsdb: manifest rename is visible but directory sync failed: %v", publication.err)
}

func (publication *segmentManifestRenamedError) Unwrap() error {
	return publication.err
}

func segmentManifestWasRenamed(err error) bool {
	var publication *segmentManifestRenamedError
	return errors.As(err, &publication)
}

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
	Tombstone bool              // true => this batch deletes the key from the exact account index
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

const (
	manifestRecordSize          = 32 + 8 + 8 + 1 + 24
	manifestRecordFlagsOffset   = 32 + 8 + 8
	manifestRecordPrevValidFlag = uint8(1 << 0)
	manifestRecordTombstoneFlag = uint8(1 << 1)
	manifestRecordKnownFlags    = manifestRecordPrevValidFlag | manifestRecordTombstoneFlag
)

const manifestFixedPrefixSize = 4 + 4 + 1 + 5*8 + 4

func (m *SegmentManifest) encode() []byte {
	var encoded bytes.Buffer
	if err := writeSegmentManifestEncoding(&encoded, m); err != nil {
		// encode is retained for compact test fixtures. Production publication
		// uses the checked streaming encoder below and never reaches this panic.
		panic(err)
	}
	return encoded.Bytes()
}

func writeSegmentManifestEncoding(writer io.Writer, m *SegmentManifest) error {
	if writer == nil {
		return errors.New("accountsdb: nil segment manifest writer")
	}
	if m == nil {
		return errors.New("accountsdb: nil segment manifest")
	}
	if uint64(len(m.Bankhashes)) > math.MaxUint32 {
		return errors.New("accountsdb: too many segment manifest bankhashes")
	}
	if uint64(len(m.ResumeCtx)) > math.MaxUint32 {
		return errors.New("accountsdb: segment manifest resume context is too large")
	}

	checksum := crc32.NewIEEE()
	body := io.MultiWriter(writer, checksum)
	var prefix [manifestFixedPrefixSize]byte
	binary.LittleEndian.PutUint32(prefix[0:4], segManifestMagic)
	binary.LittleEndian.PutUint32(prefix[4:8], segManifestVersion)
	prefix[8] = m.Kind
	binary.LittleEndian.PutUint64(prefix[9:17], m.BatchSeq)
	binary.LittleEndian.PutUint64(prefix[17:25], m.FromSlot)
	binary.LittleEndian.PutUint64(prefix[25:33], m.ThroughSlot)
	binary.LittleEndian.PutUint64(prefix[33:41], m.FileId)
	binary.LittleEndian.PutUint64(prefix[41:49], m.DataLen)
	binary.LittleEndian.PutUint32(prefix[49:53], m.DataCRC)
	if err := writeAll(body, prefix[:]); err != nil {
		return err
	}

	var count [8]byte
	binary.LittleEndian.PutUint32(count[:4], uint32(len(m.Bankhashes)))
	if err := writeAll(body, count[:4]); err != nil {
		return err
	}
	var bankRaw [40]byte
	for i := range m.Bankhashes {
		binary.LittleEndian.PutUint64(bankRaw[:8], m.Bankhashes[i].Slot)
		copy(bankRaw[8:], m.Bankhashes[i].Bankhash[:])
		if err := writeAll(body, bankRaw[:]); err != nil {
			return err
		}
	}

	binary.LittleEndian.PutUint32(count[:4], uint32(len(m.ResumeCtx)))
	if err := writeAll(body, count[:4]); err != nil {
		return err
	}
	if err := writeAll(body, m.ResumeCtx); err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(count[:], uint64(len(m.Records)))
	if err := writeAll(body, count[:]); err != nil {
		return err
	}

	var recordRaw [manifestRecordSize]byte
	for i := range m.Records {
		clear(recordRaw[:])
		record := &m.Records[i]
		copy(recordRaw[:32], record.Pubkey[:])
		binary.LittleEndian.PutUint64(recordRaw[32:40], record.Offset)
		binary.LittleEndian.PutUint64(recordRaw[40:48], record.OwnerSlot)
		if record.PrevValid {
			recordRaw[manifestRecordFlagsOffset] |= manifestRecordPrevValidFlag
		}
		if record.Tombstone {
			recordRaw[manifestRecordFlagsOffset] |= manifestRecordTombstoneFlag
		}
		record.Prev.Marshal((*[24]byte)(recordRaw[49:]))
		if err := writeAll(body, recordRaw[:]); err != nil {
			return err
		}
	}

	binary.LittleEndian.PutUint32(count[:4], checksum.Sum32())
	return writeAll(writer, count[:4])
}

// WriteSegmentManifest durably stages a manifest: tmp write + fsync + rename +
// dir fsync. Once the rename is durable, the batch commit is decided — recovery
// completes the index flip from the manifest.
func WriteSegmentManifest(acctsDir string, m *SegmentManifest) error {
	return writeSegmentManifest(acctsDir, m, fsyncDir)
}

func writeSegmentManifest(
	acctsDir string,
	m *SegmentManifest,
	syncDirectory func(string) error,
) (retErr error) {
	if m == nil {
		return errors.New("accountsdb: nil segment manifest")
	}
	if syncDirectory == nil {
		return errors.New("accountsdb: nil manifest directory sync")
	}
	final := segmentManifestPath(acctsDir, m.ThroughSlot, m.FileId)
	tmp := final + ".tmp"
	// O_EXCL prevents following or replacing a stale/symlink temporary name.
	// A normal pre-decision failure removes its own temp; a process crash leaves
	// one exact `.manifest.tmp` artifact for startup recovery to remove.
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("accountsdb: create manifest tmp: %w", err)
	}
	fileOpen := true
	renamed := false
	defer func() {
		if fileOpen {
			retErr = errors.Join(retErr, f.Close())
		}
		if !renamed {
			if removeErr := os.Remove(tmp); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				retErr = errors.Join(retErr, fmt.Errorf("accountsdb: remove manifest tmp: %w", removeErr))
			}
		}
	}()
	buffered := bufio.NewWriterSize(f, 256<<10)
	if err := writeSegmentManifestEncoding(buffered, m); err != nil {
		return fmt.Errorf("accountsdb: write manifest: %w", err)
	}
	if err := buffered.Flush(); err != nil {
		return fmt.Errorf("accountsdb: flush manifest: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("accountsdb: fsync manifest: %w", err)
	}
	closeErr := f.Close()
	fileOpen = false
	if closeErr != nil {
		return fmt.Errorf("accountsdb: close manifest: %w", closeErr)
	}
	// A final manifest is the durable commit decision. Never replace one: an
	// identity collision is corruption, not a retryable overwrite.
	if err := unix.Renameat2(unix.AT_FDCWD, tmp, unix.AT_FDCWD, final, unix.RENAME_NOREPLACE); err != nil {
		return fmt.Errorf("accountsdb: rename manifest: %w", err)
	}
	renamed = true
	if err := syncDirectory(acctsDir); err != nil {
		return &segmentManifestRenamedError{err: err}
	}
	return nil
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

func openRegularManifestForRead(path string) (*os.File, os.FileInfo, error) {
	f, openedInfo, err := openStableRegularFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: open final manifest: %w", ErrTornManifest, err)
	}
	return f, openedInfo, nil
}

func ensureManifestUnchanged(path string, f *os.File, openedInfo os.FileInfo) error {
	if err := validateStableRegularFile(f, path, openedInfo); err != nil {
		return fmt.Errorf("%w: final manifest changed while validating: %w", ErrTornManifest, err)
	}
	return nil
}

// ReadSegmentManifest streams and fully validates a manifest (trailing CRC
// included). It allocates only the decoded semantic result: unlike os.ReadFile,
// it does not retain a second whole-manifest byte slice beside the record array.
func ReadSegmentManifest(path string) (*SegmentManifest, error) {
	f, openedInfo, err := openRegularManifestForRead(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// prefix + bankhash count + context length + record count + trailing CRC.
	const minimumManifestSize = manifestFixedPrefixSize + 4 + 4 + 8 + 4
	if openedInfo.Size() < minimumManifestSize {
		return nil, ErrTornManifest
	}
	bodySize := uint64(openedInfo.Size() - 4)
	hash := crc32.NewIEEE()
	reader := io.TeeReader(io.NewSectionReader(f, 0, int64(bodySize)), hash)
	remaining := bodySize
	readFull := func(dst []byte) error {
		if uint64(len(dst)) > remaining {
			return ErrTornManifest
		}
		if _, err := io.ReadFull(reader, dst); err != nil {
			return fmt.Errorf("%w: read manifest body", ErrTornManifest)
		}
		remaining -= uint64(len(dst))
		return nil
	}

	var prefix [manifestFixedPrefixSize]byte
	if err := readFull(prefix[:]); err != nil {
		return nil, err
	}
	d := &manifestDecoder{data: prefix[:]}
	m := &SegmentManifest{}
	if err := decodeManifestPrefix(d, m); err != nil {
		return nil, err
	}

	var u32 [4]byte
	if err := readFull(u32[:]); err != nil {
		return nil, err
	}
	nBank := binary.LittleEndian.Uint32(u32[:])
	// Reserve the context length and record count before trusting nBank as an
	// allocation length. A corrupt count cannot trigger an oversized make.
	if remaining < 4+8 || uint64(nBank) > (remaining-(4+8))/40 {
		return nil, ErrTornManifest
	}
	if uint64(nBank) > uint64(^uint(0)>>1) {
		return nil, ErrTornManifest
	}
	m.Bankhashes = make([]SlotBankhash, int(nBank))
	var bankRaw [40]byte
	for i := range m.Bankhashes {
		if err := readFull(bankRaw[:]); err != nil {
			return nil, err
		}
		m.Bankhashes[i].Slot = binary.LittleEndian.Uint64(bankRaw[:8])
		copy(m.Bankhashes[i].Bankhash[:], bankRaw[8:])
	}

	if err := readFull(u32[:]); err != nil {
		return nil, err
	}
	ctxLen := uint64(binary.LittleEndian.Uint32(u32[:]))
	if remaining < 8 || ctxLen > remaining-8 || ctxLen > uint64(^uint(0)>>1) {
		return nil, ErrTornManifest
	}
	m.ResumeCtx = make([]byte, int(ctxLen))
	if err := readFull(m.ResumeCtx); err != nil {
		return nil, err
	}

	var u64 [8]byte
	if err := readFull(u64[:]); err != nil {
		return nil, err
	}
	nRecords := binary.LittleEndian.Uint64(u64[:])
	if nRecords > ^uint64(0)/manifestRecordSize ||
		nRecords*manifestRecordSize != remaining ||
		nRecords > uint64(^uint(0)>>1) {
		return nil, ErrTornManifest
	}
	m.Records = make([]ManifestRecord, int(nRecords))
	var recordRaw [manifestRecordSize]byte
	for i := range m.Records {
		if err := readFull(recordRaw[:]); err != nil {
			return nil, err
		}
		r := &m.Records[i]
		copy(r.Pubkey[:], recordRaw[:32])
		r.Offset = binary.LittleEndian.Uint64(recordRaw[32:40])
		r.OwnerSlot = binary.LittleEndian.Uint64(recordRaw[40:48])
		flags := recordRaw[manifestRecordFlagsOffset]
		if flags&^manifestRecordKnownFlags != 0 {
			return nil, fmt.Errorf("%w: manifest record %d has unknown flags", ErrTornManifest, i)
		}
		r.PrevValid = flags&manifestRecordPrevValidFlag != 0
		r.Tombstone = flags&manifestRecordTombstoneFlag != 0
		r.Prev.Unmarshal((*[24]byte)(recordRaw[49:]))
	}
	if remaining != 0 {
		return nil, ErrTornManifest
	}
	var tail [4]byte
	if _, err := f.ReadAt(tail[:], int64(bodySize)); err != nil {
		return nil, fmt.Errorf("%w: read manifest checksum", ErrTornManifest)
	}
	if hash.Sum32() != binary.LittleEndian.Uint32(tail[:]) {
		return nil, ErrTornManifest
	}
	if err := ensureManifestUnchanged(path, f, openedInfo); err != nil {
		return nil, err
	}
	return m, nil
}

// ReadSegmentManifestContext reads only the fixed prefix and resume context,
// then structurally validates the declared record tail against the file size.
// It deliberately does not scan records or validate the trailing CRC. This is
// for advisory retention bookkeeping, where a malformed context fails closed;
// recovery, rewind, and index mutation must continue using ReadSegmentManifest.
func ReadSegmentManifestContext(path string) (_ *SegmentManifest, retErr error) {
	f, openedInfo, err := openRegularManifestForRead(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		retErr = errors.Join(retErr, ensureManifestUnchanged(path, f, openedInfo), f.Close())
	}()

	var prefix [manifestFixedPrefixSize]byte
	if _, err := io.ReadFull(f, prefix[:]); err != nil {
		return nil, ErrTornManifest
	}
	d := &manifestDecoder{data: prefix[:]}
	m := &SegmentManifest{}
	if err := decodeManifestPrefix(d, m); err != nil {
		return nil, err
	}

	var u32 [4]byte
	if _, err := io.ReadFull(f, u32[:]); err != nil {
		return nil, ErrTornManifest
	}
	nBank := uint64(binary.LittleEndian.Uint32(u32[:]))
	if nBank > uint64(openedInfo.Size())/40 {
		return nil, ErrTornManifest
	}
	if _, err := f.Seek(int64(nBank*40), io.SeekCurrent); err != nil {
		return nil, ErrTornManifest
	}
	if _, err := io.ReadFull(f, u32[:]); err != nil {
		return nil, ErrTornManifest
	}
	ctxLen := uint64(binary.LittleEndian.Uint32(u32[:]))
	current, err := f.Seek(0, io.SeekCurrent)
	if err != nil || ctxLen > uint64(openedInfo.Size()) || uint64(current) > uint64(openedInfo.Size())-ctxLen {
		return nil, ErrTornManifest
	}
	m.ResumeCtx = make([]byte, int(ctxLen))
	if _, err := io.ReadFull(f, m.ResumeCtx); err != nil {
		return nil, ErrTornManifest
	}

	var u64 [8]byte
	if _, err := io.ReadFull(f, u64[:]); err != nil {
		return nil, ErrTornManifest
	}
	nRecords := binary.LittleEndian.Uint64(u64[:])
	tailStart, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, ErrTornManifest
	}
	if nRecords > ^uint64(0)/manifestRecordSize {
		return nil, ErrTornManifest
	}
	recordsBytes := nRecords * manifestRecordSize
	if recordsBytes > ^uint64(0)-4 ||
		tailStart < 0 ||
		uint64(tailStart) > ^uint64(0)-recordsBytes-4 {
		return nil, ErrTornManifest
	}
	expectedSize := uint64(tailStart) + recordsBytes + 4
	if expectedSize != uint64(openedInfo.Size()) {
		return nil, ErrTornManifest
	}
	return m, nil
}

// readManifestHeader parses only the fixed prefix — no CRC validation.
func readManifestHeader(path string) (_ ManifestHeader, retErr error) {
	f, openedInfo, err := openRegularManifestForRead(path)
	if err != nil {
		return ManifestHeader{}, err
	}
	defer func() {
		retErr = errors.Join(retErr, ensureManifestUnchanged(path, f, openedInfo), f.Close())
	}()
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
// BatchSeq. A final manifest is a durable commit record, so even an unreadable
// fixed prefix is corruption and must fail closed rather than be quarantined.
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
			return nil, fmt.Errorf("accountsdb: read final manifest header %s: %w", name, err)
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
// It fully validates every final manifest's CRC, structural layout, kind, and
// canonical pathname before returning any references. This is deliberately
// fail-closed: orphan GC must not delete a live data file merely because its
// durable commit record has suffered header or body corruption.
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
		parked := strings.HasSuffix(name, segManifestRewoundSuffix)
		if !strings.HasSuffix(name, segManifestSuffix) && !parked {
			continue
		}
		if strings.HasSuffix(name, segManifestTmpSuffix) {
			continue
		}
		path := filepath.Join(acctsDir, name)
		hdr, err := readValidatedManifestHeader(path)
		if err != nil {
			return nil, fmt.Errorf("accountsdb: validate final manifest %s: %w", path, err)
		}
		if err := validateFinalManifestHeaderIdentity(hdr, path, parked); err != nil {
			return nil, fmt.Errorf("accountsdb: validate final manifest identity: %w", err)
		}
		ids[hdr.FileId] = struct{}{}
	}
	return ids, nil
}

// readValidatedManifestHeader validates a final manifest without materializing
// its bankhash, resume-context, or record arrays. Manifests can contain many
// records, so startup/orphan discovery must remain bounded-memory even while it
// scans every byte needed for the trailing CRC.
func readValidatedManifestHeader(path string) (ManifestHeader, error) {
	f, openedInfo, err := openRegularManifestForRead(path)
	if err != nil {
		return ManifestHeader{}, err
	}
	defer f.Close()

	// prefix + bankhash count + context length + record count + trailing CRC.
	const minimumManifestSize = manifestFixedPrefixSize + 4 + 4 + 8 + 4
	if openedInfo.Size() < minimumManifestSize {
		return ManifestHeader{}, ErrTornManifest
	}
	bodySize := uint64(openedInfo.Size() - 4)

	var prefix [manifestFixedPrefixSize]byte
	if _, err := f.ReadAt(prefix[:], 0); err != nil {
		return ManifestHeader{}, fmt.Errorf("%w: read fixed prefix", ErrTornManifest)
	}
	d := &manifestDecoder{data: prefix[:]}
	var manifest SegmentManifest
	if err := decodeManifestPrefix(d, &manifest); err != nil {
		return ManifestHeader{}, err
	}
	hdr := ManifestHeader{
		Kind:        manifest.Kind,
		BatchSeq:    manifest.BatchSeq,
		FromSlot:    manifest.FromSlot,
		ThroughSlot: manifest.ThroughSlot,
		FileId:      manifest.FileId,
		DataLen:     manifest.DataLen,
		DataCRC:     manifest.DataCRC,
		Path:        path,
	}

	readU32At := func(offset uint64) (uint32, error) {
		if offset > bodySize || bodySize-offset < 4 {
			return 0, ErrTornManifest
		}
		var raw [4]byte
		if _, err := f.ReadAt(raw[:], int64(offset)); err != nil {
			return 0, ErrTornManifest
		}
		return binary.LittleEndian.Uint32(raw[:]), nil
	}
	readU64At := func(offset uint64) (uint64, error) {
		if offset > bodySize || bodySize-offset < 8 {
			return 0, ErrTornManifest
		}
		var raw [8]byte
		if _, err := f.ReadAt(raw[:], int64(offset)); err != nil {
			return 0, ErrTornManifest
		}
		return binary.LittleEndian.Uint64(raw[:]), nil
	}
	advance := func(offset, amount uint64) (uint64, error) {
		if offset > bodySize || amount > bodySize-offset {
			return 0, ErrTornManifest
		}
		return offset + amount, nil
	}

	offset := uint64(manifestFixedPrefixSize)
	nBank, err := readU32At(offset)
	if err != nil {
		return ManifestHeader{}, err
	}
	offset += 4
	offset, err = advance(offset, uint64(nBank)*40)
	if err != nil {
		return ManifestHeader{}, err
	}
	ctxLen, err := readU32At(offset)
	if err != nil {
		return ManifestHeader{}, err
	}
	offset += 4
	offset, err = advance(offset, uint64(ctxLen))
	if err != nil {
		return ManifestHeader{}, err
	}
	nRecords, err := readU64At(offset)
	if err != nil {
		return ManifestHeader{}, err
	}
	offset += 8
	recordsStart := offset
	if nRecords > ^uint64(0)/manifestRecordSize {
		return ManifestHeader{}, ErrTornManifest
	}
	offset, err = advance(offset, nRecords*manifestRecordSize)
	if err != nil || offset != bodySize {
		return ManifestHeader{}, ErrTornManifest
	}

	hash := crc32.NewIEEE()
	reader := io.NewSectionReader(f, 0, int64(bodySize))
	buffer := make([]byte, 64<<10)
	var bodyOffset uint64
	for bodyOffset < bodySize {
		chunkLen := min(uint64(len(buffer)), bodySize-bodyOffset)
		if _, err := io.ReadFull(reader, buffer[:chunkLen]); err != nil {
			return ManifestHeader{}, fmt.Errorf("%w: read manifest body", ErrTornManifest)
		}
		_, _ = hash.Write(buffer[:chunkLen])
		chunkEnd := bodyOffset + chunkLen
		if nRecords != 0 {
			firstFlag := recordsStart + manifestRecordFlagsOffset
			firstOrdinal := uint64(0)
			if bodyOffset > firstFlag {
				firstOrdinal = (bodyOffset - firstFlag + manifestRecordSize - 1) / manifestRecordSize
			}
			for ordinal := firstOrdinal; ordinal < nRecords; ordinal++ {
				flagOffset := firstFlag + ordinal*manifestRecordSize
				if flagOffset >= chunkEnd {
					break
				}
				if flagOffset >= bodyOffset && buffer[flagOffset-bodyOffset]&^manifestRecordKnownFlags != 0 {
					return ManifestHeader{}, fmt.Errorf("%w: manifest record %d has unknown flags", ErrTornManifest, ordinal)
				}
			}
		}
		bodyOffset = chunkEnd
	}
	var tail [4]byte
	if _, err := f.ReadAt(tail[:], int64(bodySize)); err != nil {
		return ManifestHeader{}, fmt.Errorf("%w: read manifest checksum", ErrTornManifest)
	}
	if hash.Sum32() != binary.LittleEndian.Uint32(tail[:]) {
		return ManifestHeader{}, ErrTornManifest
	}

	if err := ensureManifestUnchanged(path, f, openedInfo); err != nil {
		return ManifestHeader{}, err
	}
	return hdr, nil
}

func validateFinalManifestHeaderIdentity(hdr ManifestHeader, path string, parked bool) error {
	if hdr.FromSlot > hdr.ThroughSlot {
		return fmt.Errorf("manifest at %s has from slot %d beyond through slot %d", path, hdr.FromSlot, hdr.ThroughSlot)
	}
	switch hdr.Kind {
	case ManifestKindFold:
		if hdr.BatchSeq == 0 {
			return fmt.Errorf("fold manifest at %s has zero batch sequence", path)
		}
	case ManifestKindCompact:
		if parked {
			return fmt.Errorf("compact manifest at %s cannot be parked as rewound", path)
		}
		if hdr.BatchSeq != 0 || hdr.FromSlot != hdr.ThroughSlot {
			return fmt.Errorf("compact manifest at %s has invalid sequence or slot range", path)
		}
	default:
		return fmt.Errorf("manifest at %s has unknown kind %d", path, hdr.Kind)
	}
	want := segmentManifestPath(filepath.Dir(path), hdr.ThroughSlot, hdr.FileId)
	if parked {
		want += ".rewound"
	}
	if filepath.Clean(path) != filepath.Clean(want) {
		return fmt.Errorf("manifest path %s does not match slot=%d file=%d", path, hdr.ThroughSlot, hdr.FileId)
	}
	return nil
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
