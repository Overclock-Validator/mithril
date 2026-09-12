package rewards

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/Overclock-Validator/wide"
	"github.com/gagliardetto/solana-go"
)

// SpoolRecordSize is the binary size of a spool record.
// Format: stake_pubkey(32) + vote_pubkey(32) + stake_lamports(8) +
//
//	credits_observed(8) + reward_lamports(8) = 88 bytes
const SpoolRecordSize = 88

// SpoolRecord represents a single stake reward record.
type SpoolRecord struct {
	StakePubkey     solana.PublicKey
	VotePubkey      solana.PublicKey
	StakeLamports   uint64
	CreditsObserved uint64
	RewardLamports  uint64
	PartitionIndex  uint32 // Only used during calculation, not serialized
}

// encodeRecord encodes a record into the buffer (without partition index).
func encodeRecord(rec *SpoolRecord, buf []byte) {
	copy(buf[0:32], rec.StakePubkey[:])
	copy(buf[32:64], rec.VotePubkey[:])
	binary.LittleEndian.PutUint64(buf[64:72], rec.StakeLamports)
	binary.LittleEndian.PutUint64(buf[72:80], rec.CreditsObserved)
	binary.LittleEndian.PutUint64(buf[80:88], rec.RewardLamports)
}

// decodeRecord decodes a record from the buffer.
func decodeRecord(buf []byte, rec *SpoolRecord) {
	copy(rec.StakePubkey[:], buf[0:32])
	copy(rec.VotePubkey[:], buf[32:64])
	rec.StakeLamports = binary.LittleEndian.Uint64(buf[64:72])
	rec.CreditsObserved = binary.LittleEndian.Uint64(buf[72:80])
	rec.RewardLamports = binary.LittleEndian.Uint64(buf[80:88])
}

// PartitionedSpoolWriters manages per-partition spool files.
// Thread-safe - multiple goroutines can write concurrently.
// Uses buffered I/O for performance.
type PartitionedSpoolWriters struct {
	baseDir       string
	slot          uint64
	numPartitions uint64
	writers       map[uint32]*partitionWriter
	mu            sync.Mutex
	closed        bool
}

// partitionWriter is a buffered writer for a single partition file.
type partitionWriter struct {
	file  *os.File
	bufw  *bufio.Writer
	count int
}

// NewPartitionedSpoolWriters creates a new set of per-partition spool writers.
func NewPartitionedSpoolWriters(baseDir string, slot uint64, numPartitions uint64) *PartitionedSpoolWriters {
	return &PartitionedSpoolWriters{
		baseDir:       baseDir,
		slot:          slot,
		numPartitions: numPartitions,
		writers:       make(map[uint32]*partitionWriter),
	}
}

// SpoolDir returns the base directory for spool files.
func (p *PartitionedSpoolWriters) SpoolDir() string {
	return p.baseDir
}

// Slot returns the slot this spool is for.
func (p *PartitionedSpoolWriters) Slot() uint64 {
	return p.slot
}

// WriteRecord writes a record to the appropriate partition file.
// Thread-safe - lazily opens partition files as needed.
func (p *PartitionedSpoolWriters) WriteRecord(rec *SpoolRecord) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return fmt.Errorf("spool writers are closed")
	}

	partition := rec.PartitionIndex

	// Get or create writer for this partition
	w, exists := p.writers[partition]
	if !exists {
		path := partitionFilePath(p.baseDir, p.slot, partition)
		f, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("creating partition %d spool file: %w", partition, err)
		}
		// 1MB buffer for efficient sequential writes
		w = &partitionWriter{file: f, bufw: bufio.NewWriterSize(f, 1<<20)}
		p.writers[partition] = w
	}

	// Write record to buffer
	var buf [SpoolRecordSize]byte
	encodeRecord(rec, buf[:])
	if _, err := w.bufw.Write(buf[:]); err != nil {
		return fmt.Errorf("writing to partition %d: %w", partition, err)
	}
	w.count++
	return nil
}

// Close flushes buffers, syncs, and closes all partition files.
// Returns the first error encountered.
func (p *PartitionedSpoolWriters) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true

	var firstErr error
	for partition, w := range p.writers {
		// Flush buffer first
		if err := w.bufw.Flush(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("flushing partition %d: %w", partition, err)
		}
		// Sync to disk
		if err := w.file.Sync(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("syncing partition %d: %w", partition, err)
		}
		// Close file
		if err := w.file.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("closing partition %d: %w", partition, err)
		}
	}
	return firstErr
}

// TotalRecords returns the total number of records written across all partitions.
func (p *PartitionedSpoolWriters) TotalRecords() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	total := 0
	for _, w := range p.writers {
		total += w.count
	}
	return total
}

// PartitionReader reads records sequentially from a partition spool file.
// Uses buffered I/O for efficient sequential reads.
type PartitionReader struct {
	file *os.File
	bufr *bufio.Reader
	buf  [SpoolRecordSize]byte
}

// NewPartitionReader opens a partition file for sequential reading.
func NewPartitionReader(baseDir string, slot uint64, partition uint32) (*PartitionReader, error) {
	path := partitionFilePath(baseDir, slot, partition)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			// No records for this partition
			return nil, nil
		}
		return nil, fmt.Errorf("opening partition %d spool: %w", partition, err)
	}
	// 1MB buffer for efficient sequential reads
	return &PartitionReader{file: f, bufr: bufio.NewReaderSize(f, 1<<20)}, nil
}

// Next reads the next record. Returns io.EOF when done.
func (r *PartitionReader) Next() (*SpoolRecord, error) {
	_, err := io.ReadFull(r.bufr, r.buf[:])
	if err == io.EOF {
		return nil, io.EOF
	}
	if err != nil {
		return nil, fmt.Errorf("reading spool record: %w", err)
	}

	rec := &SpoolRecord{}
	decodeRecord(r.buf[:], rec)
	return rec, nil
}

// Close closes the partition file.
func (r *PartitionReader) Close() error {
	return r.file.Close()
}

// partitionFilePath returns the path for a partition spool file.
func partitionFilePath(baseDir string, slot uint64, partition uint32) string {
	return filepath.Join(baseDir, fmt.Sprintf("reward_spool_%d_p%d.bin", slot, partition))
}

// CleanupPartitionedSpoolFiles removes all partition spool files for a slot.
func CleanupPartitionedSpoolFiles(baseDir string, slot uint64, numPartitions uint64) {
	for p := uint64(0); p < numPartitions; p++ {
		path := partitionFilePath(baseDir, slot, uint32(p))
		os.Remove(path) // Ignore errors - file may not exist
	}
}

// A restart may re-execute an uncheckpointed boundary. Writers create only
// nonempty partitions, so remove this attempt's old files before calculating a
// replacement; otherwise an empty partition could consume stale rewards.
func resetPartitionedSpoolFiles(baseDir string, slot, numPartitions uint64) error {
	for p := uint64(0); p < numPartitions; p++ {
		if err := os.Remove(partitionFilePath(baseDir, slot, uint32(p))); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("reset reward partition %d at slot %d: %w", p, slot, err)
		}
	}
	return nil
}

// TempSpoolWriter writes reward records to a single temp file (no partition separation).
// Used in the first phase of reward calculation before partition count is known.
// NOT thread-safe - should be used with a single-writer pattern.
type TempSpoolWriter struct {
	file  *os.File
	bufw  *bufio.Writer
	path  string
	count int
}

// NewTempSpoolWriter creates a new temp spool writer.
func NewTempSpoolWriter(baseDir string, slot uint64) (*TempSpoolWriter, error) {
	path := tempSpoolPath(baseDir, slot)
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("creating temp spool: %w", err)
	}
	// 1MB buffer for efficient sequential writes
	return &TempSpoolWriter{
		file: f,
		bufw: bufio.NewWriterSize(f, 1<<20),
		path: path,
	}, nil
}

// WriteRecord writes a record to the temp spool.
func (w *TempSpoolWriter) WriteRecord(rec *SpoolRecord) error {
	var buf [SpoolRecordSize]byte
	encodeRecord(rec, buf[:])
	if _, err := w.bufw.Write(buf[:]); err != nil {
		return fmt.Errorf("writing temp spool record: %w", err)
	}
	w.count++
	return nil
}

// Count returns the number of records written.
func (w *TempSpoolWriter) Count() int {
	return w.count
}

// Path returns the temp spool file path.
func (w *TempSpoolWriter) Path() string {
	return w.path
}

// Close flushes and closes the temp spool file.
// NOTE: No Sync() - temp spool is only used in-process and deleted immediately after reading.
func (w *TempSpoolWriter) Close() error {
	if err := w.bufw.Flush(); err != nil {
		return fmt.Errorf("flushing temp spool: %w", err)
	}
	return w.file.Close()
}

// TempSpoolReader reads records sequentially from a temp spool file.
type TempSpoolReader struct {
	file *os.File
	bufr *bufio.Reader
	buf  [SpoolRecordSize]byte
}

// NewTempSpoolReader opens a temp spool file for sequential reading.
func NewTempSpoolReader(path string) (*TempSpoolReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening temp spool: %w", err)
	}
	// 1MB buffer for efficient sequential reads
	return &TempSpoolReader{
		file: f,
		bufr: bufio.NewReaderSize(f, 1<<20),
	}, nil
}

// Next reads the next record. Returns io.EOF when done.
func (r *TempSpoolReader) Next() (*SpoolRecord, error) {
	_, err := io.ReadFull(r.bufr, r.buf[:])
	if err == io.EOF {
		return nil, io.EOF
	}
	if err != nil {
		return nil, fmt.Errorf("reading temp spool record: %w", err)
	}

	rec := &SpoolRecord{}
	decodeRecord(r.buf[:], rec)
	return rec, nil
}

// Close closes the temp spool file.
func (r *TempSpoolReader) Close() error {
	return r.file.Close()
}

// tempSpoolPath returns the path for a temp spool file.
func tempSpoolPath(baseDir string, slot uint64) string {
	return filepath.Join(baseDir, fmt.Sprintf("reward_temp_%d.bin", slot))
}

// CleanupTempSpoolFile removes a temp spool file.
func CleanupTempSpoolFile(path string) {
	os.Remove(path) // Ignore errors - file may not exist
}

// PointsSpoolRecordSize is the binary size of a points spool record.
// Format: stake_pubkey(32) + vote_pubkey(32) + points_lo(8) + points_hi(8) +
//
//	new_credits_observed(8) + stake_lamports(8) + force_credits_update(8) = 104 bytes
const PointsSpoolRecordSize = 104

// PointsSpoolRecord stores intermediate per-stake data from Phase 1 (points calculation)
// so Phase 2 can compute rewards from sequential file I/O instead of re-scanning AccountsDB.
// ForceCreditsUpdateWithSkippedReward is fully precomputed in Phase 1 using all three triggers
// (pcs.ForceCredits, pointValue.Rewards==0, activationEpoch==rewardedEpoch).
type PointsSpoolRecord struct {
	StakePubkey                         solana.PublicKey
	VotePubkey                          solana.PublicKey
	Points                              wide.Uint128
	NewCreditsObserved                  uint64
	StakeLamports                       uint64
	ForceCreditsUpdateWithSkippedReward bool
}

func encodePointsRecord(rec *PointsSpoolRecord, buf []byte) {
	copy(buf[0:32], rec.StakePubkey[:])
	copy(buf[32:64], rec.VotePubkey[:])
	binary.LittleEndian.PutUint64(buf[64:72], rec.Points.Lo)
	binary.LittleEndian.PutUint64(buf[72:80], rec.Points.Hi)
	binary.LittleEndian.PutUint64(buf[80:88], rec.NewCreditsObserved)
	binary.LittleEndian.PutUint64(buf[88:96], rec.StakeLamports)
	var flags uint64
	if rec.ForceCreditsUpdateWithSkippedReward {
		flags = 1
	}
	binary.LittleEndian.PutUint64(buf[96:104], flags)
}

func decodePointsRecord(buf []byte, rec *PointsSpoolRecord) {
	copy(rec.StakePubkey[:], buf[0:32])
	copy(rec.VotePubkey[:], buf[32:64])
	rec.Points.Lo = binary.LittleEndian.Uint64(buf[64:72])
	rec.Points.Hi = binary.LittleEndian.Uint64(buf[72:80])
	rec.NewCreditsObserved = binary.LittleEndian.Uint64(buf[80:88])
	rec.StakeLamports = binary.LittleEndian.Uint64(buf[88:96])
	rec.ForceCreditsUpdateWithSkippedReward = binary.LittleEndian.Uint64(buf[96:104]) != 0
}

// PointsSpoolWriter writes points records to a single temp file.
// NOT thread-safe — use with a single-writer goroutine via channel.
type PointsSpoolWriter struct {
	file  *os.File
	bufw  *bufio.Writer
	path  string
	count int
}

// NewPointsSpoolWriter creates a new points spool writer.
func NewPointsSpoolWriter(baseDir string, slot uint64) (*PointsSpoolWriter, error) {
	path := pointsSpoolPath(baseDir, slot)
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("creating points spool: %w", err)
	}
	return &PointsSpoolWriter{
		file: f,
		bufw: bufio.NewWriterSize(f, 1<<20),
		path: path,
	}, nil
}

// WriteRecord writes a record to the points spool.
func (w *PointsSpoolWriter) WriteRecord(rec *PointsSpoolRecord) error {
	var buf [PointsSpoolRecordSize]byte
	encodePointsRecord(rec, buf[:])
	if _, err := w.bufw.Write(buf[:]); err != nil {
		return fmt.Errorf("writing points spool record: %w", err)
	}
	w.count++
	return nil
}

// Count returns the number of records written.
func (w *PointsSpoolWriter) Count() int {
	return w.count
}

// Path returns the points spool file path.
func (w *PointsSpoolWriter) Path() string {
	return w.path
}

// Close flushes and closes the points spool file.
// NOTE: No Sync() — points spool is only used in-process and deleted immediately after reading.
func (w *PointsSpoolWriter) Close() error {
	if err := w.bufw.Flush(); err != nil {
		return fmt.Errorf("flushing points spool: %w", err)
	}
	return w.file.Close()
}

// PointsSpoolReader reads points records sequentially from a points spool file.
type PointsSpoolReader struct {
	file *os.File
	bufr *bufio.Reader
	buf  [PointsSpoolRecordSize]byte
}

// NewPointsSpoolReader opens a points spool file for sequential reading.
func NewPointsSpoolReader(path string) (*PointsSpoolReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening points spool: %w", err)
	}
	return &PointsSpoolReader{
		file: f,
		bufr: bufio.NewReaderSize(f, 1<<20),
	}, nil
}

// Next reads the next record. Returns io.EOF when done.
func (r *PointsSpoolReader) Next() (*PointsSpoolRecord, error) {
	_, err := io.ReadFull(r.bufr, r.buf[:])
	if err == io.EOF {
		return nil, io.EOF
	}
	if err != nil {
		return nil, fmt.Errorf("reading points spool record: %w", err)
	}

	rec := &PointsSpoolRecord{}
	decodePointsRecord(r.buf[:], rec)
	return rec, nil
}

// Close closes the points spool file.
func (r *PointsSpoolReader) Close() error {
	return r.file.Close()
}

func pointsSpoolPath(baseDir string, slot uint64) string {
	return filepath.Join(baseDir, fmt.Sprintf("reward_points_%d.bin", slot))
}

// CleanupPointsSpoolFile removes a points spool file.
func CleanupPointsSpoolFile(path string) {
	os.Remove(path)
}
