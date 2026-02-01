package rewards

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

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
