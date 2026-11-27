package accountsdb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/util"
)

// ScanRecordsConfig bounds a scan of every account data file.
type ScanRecordsConfig struct {
	// Workers is the number of files read at once (default 2).
	Workers int
	// MaxBytesPerSecond caps the read rate so the scan does not starve
	// replay's account reads. Zero means unlimited.
	MaxBytesPerSecond int64
	// Programs restricts callbacks to records owned by these programs. Empty
	// means every record.
	Programs [][32]byte
	// Keys, with key sidecars enabled, restricts callbacks to records the
	// sidecars cover whose key is one of these, and reads each data file
	// through its sidecar: only matching records are read. A file without a
	// usable sidecar is read whole and its sidecar rebuilt.
	Keys [][32]byte
}

// ScanStats reports what a scan read.
type ScanStats struct {
	Files int
	// SidecarFiles counts files read through their key sidecar.
	SidecarFiles int
	Bytes        int64
	Records      int64
	Passes       int
}

// scanFileAttempts bounds re-reads of a file that failed to parse, such as a
// compaction output read while it was being written.
const scanFileAttempts = 3

// ScanRecords calls fn for every account record in the data files: live
// versions, overwritten ones, and records of closed accounts alike. Callers
// that need current state must check what they find against the index or a
// bank. fn runs on several goroutines at once, and data is only valid during
// the call.
//
// The scan runs without foldMu, so folds and compaction continue. Compaction
// moves live records into new files named after the source slot, so after the
// files listed at the start are read, files that appeared with a slot at or
// below the newest listed one are read too, until none remain. Newer fold
// segments are not read: they hold only writes made after the scan began,
// which the caller must learn about another way.
func (db *AccountsDb) ScanRecords(ctx context.Context, cfg ScanRecordsConfig, fn func(pubkey, owner [32]byte, lamports uint64, data []byte)) (ScanStats, error) {
	var stats ScanStats
	job := &scanJob{fn: fn, limiter: newByteRateLimiter(cfg.MaxBytesPerSecond)}
	workers := cfg.Workers
	if workers <= 0 {
		workers = 2
	}
	job.programs = make(map[[32]byte]struct{}, len(cfg.Programs))
	for _, p := range cfg.Programs {
		job.programs[p] = struct{}{}
	}
	job.ks = db.keySidecars.Load()
	if len(cfg.Keys) > 0 {
		if job.ks == nil {
			return stats, errors.New("accountsdb: a scan by key needs key sidecars")
		}
		for p := range job.programs {
			if _, ok := job.ks.programs[p]; !ok {
				return stats, fmt.Errorf("accountsdb: key sidecars do not cover program %x", p)
			}
		}
		job.keys = make(map[[32]byte]struct{}, len(cfg.Keys))
		job.fingerprints = make(map[[4]byte]struct{}, len(cfg.Keys))
		for _, k := range cfg.Keys {
			job.keys[k] = struct{}{}
			job.fingerprints[[4]byte(k[:4])] = struct{}{}
		}
	}

	inv, err := db.scanCompactionInventory()
	if err != nil {
		return stats, err
	}
	var maxSlot uint64
	for _, f := range inv.data {
		maxSlot = max(maxSlot, f.slot)
	}
	done := make(map[string]bool, len(inv.data))
	attempts := make(map[string]int)
	for {
		var todo []string
		for name, f := range inv.data {
			if !done[name] && f.slot <= maxSlot {
				todo = append(todo, name)
			}
		}
		if len(todo) == 0 {
			if job.ks != nil {
				live := make(map[uint64]struct{}, len(inv.data))
				var maxFileId uint64
				for _, f := range inv.data {
					live[f.fileId] = struct{}{}
					maxFileId = max(maxFileId, f.fileId)
				}
				if err := job.ks.removeStale(live, maxFileId); err != nil {
					return stats, err
				}
			}
			return stats, nil
		}
		stats.Passes++
		results := db.scanFiles(ctx, todo, workers, job)
		for i, name := range todo {
			r := results[i]
			if r.err != nil {
				if ctx.Err() != nil {
					return stats, ctx.Err()
				}
				attempts[name]++
				if attempts[name] >= scanFileAttempts {
					return stats, fmt.Errorf("accountsdb: scan %s: %w", name, r.err)
				}
				continue // retried in the next pass
			}
			done[name] = true
			stats.Files++
			if r.sidecar {
				stats.SidecarFiles++
			}
			stats.Bytes += r.bytes
			stats.Records += r.records
		}
		if inv, err = db.scanCompactionInventory(); err != nil {
			return stats, err
		}
	}
}

// scanJob is what every file of one scan is read with.
type scanJob struct {
	fn           func(pubkey, owner [32]byte, lamports uint64, data []byte)
	limiter      *byteRateLimiter
	programs     map[[32]byte]struct{}
	ks           *keySidecar
	keys         map[[32]byte]struct{} // nil: no key filter
	fingerprints map[[4]byte]struct{}
}

// visit applies the program and key filters to one record.
func (job *scanJob) visit(pubkey, owner [32]byte, lamports uint64, data []byte) {
	if _, want := job.programs[owner]; !want && len(job.programs) > 0 {
		return
	}
	if job.keys != nil {
		cfg := job.ks.cfg
		if _, covered := job.ks.programs[owner]; !covered || len(data) < cfg.MinDataLen {
			return
		}
		if _, ok := job.keys[[32]byte(data[cfg.KeyOffset:cfg.KeyOffset+32])]; !ok {
			return
		}
	}
	job.fn(pubkey, owner, lamports, data)
}

type scanFileResult struct {
	bytes, records int64
	sidecar        bool
	err            error
}

func (db *AccountsDb) scanFiles(ctx context.Context, names []string, workers int, job *scanJob) []scanFileResult {
	results := make([]scanFileResult, len(names))
	next := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var buf []byte
			for i := range next {
				results[i], buf = db.scanFile(ctx, names[i], buf, job)
			}
		}()
	}
	for i := range names {
		if ctx.Err() != nil {
			results[i].err = ctx.Err()
			continue
		}
		next <- i
	}
	close(next)
	wg.Wait()
	return results
}

// scanFile reads one data file, through its key sidecar when the scan is by
// key and the sidecar is usable, else whole into buf, reusing its capacity.
// A file that vanished was compacted away; its live records are in a newer
// file.
func (db *AccountsDb) scanFile(ctx context.Context, name string, buf []byte, job *scanJob) (scanFileResult, []byte) {
	slot, fileId, ok := parseDataFileName(name)
	if !ok {
		return scanFileResult{}, buf
	}
	f, err := os.Open(db.appendVecPath(slot, fileId))
	if errors.Is(err, fs.ErrNotExist) {
		return scanFileResult{}, buf
	}
	if err != nil {
		return scanFileResult{err: err}, buf
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return scanFileResult{err: err}, buf
	}
	size := int(info.Size())
	var sidecarBytes int64
	if job.keys != nil {
		entries, n, ok := job.ks.read(slot, fileId, int64(size))
		sidecarBytes = n
		if err := job.limiter.wait(ctx, n); err != nil {
			return scanFileResult{err: err}, buf
		}
		if ok {
			r := db.scanFileBySidecar(ctx, f, int64(size), entries, job)
			r.bytes += n
			return r, buf
		}
	}
	if cap(buf) < size {
		buf = make([]byte, size)
	}
	buf = buf[:size]
	if err := job.limiter.wait(ctx, int64(size)); err != nil {
		return scanFileResult{err: err}, buf
	}
	if _, err := io.ReadFull(f, buf); err != nil {
		return scanFileResult{err: err}, buf
	}
	sidecar := job.ks.builder()
	records, err := scanAppendVecRecords(buf, func(offset uint64, pubkey, owner [32]byte, lamports uint64, data []byte) {
		sidecar.add(offset, owner, data)
		job.visit(pubkey, owner, lamports, data)
	})
	if err == nil {
		sidecar.write(slot, fileId, uint64(size))
	}
	return scanFileResult{bytes: int64(size) + sidecarBytes, records: records, err: err}, buf
}

// scanFileBySidecar reads only the records whose key fingerprint the scan
// wants.
func (db *AccountsDb) scanFileBySidecar(ctx context.Context, f *os.File, size int64, entries []byte, job *scanJob) scanFileResult {
	var r scanFileResult
	r.sidecar = true
	var hdr [hdrLen]byte
	var data []byte
	for i := 0; i+keySidecarEntry <= len(entries); i += keySidecarEntry {
		if _, want := job.fingerprints[[4]byte(entries[i:i+4])]; !want {
			continue
		}
		offset := int64(binary.LittleEndian.Uint32(entries[i+4:])) * 8
		if offset+hdrLen > size {
			r.err = fmt.Errorf("key sidecar offset %d beyond %d-byte file", offset, size)
			return r
		}
		if err := job.limiter.wait(ctx, hdrLen); err != nil {
			r.err = err
			return r
		}
		if _, err := f.ReadAt(hdr[:], offset); err != nil {
			r.err = err
			return r
		}
		dataLen := binary.LittleEndian.Uint64(hdr[dataLenOffset:])
		if dataLen > uint64(size-offset-hdrLen) {
			r.err = fmt.Errorf("truncated appendvec account data at offset %d", offset)
			return r
		}
		if cap(data) < int(dataLen) {
			data = make([]byte, dataLen)
		}
		data = data[:dataLen]
		if err := job.limiter.wait(ctx, int64(dataLen)); err != nil {
			r.err = err
			return r
		}
		if _, err := f.ReadAt(data, offset+hdrLen); err != nil {
			r.err = err
			return r
		}
		r.bytes += hdrLen + int64(dataLen)
		r.records++
		job.visit([32]byte(hdr[pubkeyOffset:pubkeyOffset+32]), [32]byte(hdr[ownerOffset:ownerOffset+32]),
			binary.LittleEndian.Uint64(hdr[lamportsOffset:]), data)
	}
	return r
}

// scanAppendVecRecords walks records the way appendVecParser does, without
// building index entries.
func scanAppendVecRecords(buf []byte, fn func(offset uint64, pubkey, owner [32]byte, lamports uint64, data []byte)) (int64, error) {
	var records int64
	limit := uint64(len(buf))
	for offset := uint64(0); offset < limit; {
		if limit-offset < hdrLen {
			for _, b := range buf[offset:] {
				if b != 0 {
					return records, fmt.Errorf("truncated appendvec header at offset %d", offset)
				}
			}
			return records, nil
		}
		dataLen := binary.LittleEndian.Uint64(buf[offset+dataLenOffset:])
		lamports := binary.LittleEndian.Uint64(buf[offset+lamportsOffset:])
		var pubkey, owner [32]byte
		copy(pubkey[:], buf[offset+pubkeyOffset:offset+pubkeyOffset+32])
		if pubkey == ([32]byte{}) && lamports == 0 {
			return records, nil // unused appendvec storage
		}
		dataOffset := offset + hdrLen
		if dataLen > limit-dataOffset {
			return records, fmt.Errorf("truncated appendvec account data at offset %d", offset)
		}
		records++
		copy(owner[:], buf[offset+ownerOffset:offset+ownerOffset+32])
		fn(offset, pubkey, owner, lamports, buf[dataOffset:dataOffset+dataLen:dataOffset+dataLen])
		offset = dataOffset + util.AlignUp(dataLen, 8)
	}
	return records, nil
}

// byteRateLimiter spaces reads so their average rate stays under a cap.
type byteRateLimiter struct {
	perSecond int64
	mu        sync.Mutex
	next      time.Time
}

func newByteRateLimiter(perSecond int64) *byteRateLimiter {
	return &byteRateLimiter{perSecond: perSecond}
}

func (l *byteRateLimiter) wait(ctx context.Context, n int64) error {
	if l.perSecond <= 0 {
		return ctx.Err()
	}
	l.mu.Lock()
	now := time.Now()
	if l.next.Before(now) {
		l.next = now
	}
	at := l.next
	l.next = l.next.Add(time.Duration(n * int64(time.Second) / l.perSecond))
	l.mu.Unlock()
	if d := time.Until(at); d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
	return ctx.Err()
}
