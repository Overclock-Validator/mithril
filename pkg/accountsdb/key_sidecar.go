package accountsdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"
)

// A key sidecar lists, for one data file, where the records of selected
// programs are and a 4-byte fingerprint of a 32-byte key in their data, such
// as a token account's owner. A scan for a few keys then reads the sidecars
// and only the records whose fingerprint matches, instead of every data file.
//
// Data files never change after they are written, except that sparse
// compaction zeroes dead records in place, which keeps offsets. File IDs are
// never reused. So a sidecar written once stays a superset of the file's
// matching records. Sidecars are a cache: one that is missing, torn, built
// under a different configuration, or for a different file length is
// ignored, and the scan reads the data file instead and rebuilds it. They are
// written without fsync for that reason.
//
// Layout, little-endian: magic, version, config checksum (u32 each), data
// file length, entry count (u64 each), entries, CRC32 of everything before
// it. An entry is the key's first four bytes and the record offset divided
// by 8 (u32 each), so a data file of 32 GiB or more gets no sidecar.

// KeySidecarConfig selects the records a sidecar covers: those owned by one
// of Programs whose data holds at least MinDataLen bytes, keyed by the 32
// bytes at KeyOffset.
type KeySidecarConfig struct {
	Programs   [][32]byte
	KeyOffset  int
	MinDataLen int
}

const (
	keySidecarDir     = "keyidx"
	keySidecarSuffix  = ".keys"
	keySidecarMagic   = 0x3159454b // "KEY1"
	keySidecarVersion = 1
	keySidecarHeader  = 4 + 4 + 4 + 8 + 8
	keySidecarEntry   = 8
)

type keySidecar struct {
	cfg      KeySidecarConfig
	programs map[[32]byte]struct{}
	sum      uint32
	dir      string
}

// EnableKeySidecars makes folds and compaction write a key sidecar for each
// data file they create, and lets ScanRecords use them. Call it before
// replay starts folding.
func (db *AccountsDb) EnableKeySidecars(cfg KeySidecarConfig) error {
	if cfg.KeyOffset < 0 || cfg.MinDataLen < cfg.KeyOffset+32 || len(cfg.Programs) == 0 {
		return fmt.Errorf("accountsdb: invalid key sidecar config")
	}
	ks := &keySidecar{cfg: cfg, programs: make(map[[32]byte]struct{}, len(cfg.Programs)), dir: filepath.Join(db.AcctsDir, keySidecarDir)}
	var enc bytes.Buffer
	var u64 [8]byte
	binary.LittleEndian.PutUint64(u64[:], uint64(cfg.KeyOffset))
	enc.Write(u64[:])
	binary.LittleEndian.PutUint64(u64[:], uint64(cfg.MinDataLen))
	enc.Write(u64[:])
	for _, p := range cfg.Programs {
		ks.programs[p] = struct{}{}
		enc.Write(p[:])
	}
	ks.sum = crc32.ChecksumIEEE(enc.Bytes())
	if err := os.MkdirAll(ks.dir, 0o755); err != nil {
		return err
	}
	db.keySidecars.Store(ks)
	return nil
}

// KeySidecarsEnabled reports whether EnableKeySidecars was called.
func (db *AccountsDb) KeySidecarsEnabled() bool { return db.keySidecars.Load() != nil }

// keySidecarBuilder collects one data file's sidecar entries.
type keySidecarBuilder struct {
	ks      *keySidecar
	entries []byte
	tooBig  bool
}

func (ks *keySidecar) builder() *keySidecarBuilder {
	if ks == nil {
		return nil
	}
	return &keySidecarBuilder{ks: ks}
}

// add records a data file record at offset owned by owner.
func (b *keySidecarBuilder) add(offset uint64, owner [32]byte, data []byte) {
	if b == nil || len(data) < b.ks.cfg.MinDataLen {
		return
	}
	if _, ok := b.ks.programs[owner]; !ok {
		return
	}
	if offset%8 != 0 || offset/8 > math.MaxUint32 {
		b.tooBig = true
		return
	}
	var e [keySidecarEntry]byte
	copy(e[:4], data[b.ks.cfg.KeyOffset:b.ks.cfg.KeyOffset+4])
	binary.LittleEndian.PutUint32(e[4:], uint32(offset/8))
	b.entries = append(b.entries, e[:]...)
}

// addRecord adds a raw record (header, data, padding) found at offset.
func (b *keySidecarBuilder) addRecord(offset uint64, rec []byte) {
	if b == nil || len(rec) < hdrLen {
		return
	}
	dataLen := binary.LittleEndian.Uint64(rec[dataLenOffset:])
	if dataLen > uint64(len(rec)-hdrLen) {
		return
	}
	b.add(offset, [32]byte(rec[ownerOffset:ownerOffset+32]), rec[hdrLen:hdrLen+dataLen])
}

// path names a sidecar after its data file, "<slot>.<fileId>.keys".
func (ks *keySidecar) path(slot, fileId uint64) string {
	return filepath.Join(ks.dir, SegmentDataName(slot, fileId)+keySidecarSuffix)
}

// write stores the sidecar of a data file dataLen bytes long. Failures are
// ignored: the scan falls back to reading the data file.
func (b *keySidecarBuilder) write(slot, fileId, dataLen uint64) {
	if b == nil || b.tooBig {
		return
	}
	var out bytes.Buffer
	out.Grow(keySidecarHeader + len(b.entries) + 4)
	var u32 [4]byte
	var u64 [8]byte
	put32 := func(v uint32) { binary.LittleEndian.PutUint32(u32[:], v); out.Write(u32[:]) }
	put64 := func(v uint64) { binary.LittleEndian.PutUint64(u64[:], v); out.Write(u64[:]) }
	put32(keySidecarMagic)
	put32(keySidecarVersion)
	put32(b.ks.sum)
	put64(dataLen)
	put64(uint64(len(b.entries) / keySidecarEntry))
	out.Write(b.entries)
	put32(crc32.ChecksumIEEE(out.Bytes()))
	final := b.ks.path(slot, fileId)
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, out.Bytes(), 0o644); err != nil {
		os.Remove(tmp)
		return
	}
	if err := os.Rename(tmp, final); err != nil {
		os.Remove(tmp)
	}
}

// read returns the entries of a data file's sidecar if it is intact and
// matches the file's current length.
func (ks *keySidecar) read(slot, fileId uint64, dataLen int64) ([]byte, int64, bool) {
	raw, err := os.ReadFile(ks.path(slot, fileId))
	if err != nil {
		return nil, 0, false
	}
	n := int64(len(raw))
	if len(raw) < keySidecarHeader+4 {
		return nil, n, false
	}
	body, trailer := raw[:len(raw)-4], raw[len(raw)-4:]
	if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(trailer) ||
		binary.LittleEndian.Uint32(body[0:]) != keySidecarMagic ||
		binary.LittleEndian.Uint32(body[4:]) != keySidecarVersion ||
		binary.LittleEndian.Uint32(body[8:]) != ks.sum ||
		binary.LittleEndian.Uint64(body[12:]) != uint64(dataLen) {
		return nil, n, false
	}
	count := binary.LittleEndian.Uint64(body[20:])
	entries := body[keySidecarHeader:]
	if uint64(len(entries)) != count*keySidecarEntry {
		return nil, n, false
	}
	return entries, n, true
}

func (ks *keySidecar) remove(slot, fileId uint64) {
	if ks != nil {
		os.Remove(ks.path(slot, fileId))
	}
}

// removeStale deletes sidecars of files that no longer exist. Only IDs at or
// below the newest listed file are considered, so sidecars of files created
// after the listing survive.
func (ks *keySidecar) removeStale(live map[uint64]struct{}, maxFileId uint64) error {
	entries, err := os.ReadDir(ks.dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, e := range entries {
		name, ok := strings.CutSuffix(e.Name(), keySidecarSuffix)
		if !ok {
			if strings.HasSuffix(e.Name(), keySidecarSuffix+".tmp") {
				os.Remove(filepath.Join(ks.dir, e.Name()))
			}
			continue
		}
		_, id, ok := parseDataFileName(name)
		if !ok || id > maxFileId {
			continue
		}
		if _, ok := live[id]; !ok {
			os.Remove(filepath.Join(ks.dir, e.Name()))
		}
	}
	return nil
}
