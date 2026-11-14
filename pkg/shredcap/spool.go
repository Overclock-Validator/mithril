package shredcap

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
)

// ReadBankHashLog parses a node's bankhash.log: one "<slot> <base58 bank
// hash>" line per replayed block, with "#" run headers. A slot logged more
// than once keeps its last hash (a later run replayed it again).
func ReadBankHashLog(r io.Reader) (map[uint64][32]byte, error) {
	out := make(map[uint64][32]byte)
	sc := bufio.NewScanner(r)
	line := 0
	for sc.Scan() {
		line++
		text := strings.TrimSpace(sc.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		fields := strings.Fields(text)
		if len(fields) != 2 {
			return nil, fmt.Errorf("bank hash log line %d: want \"<slot> <hash>\", got %q", line, text)
		}
		slot, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("bank hash log line %d: %w", line, err)
		}
		hash, err := solana.HashFromBase58(fields[1])
		if err != nil {
			return nil, fmt.Errorf("bank hash log line %d: %w", line, err)
		}
		out[slot] = hash
	}
	return out, sc.Err()
}

// SpoolStats describes the spool files ImportSpool read.
type SpoolStats struct {
	SlotFiles int      // slot files read, across all directories
	Torn      []uint64 // slots whose file ended in a torn record (its intact records were kept)
}

// ImportSpool makes a replay corpus from a node's shred spool directories (the
// spool itself and the archive MITHRIL_SHRED_ARCHIVE_DIR kept) and the bank
// hashes its replay logged. It streams every slot's shreds, with a bank hash
// record for each logged slot, into ImportCorpus, which keeps the chain of
// shred-header parents ending at the highest complete slot with a bank hash,
// recovering data shreds from coding shreds where needed. The source
// directories are only read.
func ImportSpool(dirs []string, bankHashes map[uint64][32]byte, outDir string, opts ImportOptions) (*Manifest, ImportStats, SpoolStats, error) {
	var spoolStats SpoolStats
	files := map[uint64][]string{}
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return nil, ImportStats{}, spoolStats, err
		}
		for _, e := range entries {
			slot, ok := turbine.SpoolSlotFromName(e.Name())
			if !ok || !e.Type().IsRegular() || slot < opts.FromSlot || (opts.ToSlot != 0 && slot > opts.ToSlot) {
				continue
			}
			files[slot] = append(files[slot], filepath.Join(dir, e.Name()))
		}
	}
	if len(files) == 0 {
		return nil, ImportStats{}, spoolStats, fmt.Errorf("no spooled slots in range in %s", strings.Join(dirs, ", "))
	}
	slots := make([]uint64, 0, len(files))
	for slot := range files {
		slots = append(slots, slot)
	}
	slices.Sort(slots)

	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- writeSpoolCapture(pw, slots, files, bankHashes, &spoolStats)
	}()
	man, stats, err := ImportCorpus(pr, outDir, opts)
	pr.CloseWithError(io.ErrClosedPipe) // unblock the writer if import stopped early
	if werr := <-done; err == nil && werr != nil && werr != io.ErrClosedPipe {
		err = werr
	}
	return man, stats, spoolStats, err
}

// writeSpoolCapture writes slots' spool files to w as a shredcap capture, each
// followed by its bank hash record when one is known.
func writeSpoolCapture(pw *io.PipeWriter, slots []uint64, files map[uint64][]string, bankHashes map[uint64][32]byte, stats *SpoolStats) (err error) {
	defer func() { pw.CloseWithError(err) }()
	w, err := NewWriter(pw, "mithril corpus from-spool")
	if err != nil {
		return err
	}
	for _, slot := range slots {
		last := int64(-1)
		for _, path := range files[slot] {
			packets, torn, err := turbine.ReadSpoolFileIntact(path)
			if err != nil {
				return err
			}
			stats.SlotFiles++
			if torn {
				stats.Torn = append(stats.Torn, slot)
			}
			for _, packet := range packets {
				if shred, err := turbine.ParseShred(packet); err == nil && shred.Type == turbine.ShredTypeData && shred.LastInSlot() {
					last = int64(shred.Index)
				}
				if err := w.WriteShred(packet); err != nil {
					return err
				}
			}
		}
		if hash, ok := bankHashes[slot]; ok {
			if err := w.WriteBankHash(slot, hash, uint64(last+1)); err != nil {
				return err
			}
		}
	}
	return w.Flush()
}
