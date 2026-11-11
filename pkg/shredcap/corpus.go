package shredcap

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
)

// A replay corpus is a directory holding shreds/s<slot>.shreds (shred spool
// files), complete.idx (the spool's completion index) and manifest.json. The
// benchmark runner restores the shreds into a node's catch-up spool and
// replays the manifest's slots from its checkpoint with --offline, comparing
// each block's bank hash with the manifest.

// Manifest is a corpus's manifest.json. Captures made by Mithril also carry
// transaction counts and logged CU per slot; a shredcap import has only bank
// hashes, so those fields are absent.
type Manifest struct {
	Checkpoint        uint64             `json:"checkpoint"`
	Head              uint64             `json:"head"`
	Blocks            int                `json:"blocks"`
	Slots             []uint64           `json:"slots"`
	BankHashes        map[string]string  `json:"bankhashes"`
	TransactionCounts map[string][2]int  `json:"transaction_counts,omitempty"`
	LoggedCUMillions  map[string]float64 `json:"logged_cu_millions,omitempty"`
	ShredBytes        int64              `json:"shred_bytes"`
	Source            string             `json:"source,omitempty"`
	CapturedUTC       string             `json:"captured_utc,omitempty"`
}

// ImportOptions limits an import.
type ImportOptions struct {
	// FromSlot and ToSlot bound the imported slots (inclusive); zero ToSlot
	// means no upper bound.
	FromSlot, ToSlot uint64
	// Source is recorded in the manifest (for example the capture's file name).
	Source string
}

// ImportStats describes what an import read and kept.
type ImportStats struct {
	Shreds        int      // shred packets read
	Unparsed      int      // packets that do not parse as shreds
	Duplicates    int      // repeated shreds, stored once
	OutOfRange    int      // shreds outside FromSlot..ToSlot
	BankHashes    int      // bank hash records read
	Roots         int      // root slot records read
	CompleteSlots int      // slots with every data shred up to the last
	Incomplete    []uint64 // slots missing data shreds (not replayable)
	OffChain      []uint64 // complete slots not on the replayed chain (discarded)
	CountMismatch []uint64 // chain slots whose data shred count differs from the bank hash record
	NoBankHash    []uint64 // chain slots without a bank hash record
	// ParentBreaks are chain slots whose shred-header parent is not the
	// previous chain slot: a rooted slot is missing or incomplete there, or
	// an Alpenglow UpdateParent marker named an older parent.
	ParentBreaks []uint64
}

type slotShreds struct {
	data      []bool // data shred indices held
	last      int64  // index of the LAST_SHRED_IN_SLOT data shred, -1 until seen
	parent    uint64
	hasParent bool
}

// ImportCorpus reads a shredcap capture and writes a replay corpus to outDir,
// which must not exist. The replayed chain is the capture's rooted slots when
// it has root records, else the chain of shred-header parents ending at the
// highest complete slot with a bank hash. The checkpoint is the parent of the
// chain's first slot: the node must already hold that slot's state.
func ImportCorpus(capture io.Reader, outDir string, opts ImportOptions) (*Manifest, ImportStats, error) {
	var stats ImportStats
	if _, err := os.Stat(outDir); err == nil {
		return nil, stats, fmt.Errorf("%s already exists", outDir)
	}
	rd, err := NewReader(capture)
	if err != nil {
		return nil, stats, err
	}
	defer rd.Close()
	spoolDir := filepath.Join(outDir, "shreds")
	spool, err := turbine.OpenShredSpool(spoolDir, math.MaxInt64/2)
	if err != nil {
		return nil, stats, err
	}
	closed := false
	defer func() {
		if !closed {
			spool.Close()
		}
	}()

	slots := make(map[uint64]*slotShreds)
	hashes := make(map[uint64]Record)
	rooted := make(map[uint64]struct{})
	inRange := func(slot uint64) bool {
		return slot >= opts.FromSlot && (opts.ToSlot == 0 || slot <= opts.ToSlot)
	}
	for {
		rec, err := rd.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, stats, err
		}
		switch rec.Kind {
		case KindBankHash:
			stats.BankHashes++
			hashes[rec.Slot] = rec
		case KindRootSlot:
			stats.Roots++
			rooted[rec.Slot] = struct{}{}
		case KindShred:
			stats.Shreds++
			shred, err := turbine.ParseShred(rec.Shred)
			if err != nil {
				stats.Unparsed++
				continue
			}
			if !inRange(shred.Slot) {
				stats.OutOfRange++
				continue
			}
			// Payload is the shred at its canonical size, without the nonce
			// a repair response appends.
			if !spool.AppendShred(shred, shred.Payload) {
				stats.Duplicates++
				continue
			}
			st := slots[shred.Slot]
			if st == nil {
				st = &slotShreds{last: -1}
				slots[shred.Slot] = st
			}
			if shred.Type != turbine.ShredTypeData {
				continue
			}
			for int(shred.Index) >= len(st.data) {
				st.data = append(st.data, false)
			}
			st.data[shred.Index] = true
			if shred.LastInSlot() {
				st.last = int64(shred.Index)
			}
			if !st.hasParent {
				st.parent, st.hasParent = shred.ParentSlot(), true
			}
		}
	}

	complete := make(map[uint64]*slotShreds)
	for slot, st := range slots {
		if st.last >= 0 && int(st.last) < len(st.data) && !slices.Contains(st.data[:st.last+1], false) {
			complete[slot] = st
		} else {
			stats.Incomplete = append(stats.Incomplete, slot)
		}
	}
	slices.Sort(stats.Incomplete)
	stats.CompleteSlots = len(complete)

	chain := replayChain(complete, hashes, rooted)
	if len(chain) == 0 {
		return nil, stats, errors.New("no replayable chain: the capture has no complete slot with a bank hash or root record")
	}
	onChain := make(map[uint64]bool, len(chain))
	for _, slot := range chain {
		onChain[slot] = true
	}
	for slot := range slots {
		if !onChain[slot] {
			if _, ok := complete[slot]; ok {
				stats.OffChain = append(stats.OffChain, slot)
			}
			spool.DiscardSlot(slot)
		}
	}
	slices.Sort(stats.OffChain)

	man := &Manifest{
		Checkpoint:  complete[chain[0]].parent,
		Head:        chain[len(chain)-1],
		Blocks:      len(chain),
		Slots:       chain,
		BankHashes:  make(map[string]string, len(chain)),
		Source:      opts.Source,
		CapturedUTC: time.Now().UTC().Format(time.RFC3339),
	}
	for i, slot := range chain {
		st := complete[slot]
		if i > 0 && st.parent != chain[i-1] {
			stats.ParentBreaks = append(stats.ParentBreaks, slot)
		}
		spool.MarkComplete(slot, uint32(st.last), uint32(st.last+1))
		h, ok := hashes[slot]
		if !ok {
			stats.NoBankHash = append(stats.NoBankHash, slot)
			continue
		}
		man.BankHashes[fmt.Sprint(slot)] = solana.Hash(h.BankHash).String()
		if h.DataShreds != uint64(st.last+1) {
			stats.CountMismatch = append(stats.CountMismatch, slot)
		}
	}
	spool.Close()
	closed = true
	// Discarding off-chain slots left tombstones in the completion index.
	// Opening the spool again rewrites the index with only live slots.
	compacted, err := turbine.OpenShredSpool(spoolDir, math.MaxInt64/2)
	if err != nil {
		return nil, stats, err
	}
	compacted.Close()

	// The runner expects the completion index beside shreds/, not inside it.
	if err := os.Rename(filepath.Join(spoolDir, turbine.SpoolJournalName), filepath.Join(outDir, turbine.SpoolJournalName)); err != nil {
		return nil, stats, err
	}
	for _, slot := range chain {
		info, err := os.Stat(filepath.Join(spoolDir, turbine.SpoolFileName(slot)))
		if err != nil {
			return nil, stats, err
		}
		man.ShredBytes += info.Size()
	}
	if err := writeManifest(filepath.Join(outDir, "manifest.json"), man); err != nil {
		return nil, stats, err
	}
	return man, stats, nil
}

// replayChain picks the slots to replay, in order.
func replayChain(complete map[uint64]*slotShreds, hashes map[uint64]Record, rooted map[uint64]struct{}) []uint64 {
	var chain []uint64
	if len(rooted) > 0 {
		for slot := range rooted {
			if _, ok := complete[slot]; ok {
				chain = append(chain, slot)
			}
		}
		slices.Sort(chain)
		return chain
	}
	var head uint64
	found := false
	for slot := range complete {
		if _, ok := hashes[slot]; ok && (!found || slot > head) {
			head, found = slot, true
		}
	}
	if !found {
		return nil
	}
	for slot := head; ; {
		chain = append(chain, slot)
		st := complete[slot]
		if !st.hasParent || st.parent >= slot {
			break
		}
		if _, ok := complete[st.parent]; !ok {
			break
		}
		slot = st.parent
	}
	slices.Reverse(chain)
	return chain
}

func writeManifest(path string, man *Manifest) error {
	data, err := json.MarshalIndent(man, "", " ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}

// ReadManifest reads a corpus's manifest.json.
func ReadManifest(corpusDir string) (*Manifest, error) {
	data, err := os.ReadFile(filepath.Join(corpusDir, "manifest.json"))
	if err != nil {
		return nil, err
	}
	var man Manifest
	if err := json.Unmarshal(data, &man); err != nil {
		return nil, fmt.Errorf("%s: %w", corpusDir, err)
	}
	return &man, nil
}

// ExportCorpus writes a corpus's slots as a shredcap capture: each slot's
// shreds, its bank hash record (with the slot's data shred count) and a root
// record, in slot order. The corpus is only read.
func ExportCorpus(corpusDir string, w io.Writer, application string) (int, error) {
	man, err := ReadManifest(corpusDir)
	if err != nil {
		return 0, err
	}
	out, err := NewWriter(w, application)
	if err != nil {
		return 0, err
	}
	for _, slot := range man.Slots {
		packets, err := turbine.ReadSpoolFile(filepath.Join(corpusDir, "shreds", turbine.SpoolFileName(slot)))
		if err != nil {
			return 0, err
		}
		var dataShreds uint64
		for _, packet := range packets {
			if shred, err := turbine.ParseShred(packet); err == nil && shred.Slot == slot && shred.Type == turbine.ShredTypeData {
				dataShreds = max(dataShreds, uint64(shred.Index)+1)
			}
			if err := out.WriteShred(packet); err != nil {
				return 0, err
			}
		}
		encoded, ok := man.BankHashes[fmt.Sprint(slot)]
		if !ok {
			return 0, fmt.Errorf("corpus has no bank hash for slot %d", slot)
		}
		hash, err := solana.HashFromBase58(encoded)
		if err != nil {
			return 0, fmt.Errorf("slot %d bank hash: %w", slot, err)
		}
		if err := out.WriteBankHash(slot, hash, dataShreds); err != nil {
			return 0, err
		}
		if err := out.WriteRootSlot(slot); err != nil {
			return 0, err
		}
	}
	return len(man.Slots), out.Flush()
}
