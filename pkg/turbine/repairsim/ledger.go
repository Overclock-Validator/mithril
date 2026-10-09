// Package repairsim provides a deterministic, single-process repair harness
// around Mithril's production Turbine shred generator and slot assembler.
//
// The synthetic ledger and network are test infrastructure. Shred parsing,
// Merkle/signature validation, repair selection, Reed-Solomon reconstruction,
// component decoding, transaction verification, and completion accounting are
// production code paths.
package repairsim

import (
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
)

const (
	dataShredsPerFEC = 32
	codeShredsPerFEC = 32
)

// LedgerConfig controls deterministic canonical-ledger generation.
type LedgerConfig struct {
	StartSlot      uint64 `json:"start_slot"`
	Slots          int    `json:"slots"`
	FECsPerSlot    int    `json:"fec_sets_per_slot"`
	EntriesPerSlot int    `json:"entries_per_slot,omitempty"`
	Seed           int64  `json:"seed"`
	ShredVersion   uint16 `json:"shred_version"`
	ReferenceTick  uint8  `json:"reference_tick"`
}

// Packet is one canonical wire packet and its parsed routing metadata.
type Packet struct {
	Bytes       []byte
	Slot        uint64
	Type        turbine.ShredType
	Index       uint32
	FECSetIndex uint32
	Position    uint16
}

// FECSet contains the canonical packets for one 32+32 FEC set.
type FECSet struct {
	Index  uint32
	Data   []Packet
	Coding []Packet
}

// Slot is the complete canonical source for one generated slot.
type Slot struct {
	Number     uint64
	ParentSlot uint64
	Entries    []turbine.Entry
	FECs       []FECSet
	Data       map[uint32]Packet
	Highest    uint32
}

// Ledger is the complete data held by the in-process repair peer.
type Ledger struct {
	Config    LedgerConfig
	Leader    solana.PrivateKey
	LeaderPub solana.PublicKey
	Slots     []Slot
	bySlot    map[uint64]*Slot
}

// Slot returns a canonical slot by number.
func (l *Ledger) Slot(number uint64) (*Slot, bool) {
	if l == nil {
		return nil, false
	}
	s, ok := l.bySlot[number]
	return s, ok
}

// GenerateLedger creates authentic signed Merkle shreds using the production
// 32+32 generator. Entries contain no transactions: this isolates repair,
// reconstruction, parsing, and storage while still exercising the real block
// component codec and completion path.
func GenerateLedger(cfg LedgerConfig) (*Ledger, error) {
	if cfg.Slots <= 0 {
		return nil, fmt.Errorf("slots must be positive")
	}
	if cfg.FECsPerSlot <= 0 {
		return nil, fmt.Errorf("FEC sets per slot must be positive")
	}
	if cfg.StartSlot == 0 {
		cfg.StartSlot = 10_000
	}
	if cfg.ReferenceTick == 0 {
		cfg.ReferenceTick = 63
	}

	leader := deterministicLeader(cfg.Seed)
	entryCount := cfg.EntriesPerSlot
	if entryCount == 0 {
		var err error
		entryCount, err = findEntryCount(cfg, leader)
		if err != nil {
			return nil, err
		}
	}

	ledger := &Ledger{
		Config:    cfg,
		Leader:    leader,
		LeaderPub: leader.PublicKey(),
		Slots:     make([]Slot, 0, cfg.Slots),
		bySlot:    make(map[uint64]*Slot, cfg.Slots),
	}
	ledger.Config.EntriesPerSlot = entryCount
	for i := 0; i < cfg.Slots; i++ {
		number := cfg.StartSlot + uint64(i)
		parent := number - 1
		entries := deterministicEntries(cfg.Seed, number, entryCount)
		slot, err := generateSlot(cfg, leader, number, parent, entries)
		if err != nil {
			return nil, fmt.Errorf("generate slot %d: %w", number, err)
		}
		if got := len(slot.FECs); got != cfg.FECsPerSlot {
			return nil, fmt.Errorf("slot %d produced %d FEC sets, want %d (entries=%d)", number, got, cfg.FECsPerSlot, entryCount)
		}
		ledger.Slots = append(ledger.Slots, slot)
		ledger.bySlot[number] = &ledger.Slots[len(ledger.Slots)-1]
	}
	return ledger, nil
}

func deterministicLeader(seed int64) solana.PrivateKey {
	var input [16]byte
	for i := range input {
		input[i] = byte(uint64(seed)>>uint((i%8)*8)) ^ byte(i*29+7)
	}
	digest := sha256.Sum256(append([]byte("mithril-repair-sim-leader-v1"), input[:]...))
	return solana.PrivateKey(ed25519.NewKeyFromSeed(digest[:]))
}

func deterministicEntries(seed int64, slot uint64, count int) []turbine.Entry {
	entries := make([]turbine.Entry, count)
	for i := range entries {
		material := fmt.Sprintf("mithril-repair-sim-entry-v1:%d:%d:%d", seed, slot, i)
		h := sha256.Sum256([]byte(material))
		entries[i] = turbine.Entry{NumHashes: 1, Hash: solana.Hash(h)}
	}
	return entries
}

// findEntryCount uses the generator itself as the capacity oracle. This avoids
// duplicating signed-last-FEC payload constants in the harness.
func findEntryCount(cfg LedgerConfig, leader solana.PrivateKey) (int, error) {
	lo, hi := 1, cfg.FECsPerSlot*900
	for lo < hi {
		mid := lo + (hi-lo)/2
		entries := deterministicEntries(cfg.Seed, cfg.StartSlot, mid)
		slot, err := generateSlot(cfg, leader, cfg.StartSlot, cfg.StartSlot-1, entries)
		if err != nil {
			return 0, err
		}
		if len(slot.FECs) < cfg.FECsPerSlot {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	entries := deterministicEntries(cfg.Seed, cfg.StartSlot, lo)
	slot, err := generateSlot(cfg, leader, cfg.StartSlot, cfg.StartSlot-1, entries)
	if err != nil {
		return 0, err
	}
	if len(slot.FECs) != cfg.FECsPerSlot {
		return 0, fmt.Errorf("cannot derive %d FEC sets within %d entries (got %d)", cfg.FECsPerSlot, hi, len(slot.FECs))
	}
	return lo, nil
}

func generateSlot(cfg LedgerConfig, leader solana.PrivateKey, number, parent uint64, entries []turbine.Entry) (Slot, error) {
	component, err := turbine.NewEntryBatch(entries)
	if err != nil {
		return Slot{}, err
	}
	shredder := turbine.Shredder{
		Slot:          number,
		ParentSlot:    parent,
		Version:       cfg.ShredVersion,
		ReferenceTick: cfg.ReferenceTick,
	}
	batch, _, _, err := shredder.MakeMerkleShredsFromComponent(
		leader, component, true, solana.Hash{}, 0, 0,
	)
	if err != nil {
		return Slot{}, err
	}

	byFEC := make(map[uint32]*FECSet)
	packetByKey := make(map[packetKey][]byte, len(batch.Packets))
	for _, raw := range batch.Packets {
		shred, err := turbine.ParseShred(raw)
		if err != nil {
			return Slot{}, err
		}
		key := keyForShred(shred)
		packetByKey[key] = append([]byte(nil), raw...)
	}
	data := make(map[uint32]Packet, len(batch.DataShreds))
	var highest uint32
	for _, shred := range append(append([]*turbine.Shred(nil), batch.DataShreds...), batch.CodeShreds...) {
		fec := byFEC[shred.FECSetIndex]
		if fec == nil {
			fec = &FECSet{Index: shred.FECSetIndex}
			byFEC[shred.FECSetIndex] = fec
		}
		packet := Packet{
			Bytes:       packetByKey[keyForShred(shred)],
			Slot:        shred.Slot,
			Type:        shred.Type,
			Index:       shred.Index,
			FECSetIndex: shred.FECSetIndex,
			Position:    shred.Position,
		}
		if shred.Type == turbine.ShredTypeData {
			fec.Data = append(fec.Data, packet)
			data[shred.Index] = packet
			if shred.Index > highest {
				highest = shred.Index
			}
		} else {
			fec.Coding = append(fec.Coding, packet)
		}
	}
	fecs := make([]FECSet, 0, len(byFEC))
	for index := uint32(0); len(fecs) < len(byFEC); index += dataShredsPerFEC {
		fec := byFEC[index]
		if fec == nil {
			return Slot{}, fmt.Errorf("non-contiguous FEC sets: missing %d", index)
		}
		if len(fec.Data) != dataShredsPerFEC || len(fec.Coding) != codeShredsPerFEC {
			return Slot{}, fmt.Errorf("FEC %d has %d+%d shreds", index, len(fec.Data), len(fec.Coding))
		}
		fecs = append(fecs, *fec)
	}
	return Slot{Number: number, ParentSlot: parent, Entries: entries, FECs: fecs, Data: data, Highest: highest}, nil
}

type packetKey struct {
	type_    turbine.ShredType
	index    uint32
	fec      uint32
	position uint16
}

func keyForShred(shred *turbine.Shred) packetKey {
	return packetKey{type_: shred.Type, index: shred.Index, fec: shred.FECSetIndex, position: shred.Position}
}
