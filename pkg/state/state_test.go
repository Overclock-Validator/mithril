package state

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/mr-tron/base58"
)

// LastRootedContext (the resume bundle as of the last rooted slot) must survive a JSON round-trip
// through the state file unchanged.
func TestResumeContextRoundTrip(t *testing.T) {
	orig := &MithrilState{
		StateSchemaVersion: CurrentStateSchemaVersion,
		Stage:              "ready",
		SnapshotSlot:       50,
		LastSlot:           110,
		LastRootedSlot:     100,
		LastRootedContext: &ResumeContext{
			Slot:                    100,
			Bankhash:                base58.Encode(bh(0xAA)),
			BlockHeight:             999,
			Epoch:                   7,
			AcctsLtHash:             "bHRoYXNo",
			Clock:                   "Y2xvY2stc3lzdmFyLWFzLW9mLVI=", // guards the Clock field (the prior resume bug)
			LamportsPerSignature:    5000,
			PrevLamportsPerSig:      4999,
			NumSignatures:           42,
			RecentBlockhashes:       []BlockhashEntry{{Blockhash: base58.Encode(bh(0x01)), LamportsPerSignature: 5000}},
			EvictedBlockhash:        base58.Encode(bh(0x02)),
			Blockhash:               base58.Encode(bh(0x03)),
			SlotHashes:              []SlotHashEntry{{Slot: 99, Hash: base58.Encode(bh(0x04))}},
			Capitalization:          1_000_000,
			SlotsPerYear:            78892314.984,
			InflationInitial:        0.08,
			InflationTerminal:       0.015,
			InflationTaper:          0.15,
			InflationFoundation:     0.05,
			InflationFoundationTerm: 7.0,
		},
	}

	data, err := json.Marshal(orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got MithrilState
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(orig.LastRootedContext, got.LastRootedContext) {
		t.Fatalf("LastRootedContext round-trip mismatch:\n orig=%+v\n got=%+v", orig.LastRootedContext, got.LastRootedContext)
	}
}

// mockBankhashDb is an in-memory BankhashGetter for validation tests.
type mockBankhashDb struct {
	hashes map[uint64][]byte
}

func (m *mockBankhashDb) GetBankHashForSlot(slot uint64) ([]byte, error) {
	if h, ok := m.hashes[slot]; ok {
		return h, nil
	}
	return nil, fmt.Errorf("no bankhash for slot %d", slot)
}

func bh(b byte) []byte {
	h := make([]byte, 32)
	for i := range h {
		h[i] = b
	}
	return h
}

// GetResumeSlot must resume from the last DURABLE slot. In rooted-durable mode
// everything after the last rooted slot is RAM-only and lost on restart, so resume starts right after the rooted checkpoint
// — NOT C+1. Legacy mode (LastRootedSlot==0) keeps the old LastSlot+1 behavior.
func TestGetResumeSlot_RootedVsLegacy(t *testing.T) {
	tests := []struct {
		name           string
		snapshotSlot   uint64
		lastSlot       uint64
		lastRootedSlot uint64
		want           uint64
	}{
		{"rooted: resume from R+1 not C+1", 50, 110, 100, 101},
		{"rooted: R just behind C", 50, 101, 100, 101},
		{"legacy: resume from LastSlot+1", 50, 110, 0, 111},
		{"fresh: resume from SnapshotSlot+1", 50, 0, 0, 51},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &MithrilState{SnapshotSlot: tt.snapshotSlot, LastSlot: tt.lastSlot, LastRootedSlot: tt.lastRootedSlot}
			if got := s.GetResumeSlot(); got != tt.want {
				t.Fatalf("GetResumeSlot()=%d, want %d", got, tt.want)
			}
		})
	}
}

// DurableHighWater reports the highest slot durably committed to AccountsDB +
// bankhash_db: R in rooted mode, the replayed/snapshot slot in legacy mode.
func TestDurableHighWater(t *testing.T) {
	tests := []struct {
		name           string
		snapshotSlot   uint64
		lastSlot       uint64
		lastRootedSlot uint64
		want           uint64
	}{
		{"rooted", 50, 110, 100, 100},
		{"legacy replayed", 50, 110, 0, 110},
		{"legacy fresh", 50, 0, 0, 50},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &MithrilState{SnapshotSlot: tt.snapshotSlot, LastSlot: tt.lastSlot, LastRootedSlot: tt.lastRootedSlot}
			if got := s.DurableHighWater(); got != tt.want {
				t.Fatalf("DurableHighWater()=%d, want %d", got, tt.want)
			}
		})
	}
}

// In rooted mode, ValidateAgainstBankhashDB must assert the durable high-water
// is exactly R: bankhash_db has an entry at R (matching LastRootedBankhash).
// Rows beyond R are tolerated now (fold bankhashes are written NoSync and a
// batch can carry rows for slots above a partially-advanced state file).
func TestValidateAgainstBankhashDB_RootedMode(t *testing.T) {
	t.Run("clean: db high-water == R", func(t *testing.T) {
		s := &MithrilState{LastSlot: 110, LastRootedSlot: 100, LastRootedBankhash: base58.Encode(bh(0xAA))}
		db := &mockBankhashDb{hashes: map[uint64][]byte{100: bh(0xAA)}}
		if err := s.ValidateAgainstBankhashDB(db); err != nil {
			t.Fatalf("expected clean, got: %v", err)
		}
	})

	t.Run("bankhash beyond R is tolerated (NoSync fold rows)", func(t *testing.T) {
		// Batch folds write bankhash rows NoSync and RecoverFoldState is the
		// commit authority — rows beyond the state file's R are expected after
		// a hard kill, not evidence of a torn write.
		s := &MithrilState{LastSlot: 110, LastRootedSlot: 100, LastRootedBankhash: base58.Encode(bh(0xAA))}
		db := &mockBankhashDb{hashes: map[uint64][]byte{100: bh(0xAA), 101: bh(0xBB)}}
		if err := s.ValidateAgainstBankhashDB(db); err != nil {
			t.Fatalf("bankhash rows beyond R must be tolerated, got: %v", err)
		}
	})

	t.Run("missing: no bankhash at R", func(t *testing.T) {
		s := &MithrilState{LastSlot: 110, LastRootedSlot: 100, LastRootedBankhash: base58.Encode(bh(0xAA))}
		db := &mockBankhashDb{hashes: map[uint64][]byte{}}
		if err := s.ValidateAgainstBankhashDB(db); err == nil {
			t.Fatal("expected error for missing bankhash at R, got nil")
		}
	})

	t.Run("mismatch: db bankhash at R differs from state", func(t *testing.T) {
		s := &MithrilState{LastSlot: 110, LastRootedSlot: 100, LastRootedBankhash: base58.Encode(bh(0xAA))}
		db := &mockBankhashDb{hashes: map[uint64][]byte{100: bh(0xBB)}}
		if err := s.ValidateAgainstBankhashDB(db); err == nil {
			t.Fatal("expected error for bankhash mismatch at R, got nil")
		}
	})
}

// Legacy mode (LastRootedSlot==0) must behave exactly as before: assert against
// LastSlot.
func TestValidateAgainstBankhashDB_LegacyUnchanged(t *testing.T) {
	t.Run("clean legacy", func(t *testing.T) {
		s := &MithrilState{LastSlot: 110, LastBankhash: base58.Encode(bh(0xCC))}
		db := &mockBankhashDb{hashes: map[uint64][]byte{110: bh(0xCC)}}
		if err := s.ValidateAgainstBankhashDB(db); err != nil {
			t.Fatalf("expected clean, got: %v", err)
		}
	})

	t.Run("torn legacy: bankhash beyond LastSlot", func(t *testing.T) {
		s := &MithrilState{LastSlot: 110, LastBankhash: base58.Encode(bh(0xCC))}
		db := &mockBankhashDb{hashes: map[uint64][]byte{110: bh(0xCC), 111: bh(0xDD)}}
		if err := s.ValidateAgainstBankhashDB(db); err == nil {
			t.Fatal("expected error for bankhash beyond LastSlot, got nil")
		}
	})
}

// Older state files carry manifest_epoch_authorized_voters (removed in the
// Alpenglow-only build). Loading such a file must succeed — unknown JSON
// fields are ignored — so upgrades don't force a re-bootstrap.
func TestStateFileWithRemovedAuthorizedVotersFieldStillLoads(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, StateFileName)
	legacy := fmt.Sprintf(`{
		"state_schema_version": %d,
		"stage": "ready",
		"last_slot": 123,
		"manifest_epoch_authorized_voters": {
			"Vote111111111111111111111111111111111111111": ["Voter11111111111111111111111111111111111111"]
		}
	}`, CurrentStateSchemaVersion)
	if err := os.WriteFile(path, []byte(legacy), 0o644); err != nil {
		t.Fatalf("write legacy state file: %v", err)
	}
	st, err := LoadState(dir)
	if err != nil {
		t.Fatalf("LoadState returned error for legacy state file: %v", err)
	}
	if st == nil {
		t.Fatal("LoadState returned nil state")
	}
	if st.LastSlot != 123 {
		t.Fatalf("LastSlot = %d, want 123", st.LastSlot)
	}
}
