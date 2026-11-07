package block

import (
	"encoding/json"
	"math"
	"testing"
	"unsafe"
)

func TestSelectedEconomicsFeeValues(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		execution, priority, total uint64
		known                      bool
	}{
		{name: "zero", known: true},
		{name: "execution_and_priority", execution: 145000, priority: 2, total: 145002, known: true},
		{name: "maximum", execution: math.MaxUint64, total: math.MaxUint64, known: true},
		{name: "inconsistent", execution: 1, total: 2},
		{name: "overflow", execution: math.MaxUint64, priority: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var b Block
			b.EnableSelectedEconomics()
			b.RecordSelectedFees(1, 0, 1)
			b.RecordSelectedFees(tc.execution, tc.priority, tc.total)
			got, enabled := b.SelectedEconomics()
			if !enabled || got.FeesKnown != tc.known {
				t.Fatalf("enabled=%v known=%v, want true/%v", enabled, got.FeesKnown, tc.known)
			}
			want := SelectedFeeFacts{}
			if tc.known {
				want = SelectedFeeFacts{ExecutionLamports: tc.execution, PriorityLamports: tc.priority, TotalLamports: tc.total}
			}
			if got.Fees != want {
				t.Fatalf("fees=%+v, want %+v", got.Fees, want)
			}
		})
	}
}

func TestSelectedEconomicsResetCopyAndPrivateEncoding(t *testing.T) {
	b := &Block{Slot: 42}
	before, err := json.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	b.EnableSelectedEconomics()
	b.RecordSelectedFees(10, 2, 12)
	copy, _ := b.SelectedEconomics()
	copy.Fees.TotalLamports = 99
	got, _ := b.SelectedEconomics()
	if got.Fees.TotalLamports != 12 {
		t.Fatal("returned value aliases private capture")
	}
	after, err := json.Marshal(b)
	if err != nil || string(before) != string(after) {
		t.Fatalf("private capture changed block JSON: %v", err)
	}
	var decoded Block
	if err := json.Unmarshal(after, &decoded); err != nil {
		t.Fatal(err)
	}
	if _, enabled := decoded.SelectedEconomics(); enabled {
		t.Fatal("serialized block injected a capture request")
	}
	b.ResetSelectedFees()
	got, enabled := b.SelectedEconomics()
	if !enabled || got != (SelectedEconomics{}) {
		t.Fatal("reset retained prior fee values or disabled the request")
	}
	b.RecordSelectedFees(10, 2, 12)
	b.EnableSelectedEconomics()
	got, enabled = b.SelectedEconomics()
	if !enabled || got != (SelectedEconomics{}) {
		t.Fatal("fresh execution request reused prior facts")
	}
}

func TestSelectedEconomicsDisabledAndLayout(t *testing.T) {
	var absent *Block
	absent.EnableSelectedEconomics()
	absent.EnableSelectedLocalEconomics()
	var disabled Block
	if allocations := testing.AllocsPerRun(100, func() {
		absent.ResetSelectedFees()
		absent.RecordSelectedFees(1, 0, 1)
		disabled.ResetSelectedFees()
		disabled.RecordSelectedFees(1, 0, 1)
		if got, enabled := disabled.SelectedEconomics(); enabled || got != (SelectedEconomics{}) {
			t.Fatal("disabled capture became available")
		}
		if got, enabled := disabled.SelectedEconomicsFor(0, [32]byte{}, [32]byte{}, 0); enabled || got != (SelectedEconomics{}) {
			t.Fatal("disabled carrier became available")
		}
		if disabled.SealSelectedLocalEconomics(0, [32]byte{}, 0, SelectedNativeCost{}) || absent.SealSelectedLocalEconomics(0, [32]byte{}, 0, SelectedNativeCost{}) {
			t.Fatal("disabled request was sealed")
		}
	}); allocations != 0 {
		t.Fatalf("disabled capture allocated %v times", allocations)
	}
	if size := unsafe.Sizeof(selectedEconomicsContext{}); size > 160 {
		t.Fatalf("private context=%d bytes exceeds 160", size)
	} else {
		t.Logf("private selected context=%d bytes; detached economics=%d bytes", size, unsafe.Sizeof(SelectedEconomics{}))
	}
}

func TestSelectedFeeBurnKnownnessAndReset(t *testing.T) {
	b := new(Block)
	b.RecordSelectedFeeBurn(1)
	b.EnableSelectedEconomics()
	b.RecordSelectedFeeBurn(0)
	if got, _ := b.SelectedEconomics(); got.FeeBurnKnown {
		t.Fatal("burn was known without a fee result")
	}
	b.RecordSelectedFees(10, 2, 12)
	b.RecordSelectedFeeBurn(5)
	got, _ := b.SelectedEconomics()
	if !got.FeeBurnKnown || got.FeeBurnLamports != 5 {
		t.Fatal("known burn lost")
	}
	b.RecordSelectedFeeBurn(13)
	got, _ = b.SelectedEconomics()
	if got.FeeBurnKnown || got.FeeBurnLamports != 0 || !got.FeesKnown {
		t.Fatal("invalid burn did not remain independently unknown")
	}
	b.RecordSelectedFeeBurn(0)
	got, _ = b.SelectedEconomics()
	if !got.FeeBurnKnown || got.FeeBurnLamports != 0 {
		t.Fatal("known zero burn lost")
	}
	b.ResetSelectedFees()
	got, _ = b.SelectedEconomics()
	if got.FeeBurnKnown || got.FeeBurnLamports != 0 {
		t.Fatal("reset retained an old burn")
	}
}

func selectedLocalBlock() Block {
	b := Block{Slot: 42, AlpenglowBlockID: [32]byte{3}, HasAlpenglowBlockID: true,
		ExpectedBankhash: [32]byte{4}, HasExpectedBankhash: true}
	b.Transactions = append(b.Transactions, nil)
	b.EnableSelectedLocalEconomics()
	b.RecordSelectedFees(10, 2, 12)
	return b
}

func TestSelectedEconomicsLocalSealAndDetachedValues(t *testing.T) {
	for _, cost := range []SelectedNativeCost{{}, {BlockCost: 90, BlockCostLimit: 80}, {BlockCost: math.MaxUint64, BlockCostLimit: math.MaxUint64}} {
		b := selectedLocalBlock()
		before, err := json.Marshal(&b)
		if err != nil {
			t.Fatal(err)
		}
		if got, enabled := b.SelectedEconomics(); !enabled || got != (SelectedEconomics{}) {
			t.Fatal("unsealed local fees escaped the identity guard")
		}
		if !b.SealSelectedLocalEconomics(42, b.ExpectedBankhash, 1, cost) {
			t.Fatal("valid local seal refused")
		}
		want := SelectedEconomics{FeesKnown: true, Fees: SelectedFeeFacts{10, 2, 12}, NativeCostKnown: true, NativeCost: cost}
		got, enabled := b.SelectedEconomicsFor(42, b.AlpenglowBlockID, b.ExpectedBankhash, 1)
		if !enabled || got != want {
			t.Fatalf("local facts=%+v/%v, want %+v", got, enabled, want)
		}
		got.Fees.TotalLamports, got.NativeCost.BlockCost = 999, 999
		again, _ := b.SelectedEconomicsFor(42, b.AlpenglowBlockID, b.ExpectedBankhash, 1)
		if again != want || b.SealSelectedLocalEconomics(42, b.ExpectedBankhash, 1, SelectedNativeCost{1, 1}) {
			t.Fatal("copy or repeated seal replaced original values")
		}
		if raw, _ := b.SelectedEconomics(); raw != (SelectedEconomics{}) {
			t.Fatal("local values escaped without carrier identity")
		}
		after, err := json.Marshal(&b)
		if err != nil || string(before) != string(after) {
			t.Fatalf("sealing changed public block JSON: %v", err)
		}
		b.FromLocalProduction = true
		if allocations := testing.AllocsPerRun(100, func() {
			adopted, requested := b.SelectedEconomicsFor(42, b.AlpenglowBlockID, b.ExpectedBankhash, 1)
			if !requested || adopted != want {
				t.Fatal("local adoption lost the sealed value")
			}
		}); allocations != 0 {
			t.Fatalf("detached carrier getter allocated %v times", allocations)
		}
		b.ResetSelectedFees()
		if reset, _ := b.SelectedEconomicsFor(42, b.AlpenglowBlockID, b.ExpectedBankhash, 1); reset != (SelectedEconomics{}) {
			t.Fatal("reset retained sealed facts")
		}
	}
	// Cost can be known even when fee distribution supplied no valid result.
	b := selectedLocalBlock()
	b.ResetSelectedFees()
	if !b.SealSelectedLocalEconomics(42, b.ExpectedBankhash, 1, SelectedNativeCost{}) {
		t.Fatal("unknown fees prevented independent cost capture")
	}
	got, _ := b.SelectedEconomicsFor(42, b.AlpenglowBlockID, b.ExpectedBankhash, 1)
	if got.FeesKnown || !got.NativeCostKnown || got.NativeCost != (SelectedNativeCost{}) {
		t.Fatalf("wrong independent known-zero semantics: %+v", got)
	}
}

func TestSelectedEconomicsLocalBindingRefusals(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Block, *uint64, *[32]byte, *[32]byte, *uint64)
	}{
		{"carrier_slot", func(_ *Block, slot *uint64, _, _ *[32]byte, _ *uint64) { *slot++ }},
		{"carrier_id", func(_ *Block, _ *uint64, id, _ *[32]byte, _ *uint64) { *id = [32]byte{9} }},
		{"carrier_hash", func(_ *Block, _ *uint64, _, hash *[32]byte, _ *uint64) { *hash = [32]byte{9} }},
		{"carrier_count", func(_ *Block, _ *uint64, _, _ *[32]byte, count *uint64) { *count++ }},
		{"block_slot", func(b *Block, _ *uint64, _, _ *[32]byte, _ *uint64) { b.Slot++ }},
		{"block_id", func(b *Block, _ *uint64, _, _ *[32]byte, _ *uint64) { b.AlpenglowBlockID = [32]byte{9} }},
		{"missing_id", func(b *Block, _ *uint64, _, _ *[32]byte, _ *uint64) { b.HasAlpenglowBlockID = false }},
		{"block_hash", func(b *Block, _ *uint64, _, _ *[32]byte, _ *uint64) { b.ExpectedBankhash = [32]byte{9} }},
		{"missing_hash", func(b *Block, _ *uint64, _, _ *[32]byte, _ *uint64) { b.HasExpectedBankhash = false }},
		{"block_count", func(b *Block, _ *uint64, _, _ *[32]byte, _ *uint64) { b.Transactions = append(b.Transactions, nil) }},
		{"unsealed", func(b *Block, _ *uint64, _, _ *[32]byte, _ *uint64) { b.ResetSelectedFees() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := selectedLocalBlock()
			slot, id, hash, count := b.Slot, b.AlpenglowBlockID, b.ExpectedBankhash, uint64(1)
			if !b.SealSelectedLocalEconomics(slot, hash, count, SelectedNativeCost{7, 8}) {
				t.Fatal("fixture did not seal")
			}
			tc.change(&b, &slot, &id, &hash, &count)
			if got, enabled := b.SelectedEconomicsFor(slot, id, hash, count); !enabled || got != (SelectedEconomics{}) {
				t.Fatalf("mismatched local facts escaped: %+v/%v", got, enabled)
			}
		})
	}
}

func TestSelectedEconomicsSealRefusalsAndRemoteFees(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Block, *uint64, *[32]byte, *uint64)
	}{
		{"slot", func(_ *Block, slot *uint64, _ *[32]byte, _ *uint64) { *slot++ }},
		{"zero_hash", func(_ *Block, _ *uint64, hash *[32]byte, _ *uint64) { *hash = [32]byte{} }},
		{"hash", func(_ *Block, _ *uint64, hash *[32]byte, _ *uint64) { *hash = [32]byte{8} }},
		{"count", func(_ *Block, _ *uint64, _ *[32]byte, count *uint64) { *count++ }},
		{"zero_id", func(b *Block, _ *uint64, _ *[32]byte, _ *uint64) { b.AlpenglowBlockID = [32]byte{} }},
		{"no_id", func(b *Block, _ *uint64, _ *[32]byte, _ *uint64) { b.HasAlpenglowBlockID = false }},
		{"no_hash", func(b *Block, _ *uint64, _ *[32]byte, _ *uint64) { b.HasExpectedBankhash = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := selectedLocalBlock()
			slot, hash, count := b.Slot, b.ExpectedBankhash, uint64(1)
			tc.change(&b, &slot, &hash, &count)
			if b.SealSelectedLocalEconomics(slot, hash, count, SelectedNativeCost{7, 8}) {
				t.Fatal("invalid seal accepted")
			}
			if got, _ := b.SelectedEconomicsFor(b.Slot, b.AlpenglowBlockID, b.ExpectedBankhash, 1); got != (SelectedEconomics{}) {
				t.Fatal("failed seal exposed local fees or cost")
			}
		})
	}
	b := selectedLocalBlock()
	b.EnableSelectedEconomics()
	b.RecordSelectedFees(10, 2, 12)
	if b.SealSelectedLocalEconomics(b.Slot, b.ExpectedBankhash, 1, SelectedNativeCost{}) {
		t.Fatal("remote request accepted a local seal")
	}
	got, enabled := b.SelectedEconomicsFor(b.Slot, [32]byte{8}, [32]byte{9}, 1)
	if !enabled || !got.FeesKnown || got.Fees.TotalLamports != 12 || got.NativeCostKnown {
		t.Fatalf("remote fees were coupled to local identity/cost: %+v/%v", got, enabled)
	}
	b.FromLocalProduction = true
	if got, _ := b.SelectedEconomicsFor(b.Slot, b.AlpenglowBlockID, b.ExpectedBankhash, 1); got != (SelectedEconomics{}) {
		t.Fatal("unsealed local adoption was treated as remote")
	}
}
