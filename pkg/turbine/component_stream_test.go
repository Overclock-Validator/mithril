package turbine

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/gagliardetto/solana-go"
)

func TestUnmarshalEntryBatchRejectsOversizedCount(t *testing.T) {
	data := make([]byte, 16)
	binary.LittleEndian.PutUint64(data, 1<<32)
	if _, err := unmarshalEntryBatch(data); !errors.Is(err, ErrInvalidBlockComponent) {
		t.Fatalf("error = %v, want %v", err, ErrInvalidBlockComponent)
	}
}

func TestUnmarshalEntryBatchRejectsImpossibleTransactionCount(t *testing.T) {
	data := make([]byte, 8+minimumEntryWireSize)
	binary.LittleEndian.PutUint64(data, 1)
	binary.LittleEndian.PutUint64(data[8+8+32:], 2)
	if _, err := unmarshalEntryBatch(data); err == nil {
		t.Fatal("impossible transaction count was accepted")
	}
}

func FuzzUnmarshalBlockComponent(f *testing.F) {
	f.Add([]byte{1})
	oversized := make([]byte, 16)
	binary.LittleEndian.PutUint64(oversized, 1<<32)
	f.Add(oversized)
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = UnmarshalBlockComponent(data)
	})
}

func TestGenesisCertificateBitmapIsBounded(t *testing.T) {
	data := make([]byte, 240+513)
	binary.LittleEndian.PutUint64(data[232:240], 513)
	if _, err := unmarshalGenesisCert(data); !errors.Is(err, ErrInvalidBlockComponent) {
		t.Fatalf("error = %v, want %v", err, ErrInvalidBlockComponent)
	}
}

func TestComponentStreamChecksGenesisParentSlot(t *testing.T) {
	processor := componentStreamProcessor{slot: 1, shredParentSlot: 0}
	err := processor.consume(decodedSlotComponent{component: NewBlockHeader(7, componentHash(1))}, false)
	if !errors.Is(err, ErrHeaderParentSlotMismatch) {
		t.Fatalf("error = %v, want %v", err, ErrHeaderParentSlotMismatch)
	}
}

func TestComponentStreamRequiresFooterThenFinalAlpentick(t *testing.T) {
	processor := componentStreamProcessor{slot: 8, shredParentSlot: 7}
	components := []decodedSlotComponent{
		{component: NewBlockHeader(7, componentHash(1)), batchStart: 0},
		{component: mustEntryComponent(t, []Entry{{NumHashes: 2, Hash: componentHash(2)}}), batchStart: 32},
		{component: NewBlockFooter(BlockFooter{BankHash: componentHash(3)}), batchStart: 64},
		{component: mustEntryComponent(t, []Entry{{NumHashes: 1, Hash: componentHash(4)}}), batchStart: 96},
	}
	for index, component := range components {
		if err := processor.consume(component, index == len(components)-1); err != nil {
			t.Fatalf("consume component %d: %v", index, err)
		}
	}
	if err := processor.finish(); err != nil {
		t.Fatal(err)
	}
	if len(processor.entries) != 2 || processor.entries[1].NumHashes != 1 {
		t.Fatalf("entries = %+v", processor.entries)
	}
}

func TestComponentStreamRejectsOrdinaryEntryAfterFooter(t *testing.T) {
	processor := componentStreamProcessor{slot: 8, shredParentSlot: 7}
	if err := processor.consume(decodedSlotComponent{component: NewBlockHeader(7, componentHash(1))}, false); err != nil {
		t.Fatal(err)
	}
	if err := processor.consume(decodedSlotComponent{component: NewBlockFooter(BlockFooter{})}, false); err != nil {
		t.Fatal(err)
	}
	err := processor.consume(decodedSlotComponent{component: mustEntryComponent(t, []Entry{{NumHashes: 2}})}, true)
	if !errors.Is(err, ErrEntryBatchAfterBlockFooter) {
		t.Fatalf("error = %v, want %v", err, ErrEntryBatchAfterBlockFooter)
	}
}

func TestComponentStreamUpdateParentAbandonsEarlierEntries(t *testing.T) {
	processor := componentStreamProcessor{slot: 8, shredParentSlot: 7}
	before := Entry{NumHashes: 2, Hash: componentHash(2)}
	after := Entry{NumHashes: 3, Hash: componentHash(3)}
	components := []decodedSlotComponent{
		{component: NewBlockHeader(7, componentHash(1)), batchStart: 0},
		{component: mustEntryComponent(t, []Entry{before}), batchStart: 32},
		{component: NewUpdateParent(4, componentHash(4)), batchStart: 64},
		{component: mustEntryComponent(t, []Entry{after}), batchStart: 96},
		{component: NewBlockFooter(BlockFooter{}), batchStart: 128},
		{component: mustEntryComponent(t, []Entry{{NumHashes: 1}}), batchStart: 160},
	}
	for index, component := range components {
		if err := processor.consume(component, index == len(components)-1); err != nil {
			t.Fatalf("consume component %d: %v", index, err)
		}
	}
	if len(processor.entries) != 2 || processor.entries[0].Hash != after.Hash {
		t.Fatalf("entries after UpdateParent = %+v", processor.entries)
	}
	if processor.parent == nil || !processor.parent.FromUpdateParent || processor.parent.ParentSlot != 4 {
		t.Fatalf("parent = %+v", processor.parent)
	}
}

func TestComponentCodecRejectsNonCanonicalTrailingBytes(t *testing.T) {
	component := NewBlockHeader(7, componentHash(1))
	encoded, err := MarshalBlockComponent(component)
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, 0xff)
	if _, err := UnmarshalBlockComponent(encoded); err == nil {
		t.Fatal("non-canonical marker trailing byte was accepted")
	}

	entry, err := MarshalBlockComponent(mustEntryComponent(t, []Entry{{NumHashes: 1}}))
	if err != nil {
		t.Fatal(err)
	}
	entry = append(entry, 0xff)
	if _, err := UnmarshalBlockComponent(entry); err == nil {
		t.Fatal("non-canonical entry trailing byte was accepted")
	}
}

func mustEntryComponent(t *testing.T, entries []Entry) BlockComponent {
	t.Helper()
	component, err := NewEntryBatch(entries)
	if err != nil {
		t.Fatal(err)
	}
	return component
}

func componentHash(value byte) solana.Hash {
	var hash solana.Hash
	hash[0] = value
	return hash
}
