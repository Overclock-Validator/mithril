package turbine

import (
	"bytes"
	"testing"

	"github.com/gagliardetto/solana-go"
)

var benchmarkRecoveredShredsSink []*Shred

type recoverFECOneMissingFixture struct {
	assembler *SlotAssembler
	state     *slotState
	fecSet    uint32
	want      *Shred
}

func makeRecoverFECOneMissingFixture(tb testing.TB) recoverFECOneMissingFixture {
	tb.Helper()
	gen := ShredGenerator{Slot: 10, ParentSlot: 9, Version: 1}
	payloadSize := dataShredsPerFECBlock * dataCapacity(proofEntriesFor32x32, false)
	packets, _, _, _, err := gen.MakeShredsFromData(
		benchmarkLeaderKey(), benchmarkPayload(payloadSize), false, solana.Hash{}, 0, 0,
	)
	if err != nil {
		tb.Fatal(err)
	}
	if len(packets) != dataShredsPerFECBlock+codingShredsPerFECBlock {
		tb.Fatalf("fixture packets=%d", len(packets))
	}

	var fixture recoverFECOneMissingFixture
	for _, packet := range packets {
		shred, err := ParseShred(packet)
		if err != nil {
			tb.Fatal(err)
		}
		if fixture.state == nil {
			fixture.state = &slotState{
				slot:      shred.Slot,
				shreds:    make(map[uint32]*Shred),
				fecSets:   make(map[uint32]*fecState),
				shredVer:  shred.Version,
				lastIndex: ^uint32(0),
			}
			fixture.fecSet = shred.FECSetIndex
		}
		switch shred.Type {
		case ShredTypeData:
			if shred.Index-shred.FECSetIndex == 1 {
				fixture.want = shred
				continue
			}
			if err := fixture.state.addDataShred(shred); err != nil {
				tb.Fatal(err)
			}
		case ShredTypeCode:
			if err := fixture.state.addCodingShred(shred); err != nil {
				tb.Fatal(err)
			}
		}
	}
	if fixture.want == nil {
		tb.Fatal("fixture did not omit one data shred")
	}
	fixture.assembler = NewSlotAssembler()
	return fixture
}

func TestRecoverFECOneMissingFixed32x32(t *testing.T) {
	fixture := makeRecoverFECOneMissingFixture(t)
	recovered, err := fixture.assembler.recoverFEC(fixture.state, fixture.fecSet)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 1 {
		t.Fatalf("recovered %d shreds, want 1", len(recovered))
	}
	if !recovered[0].Recovered {
		t.Fatal("recovered shred is not marked recovered")
	}
	gotShard, err := recovered[0].erasureShard()
	if err != nil {
		t.Fatal(err)
	}
	wantShard, err := fixture.want.erasureShard()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotShard, wantShard) || !bytes.Equal(recovered[0].Data, fixture.want.Data) {
		t.Fatal("recovered erasure shard differs from the original")
	}
}

func BenchmarkRecoverFECOneMissingBoundary(b *testing.B) {
	fixture := makeRecoverFECOneMissingFixture(b)
	if _, err := fixture.assembler.recoverFEC(fixture.state, fixture.fecSet); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		recovered, err := fixture.assembler.recoverFEC(fixture.state, fixture.fecSet)
		if err != nil {
			b.Fatal(err)
		}
		if len(recovered) != 1 {
			b.Fatalf("recovered %d shreds", len(recovered))
		}
		benchmarkRecoveredShredsSink = recovered
	}
}
