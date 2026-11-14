package sealevel

import (
	"math"
	"testing"

	"github.com/gagliardetto/solana-go"
)

func TestBankTransactionCounts(t *testing.T) {
	var c BankTransactionCounts
	for _, vote := range []bool{false, true} {
		for _, failed := range []bool{false, true} {
			c.Record(vote, failed, 150)
		}
	}
	want := BankTransactionCounts{Total: 4, NonVote: 2, Failed: 2, NonVoteFailed: 1, ComputeUnits: 600}
	if c != want {
		t.Fatalf("counts = %+v, want %+v", c, want)
	}
	if allocations := testing.AllocsPerRun(100, func() { c.Record(false, false, 150) }); allocations != 0 {
		t.Fatalf("record allocated %g times", allocations)
	}
	for _, c := range []BankTransactionCounts{{Total: math.MaxUint64}, {ComputeUnits: math.MaxUint64}} {
		before := c
		c.Record(false, true, 1)
		before.Saturated = true
		if c != before {
			t.Fatalf("overflow changed counts: %+v", c)
		}
		c.Record(false, true, 1)
		if c != before {
			t.Fatal("saturation must remain sticky")
		}
	}
}

func TestBankTransactionCountsVoteProgramClassification(t *testing.T) {
	transaction := solana.Transaction{Message: solana.Message{
		AccountKeys: solana.PublicKeySlice{solana.SystemProgramID, solana.VoteProgramID},
		Instructions: []solana.CompiledInstruction{
			{ProgramIDIndex: 0},
			{ProgramIDIndex: 1},
		},
	}}
	var counts BankTransactionCounts
	counts.Record(transaction.IsVote(), true, 1)
	if counts.Total != 1 || counts.NonVote != 0 || counts.Failed != 1 || counts.NonVoteFailed != 0 {
		t.Fatalf("mixed Vote-program transaction counts = %+v", counts)
	}
	transaction.Message.Instructions = transaction.Message.Instructions[:1]
	counts.Record(transaction.IsVote(), false, 1)
	if counts.Total != 2 || counts.NonVote != 1 {
		t.Fatalf("non-Vote-program transaction counts = %+v", counts)
	}
}
