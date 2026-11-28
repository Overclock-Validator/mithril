package txstatus_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/gagliardetto/solana-go"
)

func TestResultCacheTypedExecutionErrors(t *testing.T) {
	parent := txstatus.ResultBankID{Blockhash: solana.Hash{1}}
	c, err := txstatus.NewResultCache(parent)
	if err != nil {
		t.Fatal(err)
	}
	index := uint8(3)
	cases := []struct {
		err  *replay.TransactionError
		want string
	}{
		{nil, ""},
		{&replay.TransactionError{ErrorType: replay.TransactionErrorInstructionError, InstructionIndex: &index, InstructionError: sealevel.InstrErrCustomCode{Code: 0}}, `{"InstructionError":[3,{"Custom":0}]}`},
		{&replay.TransactionError{ErrorType: replay.TransactionErrorInstructionError, InstructionIndex: &index, InstructionError: sealevel.InstrErrCustomCode{Code: 6001}}, `{"InstructionError":[3,{"Custom":6001}]}`},
		{&replay.TransactionError{ErrorType: replay.TransactionErrorProgramAccountNotFound}, `"ProgramAccountNotFound"`},
		{&replay.TransactionError{ErrorType: replay.TransactionErrorInsufficientFundsForFee}, `"InsufficientFundsForFee"`},
	}
	var input []txstatus.SignatureResult
	var signatures []solana.Signature
	for i, test := range cases {
		signature := solana.Signature{byte(i + 1)}
		var encoded []byte
		if test.err != nil {
			encoded, err = json.Marshal(test.err)
			if err != nil || string(encoded) != test.want {
				t.Fatal("typed source error mismatch", string(encoded), err)
			}
		}
		input = append(input, txstatus.SignatureResult{Signature: signature, Error: encoded})
		signatures = append(signatures, signature)
	}
	id := txstatus.ResultBankID{Slot: 10, Blockhash: solana.Hash{2}}
	if err := c.Publish(txstatus.ResultBank{ID: id, Parent: parent, Complete: true, Results: input}); err != nil {
		t.Fatal(err)
	}
	view, err := c.Lookup(signatures)
	if err != nil {
		t.Fatal(err)
	}
	for i, result := range view.Results {
		if !result.Found || result.Slot != id.Slot || string(result.Error) != cases[i].want {
			t.Fatal("cache changed typed execution outcome", i, result)
		}
	}
	unknown := &replay.TransactionError{ErrorType: replay.TransactionErrorInstructionError, InstructionIndex: &index, InstructionError: sealevel.InstrErrCustom}
	if _, err := json.Marshal(unknown); err == nil {
		t.Fatal("unresolved custom code was invented")
	}
	if err := c.Publish(txstatus.ResultBank{ID: txstatus.ResultBankID{Slot: 20, Blockhash: solana.Hash{3}}, Parent: id}); !errors.Is(err, txstatus.ErrResultUnavailable) {
		t.Fatal("unencodable outcome was not represented as a missing bank", err)
	}
	if _, err := c.Lookup(signatures); err != nil {
		t.Fatal("known typed outcomes were lost across a gap", err)
	}
}
