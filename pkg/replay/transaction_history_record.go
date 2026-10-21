package replay

import (
	"errors"
	"fmt"
	"slices"

	"github.com/Overclock-Validator/mithril/pkg/tpu/wire"
	"github.com/Overclock-Validator/mithril/pkg/transactionhistory"
	"github.com/gagliardetto/solana-go"
)

// historyRecord encodes a sealed capture without reading current accounts.
// The private capture stays owned by its replay session during conversion.
func (record *transactionMetadata) historyRecord() (transactionhistory.Record, error) {
	if record == nil {
		return transactionhistory.Record{}, errors.New("missing transaction capture")
	}
	if record.preTokens.Mints != nil || record.postTokens.Mints != nil || record.preTokens.Balances != nil || record.postTokens.Balances != nil {
		return transactionhistory.Record{}, errors.New("transaction token capture is unresolved")
	}
	view, err := wire.Sanitize(record.Transaction)
	if err != nil {
		return transactionhistory.Record{}, fmt.Errorf("history transaction: %w", err)
	}
	// Sanitize bounds wire counts before the SDK allocates decoded vectors.
	tx, err := solana.TransactionFromBytes(record.Transaction)
	if err != nil {
		return transactionhistory.Record{}, err
	}
	static := len(tx.Message.AccountKeys)
	if len(record.AccountKeys) != static+tx.Message.NumLookups() ||
		len(record.AccountKeys) > 256 || !slices.Equal(tx.Message.AccountKeys, record.AccountKeys[:static]) ||
		len(record.InnerInstructions) > 64 || len(record.PreTokenBalances) > 256 || len(record.PostTokenBalances) > 256 {
		return transactionhistory.Record{}, errors.New("history capture does not match transaction keys")
	}
	metadata := transactionhistory.Metadata{
		AccountKeys: record.AccountKeys, Fee: record.Fee, Error: record.Error,
		PreBalances: record.PreBalances, PostBalances: record.PostBalances,
		ComputeUnitsConsumed: record.ComputeUnitsConsumed,
		InnerInstructions:    make([]transactionhistory.InnerInstructionGroup, len(record.InnerInstructions)),
	}
	remaining := 64
	for i, group := range record.InnerInstructions {
		if int(group.Index) >= len(tx.Message.Instructions) || len(group.Instructions) > remaining {
			return transactionhistory.Record{}, errors.New("invalid history instruction capture")
		}
		remaining -= len(group.Instructions)
		metadata.InnerInstructions[i] = transactionhistory.InnerInstructionGroup{
			Index: group.Index, Instructions: make([]transactionhistory.InnerInstruction, len(group.Instructions)),
		}
		for j, instruction := range group.Instructions {
			metadata.InnerInstructions[i].Instructions[j] = transactionhistory.InnerInstruction{
				ProgramIDIndex: instruction.ProgramIdIndex, Accounts: instruction.Accounts,
				Data: instruction.Data, StackHeight: uint32(instruction.StackHeight),
			}
		}
	}
	metadata.PreTokenBalances = historyTokenBalances(record.PreTokenBalances)
	metadata.PostTokenBalances = historyTokenBalances(record.PostTokenBalances)
	if record.ReturnData != nil {
		metadata.ReturnData = &transactionhistory.ReturnData{ProgramID: record.ReturnData.ProgramId, Data: record.ReturnData.Data}
	}
	encoded, err := metadata.MarshalBinary()
	if err != nil {
		return transactionhistory.Record{}, err
	}
	result := transactionhistory.Record{Metadata: encoded, Transaction: slices.Clone(record.Transaction)}
	copy(result.FirstSignature[:], view.FirstSignature())
	return result, nil
}

func historyTokenBalances(balances []transactionTokenBalance) []transactionhistory.TokenBalance {
	result := make([]transactionhistory.TokenBalance, len(balances))
	for i, balance := range balances {
		result[i] = transactionhistory.TokenBalance{
			AccountIndex: balance.AccountIndex, Mint: balance.Mint, Owner: balance.Owner,
			ProgramID: balance.ProgramID, Amount: balance.Amount, Decimals: balance.Decimals,
		}
	}
	return result
}

// historyBlock converts captured execution results into a persistence candidate.
// Canonical publication and recovery are enforced separately by the reader.
func (block *blockMetadata) historyBlock() (transactionhistory.Block, error) {
	if block == nil || len(block.Transactions) > metadataTransactionLimit {
		return transactionhistory.Block{}, errors.New("invalid block capture")
	}
	result := transactionhistory.Block{
		ID:         transactionhistory.BlockID{Slot: block.Slot, Blockhash: block.Blockhash},
		ParentSlot: block.ParentSlot, PreviousBlockhash: block.PreviousBlockhash,
		Bankhash: block.Bankhash, ParentBankhash: block.ParentBankhash,
		CaptureVersion: transactionhistory.CaptureVersion,
		Records:        make([]transactionhistory.Record, len(block.Transactions)),
	}
	var bytes uint64
	for i, record := range block.Transactions {
		converted, err := record.historyRecord()
		if err != nil {
			return transactionhistory.Block{}, fmt.Errorf("transaction %d history: %w", i, err)
		}
		// The writer validates its framed-record and batch budgets separately.
		// Conversion retains at most one additional bounded record on refusal.
		size := uint64(len(converted.Transaction) + len(converted.Metadata))
		if size > metadataBlockLimit-bytes {
			return transactionhistory.Block{}, errMetadataBudget
		}
		bytes += size
		result.Records[i] = converted
	}
	return result, nil
}
