package replay

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/txverify"
	"github.com/gagliardetto/solana-go"
)

// transactionMetadata owns observations from a committed transaction. It does
// not establish block finality. Token mint information is resolved after the
// transaction group joins, rather than read from concurrently changing state.
type transactionMetadata struct {
	Transaction       []byte
	AccountKeys       []solana.PublicKey
	Fee               uint64
	Error             json.RawMessage
	PreBalances       []uint64
	PostBalances      []uint64
	InnerInstructions []InnerInstructionsList
	// LogMessages currently contains syscall output, not the complete runtime
	// log stream. It must not be served as standard RPC logMessages yet.
	LogMessages          []string
	ComputeUnitsConsumed uint64
	ReturnData           *TransactionReturnData
	PreTokenBalances     []transactionTokenBalance
	PostTokenBalances    []transactionTokenBalance
	preTokens            transactionTokenSnapshot
	postTokens           transactionTokenSnapshot
	charge               uint64
}

// Capture errors affect metadata availability, not transaction execution.
type transactionMetadataCapture struct {
	Record *transactionMetadata
	Err    error
	budget *metadataBudget
}

func prepareTransactionMetadata(slotCtx *sealevel.SlotCtx, tx *solana.Transaction, budget *metadataBudget) (*transactionMetadata, error) {
	if err := txverify.SanitizeTransaction(tx); err != nil {
		return nil, fmt.Errorf("validate transaction metadata: %w", err)
	}
	if tx.Message.NumLookups() > 0 && !tx.Message.IsResolved() {
		return nil, errors.New("transaction metadata requires resolved address lookups")
	}
	wireSize, err := txverify.TransactionWireSize(tx)
	if err != nil {
		return nil, fmt.Errorf("size transaction metadata: %w", err)
	}
	// Discount only unused maximum-wire space; fixed and per-key reserves stay.
	baseCharge := uint64(metadataRecordBaseCharge - solana.MaxTransactionSizeV1 + wireSize)
	metadata := &transactionMetadata{}
	if err := metadata.reserve(budget, baseCharge+uint64(len(tx.Message.AccountKeys))*metadataRecordKeyCharge); err != nil {
		return nil, err
	}
	wire, err := tx.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("encode transaction metadata: %w", err)
	}
	balances, err := transactionAccountBalances(slotCtx, tx.Message.AccountKeys)
	if err != nil {
		return nil, fmt.Errorf("read pre-balances: %w", err)
	}
	tokens, err := captureTransactionTokenSnapshot(slotCtx, tx)
	if err != nil {
		return nil, fmt.Errorf("read pre-token balances: %w", err)
	}
	metadata.Transaction = wire
	metadata.AccountKeys = slices.Clone(tx.Message.AccountKeys)
	metadata.PreBalances = balances
	metadata.preTokens = tokens
	return metadata, nil
}

func (metadata *transactionMetadata) finish(slotCtx *sealevel.SlotCtx, tx *solana.Transaction, output LoadAndExecuteTransactionOutput, fee uint64, budget *metadataBudget) error {
	if output.ExecCtx != nil && output.ExecCtx.InnerInstructionsCaptureError != nil {
		return fmt.Errorf("inner instruction metadata: %w", output.ExecCtx.InnerInstructionsCaptureError)
	}
	if err := metadata.reserveExecution(budget, output.ExecCtx); err != nil {
		return err
	}
	if result := output.ProcessingResult.TransactionError; result != nil && result.ErrorType == TransactionErrorInstructionError && result.InstructionError != nil && len(result.InstructionError.Error()) > metadataErrorTextLimit {
		return errMetadataBudget
	}
	balances, err := transactionAccountBalances(slotCtx, metadata.AccountKeys)
	if err != nil {
		return fmt.Errorf("read post-balances: %w", err)
	}
	tokens, err := captureTransactionTokenSnapshot(slotCtx, tx)
	if err != nil {
		return fmt.Errorf("read post-token balances: %w", err)
	}
	status, err := json.Marshal(output.ProcessingResult.TransactionError)
	if err != nil {
		return fmt.Errorf("encode transaction status: %w", err)
	}
	metadata.Fee = fee
	metadata.Error = status
	metadata.PostBalances = balances
	metadata.postTokens = tokens
	metadata.ComputeUnitsConsumed = processTransactionComputeUnits(output.ExecCtx)
	if output.ExecCtx != nil {
		if recorder, ok := output.ExecCtx.Log.(*sealevel.LogRecorder); ok {
			metadata.LogMessages = append([]string{}, recorder.Logs...)
		}
		metadata.InnerInstructions = AssembleInnerInstructions(output.ExecCtx)
		program, data := output.ExecCtx.TransactionContext.ReturnData()
		if len(data) > 0 {
			metadata.ReturnData = &TransactionReturnData{ProgramId: program, Data: slices.Clone(data)}
		}
	} else {
		metadata.LogMessages = []string{}
	}
	return nil
}

func transactionAccountBalances(slotCtx *sealevel.SlotCtx, keys []solana.PublicKey) ([]uint64, error) {
	balances := make([]uint64, len(keys))
	for i, key := range keys {
		account, err := transactionMetadataAccount(slotCtx, key, nil)
		if err != nil {
			return nil, err
		}
		if account != nil {
			balances[i] = account.Lamports
		}
	}
	return balances, nil
}

// transactionMetadataAccount reads the actual bank account, not a loader
// placeholder. A nil account without an error means the backing view proved
// the account absent; overlay tombstones remain authoritative. Returned accounts
// are read-only views. Backing reads are bounded before copying account data.
func transactionMetadataAccount(slotCtx *sealevel.SlotCtx, key solana.PublicKey, remaining *uint64) (*accounts.Account, error) {
	account, err := slotCtx.GetAccountShared(key)
	if errors.Is(err, accounts.ErrAccountNotFound) {
		bankSysvar := slotCtx.BankSysvars() != nil && sealevel.IsBankSysvarAccount(key)
		if bankSysvar {
			if account, ok := slotCtx.BankSysvars().AccountView(key); ok {
				return account, nil
			}
			return nil, fmt.Errorf("bank sysvar account %s is absent at slot %d", key, slotCtx.Slot)
		}
		if slotCtx.UnrootedRead == nil && (slotCtx.AccountsDb == nil || slotCtx.AccountsDb.Index == nil) {
			return nil, fmt.Errorf("account %s has no backing account view", key)
		}
		limit := uint64(metadataAccountDataLimit)
		if remaining != nil {
			limit = min(limit, *remaining)
		}
		if slotCtx.UnrootedRead != nil {
			reader, ok := slotCtx.UnrootedRead.(boundedAccountReader)
			if !ok {
				return nil, errors.New("metadata requires bounded backing account reads")
			}
			account, err = reader.GetAccountWithDataLimit(slotCtx.Slot, key, limit)
		} else {
			account, err = slotCtx.AccountsDb.GetAccountWithDataLimit(slotCtx.Slot, key, limit)
		}
		if errors.Is(err, accountsdb.ErrNoAccount) {
			return nil, nil
		}
		if err == nil && account != nil {
			if uint64(len(account.Data)) > limit {
				return nil, errors.New("metadata backing account read exceeded its limit")
			}
			if remaining != nil {
				*remaining -= uint64(len(account.Data))
			}
		}
	}
	if err != nil {
		return nil, fmt.Errorf("account %s: %w", key, err)
	}
	if account == nil {
		return nil, fmt.Errorf("account %s returned no state", key)
	}
	return account, nil
}
