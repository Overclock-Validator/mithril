package block

import (
	"fmt"
	"math"

	"github.com/Overclock-Validator/mithril/pkg/overcast"
	"github.com/Overclock-Validator/mithril/pkg/txverify"
	"github.com/gagliardetto/solana-go"
)

const (
	overcastPubkeySize    = 32
	overcastHashSize      = 32
	overcastSignatureSize = 64
)

// FromLightbringerStreamMsg converts a Lightbringer/Overcast slot into Mithril's
// native block representation. The Overcast schema only represents legacy and
// v0 messages. Unknown protobuf fields are rejected so that a newer message
// variant (notably TxV1) cannot be silently decoded as legacy or nil-dereferenced.
func FromLightbringerStreamMsg(resp *overcast.SlotResponse) (*Block, error) {
	if resp == nil {
		return nil, fmt.Errorf("Lightbringer returned a nil slot response")
	}
	if len(resp.Entries) == 0 {
		return nil, fmt.Errorf("Lightbringer slot %d has no entries", resp.Slot)
	}

	block := new(Block)
	block.Slot = resp.Slot
	block.SourceParentSlot = resp.GetParentSlot()
	block.Transactions = make([]*solana.Transaction, 0, 2000)
	block.Entries = make([]*TxEntry, len(resp.Entries))

	var offset uint64
	for entryIdx, entry := range resp.Entries {
		if entry == nil {
			return nil, fmt.Errorf("Lightbringer slot %d entry %d is nil", resp.Slot, entryIdx)
		}
		if len(entry.Hash) != overcastHashSize {
			return nil, fmt.Errorf("Lightbringer slot %d entry %d has invalid hash length %d", resp.Slot, entryIdx, len(entry.Hash))
		}

		txEntry := &TxEntry{
			NumHashes: entry.NumHashes,
			Hash:      entry.Hash,
			Indices:   make([]uint64, len(entry.Transactions)),
		}
		for txIdx, tx := range entry.Transactions {
			convertedTx, txVersion, err := overcastTransactionToTransaction(tx)
			if err != nil {
				return nil, fmt.Errorf("Lightbringer slot %d entry %d transaction %d: %w", resp.Slot, entryIdx, txIdx, err)
			}
			block.Transactions = append(block.Transactions, convertedTx)
			block.NumSignatures += uint64(convertedTx.Message.Header.NumRequiredSignatures)
			block.Versions = append(block.Versions, txVersion)
			txEntry.Indices[txIdx] = offset
			offset++
		}
		block.Entries[entryIdx] = txEntry
	}

	block.Blockhash = solana.HashFromBytes(resp.Entries[len(resp.Entries)-1].Hash)
	block.FromLiveStream = true

	return block, nil
}

func overcastTransactionToTransaction(overcastTx *overcast.VersionedTransaction) (*solana.Transaction, uint8, error) {
	if overcastTx == nil {
		return nil, 0, fmt.Errorf("nil transaction")
	}
	if len(overcastTx.ProtoReflect().GetUnknown()) != 0 {
		return nil, 0, fmt.Errorf("unsupported transaction fields: the Lightbringer schema represents only legacy and V0 messages, not TxV1")
	}

	var (
		header       *overcast.MessageHeader
		accountKeys  [][]byte
		recentHash   []byte
		instructions []*overcast.CompiledInstruction
		lookups      []*overcast.MessageAddressTableLookup
		version      = solana.MessageVersionLegacy
	)

	switch message := overcastTx.GetMessage().(type) {
	case *overcast.VersionedTransaction_MessageLegacy:
		if message == nil || message.MessageLegacy == nil {
			return nil, 0, fmt.Errorf("legacy transaction has no message payload")
		}
		header = message.MessageLegacy.Header
		accountKeys = message.MessageLegacy.AccountKeys
		recentHash = message.MessageLegacy.RecentBlockhash
		instructions = message.MessageLegacy.Instructions
	case *overcast.VersionedTransaction_MessageV0:
		if message == nil || message.MessageV0 == nil {
			return nil, 0, fmt.Errorf("V0 transaction has no message payload")
		}
		header = message.MessageV0.Header
		accountKeys = message.MessageV0.AccountKeys
		recentHash = message.MessageV0.RecentBlockhash
		instructions = message.MessageV0.Instructions
		lookups = message.MessageV0.AddressTableLookups
		version = solana.MessageVersionV0
	default:
		return nil, 0, fmt.Errorf("unsupported or missing transaction message: the Lightbringer schema represents only legacy and V0 messages, not TxV1")
	}

	if header == nil {
		return nil, 0, fmt.Errorf("transaction message has no header")
	}
	if header.NumReadonlySignedAccounts > math.MaxUint8 ||
		header.NumReadonlyUnsignedAccounts > math.MaxUint8 ||
		header.NumRequiredSignatures > math.MaxUint8 {
		return nil, 0, fmt.Errorf("transaction header exceeds legacy/V0 u8 limits")
	}
	if len(recentHash) != overcastHashSize {
		return nil, 0, fmt.Errorf("transaction has invalid recent blockhash length %d", len(recentHash))
	}

	tx := &solana.Transaction{}
	for sigIdx, signature := range overcastTx.Signatures {
		if len(signature) != overcastSignatureSize {
			return nil, 0, fmt.Errorf("signature %d has invalid length %d", sigIdx, len(signature))
		}
		tx.Signatures = append(tx.Signatures, solana.SignatureFromBytes(signature))
	}
	for keyIdx, accountKey := range accountKeys {
		if len(accountKey) != overcastPubkeySize {
			return nil, 0, fmt.Errorf("account key %d has invalid length %d", keyIdx, len(accountKey))
		}
		tx.Message.AccountKeys = append(tx.Message.AccountKeys, solana.PublicKeyFromBytes(accountKey))
	}

	tx.Message.Header.NumReadonlySignedAccounts = uint8(header.NumReadonlySignedAccounts)
	tx.Message.Header.NumReadonlyUnsignedAccounts = uint8(header.NumReadonlyUnsignedAccounts)
	tx.Message.Header.NumRequiredSignatures = uint8(header.NumRequiredSignatures)
	tx.Message.RecentBlockhash = solana.HashFromBytes(recentHash)

	for instrIdx, instr := range instructions {
		convertedInstr, err := overcastInstrToInstr(instr)
		if err != nil {
			return nil, 0, fmt.Errorf("instruction %d: %w", instrIdx, err)
		}
		tx.Message.Instructions = append(tx.Message.Instructions, convertedInstr)
	}

	if version == solana.MessageVersionV0 {
		convertedLookups := make([]solana.MessageAddressTableLookup, len(lookups))
		for lookupIdx, lookup := range lookups {
			convertedLookup, err := overcastAddrTableLookupToAddrTableLookup(lookup)
			if err != nil {
				return nil, 0, fmt.Errorf("address table lookup %d: %w", lookupIdx, err)
			}
			convertedLookups[lookupIdx] = convertedLookup
		}
		tx.Message.SetAddressTableLookups(convertedLookups)
	}
	if err := txverify.SanitizeTransaction(tx); err != nil {
		return nil, 0, fmt.Errorf("invalid legacy/V0 transaction: %w", err)
	}

	return tx, uint8(version), nil
}

func overcastInstrToInstr(instr *overcast.CompiledInstruction) (solana.CompiledInstruction, error) {
	if instr == nil {
		return solana.CompiledInstruction{}, fmt.Errorf("nil instruction")
	}
	if instr.ProgramIdIndex > math.MaxUint8 {
		return solana.CompiledInstruction{}, fmt.Errorf("program ID index %d exceeds legacy/V0 u8 limit", instr.ProgramIdIndex)
	}

	compiledInstr := solana.CompiledInstruction{
		ProgramIDIndex: uint16(instr.ProgramIdIndex),
		Data:           instr.Data,
	}
	compiledInstr.Accounts = make([]uint16, len(instr.Accounts))
	for idx, account := range instr.Accounts {
		if account > math.MaxUint8 {
			return solana.CompiledInstruction{}, fmt.Errorf("account index %d exceeds legacy/V0 u8 limit", account)
		}
		compiledInstr.Accounts[idx] = uint16(account)
	}
	return compiledInstr, nil
}

func overcastAddrTableLookupToAddrTableLookup(atl *overcast.MessageAddressTableLookup) (solana.MessageAddressTableLookup, error) {
	if atl == nil {
		return solana.MessageAddressTableLookup{}, fmt.Errorf("nil address table lookup")
	}
	if len(atl.AccountKey) != overcastPubkeySize {
		return solana.MessageAddressTableLookup{}, fmt.Errorf("invalid account key length %d", len(atl.AccountKey))
	}

	return solana.MessageAddressTableLookup{
		AccountKey:      solana.PublicKeyFromBytes(atl.AccountKey),
		ReadonlyIndexes: atl.ReadonlyIndexes,
		WritableIndexes: atl.WritableIndexes,
	}, nil
}
