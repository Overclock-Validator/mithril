package blockprod

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

// ParentAccounts serves accounts from a test bank's parent state. Install it
// as the bank's SlotCtx.UnrootedRead for accounts the leader reads from its
// parent rather than from the slot, such as address lookup tables.
type ParentAccounts map[solana.PublicKey]*accounts.Account

func (p ParentAccounts) GetAccount(_ uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
	if acct, ok := p[pubkey]; ok {
		return acct.Clone(), nil
	}
	return nil, fmt.Errorf("account %s not found in parent state", pubkey)
}

// LookupTable describes an address lookup table account for tests.
type LookupTable struct {
	Key solana.PublicKey
	// DeactivationSlot is math.MaxUint64 for an active table.
	DeactivationSlot uint64
	// LastExtendedSlot and LastExtendedStartIndex record the most recent
	// extension: addresses from LastExtendedStartIndex on were appended in
	// LastExtendedSlot.
	LastExtendedSlot       uint64
	LastExtendedStartIndex uint8
	Addresses              []solana.PublicKey
}

// ActiveLookupTable is an active table last extended in slot 1.
func ActiveLookupTable(key solana.PublicKey, addrs ...solana.PublicKey) LookupTable {
	return LookupTable{Key: key, DeactivationSlot: math.MaxUint64, LastExtendedSlot: 1, Addresses: addrs}
}

// Account encodes the table the way the address lookup table program stores
// it, with the fixture payer as authority.
func (lt LookupTable) Account() (*accounts.Account, error) {
	var buf bytes.Buffer
	enc := bin.NewBinEncoder(&buf)
	if err := enc.WriteUint32(sealevel.AddressLookupTableProgramStateLookupTable, bin.LE); err != nil {
		return nil, err
	}
	authority := txfixture.PayerPubkey()
	meta := sealevel.LookupTableMeta{
		DeactivationSlot:           lt.DeactivationSlot,
		LastExtendedSlot:           lt.LastExtendedSlot,
		LastExtendedSlotStartIndex: lt.LastExtendedStartIndex,
		Authority:                  &authority,
	}
	if err := meta.MarshalWithEncoder(enc); err != nil {
		return nil, err
	}
	for _, addr := range lt.Addresses {
		if err := enc.WriteBytes(addr[:], false); err != nil {
			return nil, err
		}
	}
	return &accounts.Account{
		Key:       lt.Key,
		Lamports:  1_000_000_000,
		Owner:     addresses.AddressLookupTableAddr,
		Data:      buf.Bytes(),
		RentEpoch: math.MaxUint64,
	}, nil
}

// SignedV0LookupTransferWire is a signed v0 transfer of lamports from the
// fixture payer to entry destIndex of table, loaded as a writable lookup. Its
// static keys are [payer, system program].
func SignedV0LookupTransferWire(lamports uint64, table solana.PublicKey, destIndex uint8) ([]byte, error) {
	data := make([]byte, 12)
	binary.LittleEndian.PutUint32(data, 2) // SystemInstruction::Transfer
	binary.LittleEndian.PutUint64(data[4:], lamports)
	msg := solana.Message{
		Header:          solana.MessageHeader{NumRequiredSignatures: 1, NumReadonlyUnsignedAccounts: 1},
		AccountKeys:     solana.PublicKeySlice{txfixture.PayerPubkey(), solana.SystemProgramID},
		RecentBlockhash: txfixture.TestBlockhash(),
		Instructions: []solana.CompiledInstruction{{
			ProgramIDIndex: 1,
			Accounts:       []uint16{0, 2},
			Data:           data,
		}},
		AddressTableLookups: solana.MessageAddressTableLookupSlice{{AccountKey: table, WritableIndexes: []uint8{destIndex}}},
	}
	if _, err := msg.SetVersion(solana.MessageVersionV0); err != nil {
		return nil, err
	}
	tx := &solana.Transaction{Message: msg}
	payerKey := txfixture.PayerPrivateKey()
	if _, err := tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
		if key.Equals(txfixture.PayerPubkey()) {
			return &payerKey
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return tx.MarshalBinary()
}
