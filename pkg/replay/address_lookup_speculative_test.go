package replay

import (
	"encoding/binary"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

func TestResolveAddressLookupTableFromSpeculativeParent(t *testing.T) {
	tableKey := solana.PublicKey{1}
	loadedKey := solana.PublicKey{9}
	data := make([]byte, sealevel.AddressLookupTableMetaSize+solana.PublicKeyLength)
	binary.LittleEndian.PutUint32(data[:4], sealevel.AddressLookupTableProgramStateLookupTable)
	copy(data[sealevel.AddressLookupTableMetaSize:], loadedKey[:])

	spec := NewSpeculativeReplay()
	spec.Enable()
	spec.store.SetFinalizedSlot(100)
	recordSpeculativeLayer(t, spec.store, 101, 100, &accounts.Account{Key: tableKey, Data: data})

	msg := solana.Message{
		AccountKeys: []solana.PublicKey{{2}},
		AddressTableLookups: solana.MessageAddressTableLookupSlice{{
			AccountKey: tableKey, WritableIndexes: []uint8{0},
		}},
	}
	msg.SetVersion(solana.MessageVersionV0)
	block := &b.Block{Slot: 102, ParentSlot: 101, Transactions: []*solana.Transaction{{Message: msg}}}
	if err := resolveAddrTableLookups(nil, block, spec); err != nil {
		t.Fatal(err)
	}
	keys := block.Transactions[0].Message.AccountKeys
	if len(keys) != 2 || keys[1] != loadedKey {
		t.Fatalf("resolved account keys = %v, want loaded key %s", keys, loadedKey)
	}
}
