package block

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/overcast"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestFromLightbringerStreamMsgLeavesBlockHeightUnset(t *testing.T) {
	var entryHash [32]byte
	entryHash[0] = 0xAB

	resp := &overcast.SlotResponse{
		Slot:       123,
		ParentSlot: 122,
		Entries: []*overcast.Entry{
			{
				NumHashes: 1,
				Hash:      entryHash[:],
			},
		},
	}

	block, err := FromLightbringerStreamMsg(resp)
	require.NoError(t, err)

	if block.BlockHeight != 0 {
		t.Fatalf("expected block height to be unset, got %d", block.BlockHeight)
	}
	if block.SourceParentSlot != 122 {
		t.Fatalf("expected parent slot 122, got %d", block.SourceParentSlot)
	}
	if block.Blockhash != solana.HashFromBytes(entryHash[:]) {
		t.Fatalf("expected blockhash %s, got %s", solana.HashFromBytes(entryHash[:]), block.Blockhash)
	}
}

func TestFromLightbringerStreamMsgRejectsUnknownTransactionVariant(t *testing.T) {
	entryHash := make([]byte, overcastHashSize)
	entryHash[0] = 0xAB

	// Field 4 is outside the legacy/V0 oneof in the checked-in Overcast
	// schema. It models how a future TxV1 oneof arrives through an older
	// protobuf client: the message oneof is nil and the payload is retained as
	// unknown bytes.
	futureTx := &overcast.VersionedTransaction{}
	futureTx.ProtoReflect().SetUnknown([]byte{0x22, 0x00})
	resp := &overcast.SlotResponse{
		Slot: 123,
		Entries: []*overcast.Entry{{
			Hash:         entryHash,
			Transactions: []*overcast.VersionedTransaction{futureTx},
		}},
	}

	var (
		got *Block
		err error
	)
	require.NotPanics(t, func() {
		got, err = FromLightbringerStreamMsg(resp)
	})
	require.Nil(t, got)
	require.ErrorContains(t, err, "not TxV1")
}

func TestFromLightbringerStreamMsgRejectsMissingTransactionMessage(t *testing.T) {
	resp := &overcast.SlotResponse{
		Slot: 123,
		Entries: []*overcast.Entry{{
			Hash:         make([]byte, overcastHashSize),
			Transactions: []*overcast.VersionedTransaction{{}},
		}},
	}

	got, err := FromLightbringerStreamMsg(resp)
	require.Nil(t, got)
	require.ErrorContains(t, err, "unsupported or missing transaction message")
}

func TestFromLightbringerStreamMsgConvertsV0(t *testing.T) {
	entryHash := make([]byte, overcastHashSize)
	recentBlockhash := make([]byte, overcastHashSize)
	accountKey := make([]byte, overcastPubkeySize)
	tableKey := make([]byte, overcastPubkeySize)
	resp := &overcast.SlotResponse{
		Slot: 123,
		Entries: []*overcast.Entry{{
			Hash: entryHash,
			Transactions: []*overcast.VersionedTransaction{{
				Signatures: [][]byte{make([]byte, overcastSignatureSize)},
				Message: &overcast.VersionedTransaction_MessageV0{
					MessageV0: &overcast.VersionedMessageV0{
						Header: &overcast.MessageHeader{
							NumRequiredSignatures: 1,
						},
						AccountKeys:     [][]byte{accountKey},
						RecentBlockhash: recentBlockhash,
						AddressTableLookups: []*overcast.MessageAddressTableLookup{{
							AccountKey:      tableKey,
							WritableIndexes: []byte{1},
						}},
					},
				},
			}},
		}},
	}

	got, err := FromLightbringerStreamMsg(resp)
	require.NoError(t, err)
	require.Len(t, got.Transactions, 1)
	require.Equal(t, solana.MessageVersionV0, got.Transactions[0].Message.GetVersion())
	require.Equal(t, []uint8{uint8(solana.MessageVersionV0)}, got.Versions)
}

func TestFromLightbringerStreamMsgSanitizesConvertedTransaction(t *testing.T) {
	resp := &overcast.SlotResponse{
		Slot: 123,
		Entries: []*overcast.Entry{{
			Hash: make([]byte, overcastHashSize),
			Transactions: []*overcast.VersionedTransaction{{
				Signatures: [][]byte{
					make([]byte, overcastSignatureSize),
					make([]byte, overcastSignatureSize),
				},
				Message: &overcast.VersionedTransaction_MessageLegacy{
					MessageLegacy: &overcast.VersionedMessageLegacy{
						Header: &overcast.MessageHeader{
							NumRequiredSignatures: 2,
						},
						AccountKeys:     [][]byte{make([]byte, overcastPubkeySize)},
						RecentBlockhash: make([]byte, overcastHashSize),
					},
				},
			}},
		}},
	}

	var (
		got *Block
		err error
	)
	require.NotPanics(t, func() {
		got, err = FromLightbringerStreamMsg(resp)
	})
	require.Nil(t, got)
	require.ErrorContains(t, err, "more signatures (2) than static account keys (1)")
}
