package block

import (
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/require"
)

func TestFromBlockResultRejectsMalformedTransactionWithoutPanic(t *testing.T) {
	result := &rpc.GetBlockResult{
		Transactions: []rpc.TransactionWithMeta{{
			Transaction: rpc.DataBytesOrJSONFromBytes([]byte{0x81}),
		}},
	}

	var err error
	require.NotPanics(t, func() {
		_, err = FromBlockResult(result, 42, nil)
	})
	require.Error(t, err)
}

func TestFromBlockResultRejectsNilResult(t *testing.T) {
	block, err := FromBlockResult(nil, 42, nil)
	require.Error(t, err)
	require.Nil(t, block)
}

func TestFromBlockResultRejectsTrailingWireBytes(t *testing.T) {
	tx := &solana.Transaction{
		Signatures: []solana.Signature{{}},
		Message: solana.Message{
			Header:          solana.MessageHeader{NumRequiredSignatures: 1, NumReadonlyUnsignedAccounts: 1},
			AccountKeys:     []solana.PublicKey{{1}, {2}},
			RecentBlockhash: solana.Hash{3},
			Instructions: []solana.CompiledInstruction{{
				ProgramIDIndex: 1,
				Accounts:       []uint16{0},
			}},
		},
	}
	wire, err := tx.MarshalBinary()
	require.NoError(t, err)
	wire = append(wire, 0)

	result := &rpc.GetBlockResult{
		Transactions: []rpc.TransactionWithMeta{{
			Transaction: rpc.DataBytesOrJSONFromBytes(wire),
		}},
	}
	block, err := FromBlockResult(result, 42, nil)
	require.Error(t, err)
	require.Nil(t, block)
}
