package block

import (
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/yellowstone/geyserpb"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func validLaserstreamTransaction() *geyserpb.Transaction {
	return &geyserpb.Transaction{
		Signatures: [][]byte{make([]byte, solana.SignatureLength)},
		Message: &geyserpb.Message{
			Header: &geyserpb.MessageHeader{
				NumRequiredSignatures: 1,
			},
			AccountKeys:     [][]byte{make([]byte, solana.PublicKeyLength)},
			RecentBlockhash: make([]byte, len(solana.Hash{})),
		},
	}
}

func validLaserstreamBlock() *geyserpb.SubscribeUpdateBlock {
	return &geyserpb.SubscribeUpdateBlock{
		Slot:            42,
		Blockhash:       (solana.Hash{}).String(),
		ParentBlockhash: (solana.Hash{}).String(),
		BlockTime:       &geyserpb.UnixTimestamp{Timestamp: 123},
		Rewards:         &geyserpb.Rewards{},
		Transactions: []*geyserpb.SubscribeUpdateTransactionInfo{{
			Transaction: validLaserstreamTransaction(),
			Meta: &geyserpb.TransactionStatusMeta{
				PreBalances:  []uint64{1},
				PostBalances: []uint64{1},
			},
		}},
	}
}

func TestLaserstreamTransactionRejectsUnknownTxV1Config(t *testing.T) {
	message := &geyserpb.Message{
		Header:    &geyserpb.MessageHeader{},
		Versioned: true,
	}
	// Current Solana storage-proto uses field 7 for TransactionConfig. This
	// older Laserstream dependency has no such field, so it survives only as
	// protobuf unknown bytes.
	message.ProtoReflect().SetUnknown([]byte{0x3a, 0x00})

	tx, err := lsTransactionToTransaction(&geyserpb.Transaction{Message: message})
	require.Nil(t, tx)
	require.ErrorContains(t, err, "cannot represent TxV1")
}

func TestLaserstreamTransactionRejectsAmbiguousVersionedMessage(t *testing.T) {
	tx, err := lsTransactionToTransaction(&geyserpb.Transaction{
		Message: &geyserpb.Message{
			Header:    &geyserpb.MessageHeader{},
			Versioned: true,
		},
	})
	require.Nil(t, tx)
	require.ErrorContains(t, err, "cannot distinguish V0 from TxV1")
}

func TestLaserstreamTransactionConvertsUnambiguousV0(t *testing.T) {
	input := validLaserstreamTransaction()
	input.Message.Versioned = true
	input.Message.AddressTableLookups = []*geyserpb.MessageAddressTableLookup{{
		AccountKey:      make([]byte, solana.PublicKeyLength),
		WritableIndexes: []byte{0},
	}}
	tx, err := lsTransactionToTransaction(input)
	require.NoError(t, err)
	require.Equal(t, solana.MessageVersionV0, tx.Message.GetVersion())
}

func TestLaserstreamTransactionLeavesLegacyUnversioned(t *testing.T) {
	tx, err := lsTransactionToTransaction(validLaserstreamTransaction())
	require.NoError(t, err)
	require.Equal(t, solana.MessageVersionLegacy, tx.Message.GetVersion())
}

func TestLaserstreamTransactionRejectsMalformedFields(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*geyserpb.Transaction)
		want   string
	}{
		{
			name: "short signature",
			mutate: func(tx *geyserpb.Transaction) {
				tx.Signatures[0] = make([]byte, solana.SignatureLength-1)
			},
			want: "signature 0 has invalid length",
		},
		{
			name: "short account key",
			mutate: func(tx *geyserpb.Transaction) {
				tx.Message.AccountKeys[0] = make([]byte, solana.PublicKeyLength-1)
			},
			want: "account key 0 has invalid length",
		},
		{
			name: "short recent blockhash",
			mutate: func(tx *geyserpb.Transaction) {
				tx.Message.RecentBlockhash = make([]byte, len(solana.Hash{})-1)
			},
			want: "recent blockhash has invalid length",
		},
		{
			name: "required signatures overflow",
			mutate: func(tx *geyserpb.Transaction) {
				tx.Message.Header.NumRequiredSignatures = math.MaxUint8 + 1
			},
			want: "header exceeds legacy/V0 u8 limits",
		},
		{
			name: "readonly signed overflow",
			mutate: func(tx *geyserpb.Transaction) {
				tx.Message.Header.NumReadonlySignedAccounts = math.MaxUint8 + 1
			},
			want: "header exceeds legacy/V0 u8 limits",
		},
		{
			name: "readonly unsigned overflow",
			mutate: func(tx *geyserpb.Transaction) {
				tx.Message.Header.NumReadonlyUnsignedAccounts = math.MaxUint8 + 1
			},
			want: "header exceeds legacy/V0 u8 limits",
		},
		{
			name: "nil instruction",
			mutate: func(tx *geyserpb.Transaction) {
				tx.Message.Instructions = []*geyserpb.CompiledInstruction{nil}
			},
			want: "nil instruction",
		},
		{
			name: "program index overflow",
			mutate: func(tx *geyserpb.Transaction) {
				tx.Message.Instructions = []*geyserpb.CompiledInstruction{{ProgramIdIndex: math.MaxUint8 + 1}}
			},
			want: "exceeds legacy/V0 u8 limit",
		},
		{
			name: "nil lookup",
			mutate: func(tx *geyserpb.Transaction) {
				tx.Message.Versioned = true
				tx.Message.AddressTableLookups = []*geyserpb.MessageAddressTableLookup{nil}
			},
			want: "nil address table lookup",
		},
		{
			name: "short lookup key",
			mutate: func(tx *geyserpb.Transaction) {
				tx.Message.Versioned = true
				tx.Message.AddressTableLookups = []*geyserpb.MessageAddressTableLookup{{
					AccountKey:      make([]byte, solana.PublicKeyLength-1),
					WritableIndexes: []byte{0},
				}}
			},
			want: "account key has invalid length",
		},
		{
			name: "structurally invalid transaction",
			mutate: func(tx *geyserpb.Transaction) {
				tx.Message.Header.NumRequiredSignatures = 2
				tx.Signatures = append(tx.Signatures, make([]byte, solana.SignatureLength))
			},
			want: "more signatures (2) than static account keys (1)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := validLaserstreamTransaction()
			test.mutate(input)

			var (
				got *solana.Transaction
				err error
			)
			require.NotPanics(t, func() {
				got, err = lsTransactionToTransaction(input)
			})
			require.Nil(t, got)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestFromLaserStreamRejectsMalformedInputWithoutPanicking(t *testing.T) {
	tests := []struct {
		name   string
		input  func() *geyserpb.SubscribeUpdateBlock
		mutate func(*geyserpb.SubscribeUpdateBlock)
		want   string
	}{
		{
			name:  "nil block",
			input: func() *geyserpb.SubscribeUpdateBlock { return nil },
			want:  "nil block",
		},
		{
			name: "nil transaction update",
			mutate: func(block *geyserpb.SubscribeUpdateBlock) {
				block.Transactions[0] = nil
			},
			want: "transaction 0 is nil",
		},
		{
			name: "nil transaction payload",
			mutate: func(block *geyserpb.SubscribeUpdateBlock) {
				block.Transactions[0].Transaction = nil
			},
			want: "nil transaction",
		},
		{
			name: "nil transaction metadata",
			mutate: func(block *geyserpb.SubscribeUpdateBlock) {
				block.Transactions[0].Meta = nil
			},
			want: "nil transaction metadata",
		},
		{
			name: "nil block time",
			mutate: func(block *geyserpb.SubscribeUpdateBlock) {
				block.BlockTime = nil
			},
			want: "has no block time",
		},
		{
			name: "nil rewards",
			mutate: func(block *geyserpb.SubscribeUpdateBlock) {
				block.Rewards = nil
			},
			want: "has no rewards",
		},
		{
			name: "invalid blockhash",
			mutate: func(block *geyserpb.SubscribeUpdateBlock) {
				block.Blockhash = "not-base58!"
			},
			want: "invalid blockhash",
		},
		{
			name: "invalid parent blockhash",
			mutate: func(block *geyserpb.SubscribeUpdateBlock) {
				block.ParentBlockhash = "not-base58!"
			},
			want: "invalid parent blockhash",
		},
		{
			name: "nil reward",
			mutate: func(block *geyserpb.SubscribeUpdateBlock) {
				block.Rewards.Rewards = []*geyserpb.Reward{nil}
			},
			want: "nil reward",
		},
		{
			name: "invalid reward pubkey",
			mutate: func(block *geyserpb.SubscribeUpdateBlock) {
				block.Rewards.Rewards = []*geyserpb.Reward{{Pubkey: "not-base58!"}}
			},
			want: "invalid reward pubkey",
		},
		{
			name: "negative fee reward",
			mutate: func(block *geyserpb.SubscribeUpdateBlock) {
				block.Rewards.Rewards = []*geyserpb.Reward{{
					Pubkey:     (solana.PublicKey{}).String(),
					Lamports:   -1,
					RewardType: geyserpb.RewardType_Fee,
				}}
			},
			want: "fee reward has negative lamports",
		},
		{
			name: "short readonly loaded address",
			mutate: func(block *geyserpb.SubscribeUpdateBlock) {
				block.Transactions[0].Meta.LoadedReadonlyAddresses = [][]byte{make([]byte, solana.PublicKeyLength-1)}
			},
			want: "readonly loaded address 0 has invalid length",
		},
		{
			name: "short writable loaded address",
			mutate: func(block *geyserpb.SubscribeUpdateBlock) {
				block.Transactions[0].Meta.LoadedWritableAddresses = [][]byte{make([]byte, solana.PublicKeyLength-1)}
			},
			want: "writable loaded address 0 has invalid length",
		},
		{
			name: "short balance vectors",
			mutate: func(block *geyserpb.SubscribeUpdateBlock) {
				block.Transactions[0].Meta.PreBalances = nil
			},
			want: "metadata balance count mismatch",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var input *geyserpb.SubscribeUpdateBlock
			if test.input != nil {
				input = test.input()
			} else {
				input = validLaserstreamBlock()
			}
			if test.mutate != nil {
				test.mutate(input)
			}

			var (
				got *Block
				err error
			)
			require.NotPanics(t, func() {
				got, err = FromLaserStream(input, nil)
			})
			require.Nil(t, got)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestFromLaserStreamConvertsValidBlock(t *testing.T) {
	got, err := FromLaserStream(validLaserstreamBlock(), nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, uint64(42), got.Slot)
	require.Equal(t, int64(123), got.UnixTimestamp)
	require.Len(t, got.Transactions, 1)
	require.Len(t, got.TxMetas, 1)
}
