package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractAndDedupeBlockAcctsPublicationCapacity(t *testing.T) {
	payer := publicationConcurrencyTestKey(1)
	firstWritable := publicationConcurrencyTestKey(2)
	secondWritable := publicationConcurrencyTestKey(3)
	readonly := publicationConcurrencyTestKey(4)
	updated := publicationConcurrencyTestKey(5)
	lookupProgram := publicationConcurrencyTestKey(6)
	dynamicReadonly := publicationConcurrencyTestKey(7)
	tableID := publicationConcurrencyTestKey(8)

	newTransaction := func(writable solana.PublicKey) *solana.Transaction {
		return &solana.Transaction{Message: solana.Message{
			Header: solana.MessageHeader{
				NumRequiredSignatures:       1,
				NumReadonlyUnsignedAccounts: 1,
			},
			AccountKeys: []solana.PublicKey{payer, writable, readonly},
		}}
	}

	resolvedTransaction := &solana.Transaction{Message: solana.Message{
		Header: solana.MessageHeader{
			NumRequiredSignatures:       1,
			NumReadonlyUnsignedAccounts: 1,
		},
		AccountKeys: []solana.PublicKey{payer, lookupProgram},
	}}
	resolvedTransaction.Message.SetAddressTableLookups([]solana.MessageAddressTableLookup{{
		AccountKey:      tableID,
		WritableIndexes: []byte{0},
		ReadonlyIndexes: []byte{1},
	}})
	require.NoError(t, resolvedTransaction.Message.SetAddressTables(map[solana.PublicKey]solana.PublicKeySlice{
		tableID: {readonly, dynamicReadonly},
	}))
	require.NoError(t, resolvedTransaction.Message.ResolveLookups())
	assert.ElementsMatch(t, []solana.PublicKey{payer, readonly}, messageWritableAccounts(&resolvedTransaction.Message))

	block := &b.Block{
		Transactions: []*solana.Transaction{
			newTransaction(firstWritable),
			newTransaction(secondWritable),
			resolvedTransaction,
		},
		UpdatedAccts:           []solana.PublicKey{updated},
		EpochUpdatedAccts:      make([]*accounts.Account, 2),
		EpochStakesPerVoteAcct: make(map[solana.PublicKey]uint64, 100),
	}
	for idx := uint64(0); idx < 100; idx++ {
		block.EpochStakesPerVoteAcct[publicationConcurrencyTestKey(100+idx)] = idx
	}

	pubkeys, uniqueWritableAccounts := extractAndDedupeBlockAccts(block)
	require.Len(t, pubkeys, 7)
	assert.ElementsMatch(t, []solana.PublicKey{
		payer, firstWritable, secondWritable, readonly, updated, lookupProgram, dynamicReadonly,
	}, pubkeys)
	assert.Equal(t, 5, uniqueWritableAccounts, "readonly key must upgrade to writable across messages")
	nonTransactionCapacity := len(block.EpochUpdatedAccts) + transactionPublicationNonTransactionSlack
	assert.Equal(t, 5+nonTransactionCapacity, publicationMapCapacity(block, uniqueWritableAccounts, false))
	assert.Equal(t, 5+nonTransactionCapacity+len(block.EpochStakesPerVoteAcct), publicationMapCapacity(block, uniqueWritableAccounts, true))
	assert.Equal(t, len(block.Transactions)*expectedTouchedAccountsPerTransaction+nonTransactionCapacity, publicationMapCapacity(block, 20, false))
}

func TestIncludeAlpenglowParentStateAccountsPinsNanosecondClockOnce(t *testing.T) {
	other := solana.PublicKey{1}
	nanoClock := NanosecondClockAccountAddr()

	require.Equal(t, []solana.PublicKey{other}, includeAlpenglowParentStateAccounts([]solana.PublicKey{other}, false))
	require.Equal(t, []solana.PublicKey{other, nanoClock}, includeAlpenglowParentStateAccounts([]solana.PublicKey{other}, true))
	require.Equal(t, []solana.PublicKey{other, nanoClock}, includeAlpenglowParentStateAccounts([]solana.PublicKey{other, nanoClock}, true))
}
