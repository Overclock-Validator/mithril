package replay

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// lookupTestSlot matches newCommitTestSlotCtx.
const lookupTestSlot = 42

func encodeLookupTable(t *testing.T, key solana.PublicKey, meta sealevel.LookupTableMeta, addrs ...solana.PublicKey) *accounts.Account {
	t.Helper()
	var buf bytes.Buffer
	enc := bin.NewBinEncoder(&buf)
	require.NoError(t, enc.WriteUint32(sealevel.AddressLookupTableProgramStateLookupTable, bin.LE))
	require.NoError(t, meta.MarshalWithEncoder(enc))
	for len(buf.Bytes()) < sealevel.AddressLookupTableMetaSize {
		require.NoError(t, enc.WriteByte(0))
	}
	for _, addr := range addrs {
		require.NoError(t, enc.WriteBytes(addr[:], false))
	}
	return &accounts.Account{Key: key, Lamports: 1_000_000, Owner: addresses.AddressLookupTableAddr, Data: buf.Bytes(), RentEpoch: math.MaxUint64}
}

func activeTableMeta() sealevel.LookupTableMeta {
	authority := txfixture.PayerPubkey()
	return sealevel.LookupTableMeta{DeactivationSlot: math.MaxUint64, LastExtendedSlot: 1, Authority: &authority}
}

func lookupTestAddresses(n int) []solana.PublicKey {
	addrs := make([]solana.PublicKey, n)
	for i := range addrs {
		addrs[i] = solana.NewWallet().PublicKey()
	}
	return addrs
}

// lookupTestTx is a v0 message with static keys [payer, system] and the given
// lookups; the instruction only has to make the message well formed.
func lookupTestTx(t *testing.T, lookups ...solana.MessageAddressTableLookup) *solana.Transaction {
	t.Helper()
	msg := solana.Message{
		Header:              solana.MessageHeader{NumRequiredSignatures: 1, NumReadonlyUnsignedAccounts: 1},
		AccountKeys:         solana.PublicKeySlice{txfixture.PayerPubkey(), solana.SystemProgramID},
		RecentBlockhash:     txfixture.TestBlockhash(),
		Instructions:        []solana.CompiledInstruction{{ProgramIDIndex: 1, Accounts: []uint16{0}}},
		AddressTableLookups: lookups,
	}
	_, err := msg.SetVersion(solana.MessageVersionV0)
	require.NoError(t, err)
	tx := &solana.Transaction{Message: msg}
	payerKey := txfixture.PayerPrivateKey()
	_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
		if key.Equals(txfixture.PayerPubkey()) {
			return &payerKey
		}
		return nil
	})
	require.NoError(t, err)
	return tx
}

// lookupParentState is a bank's parent state for lookup tests. It serves both
// as the slot's UnrootedRead and as replay's block account source.
type lookupParentState map[solana.PublicKey]*accounts.Account

func (s lookupParentState) GetAccount(_ uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
	if acct, ok := s[pubkey]; ok {
		return acct, nil
	}
	return nil, fmt.Errorf("account %s not found", pubkey)
}

func (s lookupParentState) GetAccountsBatch(_ context.Context, slot uint64, pks []solana.PublicKey) ([]*accounts.Account, error) {
	out := make([]*accounts.Account, len(pks))
	for i, pk := range pks {
		out[i], _ = s.GetAccount(slot, pk)
	}
	return out, nil
}

// newLookupTestSlotCtx is a bank at lookupTestSlot whose parent state holds
// tables.
func newLookupTestSlotCtx(t *testing.T, tables ...*accounts.Account) *sealevel.SlotCtx {
	t.Helper()
	slotCtx, cleanup := newCommitTestSlotCtx()
	t.Cleanup(cleanup)
	prev := sealevel.SysvarCache.SlotHashes.Sysvar
	slotHashes := sealevel.SysvarSlotHashes{{Slot: lookupTestSlot - 1}, {Slot: lookupTestSlot - 2}}
	sealevel.SysvarCache.SlotHashes.Sysvar = &slotHashes
	t.Cleanup(func() { sealevel.SysvarCache.SlotHashes.Sysvar = prev })
	parent := lookupParentState{}
	for _, table := range tables {
		parent[table.Key] = table
	}
	slotCtx.UnrootedRead = parent
	return slotCtx
}

func requireLookupError(t *testing.T, want TransactionErrorType, wantErr error, resolved *solana.Transaction, got *TransactionError) {
	t.Helper()
	require.Nil(t, resolved)
	require.NotNil(t, got)
	assert.Equal(t, want, got.ErrorType, "got %s", got.ErrorType.String())
	assert.ErrorIs(t, got.InstructionError, wantErr)
}

func TestResolveAddressTableLookupsOrdersWritableThenReadonly(t *testing.T) {
	t1, t2 := solana.NewWallet().PublicKey(), solana.NewWallet().PublicKey()
	a1, a2 := lookupTestAddresses(3), lookupTestAddresses(2)
	slotCtx := newLookupTestSlotCtx(t,
		encodeLookupTable(t, t1, activeTableMeta(), a1...),
		encodeLookupTable(t, t2, activeTableMeta(), a2...))
	tx := lookupTestTx(t,
		solana.MessageAddressTableLookup{AccountKey: t1, WritableIndexes: []uint8{2}, ReadonlyIndexes: []uint8{0}},
		solana.MessageAddressTableLookup{AccountKey: t2, WritableIndexes: []uint8{1}, ReadonlyIndexes: []uint8{0}},
	)
	wire, err := tx.MarshalBinary()
	require.NoError(t, err)

	resolved, txErr := ResolveAddressTableLookups(slotCtx, tx)
	require.Nil(t, txErr)

	static := tx.Message.AccountKeys
	assert.Equal(t, solana.PublicKeySlice{static[0], static[1], a1[2], a2[1], a1[0], a2[0]}, resolved.Message.AccountKeys)
	for i, want := range []bool{true, false, true, true, false, false} {
		writable, err := resolved.Message.IsWritable(resolved.Message.AccountKeys[i])
		require.NoError(t, err)
		assert.Equal(t, want, writable, "key %d", i)
	}

	// The input is untouched and both serialize to the original wire.
	assert.False(t, tx.Message.IsResolved())
	assert.Len(t, tx.Message.AccountKeys, 2)
	gotWire, err := resolved.MarshalBinary()
	require.NoError(t, err)
	assert.Equal(t, wire, gotWire)
	origWire, err := tx.MarshalBinary()
	require.NoError(t, err)
	assert.Equal(t, wire, origWire)
}

func TestResolveAddressTableLookupsSameTableTwice(t *testing.T) {
	table := solana.NewWallet().PublicKey()
	addrs := lookupTestAddresses(2)
	slotCtx := newLookupTestSlotCtx(t, encodeLookupTable(t, table, activeTableMeta(), addrs...))
	tx := lookupTestTx(t,
		solana.MessageAddressTableLookup{AccountKey: table, WritableIndexes: []uint8{0}},
		solana.MessageAddressTableLookup{AccountKey: table, ReadonlyIndexes: []uint8{1}},
	)
	resolved, txErr := ResolveAddressTableLookups(slotCtx, tx)
	require.Nil(t, txErr)
	assert.Equal(t, addrs, []solana.PublicKey(resolved.Message.AccountKeys[2:]))
}

func TestResolveAddressTableLookupsActivation(t *testing.T) {
	cases := []struct {
		name             string
		deactivationSlot uint64
		wantNotFound     bool
	}{
		{"activated", math.MaxUint64, false},
		{"deactivating in the current slot", lookupTestSlot, false},
		{"deactivating, still in SlotHashes", lookupTestSlot - 2, false},
		{"deactivated, gone from SlotHashes", lookupTestSlot - 10, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			table := solana.NewWallet().PublicKey()
			meta := activeTableMeta()
			meta.DeactivationSlot = tc.deactivationSlot
			slotCtx := newLookupTestSlotCtx(t, encodeLookupTable(t, table, meta, lookupTestAddresses(1)...))
			tx := lookupTestTx(t, solana.MessageAddressTableLookup{AccountKey: table, ReadonlyIndexes: []uint8{0}})

			resolved, txErr := ResolveAddressTableLookups(slotCtx, tx)
			if tc.wantNotFound {
				requireLookupError(t, TransactionErrorAddressLookupTableNotFound, TxErrAddressLookupTableNotFound, resolved, txErr)
				return
			}
			require.Nil(t, txErr)
			assert.Len(t, resolved.Message.AccountKeys, 3)
		})
	}
}

func TestResolveAddressTableLookupsExcludesAddressesAppendedThisSlot(t *testing.T) {
	table := solana.NewWallet().PublicKey()
	addrs := lookupTestAddresses(3)
	extendedNow := activeTableMeta()
	extendedNow.LastExtendedSlot = lookupTestSlot
	extendedNow.LastExtendedSlotStartIndex = 2

	slotCtx := newLookupTestSlotCtx(t, encodeLookupTable(t, table, extendedNow, addrs...))
	resolved, txErr := ResolveAddressTableLookups(slotCtx, lookupTestTx(t, solana.MessageAddressTableLookup{AccountKey: table, WritableIndexes: []uint8{1}}))
	require.Nil(t, txErr, "addresses appended before this slot are active")
	assert.Equal(t, addrs[1], resolved.Message.AccountKeys[2])

	// Index 2 was appended in this slot, so it is not active yet.
	resolved, txErr = ResolveAddressTableLookups(slotCtx, lookupTestTx(t, solana.MessageAddressTableLookup{AccountKey: table, ReadonlyIndexes: []uint8{2}}))
	requireLookupError(t, TransactionErrorInvalidAddressLookupTableIndex, TxErrInvalidAddressLookupTableIndex, resolved, txErr)

	// Extended in the previous slot: every address is active.
	extendedBefore := extendedNow
	extendedBefore.LastExtendedSlot = lookupTestSlot - 1
	slotCtx = newLookupTestSlotCtx(t, encodeLookupTable(t, table, extendedBefore, addrs...))
	resolved, txErr = ResolveAddressTableLookups(slotCtx, lookupTestTx(t, solana.MessageAddressTableLookup{AccountKey: table, ReadonlyIndexes: []uint8{2}}))
	require.Nil(t, txErr)
	assert.Equal(t, addrs[2], resolved.Message.AccountKeys[2])
}

func TestResolveAddressTableLookupsRejectsBadTables(t *testing.T) {
	table := solana.NewWallet().PublicKey()
	good := encodeLookupTable(t, table, activeTableMeta(), lookupTestAddresses(2)...)

	zeroLamports := *good
	zeroLamports.Lamports = 0
	wrongOwner := *good
	wrongOwner.Owner = addresses.SystemProgramAddr
	garbage := *good
	garbage.Data = []byte{1, 2, 3}
	badStart := activeTableMeta()
	badStart.LastExtendedSlot = lookupTestSlot
	badStart.LastExtendedSlotStartIndex = 9

	cases := []struct {
		name    string
		table   *accounts.Account
		index   uint8
		want    TransactionErrorType
		wantErr error
	}{
		{"missing table", nil, 0, TransactionErrorAddressLookupTableNotFound, TxErrAddressLookupTableNotFound},
		{"zero lamports", &zeroLamports, 0, TransactionErrorAddressLookupTableNotFound, TxErrAddressLookupTableNotFound},
		{"wrong owner", &wrongOwner, 0, TransactionErrorInvalidAddressLookupTableOwner, TxErrInvalidAddressLookupTableOwner},
		{"undecodable data", &garbage, 0, TransactionErrorInvalidAddressLookupTableData, TxErrInvalidAddressLookupTableData},
		{"active length past the end", encodeLookupTable(t, table, badStart, lookupTestAddresses(2)...), 0, TransactionErrorInvalidAddressLookupTableData, TxErrInvalidAddressLookupTableData},
		{"index out of range", good, 2, TransactionErrorInvalidAddressLookupTableIndex, TxErrInvalidAddressLookupTableIndex},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var tables []*accounts.Account
			if tc.table != nil {
				tables = append(tables, tc.table)
			}
			slotCtx := newLookupTestSlotCtx(t, tables...)
			tx := lookupTestTx(t, solana.MessageAddressTableLookup{AccountKey: table, ReadonlyIndexes: []uint8{tc.index}})
			resolved, txErr := ResolveAddressTableLookups(slotCtx, tx)
			requireLookupError(t, tc.want, tc.wantErr, resolved, txErr)
		})
	}
}

func TestResolveAddressTableLookupsPassesThroughAndRefusesResolved(t *testing.T) {
	slotCtx := newLookupTestSlotCtx(t)
	legacy, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(0))
	require.NoError(t, err)
	got, txErr := ResolveAddressTableLookups(slotCtx, legacy)
	require.Nil(t, txErr)
	assert.Same(t, legacy, got)

	table := solana.NewWallet().PublicKey()
	tx := lookupTestTx(t, solana.MessageAddressTableLookup{AccountKey: table, ReadonlyIndexes: []uint8{0}})
	require.NoError(t, tx.Message.SetAddressTables(map[solana.PublicKey]solana.PublicKeySlice{table: lookupTestAddresses(1)}))
	require.NoError(t, tx.Message.ResolveLookups())
	got, txErr = ResolveAddressTableLookups(slotCtx, tx)
	requireLookupError(t, TransactionErrorSanitizeFailure, TxErrSanitizeFailure, got, txErr)
}

// For an active table, resolving on the leader must give the account keys
// replay resolves for the same transaction in the produced block.
func TestResolveAddressTableLookupsMatchesReplayResolution(t *testing.T) {
	t1, t2 := solana.NewWallet().PublicKey(), solana.NewWallet().PublicKey()
	tables := []*accounts.Account{
		encodeLookupTable(t, t1, activeTableMeta(), lookupTestAddresses(4)...),
		encodeLookupTable(t, t2, activeTableMeta(), lookupTestAddresses(3)...),
	}
	slotCtx := newLookupTestSlotCtx(t, tables...)
	wire, err := lookupTestTx(t,
		solana.MessageAddressTableLookup{AccountKey: t2, WritableIndexes: []uint8{2, 0}, ReadonlyIndexes: []uint8{1}},
		solana.MessageAddressTableLookup{AccountKey: t1, WritableIndexes: []uint8{3}, ReadonlyIndexes: []uint8{0, 2}},
	).MarshalBinary()
	require.NoError(t, err)

	leaderTx, err := solana.TransactionFromBytes(wire)
	require.NoError(t, err)
	leaderResolved, txErr := ResolveAddressTableLookups(slotCtx, leaderTx)
	require.Nil(t, txErr)

	replayTx, err := solana.TransactionFromBytes(wire)
	require.NoError(t, err)
	source := lookupParentState{t1: tables[0], t2: tables[1]}
	slotHashes, ok := slotHashesForAddressLookups(slotCtx)
	require.True(t, ok)
	require.NoError(t, resolveAddrTableLookups(source, &b.Block{Slot: lookupTestSlot, Transactions: []*solana.Transaction{replayTx}}, slotHashes))
	require.True(t, replayTx.Message.IsResolved())

	assert.Equal(t, replayTx.Message.AccountKeys, leaderResolved.Message.AccountKeys)
}

// Resolving from the parent copy of a table is only sound because whatever
// the slot does to the table cannot change lookups in that slot. The parent
// copy has 2 addresses; the slot then extends it twice to 4. Only the slot's
// first extension records the start index, at the pre-extension count, so
// both copies must give identical results at the executing slot. This fails
// if the address lookup table program ever moved the start index on later
// extensions of the same slot.
func TestLookupTableParentAndSlotCopiesAgree(t *testing.T) {
	table := solana.NewWallet().PublicKey()
	addrs := lookupTestAddresses(4)
	parentMeta := activeTableMeta()
	parentMeta.LastExtendedSlot = lookupTestSlot - 1
	parentCopy := encodeLookupTable(t, table, parentMeta, addrs[:2]...)
	slotMeta := activeTableMeta()
	slotMeta.LastExtendedSlot = lookupTestSlot
	slotMeta.LastExtendedSlotStartIndex = 2
	slotCopy := encodeLookupTable(t, table, slotMeta, addrs...)
	slotHashes := sealevel.SysvarSlotHashes{{Slot: lookupTestSlot - 1}, {Slot: lookupTestSlot - 2}}
	resolve := func(acct *accounts.Account, lookup *solana.MessageAddressTableLookup) (solana.PublicKeySlice, *TransactionError) {
		active, txErr := activeLookupTable(acct, lookupTestSlot, slotHashes)
		if txErr == nil {
			txErr = checkLookupIndexes(active, lookup)
		}
		if txErr != nil {
			return nil, txErr
		}
		return active, nil
	}

	for _, idx := range []uint8{0, 1, 2, 3} {
		lookup := &solana.MessageAddressTableLookup{AccountKey: table, ReadonlyIndexes: []uint8{idx}}
		fromParent, parentErr := resolve(parentCopy, lookup)
		fromSlot, slotErr := resolve(slotCopy, lookup)
		assert.Equal(t, parentErr, slotErr, "index %d", idx)
		assert.Equal(t, fromParent, fromSlot, "index %d", idx)
		if idx < 2 {
			require.Nil(t, parentErr, "index %d", idx)
			assert.Equal(t, addrs[idx], fromParent[idx])
		} else {
			require.NotNil(t, parentErr, "index %d", idx)
			assert.ErrorIs(t, parentErr.InstructionError, TxErrInvalidAddressLookupTableIndex)
		}
	}
}

// Writes the slot itself made to a table are not read.
func TestResolveAddressTableLookupsIgnoresSlotWrites(t *testing.T) {
	table := solana.NewWallet().PublicKey()
	addrs := lookupTestAddresses(2)

	// Created in the slot: absent from the parent, so not found.
	slotCtx := newLookupTestSlotCtx(t)
	require.NoError(t, slotCtx.SetAccount(table, encodeLookupTable(t, table, activeTableMeta(), addrs...)))
	resolved, txErr := ResolveAddressTableLookups(slotCtx, lookupTestTx(t, solana.MessageAddressTableLookup{AccountKey: table, ReadonlyIndexes: []uint8{0}}))
	requireLookupError(t, TransactionErrorAddressLookupTableNotFound, TxErrAddressLookupTableNotFound, resolved, txErr)

	// Closed in the slot: the parent copy is what counts.
	slotCtx = newLookupTestSlotCtx(t, encodeLookupTable(t, table, activeTableMeta(), addrs...))
	closed := encodeLookupTable(t, table, activeTableMeta(), addrs...)
	closed.Lamports = 0
	require.NoError(t, slotCtx.SetAccount(table, closed))
	resolved, txErr = ResolveAddressTableLookups(slotCtx, lookupTestTx(t, solana.MessageAddressTableLookup{AccountKey: table, ReadonlyIndexes: []uint8{1}}))
	require.Nil(t, txErr)
	assert.Equal(t, addrs[1], resolved.Message.AccountKeys[2])
}
