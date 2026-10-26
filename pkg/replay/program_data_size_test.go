package replay

import (
	"errors"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

type sizeOnlyReader struct {
	size           uint64
	err            error
	reads, lengths int
}

func (r *sizeOnlyReader) GetAccount(uint64, solana.PublicKey) (*accounts.Account, error) {
	r.reads++
	return nil, errors.New("account data must not be loaded for a size query")
}
func (r *sizeOnlyReader) GetAccountDataLen(uint64, solana.PublicKey) (uint64, error) {
	r.lengths++
	return r.size, r.err
}

type accountOnlyReader struct{ source sealevel.AccountReader }

func (r accountOnlyReader) GetAccount(slot uint64, key solana.PublicKey) (*accounts.Account, error) {
	return r.source.GetAccount(slot, key)
}
func sizeTestProgram(t testing.TB, dataKey solana.PublicKey) *accounts.Account {
	t.Helper()
	data, err := marshalUpgradeableLoaderStateSized(&sealevel.UpgradeableLoaderState{
		Type:    sealevel.UpgradeableLoaderStateTypeProgram,
		Program: sealevel.UpgradeableLoaderStateProgram{ProgramDataAddress: dataKey},
	}, upgradeableLoaderProgramStateSize)
	if err != nil {
		t.Fatal(err)
	}
	return &accounts.Account{Key: testPubkey(88), Owner: addresses.BpfLoaderUpgradeableAddr, Lamports: 1, Data: data}
}
func TestProgramDataSizeMetadataAccounting(t *testing.T) {
	key := testPubkey(89)
	program := sizeTestProgram(t, key)
	base := uint64(txAcctBaseSize + len(program.Data))
	extra := uint64(txAcctBaseSize) + (2 << 20)
	for _, tc := range []struct {
		name                     string
		limit                    uint64
		explicit, local, missing bool
		want                     uint64
		wantErr                  bool
		calls                    int
	}{
		{name: "exact limit", limit: base + extra, want: base + extra, calls: 1},
		{name: "over limit", limit: base + extra - 1, want: base + extra - 1, wantErr: true, calls: 1},
		{name: "program itself over limit", limit: base - 1, want: base - 1, wantErr: true},
		{name: "explicit programdata already counted", limit: math.MaxUint32, explicit: true, want: base},
		{name: "current bank overrides parent", limit: math.MaxUint32, local: true, want: base + txAcctBaseSize + 17},
		{name: "missing programdata ignored", limit: math.MaxUint32, missing: true, want: base, calls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := newSimd186SlotCtx()
			reader := &sizeOnlyReader{size: 2 << 20}
			ctx.UnrootedRead = reader
			if tc.missing {
				reader.err = accountsdb.ErrNoAccount
			}
			keys := []solana.PublicKey{program.Key}
			if tc.explicit {
				keys = append(keys, key)
			}
			if tc.local {
				require.NoError(t, ctx.Accounts.SetAccountWithoutLock(key, &accounts.Account{Key: key, Data: make([]byte, 17)}))
			}
			acc := NewLoadedAcctSizeAccumulatorSimd186(ctx, tc.limit, keys)
			err := acc.collectAcct(program)
			if tc.wantErr {
				require.ErrorIs(t, err, TxErrMaxLoadedAccountsDataSizeExceeded)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, uint32(tc.want), acc.loadedAccountsDataSize())
			require.Zero(t, reader.reads)
			require.Equal(t, tc.calls, reader.lengths)
		})
	}
	ctx := newSimd186SlotCtx()
	reader := &sizeOnlyReader{size: 2 << 20}
	ctx.UnrootedRead = reader
	acc := NewLoadedAcctSizeAccumulatorSimd186(ctx, math.MaxUint32, []solana.PublicKey{program.Key})
	require.NoError(t, acc.collectAcct(program))
	require.NoError(t, acc.collectAcct(program))
	require.Equal(t, uint32(2*base+extra), acc.loadedAccountsDataSize())
	require.Equal(t, 1, reader.lengths)
}
func TestUnrootedAccountDataLengthOwnershipAndPrecedence(t *testing.T) {
	key := testPubkey(89)
	durable := &sharedAccountDurable{acct: &accounts.Account{Key: key, Data: []byte{1, 2, 3}}}
	tail := newUnrootedTail(durable, &fakeCommitter{}, 512, 1, "")
	size, err := tail.GetAccountDataLen(8, key)
	require.NoError(t, err)
	require.Equal(t, uint64(3), size)
	tail.overlay.Add(9, []*accounts.Account{{Key: key, Data: []byte{4, 5, 6, 7, 8}}})
	size, err = tail.GetAccountDataLen(10, key)
	require.NoError(t, err)
	require.Equal(t, uint64(5), size)
	mutable, err := tail.GetAccount(10, key)
	require.NoError(t, err)
	mutable.Data[0] = 99
	mutable.Data = append(mutable.Data, 9)
	size, err = tail.GetAccountDataLen(10, key)
	require.NoError(t, err)
	require.Equal(t, uint64(5), size)
	again, err := tail.GetAccount(10, key)
	require.NoError(t, err)
	require.Equal(t, byte(4), again.Data[0])
	require.Equal(t, []byte{1, 2, 3}, durable.acct.Data)
	ctx := newSimd186SlotCtx()
	ctx.UnrootedRead = accountOnlyReader{source: tail}
	size, err = ctx.GetAccountDataLenFromAccountsDb(key)
	require.NoError(t, err)
	require.Equal(t, uint64(5), size)
}
func BenchmarkLoadedProgramDataSize(b *testing.B) {
	key := testPubkey(89)
	program := sizeTestProgram(b, key)
	durable := &sharedAccountDurable{acct: &accounts.Account{Key: key, Data: make([]byte, 2<<20)}}
	tail := newUnrootedTail(durable, &fakeCommitter{}, 512, 1, "")
	for _, name := range []string{"clone", "metadata"} {
		b.Run(name, func(b *testing.B) {
			ctx := newSimd186SlotCtx()
			ctx.UnrootedRead = tail
			if name == "clone" {
				ctx.UnrootedRead = accountOnlyReader{source: tail}
			}
			keys := []solana.PublicKey{program.Key}
			want := uint32(2*txAcctBaseSize + len(program.Data) + len(durable.acct.Data))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				acc := NewLoadedAcctSizeAccumulatorSimd186(ctx, math.MaxUint32, keys)
				if err := acc.collectAcct(program); err != nil || acc.loadedAccountsDataSize() != want {
					b.Fatalf("size=%d err=%v", acc.loadedAccountsDataSize(), err)
				}
			}
		})
	}
}
