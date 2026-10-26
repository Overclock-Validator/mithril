package sealevel

import (
	"bytes"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/maypok86/otter"
	"github.com/stretchr/testify/require"
)

type programReadSource struct {
	account       atomic.Pointer[accounts.Account]
	owned, shared atomic.Int64
	err           error
}

func (s *programReadSource) GetAccount(_ uint64, _ solana.PublicKey) (*accounts.Account, error) {
	s.owned.Add(1)
	if s.err != nil {
		return nil, s.err
	}
	return s.account.Load().Clone(), nil
}

func (s *programReadSource) GetAccountShared(_ uint64, _ solana.PublicKey) (*accounts.Account, error) {
	s.shared.Add(1)
	return s.account.Load(), s.err
}

// Hide the optional shared interface to exercise the owned-read fallback.
type ownedProgramReadSource struct{ AccountReader }

func upgradeableReadFixture(t testing.TB, w programWorkload) (accounts.Account, *accounts.Account) {
	t.Helper()
	dataKey := benchPubkey(0x92)
	var encoded bytes.Buffer
	state := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram,
		Program: UpgradeableLoaderStateProgram{ProgramDataAddress: dataKey}}
	require.NoError(t, state.MarshalWithEncoder(bin.NewBinEncoder(&encoded)))
	program := accounts.Account{Key: w.program, Owner: a.BpfLoaderUpgradeableAddr,
		Executable: true, Lamports: 10000000, Data: bytes.Clone(encoded.Bytes())}
	encoded.Reset()
	state = UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData,
		ProgramData: UpgradeableLoaderStateProgramData{Slot: 1336}}
	require.NoError(t, state.MarshalWithEncoder(bin.NewBinEncoder(&encoded)))
	data := make([]byte, int(upgradeableLoaderSizeOfProgramDataMetaData)+len(w.elf))
	copy(data, encoded.Bytes())
	copy(data[upgradeableLoaderSizeOfProgramDataMetaData:], w.elf)
	return program, &accounts.Account{Key: dataKey, Owner: a.BpfLoaderUpgradeableAddr,
		Lamports: 10000000, Data: data}
}

func upgradeableReadRunner(t testing.TB, w programWorkload, vasa bool, reader AccountReader, local *accounts.Account) func() (*ExecutionCtx, error) {
	t.Helper()
	program, _ := upgradeableReadFixture(t, w)
	cache, err := otter.MustBuilder[solana.PublicKey, *accountsdb.ProgramCacheEntry](1024).
		Cost(func(solana.PublicKey, *accountsdb.ProgramCacheEntry) uint32 { return 1 }).Build()
	require.NoError(t, err)
	t.Cleanup(cache.Close)
	db := &accountsdb.AccountsDb{ProgramCache: cache}
	return func() (*ExecutionCtx, error) {
		list := make([]accounts.Account, len(w.accts)+1)
		list[0] = program
		for i, acct := range w.accts {
			list[i+1] = *acct.Clone()
		}
		tx := NewTransactionAccounts(list)
		ctx := newBenchExecCtx(tx, 1337)
		ctx.TransactionContext.ComputeBudgetLimits = &ComputeBudgetLimits{UpdatedHeapBytes: 32768}
		if vasa {
			ctx.Features.EnableFeature(features.VirtualAddressSpaceAdjustments, 0)
		}
		ctx.ComputeMeter = cu.NewComputeMeter(1400000)
		if local != nil {
			require.NoError(t, ctx.Accounts.SetAccount((*[32]byte)(&local.Key), local))
		}
		ctx.SlotCtx = &SlotCtx{Slot: 1337, Accounts: ctx.Accounts, AccountsDb: db, UnrootedRead: reader}
		ctx.Log = &LogRecorder{}
		ctx.RecordInnerInstructions = true
		err := ctx.ProcessInstruction(w.instruction, InstructionAcctsFromAccountMetas(w.metas, *tx), []uint64{0})
		return ctx, err
	}
}

func TestReadOnlyProgramLoading(t *testing.T) {
	for _, w := range expandedProgramWorkloads(t) {
		for _, vasa := range []bool{false, true} {
			name := w.name
			if vasa {
				name += "_VASA"
			}
			t.Run(name, func(t *testing.T) {
				_, data := upgradeableReadFixture(t, w)
				before := bytes.Clone(data.Data)
				source := &programReadSource{}
				source.account.Store(data)
				reference := upgradeableReadRunner(t, w, vasa, ownedProgramReadSource{source}, nil)
				want, err := reference()
				require.NoError(t, err)
				w.check(t, want)
				for _, local := range []bool{false, true} {
					reader := &programReadSource{}
					reader.account.Store(data)
					var current *accounts.Account
					if local {
						current = data
						reader.err = errors.New("current bank must win over parent")
					}
					run := upgradeableReadRunner(t, w, vasa, reader, current)
					for i := 0; i < 3; i++ { // Cold load, then warm reuse (including CPI workloads).
						got, err := run()
						require.NoError(t, err)
						w.check(t, got)
						require.Equal(t, want.ComputeMeter.Used(), got.ComputeMeter.Used())
						require.Equal(t, want.InnerInstrs, got.InnerInstrs)
						require.Equal(t, before, data.Data, "loader or VM mutated borrowed source")
					}
					require.Zero(t, reader.owned.Load())
					if local {
						require.Zero(t, reader.shared.Load())
					} else {
						require.Equal(t, 3*source.owned.Load(), reader.shared.Load())
					}
				}
			})
		}
	}
}

func TestReadOnlyProgramWarmCacheRevalidatesSource(t *testing.T) {
	w := expandedProgramWorkloads(t)[0]
	for _, kind := range []string{"same-slot-fork", "current-slot-deployment", "closed", "missing"} {
		t.Run(kind, func(t *testing.T) {
			_, data := upgradeableReadFixture(t, w)
			before := bytes.Clone(data.Data)
			source := &programReadSource{}
			source.account.Store(data)
			run := upgradeableReadRunner(t, w, false, source, nil)
			reference := upgradeableReadRunner(t, w, false, ownedProgramReadSource{source}, nil)
			_, err := reference()
			require.NoError(t, err)
			_, err = run()
			require.NoError(t, err)
			replacement := data.Clone()
			switch kind {
			case "same-slot-fork":
				replacement.Data[upgradeableLoaderSizeOfProgramDataMetaData] = 0 // Invalid ELF, same deployment slot.
			case "current-slot-deployment":
				var encoded bytes.Buffer
				state := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{Slot: 1337}}
				require.NoError(t, state.MarshalWithEncoder(bin.NewBinEncoder(&encoded)))
				copy(replacement.Data, encoded.Bytes())
			case "closed":
				clear(replacement.Data[:4]) // Uninitialized loader state.
			case "missing":
				source.err = accountsdb.ErrNoAccount
			}
			source.account.Store(replacement)
			_, wantErr := reference()
			require.Error(t, wantErr)
			_, err = run()
			require.ErrorIs(t, err, wantErr)
			require.Equal(t, before, data.Data)
		})
	}
}

func TestReadOnlyAccountSourceFallbackAndBankSysvars(t *testing.T) {
	key := benchPubkey(0x92)
	source := &programReadSource{}
	source.account.Store(&accounts.Account{Key: key, Data: []byte{1, 2, 3}})
	ctx := &SlotCtx{Slot: 42, UnrootedRead: source}
	shared, err := ctx.getAccountFromAccountsDbReadOnly(key)
	require.NoError(t, err)
	require.Same(t, source.account.Load(), shared)
	ctx.UnrootedRead = ownedProgramReadSource{source}
	owned, err := ctx.getAccountFromAccountsDbReadOnly(key)
	require.NoError(t, err)
	owned.Data[0] = 9
	require.Equal(t, byte(1), shared.Data[0])
	ctx.UnrootedRead = source
	bank, err := NewBankSysvars(42, clockSysvarTestAccount(SysvarClock{Slot: 42}))
	require.NoError(t, err)
	require.NoError(t, ctx.PublishBankSysvars(bank))
	reads := source.shared.Load()
	got, err := ctx.getAccountFromAccountsDbReadOnly(solana.PublicKey(SysvarClockAddr))
	require.NoError(t, err)
	want, err := ctx.GetAccountFromAccountsDb(solana.PublicKey(SysvarClockAddr))
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, reads, source.shared.Load())
}

func BenchmarkUpgradeableProgramRead(b *testing.B) {
	for _, w := range expandedProgramWorkloads(b) {
		b.Run(w.name, func(b *testing.B) {
			_, data := upgradeableReadFixture(b, w)
			source := &programReadSource{}
			source.account.Store(data)
			run := upgradeableReadRunner(b, w, false, source, nil)
			want, err := run()
			require.NoError(b, err)
			w.check(b, want)
			used := want.ComputeMeter.Used()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got, err := run()
				if err != nil || got.ComputeMeter.Used() != used {
					b.Fatalf("execution/CU changed: %v", err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(used), "cu/op")
		})
	}
}
