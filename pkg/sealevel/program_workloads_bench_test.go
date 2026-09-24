package sealevel

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/fixtures"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/Overclock-Validator/mithril/pkg/sbpf/loader"
	"github.com/gagliardetto/solana-go"
	"github.com/maypok86/otter"
	"github.com/stretchr/testify/require"
)

// External ELF inputs are pinned by SHA-256 in docs/sbpf-interpreter-benchmarks.md.
// No live account writes or network calls occur in this harness. Each invocation
// gets fresh account data; the program cache is warm and shared between runs.
type programWorkload struct {
	name        string
	elf         []byte
	program     solana.PublicKey
	accts       []accounts.Account
	metas       []AccountMeta
	instruction []byte
	check       func(testing.TB, *ExecutionCtx)
}

func expandedProgramWorkloads(t testing.TB) []programWorkload {
	t.Helper()
	program := benchPubkey(0x91)
	from := accounts.Account{Key: benchPubkey(0x31), Owner: program, Lamports: 10000}
	to := accounts.Account{Key: benchPubkey(0x32), Owner: program, Lamports: 10000}
	cases := []programWorkload{{name: "BPF_LamportTransfer", elf: fixtures.Load(t, "sbpf", "cpi_c_to_bpf.so"), program: program, accts: []accounts.Account{from, to}, metas: []AccountMeta{{Pubkey: program}, {Pubkey: from.Key, IsSigner: true, IsWritable: true}, {Pubkey: to.Key, IsSigner: true, IsWritable: true}}, instruction: []byte{0}, check: func(t testing.TB, ctx *ExecutionCtx) {
		src, err := ctx.TransactionContext.Accounts.GetAccount(1)
		require.NoError(t, err)
		dst, err := ctx.TransactionContext.Accounts.GetAccount(2)
		require.NoError(t, err)
		require.Equal(t, uint64(9000), src.Lamports)
		require.Equal(t, uint64(11000), dst.Lamports)
	}}}

	pda, bump, err := solana.FindProgramAddress([][]byte{[]byte("You pass butter")}, program)
	require.NoError(t, err)
	cases = append(cases, programWorkload{name: "CPI_Rust_SystemAllocate", elf: fixtures.Load(t, "sbpf", "cpi_rust_to_system_program_allocate.so"), program: program,
		accts: []accounts.Account{{Key: a.SystemProgramAddr, Owner: a.NativeLoaderAddr, Executable: true, Lamports: 10000}, {Key: pda, Owner: a.SystemProgramAddr, Lamports: 10000}},
		metas: []AccountMeta{{Pubkey: a.SystemProgramAddr}, {Pubkey: pda, IsSigner: true, IsWritable: true}}, instruction: []byte{bump}, check: func(t testing.TB, ctx *ExecutionCtx) {
			acct, e := ctx.TransactionContext.Accounts.GetAccount(2)
			require.NoError(t, e)
			require.Len(t, acct.Data, 1337)
			require.NotEmpty(t, ctx.InnerInstrs)
		}})
	dir := os.Getenv("MITHRIL_PROGRAM_BENCH_DIR")
	if dir == "" {
		return cases
	}
	arithmetic, err := os.ReadFile(filepath.Join(dir, "rotation_compute.so"))
	require.NoError(t, err)
	for _, iterations := range []uint32{500, 5000} {
		n := iterations
		data := append([]byte("RC01"), 0, 0, 0, 0)
		binary.LittleEndian.PutUint32(data[4:], n)
		name := "Arithmetic_500"
		if n == 5000 {
			name = "Arithmetic_5000"
		}
		cases = append(cases, programWorkload{name: name, elf: arithmetic, program: program, instruction: data, check: func(t testing.TB, ctx *ExecutionCtx) {
			x := uint64(0x9e3779b97f4a7c15)
			for i := uint32(0); i < n; i++ {
				x = ((x << 7) | (x >> 57)) ^ (uint64(i) + 0x517cc1b727220a95)
			}
			_, got := ctx.TransactionContext.ReturnData()
			require.Len(t, got, 8)
			require.Equal(t, x, binary.LittleEndian.Uint64(got))
		}})
	}
	token, err := os.ReadFile(filepath.Join(dir, "token2022.so"))
	require.NoError(t, err)
	mint, auth := benchPubkey(0x51), benchPubkey(0x52)
	tokenData := func(amount uint64) []byte {
		d := make([]byte, 165)
		copy(d, mint[:])
		copy(d[32:], auth[:])
		binary.LittleEndian.PutUint64(d[64:], amount)
		d[108] = 1
		return d
	}
	mintData := make([]byte, 82)
	mintData[44] = 6
	mintData[45] = 1
	src := accounts.Account{Key: benchPubkey(0x53), Owner: solana.Token2022ProgramID, Lamports: 10000000, Data: tokenData(1000000)}
	dst := accounts.Account{Key: benchPubkey(0x54), Owner: solana.Token2022ProgramID, Lamports: 10000000, Data: tokenData(5)}
	instr := append([]byte{12}, binary.LittleEndian.AppendUint64(nil, 1000)...)
	instr = append(instr, 6)
	cases = append(cases, programWorkload{name: "Token2022_TransferChecked", elf: token, program: solana.Token2022ProgramID,
		accts: []accounts.Account{src, {Key: mint, Owner: solana.Token2022ProgramID, Lamports: 10000000, Data: mintData}, dst, {Key: auth, Owner: a.SystemProgramAddr, Lamports: 10000000}},
		metas: []AccountMeta{{Pubkey: src.Key, IsWritable: true}, {Pubkey: mint}, {Pubkey: dst.Key, IsWritable: true}, {Pubkey: auth, IsSigner: true}}, instruction: instr,
		check: func(t testing.TB, ctx *ExecutionCtx) {
			s, e := ctx.TransactionContext.Accounts.GetAccount(1)
			require.NoError(t, e)
			d, e := ctx.TransactionContext.Accounts.GetAccount(3)
			require.NoError(t, e)
			require.Equal(t, uint64(999000), binary.LittleEndian.Uint64(s.Data[64:]))
			require.Equal(t, uint64(1005), binary.LittleEndian.Uint64(d.Data[64:]))
		}})
	return cases
}
func workloadRunner(t testing.TB, w programWorkload, vasa bool) func() (*ExecutionCtx, error) {
	t.Helper()
	f := features.NewFeaturesDefault()
	if vasa {
		f.EnableFeature(features.VirtualAddressSpaceAdjustments, 0)
	}
	l, err := loader.NewLoaderWithSyscalls(w.elf, func(h uint32) (sbpf.Syscall, bool) { return Syscalls(f, false, h) }, false, f)
	require.NoError(t, err)
	prog, err := l.Load()
	require.NoError(t, err)
	require.NoError(t, prog.Verify())
	cache, err := otter.MustBuilder[solana.PublicKey, *accountsdb.ProgramCacheEntry](1024).Cost(func(solana.PublicKey, *accountsdb.ProgramCacheEntry) uint32 { return 1 }).Build()
	require.NoError(t, err)
	t.Cleanup(cache.Close)
	db := &accountsdb.AccountsDb{ProgramCache: cache}
	db.AddProgramToCache(w.program, &accountsdb.ProgramCacheEntry{Program: prog})
	_, cached := db.MaybeGetProgramFromCache(w.program)
	require.True(t, cached, "warm program must be cached")
	return func() (*ExecutionCtx, error) {
		list := make([]accounts.Account, len(w.accts)+1)
		list[0] = accounts.Account{Key: w.program, Owner: a.BpfLoader2Addr, Lamports: 10000000, Executable: true, Data: w.elf}
		for i, acct := range w.accts {
			list[i+1] = acct
			list[i+1].Data = append([]byte(nil), acct.Data...)
		}
		tx := NewTransactionAccounts(list)
		ctx := newBenchExecCtx(tx, 1337)
		ctx.TransactionContext.ComputeBudgetLimits = &ComputeBudgetLimits{UpdatedHeapBytes: 32768}
		ctx.Features = *f
		ctx.ComputeMeter = cu.NewComputeMeter(1400000)
		ctx.SlotCtx = &SlotCtx{Slot: 1337, AccountsDb: db}
		ctx.Log = &LogRecorder{}
		ctx.RecordInnerInstructions = true
		err := ctx.ProcessInstruction(w.instruction, InstructionAcctsFromAccountMetas(w.metas, *tx), []uint64{0})
		return ctx, err
	}
}
func TestProgramWorkloadResults(t *testing.T) {
	for _, w := range expandedProgramWorkloads(t) {
		for _, vasa := range []bool{false, true} {
			name := w.name
			if vasa {
				name += "_VASA"
			}
			t.Run(name, func(t *testing.T) {
				run := workloadRunner(t, w, vasa)
				ctx, err := run()
				require.NoError(t, err)
				w.check(t, ctx)
				t.Logf("cu=%d inner=%d", ctx.ComputeMeter.Used(), len(ctx.InnerInstrs))
			})
		}
	}
}
func BenchmarkProgramWorkloads(b *testing.B) {
	for _, w := range expandedProgramWorkloads(b) {
		for _, vasa := range []bool{false, true} {
			name := w.name
			if vasa {
				name += "_VASA"
			}
			b.Run(name, func(b *testing.B) {
				run := workloadRunner(b, w, vasa)
				ctx, err := run()
				require.NoError(b, err)
				w.check(b, ctx)
				used := ctx.ComputeMeter.Used()
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					ctx, err = run()
					if err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				w.check(b, ctx)
				b.ReportMetric(float64(used), "cu/op")
			})
		}
	}
}
