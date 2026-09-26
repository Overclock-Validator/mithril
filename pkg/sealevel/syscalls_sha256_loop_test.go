package sealevel

import (
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/stretchr/testify/require"
)

// This is text slots 518..545 from the captured AogGeA81 program's hash loop.
// Captured ELF SHA-256: b3286f96f5611ee7db62dc11ea9aab1afe80908c08a0fbd3fdd783879d70ed88.
// The captured ELF uses SBF v0. Only the unresolved syscall relocation is replaced. The harness supplies a
// loop bound and zero initial state; transaction loading, CPI and the rest of
// the program are deliberately excluded.
func sha256LoopProgram(iterations uint32) *sbpf.Program {
	text := []sbpf.Slot{
		sbpf.Slot(sbpf.OpMov64Imm) | 7<<8 | sbpf.Slot(iterations)<<32,
		0xa1bf, 0xffffffb000000107, 0xfe701a7b, 0xa1bf, 0xfffffe1000000107,
		0xfe601a7b, 0xffb08a63, 0x4fe780a7a, 0x20fe680a7a, 0xa1bf,
		0xfffffe6000000107, 0xa3bf, 0xffffffe000000307, 0x2000002b7,
		sbpf.Slot(sbpf.OpCall) | sbpf.Slot(hash_sol_sha256)<<32,
		0xffe0a179, 0xfe101a7b, 0xffe8a179, 0xfe181a7b, 0xfff0a179,
		0xfe201a7b, 0xfff8a179, 0xfe281a7b, 0x100000807, 0x81bf,
		0x2000000167, 0x2000000177, 0xffe471ad,
		// Return the first digest word so the harness can check the computation.
		0xfe10a079, sbpf.Slot(sbpf.OpExit),
	}
	return &sbpf.Program{Text: text}
}

func runSha256Loop(p *sbpf.Program, fn sha256Call) (uint64, uint64, error) {
	ctx := &ExecutionCtx{ComputeMeter: cu.NewComputeMeter(10000000)}
	vm := sbpf.NewInterpreter(p, &sbpf.VMOpts{Context: ctx, ComputeMeter: &ctx.ComputeMeter, Syscalls: func(hash uint32) (sbpf.Syscall, bool) { return sbpf.SyscallFunc3(fn), hash == hash_sol_sha256 }})
	ret, _, err := vm.Run()
	used := ctx.ComputeMeter.Used()
	vm.Finish()
	return ret, used, err
}

func TestSha256CapturedLoop(t *testing.T) {
	const iterations = 1000
	p := sha256LoopProgram(iterations)
	require.NoError(t, p.Verify())
	var data [36]byte
	for i := uint32(0); i < iterations; i++ {
		binary.LittleEndian.PutUint32(data[32:], i)
		d := sha256.Sum256(data[:])
		copy(data[:32], d[:])
	}
	want := binary.LittleEndian.Uint64(data[:8])
	var wantCU uint64
	for _, fn := range []sha256Call{sha256BaselineReference, SyscallSha256Impl} {
		got, used, err := runSha256Loop(p, fn)
		require.NoError(t, err)
		require.Equal(t, want, got)
		if wantCU == 0 {
			wantCU = used
		}
		require.Equal(t, wantCU, used)
	}
}

func BenchmarkSha256CapturedLoop(b *testing.B) {
	const iterations = 1000
	p := sha256LoopProgram(iterations)
	for _, v := range []struct {
		name string
		fn   sha256Call
	}{
		{"baseline", sha256BaselineReference}, {"lean", SyscallSha256Impl},
		// Diagnostic lower bound: no hashing, translations or syscall CU charging.
		// It is not a valid implementation and must never be used in replay.
		{"dispatch_only", func(sbpf.VM, uint64, uint64, uint64) (uint64, error) { return 0, nil }},
	} {
		b.Run(v.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, _, err := runSha256Loop(p, v.fn); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*iterations), "ns/hash")
		})
	}
	b.Run("raw_chain", func(b *testing.B) {
		var data [36]byte
		b.ReportAllocs()
		for j := 0; j < b.N; j++ {
			clear(data[:])
			for i := uint32(0); i < iterations; i++ {
				binary.LittleEndian.PutUint32(data[32:], i)
				d := sha256.Sum256(data[:])
				copy(data[:32], d[:])
			}
		}
		sha256BenchDigest = sha256.Sum256(data[:])
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*iterations), "ns/hash")
	})
}
