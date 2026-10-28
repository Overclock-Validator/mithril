package sealevel

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/gagliardetto/solana-go"
	"reflect"
	"testing"
)

// Frozen pre-candidate translators retain reader decoding, so differential tests
// cover output, error precedence, CU consumption and VM access order together.
func referenceAccountInfosC(vm sbpf.VM, accountInfosAddr, accountInfosLen uint64) ([]SolAccountInfoC, []solana.PublicKey, error) {
	size := safemath.SaturatingMulU64(accountInfosLen, SolAccountInfoCSize)
	execCtx := executionCtx(vm)
	if syscallParameterAddressRangeRestricted(execCtx, accountInfosAddr, size) {
		return nil, nil, SyscallErrInvalidPointer
	}

	accountInfosData, err := vm.Translate(accountInfosAddr, size, false)
	if err != nil {
		return nil, nil, err
	}

	reader := bytes.NewReader(accountInfosData)
	accountInfos := make([]SolAccountInfoC, 0, accountInfosLen)

	for count := uint64(0); count < accountInfosLen; count++ {
		var acctInfo SolAccountInfoC
		err = acctInfo.Unmarshal(reader)
		if err != nil {
			return nil, nil, err
		}
		accountInfos = append(accountInfos, acctInfo)
	}

	err = checkAccountInfos(execCtx, uint64(len(accountInfos)))
	if err != nil {
		return nil, nil, err
	}

	if execCtx.Features.IsActive(features.IncreaseCpiAccountInfoLimit) {
		acctInfosBytes := safemath.SaturatingMulU64(uint64(len(accountInfos)), AccountInfoByteSize)
		err = execCtx.ComputeMeter.Consume(acctInfosBytes / cu.CUCpiBytesPerUnit)
		if err != nil {
			return nil, nil, err
		}
	}

	accountInfoKeys := make([]solana.PublicKey, 0, len(accountInfos))
	for _, acctInfo := range accountInfos {
		keyData, err := vm.Translate(acctInfo.KeyAddr, 32, false)
		if err != nil {
			return nil, nil, err
		}
		key := solana.PublicKeyFromBytes(keyData)
		accountInfoKeys = append(accountInfoKeys, key)
	}

	return accountInfos, accountInfoKeys, nil
}

func referenceAccountInfosRust(vm sbpf.VM, accountInfosAddr, accountInfosLen uint64) ([]SolAccountInfoRust, []solana.PublicKey, error) {
	size := safemath.SaturatingMulU64(accountInfosLen, SolAccountInfoRustSize)
	execCtx := executionCtx(vm)
	if syscallParameterAddressRangeRestricted(execCtx, accountInfosAddr, size) {
		return nil, nil, SyscallErrInvalidPointer
	}

	accountInfosData, err := vm.Translate(accountInfosAddr, size, false)
	if err != nil {
		return nil, nil, err
	}

	reader := bytes.NewReader(accountInfosData)
	accountInfos := make([]SolAccountInfoRust, 0, accountInfosLen)

	for count := uint64(0); count < accountInfosLen; count++ {
		var acctInfo SolAccountInfoRust
		err = acctInfo.Unmarshal(reader)
		if err != nil {
			return nil, nil, err
		}
		accountInfos = append(accountInfos, acctInfo)
	}

	err = checkAccountInfos(execCtx, uint64(len(accountInfos)))
	if err != nil {
		return nil, nil, err
	}

	if execCtx.Features.IsActive(features.IncreaseCpiAccountInfoLimit) {
		acctInfosBytes := safemath.SaturatingMulU64(uint64(len(accountInfos)), AccountInfoByteSize)
		err = execCtx.ComputeMeter.Consume(acctInfosBytes / cu.CUCpiBytesPerUnit)
		if err != nil {
			return nil, nil, err
		}
	}

	accountInfoKeys := make([]solana.PublicKey, 0, len(accountInfos))
	for _, acctInfo := range accountInfos {
		keyData, err := vm.Translate(acctInfo.PubkeyAddr, 32, false)
		if err != nil {
			return nil, nil, err
		}
		key := solana.PublicKeyFromBytes(keyData)
		accountInfoKeys = append(accountInfoKeys, key)
	}

	return accountInfos, accountInfoKeys, nil
}

type cpiTranslationAccess struct {
	Addr, Size uint64
	Write      bool
}
type recordingCPIVM struct {
	sbpf.VM
	accesses []cpiTranslationAccess
}

func (vm *recordingCPIVM) Translate(addr, size uint64, write bool) ([]byte, error) {
	vm.accesses = append(vm.accesses, cpiTranslationAccess{addr, size, write})
	return vm.VM.Translate(addr, size, write)
}
func TestCPIDecodeTranslationParity(t *testing.T) {
	for _, rust := range []bool{false, true} {
		for _, n := range []int{0, 1, 8, 64, 65, 256, 257} {
			for _, mode := range []string{"valid", "truncated", "bad-key", "restricted", "low-cu"} {
				for _, increased := range []bool{false, true} {
					t.Run(fmt.Sprintf("rust=%v/n=%d/%s/increased=%v", rust, n, mode, increased), func(t *testing.T) {
						size := SolAccountInfoCSize
						if rust {
							size = SolAccountInfoRustSize
						}
						input := make([]byte, n*size+32)
						for i := 0; i < n; i++ {
							binary.LittleEndian.PutUint64(input[i*size:], sbpf.VaddrInput+uint64(n*size))
						}
						for i := n * size; i < len(input); i++ {
							input[i] = byte(i)
						}
						if mode == "truncated" && n > 0 {
							input = input[:n*size-1]
						}
						if mode == "bad-key" && n > 0 {
							binary.LittleEndian.PutUint64(input[(n-1)*size:], sbpf.VaddrInput+uint64(len(input)))
						}
						type result struct {
							Infos     any
							Keys      []solana.PublicKey
							Err       string
							Remaining uint64
							Accesses  []cpiTranslationAccess
						}
						run := func(reference bool) result {
							f := features.NewFeaturesDefault()
							f.EnableFeature(features.LoosenCpiSizeRestriction, 0)
							if increased {
								f.EnableFeature(features.IncreaseCpiAccountInfoLimit, 0)
							}
							if mode == "restricted" {
								f.EnableFeature(features.SyscallParameterAddressRestrictions, 0)
							}
							budget := uint64(100000)
							if mode == "low-cu" {
								budget = 0
							}
							meter := cu.NewComputeMeter(budget)
							ctx := &ExecutionCtx{Features: *f, ComputeMeter: meter}
							ip := sbpf.NewInterpreter(&sbpf.Program{TextVA: sbpf.VaddrProgram, Funcs: map[uint32]int64{}}, &sbpf.VMOpts{Input: input, Context: ctx, ComputeMeter: &ctx.ComputeMeter})
							defer ip.Finish()
							vm := &recordingCPIVM{VM: ip}
							var infos any
							var keys []solana.PublicKey
							var err error
							if rust {
								if reference {
									infos, keys, err = referenceAccountInfosRust(vm, sbpf.VaddrInput, uint64(n))
								} else {
									infos, keys, err = translateAccountInfosRust(vm, sbpf.VaddrInput, uint64(n))
								}
							} else {
								if reference {
									infos, keys, err = referenceAccountInfosC(vm, sbpf.VaddrInput, uint64(n))
								} else {
									infos, keys, err = translateAccountInfosC(vm, sbpf.VaddrInput, uint64(n))
								}
							}
							return result{infos, keys, fmt.Sprint(err), ctx.ComputeMeter.Remaining(), vm.accesses}
						}
						a, b := run(true), run(false)
						if !reflect.DeepEqual(a, b) {
							t.Fatalf("reference=%+v candidate=%+v", a, b)
						}
					})
				}
			}
		}
	}
}

var translationKeysSink []solana.PublicKey

func BenchmarkCPITranslateAccountInfos(b *testing.B) {
	for _, rust := range []bool{false, true} {
		for _, n := range []int{8, 32} {
			for _, reference := range []bool{true, false} {
				b.Run(fmt.Sprintf("rust=%v/n=%d/reference=%v", rust, n, reference), func(b *testing.B) {
					size := SolAccountInfoCSize
					if rust {
						size = SolAccountInfoRustSize
					}
					input := make([]byte, n*size+32)
					for i := 0; i < n; i++ {
						binary.LittleEndian.PutUint64(input[i*size:], sbpf.VaddrInput+uint64(n*size))
					}
					f := features.NewFeaturesDefault()
					f.EnableFeature(features.LoosenCpiSizeRestriction, 0)
					ctx := &ExecutionCtx{Features: *f, ComputeMeter: cu.NewComputeMeter(100000)}
					vm := sbpf.NewInterpreter(&sbpf.Program{TextVA: sbpf.VaddrProgram, Funcs: map[uint32]int64{}}, &sbpf.VMOpts{Input: input, Context: ctx, ComputeMeter: &ctx.ComputeMeter})
					defer vm.Finish()
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						var err error
						if rust {
							if reference {
								studySinkRust, translationKeysSink, err = referenceAccountInfosRust(vm, sbpf.VaddrInput, uint64(n))
							} else {
								studySinkRust, translationKeysSink, err = translateAccountInfosRust(vm, sbpf.VaddrInput, uint64(n))
							}
						} else {
							if reference {
								studySinkC, translationKeysSink, err = referenceAccountInfosC(vm, sbpf.VaddrInput, uint64(n))
							} else {
								studySinkC, translationKeysSink, err = translateAccountInfosC(vm, sbpf.VaddrInput, uint64(n))
							}
						}
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}
