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
	"io"
	"reflect"
	"testing"
)

func previousCPIInstructionC(vm sbpf.VM, addr uint64) (Instruction, error) {
	ixData, err := vm.Translate(addr, SolInstructionCStructSize, false)
	if err != nil {
		return Instruction{}, err
	}

	byteReader := bytes.NewReader(ixData)
	var ix SolInstructionC

	err = ix.Unmarshal(byteReader)
	if err != nil {
		return Instruction{}, err
	}

	err = checkInstructionSize(executionCtx(vm), ix.AccountsLen, ix.DataLen)
	if err != nil {
		return Instruction{}, err
	}

	pkData, err := vm.Translate(ix.ProgramIdAddr, solana.PublicKeyLength, false)
	if err != nil {
		return Instruction{}, err
	}
	programId := solana.PublicKeyFromBytes(pkData)

	accountMetasData, err := vm.Translate(ix.AccountsAddr, SolAccountMetaCSize*ix.AccountsLen, false)
	if err != nil {
		return Instruction{}, err
	}

	accountMetaReader := bytes.NewReader(accountMetasData)

	accountMetas := make([]SolAccountMetaC, 0, ix.AccountsLen)
	for count := uint64(0); count < ix.AccountsLen; count++ {
		var am SolAccountMetaC
		err = am.localOriginalUnmarshal(accountMetaReader)
		if err != nil {
			return Instruction{}, err
		}
		accountMetas = append(accountMetas, am)
	}

	data, err := vm.Translate(ix.DataAddr, ix.DataLen, false)
	if err != nil {
		return Instruction{}, err
	}

	execCtx := executionCtx(vm)
	if execCtx.Features.IsActive(features.LoosenCpiSizeRestriction) {
		totalCuTranslationCost := ix.DataLen / cu.CUCpiBytesPerUnit

		if execCtx.Features.IsActive(features.IncreaseCpiAccountInfoLimit) {
			acctMetaTranslationCost := safemath.SaturatingMulU64(ix.AccountsLen, AccountMetaSize) / cu.CUCpiBytesPerUnit
			totalCuTranslationCost = safemath.SaturatingAddU64(totalCuTranslationCost, acctMetaTranslationCost)
		}

		err = execCtx.ComputeMeter.Consume(totalCuTranslationCost)
		if err != nil {
			return Instruction{}, err
		}
	}

	accounts := make([]AccountMeta, 0, len(accountMetas))
	for _, accountMeta := range accountMetas {
		if accountMeta.IsSigner > 1 || accountMeta.IsWritable > 1 {
			return Instruction{}, SyscallErrInvalidArgument
		}

		pubkeyData, err := vm.Translate(accountMeta.PubkeyAddr, solana.PublicKeyLength, false)
		if err != nil {
			return Instruction{}, err
		}
		pubkey := solana.PublicKeyFromBytes(pubkeyData)

		var isSigner bool
		var isWritable bool
		if accountMeta.IsSigner == 1 {
			isSigner = true
		}
		if accountMeta.IsWritable == 1 {
			isWritable = true
		}

		newAccountMeta := AccountMeta{Pubkey: pubkey, IsSigner: isSigner, IsWritable: isWritable}
		accounts = append(accounts, newAccountMeta)
	}

	return Instruction{Accounts: accounts, Data: data, ProgramId: programId}, nil
}

func previousCPIInstructionRust(vm sbpf.VM, addr uint64) (Instruction, error) {
	ixData, err := vm.Translate(addr, SolInstructionRustStructSize, false)
	if err != nil {
		return Instruction{}, err
	}

	byteReader := bytes.NewReader(ixData)

	var ix SolInstructionRust
	err = ix.Unmarshal(byteReader)
	if err != nil {
		return Instruction{}, err
	}

	err = checkInstructionSize(executionCtx(vm), ix.Accounts.Len, ix.Data.Len)
	if err != nil {
		return Instruction{}, err
	}

	accountMetasData, err := vm.Translate(ix.Accounts.Addr, AccountMetaSize*ix.Accounts.Len, false)
	if err != nil {
		return Instruction{}, err
	}

	byteReader.Reset(accountMetasData)
	accountMetas := make([]AccountMeta, 0, ix.Accounts.Len)

	for i := uint64(0); i < ix.Accounts.Len; i++ {
		var accountMeta AccountMeta
		err = accountMeta.localOriginalUnmarshal(byteReader)
		if err != nil {
			return Instruction{}, err
		}
		accountMetas = append(accountMetas, accountMeta)
	}

	data, err := vm.Translate(ix.Data.Addr, ix.Data.Len, false)
	if err != nil {
		return Instruction{}, err
	}

	execCtx := executionCtx(vm)
	if execCtx.Features.IsActive(features.LoosenCpiSizeRestriction) {
		totalCuTranslationCost := ix.Data.Len / cu.CUCpiBytesPerUnit

		if execCtx.Features.IsActive(features.IncreaseCpiAccountInfoLimit) {
			acctMetaTranslationCost := safemath.SaturatingMulU64(ix.Accounts.Len, AccountMetaSize) / cu.CUCpiBytesPerUnit
			totalCuTranslationCost = safemath.SaturatingAddU64(totalCuTranslationCost, acctMetaTranslationCost)
		}

		err = execCtx.ComputeMeter.Consume(totalCuTranslationCost)
		if err != nil {
			return Instruction{}, err
		}
	}

	return Instruction{Accounts: accountMetas, Data: data, ProgramId: ix.Pubkey}, nil
}

type localMetaFixture struct {
	input                                 []byte
	metaStart, dataStart, keyStart, width int
}

func makeLocalMetaFixture(rust bool, n int, mode string) localMetaFixture {
	h := 40
	width := SolAccountMetaCSize
	if rust {
		h = 80
		width = AccountMetaSize
	}
	key := h + n*width
	data := key + 64
	input := make([]byte, data+300)
	base := uint64(sbpf.VaddrInput)
	put := func(off int, x uint64) { binary.LittleEndian.PutUint64(input[off:], x) }
	badAddr := base + uint64(len(input)) + 4096
	metaAddr, dataAddr := base+uint64(h), base+uint64(data)
	if mode == "truncated-meta" {
		metaAddr = base + uint64(len(input)) - 1
	}
	if mode == "bad-data" || mode == "bad-data-and-flag" {
		dataAddr = badAddr
	}
	if rust {
		put(0, metaAddr)
		put(8, uint64(n))
		put(16, uint64(n))
		put(24, dataAddr)
		put(32, 300)
		put(40, 300)
	} else {
		put(0, base+uint64(key))
		put(8, metaAddr)
		put(16, uint64(n))
		put(24, dataAddr)
		put(32, 300)
	}
	for i := 0; i < n; i++ {
		record := h + i*width
		bad := i == n/2 && (mode == "bad-flag" || mode == "bad-data-and-flag" || mode == "low-cu-and-flag")
		if rust {
			for j := 0; j < 32; j++ {
				input[record+j] = byte(i%2 + j)
			}
			input[record+32] = byte(i % 2)
			input[record+33] = byte(i / 2 % 2)
			if bad {
				input[record+33] = 7
			}
		} else {
			addr := base + uint64(key+32*(i%2))
			if i == n/2 && mode == "bad-key" {
				addr = badAddr
			}
			put(record, addr)
			input[record+8] = byte(i / 2 % 2)
			input[record+9] = byte(i % 2)
			input[record+13] = 129
			if bad {
				input[record+8] = 7
			}
		}
	}
	for i := key; i < len(input); i++ {
		input[i] = byte(i)
	}
	return localMetaFixture{input, h, data, key, width}
}
func localMetaVM(input []byte, featuresMask int, budget uint64) (*sbpf.Interpreter, *ExecutionCtx) {
	f := features.NewFeaturesDefault()
	if featuresMask&1 != 0 {
		f.EnableFeature(features.LoosenCpiSizeRestriction, 0)
	}
	if featuresMask&2 != 0 {
		f.EnableFeature(features.IncreaseCpiAccountInfoLimit, 0)
	}
	ctx := &ExecutionCtx{Features: *f, ComputeMeter: cu.NewComputeMeter(budget)}
	return sbpf.NewInterpreter(&sbpf.Program{TextVA: sbpf.VaddrProgram, Funcs: map[uint32]int64{}}, &sbpf.VMOpts{Input: input, Context: ctx, ComputeMeter: &ctx.ComputeMeter}), ctx
}

type localTranslate func(sbpf.VM, uint64) (Instruction, error)

func localMetaTranslators(rust bool) (localTranslate, localTranslate) {
	if rust {
		return previousCPIInstructionRust, translateInstructionRust
	}
	return previousCPIInstructionC, translateInstructionC
}
func TestLocalCPIInstructionMetaParity(t *testing.T) {
	for _, rust := range []bool{false, true} {
		previous, candidate := localMetaTranslators(rust)
		for _, n := range []int{0, 1, 6, 16, 255, 256} {
			for _, mode := range []string{"valid", "bad-flag", "bad-key", "truncated-meta", "bad-data", "bad-data-and-flag", "low-cu", "low-cu-and-flag"} {
				for mask := 0; mask < 4; mask++ {
					fixture := makeLocalMetaFixture(rust, n, mode)
					budget := uint64(100000)
					if mode == "low-cu" || mode == "low-cu-and-flag" {
						budget = 0
					}
					type outcome struct {
						Ix        Instruction
						Err       string
						Remaining uint64
						Accesses  []cpiTranslationAccess
					}
					run := func(f localTranslate) outcome {
						ip, ctx := localMetaVM(fixture.input, mask, budget)
						defer ip.Finish()
						vm := &recordingCPIVM{VM: ip}
						ix, err := f(vm, sbpf.VaddrInput)
						return outcome{ix, fmt.Sprintf("%T:%v", err, err), ctx.ComputeMeter.Remaining(), vm.accesses}
					}
					a, b := run(previous), run(candidate)
					if !reflect.DeepEqual(a, b) {
						t.Fatalf("rust=%v n=%d mode=%s mask=%d: baseline %+v candidate %+v", rust, n, mode, mask, a, b)
					}
					if mode == "valid" && n == 6 && (len(b.Ix.Accounts) != 6 || b.Ix.Accounts[0].Pubkey != b.Ix.Accounts[2].Pubkey || b.Ix.Accounts[0].IsWritable == b.Ix.Accounts[2].IsWritable) {
						t.Fatal("lost duplicates or flags")
					}
				}
			}
		}
	}
}

var localCPIResult Instruction

func (accountMeta *AccountMeta) localOriginalUnmarshal(buf io.Reader) error {
	var accountMetaBytes [AccountMetaSize]byte

	_, err := buf.Read(accountMetaBytes[:])
	if err != nil {
		return err
	}

	copy(accountMeta.Pubkey[:], accountMetaBytes[:32])
	if accountMetaBytes[32] > 1 || accountMetaBytes[33] > 1 {
		return InstrErrInvalidArgument
	}
	accountMeta.IsSigner = accountMetaBytes[32] != 0
	accountMeta.IsWritable = accountMetaBytes[33] != 0

	return nil
}

func (accountMeta *SolAccountMetaC) localOriginalUnmarshal(buf io.Reader) error {
	var acctMetaBytes [SolAccountMetaCSize]byte

	_, err := buf.Read(acctMetaBytes[:])
	if err != nil {
		return err
	}

	accountMeta.PubkeyAddr = binary.LittleEndian.Uint64(acctMetaBytes[:8])
	accountMeta.IsWritable = acctMetaBytes[8]
	accountMeta.IsSigner = acctMetaBytes[9]

	return err
}

func TestReplayCPIMetaSelection(t *testing.T) {
	for _, rust := range []bool{false, true} {
		input := makeLocalMetaFixture(rust, 6, "valid").input
		before, after := localMetaTranslators(rust)
		vm, _ := localMetaVM(input, 3, ^uint64(0))
		allocs := func(f localTranslate) float64 {
			return testing.AllocsPerRun(50, func() {
				var err error
				localCPIResult, err = f(vm, sbpf.VaddrInput)
				if err != nil {
					t.Fatal(err)
				}
			})
		}
		old, selected := allocs(before), allocs(after)
		vm.Finish()
		t.Logf("rust=%t original=%.0f selected=%.0f allocations per translator call", rust, old, selected)
		if selected >= old {
			t.Fatal("unexpected decoder allocation profile")
		}
	}
}
