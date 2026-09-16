package sealevel

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/fixtures"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/Overclock-Validator/mithril/pkg/sbpf/loader"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type MapSyscallRegistry map[uint32]sbpf.Syscall

func NewSyscallRegistry() MapSyscallRegistry {
	return make(MapSyscallRegistry)
}

func (s MapSyscallRegistry) Register(name string, syscall sbpf.Syscall) (hash uint32, ok bool) {
	hash = sbpf.SymbolHash(name)
	if _, exist := s[hash]; exist {
		return 0, false // collision or duplicate
	}
	s[hash] = syscall
	ok = true
	return
}

func ToFunc(m MapSyscallRegistry) sbpf.SyscallRegistry {
	return sbpf.SyscallRegistry(func(u uint32) (sbpf.Syscall, bool) {
		f, ok := m[u]
		return f, ok
	})
}

func TestInterpreter_Noop(t *testing.T) {
	// TODO simplify API?
	loader, err := loader.NewLoaderFromBytes(fixtures.Load(t, "sbpf", "noop.so"))
	require.NoError(t, err)
	require.NotNil(t, loader)

	program, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, program)

	require.NoError(t, program.Verify())

	syscalls := NewSyscallRegistry()
	syscalls.Register("log", SyscallLog)
	syscalls.Register("log_64", SyscallLog64)

	var log LogRecorder

	interpreter := sbpf.NewInterpreter(program, &sbpf.VMOpts{
		HeapMax:  32 * 1024,
		Input:    nil,
		MaxCU:    10000,
		Syscalls: ToFunc(syscalls),
		Context:  &ExecutionCtx{Log: &log, ComputeMeter: cu.NewComputeMeterDefault()},
	})
	require.NotNil(t, interpreter)

	_, _, err = interpreter.Run()
	require.NoError(t, err)

	assert.Equal(t, log.Logs, []string{
		"Program log: entrypoint\x00",
		"Program log: 0x1, 0x2, 0x3, 0x4, 0x5\n",
	})
}

// The TestInterpreter_Memcpy_Strings_Match tests that memcpy works as expected
// by running an SBPF program that uses the memcpy syscall to copy a string
// literal to a stack buffer, before testing for equality using memcmp.
// The expected result is that the two match.
func TestInterpreter_Memcpy_Strings_Match(t *testing.T) {
	loader, err := loader.NewLoaderFromBytes(fixtures.Load(t, "sbpf", "memcpy_and_memmove_test_matched.so"))
	require.NoError(t, err)
	require.NotNil(t, loader)

	program, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, program)

	require.NoError(t, program.Verify())

	syscalls := NewSyscallRegistry()
	syscalls.Register("sol_log_", SyscallLog)
	syscalls.Register("log_64", SyscallLog64)
	syscalls.Register("my_copy", SyscallMemcpy)

	var log LogRecorder

	interpreter := sbpf.NewInterpreter(program, &sbpf.VMOpts{
		HeapMax:  32 * 1024,
		Input:    nil,
		Syscalls: ToFunc(syscalls),
		Context:  &ExecutionCtx{Log: &log, ComputeMeter: cu.NewComputeMeterDefault()},
	})
	require.NotNil(t, interpreter)

	_, _, err = interpreter.Run()
	assert.Equal(t, log.Logs, []string{
		"Program log: Strings matched after copy.",
	})
	require.NoError(t, err)
}

// The TestInterpreter_Memcpy_Do_Not_Match tests that memcpy works as expected
// by running an SBPF program that uses the memcpy syscall to copy a string
// literal to a stack buffer, with the destination then modified before testing
// for equality using memcmp. The expected result  is that the two do NOT match,
// because of the modification before comparison.
func TestInterpreter_Memcpy_Do_Not_Match(t *testing.T) {
	loader, err := loader.NewLoaderFromBytes(fixtures.Load(t, "sbpf", "memcpy_and_memmove_test_not_matched.so"))
	require.NoError(t, err)
	require.NotNil(t, loader)

	program, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, program)

	require.NoError(t, program.Verify())

	syscalls := NewSyscallRegistry()
	syscalls.Register("sol_log_", SyscallLog)
	syscalls.Register("log_64", SyscallLog64)
	syscalls.Register("my_copy", SyscallMemcpy)

	var log LogRecorder

	interpreter := sbpf.NewInterpreter(program, &sbpf.VMOpts{
		HeapMax:  32 * 1024,
		Input:    nil,
		MaxCU:    10000,
		Syscalls: ToFunc(syscalls),
		Context:  &ExecutionCtx{Log: &log, ComputeMeter: cu.NewComputeMeterDefault()},
	})
	require.NotNil(t, interpreter)

	_, _, err = interpreter.Run()
	assert.Equal(t, log.Logs, []string{
		"Program log: Strings did not match after copy.",
	})
	require.NoError(t, err)
}

// The TestInterpreter_Memmove_Strings_Match tests that memove works as expected
// by running an SBPF program that uses the memcpy syscall to copy a string
// literal to a stack buffer, before testing for equality using memcmp.
// The expected result is that the two match.
func TestInterpreter_Memmove_Strings_Match(t *testing.T) {
	loader, err := loader.NewLoaderFromBytes(fixtures.Load(t, "sbpf", "memcpy_and_memmove_test_matched.so"))
	require.NoError(t, err)
	require.NotNil(t, loader)

	program, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, program)

	require.NoError(t, program.Verify())

	syscalls := NewSyscallRegistry()
	syscalls.Register("sol_log_", SyscallLog)
	syscalls.Register("log_64", SyscallLog64)
	syscalls.Register("my_copy", SyscallMemmove)

	var log LogRecorder

	interpreter := sbpf.NewInterpreter(program, &sbpf.VMOpts{
		HeapMax:  32 * 1024,
		Input:    nil,
		MaxCU:    10000,
		Syscalls: ToFunc(syscalls),
		Context:  &ExecutionCtx{Log: &log, ComputeMeter: cu.NewComputeMeterDefault()},
	})
	require.NotNil(t, interpreter)

	_, _, err = interpreter.Run()
	assert.Equal(t, log.Logs, []string{
		"Program log: Strings matched after copy.",
	})
	require.NoError(t, err)
}

// The TestInterpreter_Memmove_Do_Not_Match function tests that memmove works
// as expected by running an SBPF program that uses the memcpy syscall to
// copy a string literal to a stack buffer, with the destination then
// modified before testing for equality using memcmp. The expected result is
// that the two do NOT match, because of the modification before comparison.
func TestInterpreter_Memmove_Do_Not_Match(t *testing.T) {
	loader, err := loader.NewLoaderFromBytes(fixtures.Load(t, "sbpf", "memcpy_and_memmove_test_not_matched.so"))
	require.NoError(t, err)
	require.NotNil(t, loader)

	program, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, program)

	require.NoError(t, program.Verify())

	syscalls := NewSyscallRegistry()
	syscalls.Register("sol_log_", SyscallLog)
	syscalls.Register("log_64", SyscallLog64)
	syscalls.Register("my_copy", SyscallMemmove)

	var log LogRecorder

	interpreter := sbpf.NewInterpreter(program, &sbpf.VMOpts{
		HeapMax:  32 * 1024,
		Input:    nil,
		MaxCU:    10000,
		Syscalls: ToFunc(syscalls),
		Context:  &ExecutionCtx{Log: &log, ComputeMeter: cu.NewComputeMeterDefault()},
	})
	require.NotNil(t, interpreter)

	_, _, err = interpreter.Run()
	assert.Equal(t, log.Logs, []string{
		"Program log: Strings did not match after copy.",
	})
	require.NoError(t, err)
}

// The TestInterpreter_Memcpy_Overlapping function tests that memcpy works
// as expected by attempting to do a copy involving two overlapping buffers.
// The expected result is an "Overlapping copy" error being returned.
func TestInterpreter_Memcpy_Overlapping(t *testing.T) {
	loader, err := loader.NewLoaderFromBytes(fixtures.Load(t, "sbpf", "memcpy_overlapping.so"))
	require.NoError(t, err)
	require.NotNil(t, loader)

	program, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, program)

	require.NoError(t, program.Verify())

	syscalls := NewSyscallRegistry()
	syscalls.Register("sol_log_", SyscallLog)
	syscalls.Register("log_64", SyscallLog64)
	syscalls.Register("my_copy", SyscallMemcpy)

	var log LogRecorder

	interpreter := sbpf.NewInterpreter(program, &sbpf.VMOpts{
		HeapMax:  32 * 1024,
		Input:    nil,
		MaxCU:    10000,
		Syscalls: ToFunc(syscalls),
		Context:  &ExecutionCtx{Log: &log, ComputeMeter: cu.NewComputeMeterDefault()},
	})
	require.NotNil(t, interpreter)

	_, _, err = interpreter.Run()

	// expecting an error here because the src and dst are overlapping in the
	// program being run.
	require.Error(t, err)
}

// The TestInterpreter_Memcmp_Matches function tests that the memcmp
// syscall works as expected by comparing two instances of "abcdabcd1234"
// The expected result is that the two strings match and the program
// writes "Memory chunks matched." to the program log.
func TestInterpreter_Memcmp_Matches(t *testing.T) {
	loader, err := loader.NewLoaderFromBytes(fixtures.Load(t, "sbpf", "memcmp_matched.so"))
	require.NoError(t, err)
	require.NotNil(t, loader)

	program, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, program)

	require.NoError(t, program.Verify())

	syscalls := NewSyscallRegistry()
	syscalls.Register("sol_log_", SyscallLog)
	syscalls.Register("log_64", SyscallLog64)
	syscalls.Register("my_memcmp", SyscallMemcmp)

	var log LogRecorder

	interpreter := sbpf.NewInterpreter(program, &sbpf.VMOpts{
		HeapMax:  32 * 1024,
		Input:    nil,
		MaxCU:    10000,
		Syscalls: ToFunc(syscalls),
		Context:  &ExecutionCtx{Log: &log, ComputeMeter: cu.NewComputeMeterDefault()},
	})
	require.NotNil(t, interpreter)

	_, _, err = interpreter.Run()
	require.NoError(t, err)

	assert.Equal(t, log.Logs, []string{
		"Program log: Memory chunks matched.",
	})
}

// The TestInterpreter_Memcmp_Does_Not_Match function tests that the memcmp
// syscall works as expected by comparing the string literals "Bbcdabcd1234"
// and "aLAHabcd1234"
// The expected result is that the two strings do not match and the difference
// between the first non-matching characters (0x42 - 0x61 = -0x1f) is returned,
// and the program checks these and returns messages accordingly.
func TestInterpreter_Memcmp_Does_Not_Match(t *testing.T) {
	loader, err := loader.NewLoaderFromBytes(fixtures.Load(t, "sbpf", "memcmp_not_matched.so"))
	require.NoError(t, err)
	require.NotNil(t, loader)

	program, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, program)

	require.NoError(t, program.Verify())

	syscalls := NewSyscallRegistry()
	syscalls.Register("sol_log_", SyscallLog)
	syscalls.Register("log_64", SyscallLog64)
	syscalls.Register("my_memcmp", SyscallMemcmp)

	var log LogRecorder

	interpreter := sbpf.NewInterpreter(program, &sbpf.VMOpts{
		HeapMax:  32 * 1024,
		Input:    nil,
		MaxCU:    10000,
		Syscalls: ToFunc(syscalls),
		Context:  &ExecutionCtx{Log: &log, ComputeMeter: cu.NewComputeMeterDefault()},
	})
	require.NotNil(t, interpreter)

	_, _, err = interpreter.Run()
	require.NoError(t, err)

	assert.Equal(t, log.Logs, []string{
		"Program log: Memory chunks did not match.",
		"Program log: Difference between non-matching character was correctly returned.",
	})
}

// The TestInterpreter_Memset_Check_Correct function tests that the memset
// syscall works as expected by calling the syscall to fill a 16-byte buffer
// with 'x' (0x78) characters. A call to the memcmp syscall is used to check
// that the buffer was filled with 16 'x's as expected.
func TestInterpreter_Memset_Check_Correct(t *testing.T) {
	loader, err := loader.NewLoaderFromBytes(fixtures.Load(t, "sbpf", "memset_check_correct.so"))
	require.NoError(t, err)
	require.NotNil(t, loader)

	program, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, program)

	require.NoError(t, program.Verify())

	syscalls := NewSyscallRegistry()
	syscalls.Register("sol_log_", SyscallLog)
	syscalls.Register("log_64", SyscallLog64)
	syscalls.Register("my_memset", SyscallMemset)
	syscalls.Register("my_memcmp", SyscallMemcmp)

	var log LogRecorder

	interpreter := sbpf.NewInterpreter(program, &sbpf.VMOpts{
		HeapMax:  32 * 1024,
		Input:    nil,
		MaxCU:    10000,
		Syscalls: ToFunc(syscalls),
		Context:  &ExecutionCtx{Log: &log, ComputeMeter: cu.NewComputeMeterDefault()},
	})
	require.NotNil(t, interpreter)

	_, _, err = interpreter.Run()
	require.NoError(t, err)

	assert.Equal(t, log.Logs, []string{
		"Program log: Memory chunks matched as 16-byte 'x' strings",
	})
}

// The TestInterpreter_Sha256 function tests that the sol_sha256 syscall
// works as expected by running a program that calls the sha256 syscall
// twice with two different chunks of data, and checks that the hashes
// returned are as expected.
func TestInterpreter_Sha256(t *testing.T) {
	loader, err := loader.NewLoaderFromBytes(fixtures.Load(t, "sbpf", "sha256.so"))
	require.NoError(t, err)
	require.NotNil(t, loader)

	program, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, program)

	require.NoError(t, program.Verify())

	syscalls := NewSyscallRegistry()
	syscalls.Register("sol_log_", SyscallLog)
	syscalls.Register("log_64", SyscallLog64)
	syscalls.Register("my_sha256", SyscallSha256)
	syscalls.Register("my_memcmp", SyscallMemcmp)

	var log LogRecorder
	ctx := &ExecutionCtx{Log: &log, ComputeMeter: cu.NewComputeMeterDefault()}

	interpreter := sbpf.NewInterpreter(program, &sbpf.VMOpts{
		HeapMax:      32 * 1024,
		Input:        nil,
		MaxCU:        10000,
		Syscalls:     ToFunc(syscalls),
		Context:      ctx,
		ComputeMeter: &ctx.ComputeMeter,
	})
	require.NotNil(t, interpreter)

	_, _, err = interpreter.Run()
	require.NoError(t, err)

	assert.Equal(t, log.Logs, []string{
		"Program log: 1: hash returned matched",
		"Program log: 2: hash returned matched",
	})
}

// The TestInterpreter_Blake3 function tests that the sol_blake3 syscall
// works as expected by running a program that calls the blake3 syscall
// twice with two different chunks of data, and checks that the hashes
// returned are as expected.
func TestInterpreter_Blake3(t *testing.T) {
	loader, err := loader.NewLoaderFromBytes(fixtures.Load(t, "sbpf", "blake3.so"))
	require.NoError(t, err)
	require.NotNil(t, loader)

	program, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, program)

	require.NoError(t, program.Verify())

	syscalls := NewSyscallRegistry()
	syscalls.Register("sol_log_", SyscallLog)
	syscalls.Register("log_64", SyscallLog64)
	syscalls.Register("my_blake3", SyscallBlake3)
	syscalls.Register("my_memcmp", SyscallMemcmp)

	var log LogRecorder

	interpreter := sbpf.NewInterpreter(program, &sbpf.VMOpts{
		HeapMax:  32 * 1024,
		Input:    nil,
		MaxCU:    10000,
		Syscalls: ToFunc(syscalls),
		Context:  &ExecutionCtx{Log: &log, ComputeMeter: cu.NewComputeMeterDefault()},
	})
	require.NotNil(t, interpreter)

	_, _, err = interpreter.Run()
	require.NoError(t, err)

	assert.Equal(t, log.Logs, []string{
		"Program log: 1: hash returned matched",
		"Program log: 2: hash returned matched",
	})
}

// The TestInterpreter_Keccak256 function tests that the sol_keccak256 syscall
// works as expected by running a program that calls the keccak256 syscall
// twice with two different chunks of data, and checks that the hashes
// returned are as expected.
func TestInterpreter_Keccak256(t *testing.T) {
	loader, err := loader.NewLoaderFromBytes(fixtures.Load(t, "sbpf", "keccak256.so"))
	require.NoError(t, err)
	require.NotNil(t, loader)

	program, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, program)

	require.NoError(t, program.Verify())

	syscalls := NewSyscallRegistry()
	syscalls.Register("sol_log_", SyscallLog)
	syscalls.Register("log_64", SyscallLog64)
	syscalls.Register("my_keccak256", SyscallKeccak256)
	syscalls.Register("my_memcmp", SyscallMemcmp)

	var log LogRecorder

	interpreter := sbpf.NewInterpreter(program, &sbpf.VMOpts{
		HeapMax:  32 * 1024,
		Input:    nil,
		MaxCU:    10000,
		Syscalls: ToFunc(syscalls),
		Context:  &ExecutionCtx{Log: &log, ComputeMeter: cu.NewComputeMeterDefault()},
	})
	require.NotNil(t, interpreter)

	_, _, err = interpreter.Run()
	require.NoError(t, err)

	assert.Equal(t, log.Logs, []string{
		"Program log: 1: hash returned matched",
		"Program log: 2: hash returned matched",
	})
}

// The TestInterpreter_CreateProgramAddress function tests the
// sol_create_program_address syscall. Two testcases are used,
// each with two input seeds, and both calls to the create_program_address
// must turn up the expected address for the test to pass.
func TestInterpreter_CreateProgramAddress(t *testing.T) {
	loader, err := loader.NewLoaderFromBytes(fixtures.Load(t, "sbpf", "create_program_address.so"))
	require.NoError(t, err)
	require.NotNil(t, loader)

	program, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, program)

	require.NoError(t, program.Verify())

	syscalls := NewSyscallRegistry()
	syscalls.Register("sol_log_", SyscallLog)
	syscalls.Register("log_64", SyscallLog64)
	syscalls.Register("my_create_program_address", SyscallCreateProgramAddress)
	syscalls.Register("my_memcmp", SyscallMemcmp)
	syscalls.Register("sol_panic_", SyscallPanic)

	var log LogRecorder

	interpreter := sbpf.NewInterpreter(program, &sbpf.VMOpts{
		HeapMax:  32 * 1024,
		Input:    nil,
		MaxCU:    10000,
		Syscalls: ToFunc(syscalls),
		Context:  &ExecutionCtx{Log: &log, ComputeMeter: cu.NewComputeMeterDefault()},
	})
	require.NotNil(t, interpreter)

	_, _, err = interpreter.Run()
	require.NoError(t, err)

	assert.Equal(t, log.Logs, []string{
		"Program log: 1: address returned was the expected address",
		"Program log: 2: address returned was the expected address",
	})
}

// The TestInterpreter_TryFindProgramAddress function tests the
// sol_try_find_program_address syscall. The testcase uses some seeds
// to derive an address via sol_try_find_program_address, and then checks
// that the same value is derived by calling sol_create_program_address
// with those same seeds (original seeds + bump seed returned by
// sol_try_find_program_address)
func TestInterpreter_TryFindProgramAddress(t *testing.T) {
	loader, err := loader.NewLoaderFromBytes(fixtures.Load(t, "sbpf", "try_find_program_address.so"))
	require.NoError(t, err)
	require.NotNil(t, loader)

	program, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, program)

	require.NoError(t, program.Verify())

	syscalls := NewSyscallRegistry()
	syscalls.Register("sol_log_", SyscallLog)
	syscalls.Register("log_64", SyscallLog64)
	syscalls.Register("my_create_program_address", SyscallCreateProgramAddress)
	syscalls.Register("my_try_find_program_address", SyscallTryFindProgramAddress)
	syscalls.Register("my_memcmp", SyscallMemcmp)
	syscalls.Register("sol_panic_", SyscallPanic)

	var log LogRecorder

	interpreter := sbpf.NewInterpreter(program, &sbpf.VMOpts{
		HeapMax:  32 * 1024,
		Input:    nil,
		MaxCU:    10000,
		Syscalls: ToFunc(syscalls),
		Context:  &ExecutionCtx{Log: &log, ComputeMeter: cu.NewComputeMeterDefault()},
	})
	require.NotNil(t, interpreter)

	_, _, err = interpreter.Run()
	require.NoError(t, err)

	assert.Equal(t, log.Logs, []string{
		"Program log: try_find_program_address success",
		"Program log: address returned by try_find_program_address matches create_program_address with equivalent seeds",
	})
}

// The TestInterpreter_TestPanic function tests the
// panic syscall.
func TestInterpreter_TestPanic(t *testing.T) {
	loader, err := loader.NewLoaderFromBytes(fixtures.Load(t, "sbpf", "panic.so"))
	require.NoError(t, err)
	require.NotNil(t, loader)

	program, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, program)

	require.NoError(t, program.Verify())

	syscalls := NewSyscallRegistry()
	syscalls.Register("sol_log_", SyscallLog)
	syscalls.Register("log_64", SyscallLog64)
	syscalls.Register("my_panic", SyscallPanic)

	var log LogRecorder

	interpreter := sbpf.NewInterpreter(program, &sbpf.VMOpts{
		HeapMax:  32 * 1024,
		Input:    nil,
		MaxCU:    10000,
		Syscalls: ToFunc(syscalls),
		Context:  &ExecutionCtx{Log: &log, ComputeMeter: cu.NewComputeMeterDefault()},
	})
	require.NotNil(t, interpreter)

	_, _, err = interpreter.Run()
	require.Error(t, err)
	assert.Equal(t, err.Error(), "exception at 16: SBF program Panicked in some_file_1234.c at 1337:10")
}

func TestInterpreter_Secp256k1_Syscall(t *testing.T) {
	loader, err := loader.NewLoaderFromBytes(fixtures.Load(t, "sbpf", "secp256k1_recover.so"))
	require.NoError(t, err)
	require.NotNil(t, loader)

	program, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, program)

	require.NoError(t, program.Verify())

	syscalls := sbpf.SyscallRegistry(func(u uint32) (sbpf.Syscall, bool) {
		return Syscalls(new(features.Features), false, u)
	})

	var log LogRecorder

	interpreter := sbpf.NewInterpreter(program, &sbpf.VMOpts{
		HeapMax:  32 * 1024,
		Input:    nil,
		MaxCU:    10000,
		Syscalls: syscalls,
		Context:  &ExecutionCtx{Log: &log, ComputeMeter: cu.NewComputeMeterDefault()},
	})
	require.NotNil(t, interpreter)

	_, _, err = interpreter.Run()
	require.NoError(t, err)
}

func TestInterpreter_Get_Stack_Height_Syscall(t *testing.T) {
	// program data account
	programDataPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataPrivKey.PublicKey()
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{Slot: 0, UpgradeAuthorityAddress: nil}}
	validProgramBytes := fixtures.Load(t, "sbpf", "get_stack_height.so")
	programDataStateWriter := new(bytes.Buffer)
	programDataStateEncoder := bin.NewBinEncoder(programDataStateWriter)
	err = programDataAcctState.MarshalWithEncoder(programDataStateEncoder)
	assert.NoError(t, err)
	programDataStateWriter.Write(validProgramBytes)
	programDataStateBytes := make([]byte, len(validProgramBytes)+upgradeableLoaderSizeOfProgramDataMetaData)
	copy(programDataStateBytes, programDataStateWriter.Bytes())
	copy(programDataStateBytes[upgradeableLoaderSizeOfProgramDataMetaData:], validProgramBytes)

	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataStateBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// program account
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: programDataAcct.Key}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programData := make([]byte, 5000)
	copy(programData, programBytes)
	programAcct := accounts.Account{Key: programPubkey, Lamports: 10000, Data: programData, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	instrData := make([]byte, 0)

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct})

	acctMetas := []AccountMeta{{Pubkey: programAcct.Key, IsSigner: false, IsWritable: false}}

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeter(10000000000)}

	execCtx.Accounts = accounts.NewMemAccounts()
	var clock SysvarClock
	clock.Slot = 1234
	clockAcct := accounts.Account{}
	clockAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarClockAddr, &clockAcct)
	WriteClockSysvar(&execCtx.Accounts, clock)

	var rent SysvarRent
	rent.LamportsPerUint8Year = 1
	rent.ExemptionThreshold = 1
	rent.BurnPercent = 0

	rentAcct := accounts.Account{}
	rentAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarRentAddr, &rentAcct)
	WriteRentSysvar(&execCtx.Accounts, rent)

	pk := [32]byte(programDataAcct.Key)
	err = execCtx.Accounts.SetAccount(&pk, &programDataAcct)
	assert.NoError(t, err)

	execCtx.SlotCtx = new(SlotCtx)
	execCtx.SlotCtx.Slot = 1337

	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)
}

func TestInterpreter_ReturnData_Syscalls(t *testing.T) {
	// program data account
	programDataPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataPrivKey.PublicKey()
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{Slot: 0, UpgradeAuthorityAddress: nil}}
	validProgramBytes := fixtures.Load(t, "sbpf", "return_data.so")
	programDataStateWriter := new(bytes.Buffer)
	programDataStateEncoder := bin.NewBinEncoder(programDataStateWriter)
	err = programDataAcctState.MarshalWithEncoder(programDataStateEncoder)
	assert.NoError(t, err)
	programDataStateWriter.Write(validProgramBytes)
	programDataStateBytes := make([]byte, len(validProgramBytes)+upgradeableLoaderSizeOfProgramDataMetaData)
	copy(programDataStateBytes, programDataStateWriter.Bytes())
	copy(programDataStateBytes[upgradeableLoaderSizeOfProgramDataMetaData:], validProgramBytes)

	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataStateBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// program account
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: programDataAcct.Key}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programData := make([]byte, 5000)
	copy(programData, programBytes)
	programAcct := accounts.Account{Key: programPubkey, Lamports: 10000, Data: programData, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	instrData := make([]byte, 0)

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct})

	acctMetas := []AccountMeta{{Pubkey: programAcct.Key, IsSigner: false, IsWritable: false}}

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeter(10000000000)}

	execCtx.Accounts = accounts.NewMemAccounts()
	var clock SysvarClock
	clock.Slot = 1234
	clockAcct := accounts.Account{}
	clockAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarClockAddr, &clockAcct)
	WriteClockSysvar(&execCtx.Accounts, clock)

	var rent SysvarRent
	rent.LamportsPerUint8Year = 1
	rent.ExemptionThreshold = 1
	rent.BurnPercent = 0

	rentAcct := accounts.Account{}
	rentAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarRentAddr, &rentAcct)
	WriteRentSysvar(&execCtx.Accounts, rent)

	pk := [32]byte(programDataAcct.Key)
	err = execCtx.Accounts.SetAccount(&pk, &programDataAcct)
	assert.NoError(t, err)

	execCtx.SlotCtx = new(SlotCtx)
	execCtx.SlotCtx.Slot = 1337

	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)

	pubkey, returnData := execCtx.TransactionContext.ReturnData()

	// the bpf testcase program itself tests that the return data string is as expected, but test here
	// again just for completeness.
	expectedString := "the quick brown fox jumps over the lazy dog"
	expectedBytes := make([]byte, len(expectedString)+1) // +1 for the NULL terminator
	copy(expectedBytes, expectedString)
	assert.Equal(t, expectedBytes, returnData)

	// and test also the programID
	assert.Equal(t, programPubkey[:], pubkey[:])
}

func TestInterpreter_Poseidon_Syscall(t *testing.T) {
	// program data account
	programDataPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataPrivKey.PublicKey()
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{Slot: 0, UpgradeAuthorityAddress: nil}}
	validProgramBytes := fixtures.Load(t, "sbpf", "poseidon.so")
	programDataStateWriter := new(bytes.Buffer)
	programDataStateEncoder := bin.NewBinEncoder(programDataStateWriter)
	err = programDataAcctState.MarshalWithEncoder(programDataStateEncoder)
	assert.NoError(t, err)
	programDataStateWriter.Write(validProgramBytes)
	programDataStateBytes := make([]byte, len(validProgramBytes)+upgradeableLoaderSizeOfProgramDataMetaData)
	copy(programDataStateBytes, programDataStateWriter.Bytes())
	copy(programDataStateBytes[upgradeableLoaderSizeOfProgramDataMetaData:], validProgramBytes)

	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataStateBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// program account
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: programDataAcct.Key}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programData := make([]byte, 5000)
	copy(programData, programBytes)
	programAcct := accounts.Account{Key: programPubkey, Lamports: 10000, Data: programData, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	instrData := make([]byte, 0)

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct})

	acctMetas := []AccountMeta{{Pubkey: programAcct.Key, IsSigner: false, IsWritable: false}}

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	var log LogRecorder
	execCtx := ExecutionCtx{Log: &log, TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeter(10000000000)}

	execCtx.Accounts = accounts.NewMemAccounts()
	var clock SysvarClock
	clock.Slot = 1234
	clockAcct := accounts.Account{}
	clockAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarClockAddr, &clockAcct)
	WriteClockSysvar(&execCtx.Accounts, clock)

	var rent SysvarRent
	rent.LamportsPerUint8Year = 1
	rent.ExemptionThreshold = 1
	rent.BurnPercent = 0

	rentAcct := accounts.Account{}
	rentAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarRentAddr, &rentAcct)
	WriteRentSysvar(&execCtx.Accounts, rent)

	pk := [32]byte(programDataAcct.Key)
	err = execCtx.Accounts.SetAccount(&pk, &programDataAcct)
	assert.NoError(t, err)

	execCtx.SlotCtx = new(SlotCtx)
	execCtx.SlotCtx.Slot = 1337

	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)
}

func TestInterpreter_Get_Sysvar_Syscalls(t *testing.T) {
	// program data account
	programDataPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataPrivKey.PublicKey()
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{Slot: 0, UpgradeAuthorityAddress: nil}}
	validProgramBytes := fixtures.Load(t, "sbpf", "sysvars.so")
	programDataStateWriter := new(bytes.Buffer)
	programDataStateEncoder := bin.NewBinEncoder(programDataStateWriter)
	err = programDataAcctState.MarshalWithEncoder(programDataStateEncoder)
	assert.NoError(t, err)
	programDataStateWriter.Write(validProgramBytes)
	programDataStateBytes := make([]byte, len(validProgramBytes)+upgradeableLoaderSizeOfProgramDataMetaData)
	copy(programDataStateBytes, programDataStateWriter.Bytes())
	copy(programDataStateBytes[upgradeableLoaderSizeOfProgramDataMetaData:], validProgramBytes)

	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataStateBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// program account
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: programDataAcct.Key}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programData := make([]byte, 5000)
	copy(programData, programBytes)
	programAcct := accounts.Account{Key: programPubkey, Lamports: 10000, Data: programData, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	instrData := make([]byte, 0)

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct})

	acctMetas := []AccountMeta{{Pubkey: programAcct.Key, IsSigner: false, IsWritable: false}}

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	var log LogRecorder
	execCtx := ExecutionCtx{Log: &log, TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeter(10000000000)}

	execCtx.Accounts = accounts.NewMemAccounts()
	var clock SysvarClock
	clock.Slot = 1234
	clock.Epoch = 1111
	clock.EpochStartTimestamp = 2222
	clock.UnixTimestamp = 3
	clock.LeaderScheduleEpoch = 100000
	clockAcct := accounts.Account{}
	clockAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarClockAddr, &clockAcct)
	WriteClockSysvar(&execCtx.Accounts, clock)

	var rent SysvarRent
	rent.LamportsPerUint8Year = 12
	rent.ExemptionThreshold = 34
	rent.BurnPercent = 56

	rentAcct := accounts.Account{}
	rentAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarRentAddr, &rentAcct)
	WriteRentSysvar(&execCtx.Accounts, rent)

	var epochSchedule SysvarEpochSchedule
	epochSchedule.SlotsPerEpoch = 1111
	epochSchedule.LeaderScheduleSlotOffset = 2222
	epochSchedule.Warmup = true
	epochSchedule.FirstNormalEpoch = 4444
	epochSchedule.FirstNormalSlot = 5555

	epochScheduleAcct := accounts.Account{}
	epochScheduleAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarEpochScheduleAddr, &epochScheduleAcct)
	WriteEpochScheduleSysvar(&execCtx.Accounts, epochSchedule)

	var lastRestartSlot SysvarLastRestartSlot
	lastRestartSlot.LastRestartSlot = 989898
	lastRestartSlotAcct := accounts.Account{}
	lastRestartSlotAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarLastRestartSlotAddr, &lastRestartSlotAcct)
	WriteLastRestartSlotSysvar(&execCtx.Accounts, lastRestartSlot)

	var epochRewards SysvarEpochRewards
	epochRewards.DistributionStartingBlockHeight = 1234
	epochRewards.NumPartitions = 4321
	copy(epochRewards.ParentBlockhash[:], "abaaaaaaaaaaaaaaaaaaaaaaaaaaaada")
	epochRewards.TotalPoints.Lo = 0xffffffffffffffff
	epochRewards.TotalPoints.Hi = 0xeeeeeeeeeeeeeeee
	epochRewards.TotalRewards = 5656
	epochRewards.DistributedRewards = 6767
	epochRewards.Active = false
	epochRewardsAcct := accounts.Account{}
	epochRewardsAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarEpochRewardsAddr, &epochRewardsAcct)
	WriteEpochRewardsSysvar(&execCtx.Accounts, epochRewards)

	f := features.NewFeaturesDefault()
	f.EnableFeature(features.LastRestartSlotSysvar, 0)
	f.EnableFeature(features.EnablePartitionedEpochReward, 0)
	execCtx.Features = *f

	pk := [32]byte(programDataAcct.Key)
	err = execCtx.Accounts.SetAccount(&pk, &programDataAcct)
	assert.NoError(t, err)

	execCtx.SlotCtx = new(SlotCtx)
	execCtx.SlotCtx.Slot = 1337

	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)

	for _, l := range log.Logs {
		fmt.Printf("log: %s\n", l)
	}
}

func TestInterpreter_AltBn128_Ops_Syscall(t *testing.T) {
	// program data account
	programDataPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataPrivKey.PublicKey()
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{Slot: 0, UpgradeAuthorityAddress: nil}}
	validProgramBytes := fixtures.Load(t, "sbpf", "alt_bn128.so")
	programDataStateWriter := new(bytes.Buffer)
	programDataStateEncoder := bin.NewBinEncoder(programDataStateWriter)
	err = programDataAcctState.MarshalWithEncoder(programDataStateEncoder)
	assert.NoError(t, err)
	programDataStateWriter.Write(validProgramBytes)
	programDataStateBytes := make([]byte, len(validProgramBytes)+upgradeableLoaderSizeOfProgramDataMetaData)
	copy(programDataStateBytes, programDataStateWriter.Bytes())
	copy(programDataStateBytes[upgradeableLoaderSizeOfProgramDataMetaData:], validProgramBytes)

	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataStateBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// program account
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: programDataAcct.Key}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programData := make([]byte, 5000)
	copy(programData, programBytes)
	programAcct := accounts.Account{Key: programPubkey, Lamports: 10000, Data: programData, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	instrData := make([]byte, 0)

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct})

	acctMetas := []AccountMeta{{Pubkey: programAcct.Key, IsSigner: false, IsWritable: false}}

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	var log LogRecorder
	execCtx := ExecutionCtx{Log: &log, TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeter(10000000000)}

	execCtx.SlotCtx = new(SlotCtx)
	execCtx.SlotCtx.Slot = 1337

	execCtx.SlotCtx.Accounts = accounts.NewMemAccounts()
	var clock SysvarClock
	clock.Slot = 1234
	clock.Epoch = 1111
	clock.EpochStartTimestamp = 2222
	clock.UnixTimestamp = 3
	clock.LeaderScheduleEpoch = 100000
	clockAcct := accounts.Account{}
	clockAcct.Lamports = 1
	execCtx.SlotCtx.Accounts.SetAccount(&SysvarClockAddr, &clockAcct)
	WriteClockSysvar(&execCtx.SlotCtx.Accounts, clock)

	var rent SysvarRent
	rent.LamportsPerUint8Year = 12
	rent.ExemptionThreshold = 34
	rent.BurnPercent = 56

	rentAcct := accounts.Account{}
	rentAcct.Lamports = 1
	execCtx.SlotCtx.Accounts.SetAccount(&SysvarRentAddr, &rentAcct)
	WriteRentSysvar(&execCtx.SlotCtx.Accounts, rent)

	var epochSchedule SysvarEpochSchedule
	epochSchedule.SlotsPerEpoch = 1111
	epochSchedule.LeaderScheduleSlotOffset = 2222
	epochSchedule.Warmup = true
	epochSchedule.FirstNormalEpoch = 4444
	epochSchedule.FirstNormalSlot = 5555

	epochScheduleAcct := accounts.Account{}
	epochScheduleAcct.Lamports = 1
	execCtx.SlotCtx.Accounts.SetAccount(&SysvarEpochScheduleAddr, &epochScheduleAcct)
	WriteEpochScheduleSysvar(&execCtx.SlotCtx.Accounts, epochSchedule)

	var lastRestartSlot SysvarLastRestartSlot
	lastRestartSlot.LastRestartSlot = 989898
	lastRestartSlotAcct := accounts.Account{}
	lastRestartSlotAcct.Lamports = 1
	execCtx.SlotCtx.Accounts.SetAccount(&SysvarLastRestartSlotAddr, &lastRestartSlotAcct)
	WriteLastRestartSlotSysvar(&execCtx.SlotCtx.Accounts, lastRestartSlot)

	var epochRewards SysvarEpochRewards
	epochRewards.DistributionStartingBlockHeight = 1234
	epochRewards.NumPartitions = 4321
	copy(epochRewards.ParentBlockhash[:], "abaaaaaaaaaaaaaaaaaaaaaaaaaaaada")
	epochRewards.TotalPoints.Lo = 0xffffffffffffffff
	epochRewards.TotalPoints.Hi = 0xeeeeeeeeeeeeeeee
	epochRewards.TotalRewards = 5656
	epochRewards.DistributedRewards = 6767
	epochRewards.Active = false
	epochRewardsAcct := accounts.Account{}
	epochRewardsAcct.Lamports = 1
	execCtx.SlotCtx.Accounts.SetAccount(&SysvarEpochRewardsAddr, &epochRewardsAcct)
	WriteEpochRewardsSysvar(&execCtx.SlotCtx.Accounts, epochRewards)

	f := features.NewFeaturesDefault()
	f.EnableFeature(features.LastRestartSlotSysvar, 0)
	f.EnableFeature(features.EnablePartitionedEpochReward, 0)
	f.EnableFeature(features.EnableAltBn128Syscall, 0)
	execCtx.Features = *f

	pk := [32]byte(programDataAcct.Key)
	err = execCtx.SlotCtx.Accounts.SetAccount(&pk, &programDataAcct)
	assert.NoError(t, err)

	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)

	for _, l := range log.Logs {
		fmt.Printf("log: %s\n", l)
	}
}

func TestInterpreter_Alloc_Free_Syscall(t *testing.T) {
	// program data account
	programDataPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataPrivKey.PublicKey()
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{Slot: 0, UpgradeAuthorityAddress: nil}}
	validProgramBytes := fixtures.Load(t, "sbpf", "alloc_free.so")
	programDataStateWriter := new(bytes.Buffer)
	programDataStateEncoder := bin.NewBinEncoder(programDataStateWriter)
	err = programDataAcctState.MarshalWithEncoder(programDataStateEncoder)
	assert.NoError(t, err)
	programDataStateWriter.Write(validProgramBytes)
	programDataStateBytes := make([]byte, len(validProgramBytes)+upgradeableLoaderSizeOfProgramDataMetaData)
	copy(programDataStateBytes, programDataStateWriter.Bytes())
	copy(programDataStateBytes[upgradeableLoaderSizeOfProgramDataMetaData:], validProgramBytes)

	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataStateBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// program account
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: programDataAcct.Key}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programData := make([]byte, 5000)
	copy(programData, programBytes)
	programAcct := accounts.Account{Key: programPubkey, Lamports: 10000, Data: programData, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	instrData := make([]byte, 0)

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct})

	acctMetas := []AccountMeta{{Pubkey: programAcct.Key, IsSigner: false, IsWritable: false}}

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	var log LogRecorder
	execCtx := ExecutionCtx{Log: &log, TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeter(10000000000)}

	execCtx.Accounts = accounts.NewMemAccounts()

	pk := [32]byte(programDataAcct.Key)
	err = execCtx.Accounts.SetAccount(&pk, &programDataAcct)
	assert.NoError(t, err)

	execCtx.SlotCtx = new(SlotCtx)
	execCtx.SlotCtx.Slot = 1337

	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)

	for _, l := range log.Logs {
		fmt.Printf("log: %s\n", l)
	}
}

func TestInterpreter_Alt_Bn128_Compression_Syscall(t *testing.T) {
	// program data account
	programDataPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataPrivKey.PublicKey()
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{Slot: 0, UpgradeAuthorityAddress: nil}}
	validProgramBytes := fixtures.Load(t, "sbpf", "alt_bn128_compression.so")
	programDataStateWriter := new(bytes.Buffer)
	programDataStateEncoder := bin.NewBinEncoder(programDataStateWriter)
	err = programDataAcctState.MarshalWithEncoder(programDataStateEncoder)
	assert.NoError(t, err)
	programDataStateWriter.Write(validProgramBytes)
	programDataStateBytes := make([]byte, len(validProgramBytes)+upgradeableLoaderSizeOfProgramDataMetaData)
	copy(programDataStateBytes, programDataStateWriter.Bytes())
	copy(programDataStateBytes[upgradeableLoaderSizeOfProgramDataMetaData:], validProgramBytes)

	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataStateBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// program account
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: programDataAcct.Key}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programData := make([]byte, 5000)
	copy(programData, programBytes)
	programAcct := accounts.Account{Key: programPubkey, Lamports: 10000, Data: programData, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	instrData := make([]byte, 0)

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct})

	acctMetas := []AccountMeta{{Pubkey: programAcct.Key, IsSigner: false, IsWritable: false}}

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	var log LogRecorder
	execCtx := ExecutionCtx{Log: &log, TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeter(10000000000)}
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.EnableAltbn128CompressionSyscall, 0)
	execCtx.Features = *f

	execCtx.Accounts = accounts.NewMemAccounts()

	pk := [32]byte(programDataAcct.Key)
	err = execCtx.Accounts.SetAccount(&pk, &programDataAcct)
	assert.NoError(t, err)

	execCtx.SlotCtx = new(SlotCtx)
	execCtx.SlotCtx.Slot = 1337

	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)

	for _, l := range log.Logs {
		fmt.Printf("log: %s\n", l)
	}
}

func TestInterpreter_Validate_Point_Syscall(t *testing.T) {
	// program data account
	programDataPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataPrivKey.PublicKey()
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{Slot: 0, UpgradeAuthorityAddress: nil}}
	validProgramBytes := fixtures.Load(t, "sbpf", "validate_point.so")
	programDataStateWriter := new(bytes.Buffer)
	programDataStateEncoder := bin.NewBinEncoder(programDataStateWriter)
	err = programDataAcctState.MarshalWithEncoder(programDataStateEncoder)
	assert.NoError(t, err)
	programDataStateWriter.Write(validProgramBytes)
	programDataStateBytes := make([]byte, len(validProgramBytes)+upgradeableLoaderSizeOfProgramDataMetaData)
	copy(programDataStateBytes, programDataStateWriter.Bytes())
	copy(programDataStateBytes[upgradeableLoaderSizeOfProgramDataMetaData:], validProgramBytes)

	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataStateBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// program account
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: programDataAcct.Key}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programData := make([]byte, 5000)
	copy(programData, programBytes)
	programAcct := accounts.Account{Key: programPubkey, Lamports: 10000, Data: programData, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	instrData := make([]byte, 0)

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct})

	acctMetas := []AccountMeta{{Pubkey: programAcct.Key, IsSigner: false, IsWritable: false}}

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	var log LogRecorder
	execCtx := ExecutionCtx{Log: &log, TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeter(10000000000)}
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.Curve25519SyscallEnabled, 0)
	execCtx.Features = *f

	execCtx.Accounts = accounts.NewMemAccounts()

	pk := [32]byte(programDataAcct.Key)
	err = execCtx.Accounts.SetAccount(&pk, &programDataAcct)
	assert.NoError(t, err)

	execCtx.SlotCtx = new(SlotCtx)
	execCtx.SlotCtx.Slot = 1337

	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)

	for _, l := range log.Logs {
		fmt.Printf("log: %s\n", l)
	}
}

func TestInterpreter_Curve_Group_Ops_Syscall(t *testing.T) {
	// program data account
	programDataPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataPrivKey.PublicKey()
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{Slot: 0, UpgradeAuthorityAddress: nil}}
	validProgramBytes := fixtures.Load(t, "sbpf", "curve_group_ops.so")
	programDataStateWriter := new(bytes.Buffer)
	programDataStateEncoder := bin.NewBinEncoder(programDataStateWriter)
	err = programDataAcctState.MarshalWithEncoder(programDataStateEncoder)
	assert.NoError(t, err)
	programDataStateWriter.Write(validProgramBytes)
	programDataStateBytes := make([]byte, len(validProgramBytes)+upgradeableLoaderSizeOfProgramDataMetaData)
	copy(programDataStateBytes, programDataStateWriter.Bytes())
	copy(programDataStateBytes[upgradeableLoaderSizeOfProgramDataMetaData:], validProgramBytes)

	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataStateBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// program account
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: programDataAcct.Key}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programData := make([]byte, 5000)
	copy(programData, programBytes)
	programAcct := accounts.Account{Key: programPubkey, Lamports: 10000, Data: programData, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	instrData := make([]byte, 0)

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct})

	acctMetas := []AccountMeta{{Pubkey: programAcct.Key, IsSigner: false, IsWritable: false}}

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	var log LogRecorder
	execCtx := ExecutionCtx{Log: &log, TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeter(10000000000)}
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.Curve25519SyscallEnabled, 0)
	execCtx.Features = *f

	execCtx.Accounts = accounts.NewMemAccounts()

	pk := [32]byte(programDataAcct.Key)
	err = execCtx.Accounts.SetAccount(&pk, &programDataAcct)
	assert.NoError(t, err)

	execCtx.SlotCtx = new(SlotCtx)
	execCtx.SlotCtx.Slot = 1337

	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)

	for _, l := range log.Logs {
		fmt.Printf("log: %s\n", l)
	}
}

func TestInterpreter_Curve_Multiscalar_Mul_Syscall(t *testing.T) {
	// program data account
	programDataPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataPrivKey.PublicKey()
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{Slot: 0, UpgradeAuthorityAddress: nil}}
	validProgramBytes := fixtures.Load(t, "sbpf", "curve_multiscalar_mul.so")
	programDataStateWriter := new(bytes.Buffer)
	programDataStateEncoder := bin.NewBinEncoder(programDataStateWriter)
	err = programDataAcctState.MarshalWithEncoder(programDataStateEncoder)
	assert.NoError(t, err)
	programDataStateWriter.Write(validProgramBytes)
	programDataStateBytes := make([]byte, len(validProgramBytes)+upgradeableLoaderSizeOfProgramDataMetaData)
	copy(programDataStateBytes, programDataStateWriter.Bytes())
	copy(programDataStateBytes[upgradeableLoaderSizeOfProgramDataMetaData:], validProgramBytes)

	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataStateBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// program account
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: programDataAcct.Key}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programData := make([]byte, 5000)
	copy(programData, programBytes)
	programAcct := accounts.Account{Key: programPubkey, Lamports: 10000, Data: programData, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	instrData := make([]byte, 0)

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct})

	acctMetas := []AccountMeta{{Pubkey: programAcct.Key, IsSigner: false, IsWritable: false}}

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	var log LogRecorder
	execCtx := ExecutionCtx{Log: &log, TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeter(10000000000)}
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.Curve25519SyscallEnabled, 0)
	execCtx.Features = *f

	execCtx.Accounts = accounts.NewMemAccounts()

	pk := [32]byte(programDataAcct.Key)
	err = execCtx.Accounts.SetAccount(&pk, &programDataAcct)
	assert.NoError(t, err)

	execCtx.SlotCtx = new(SlotCtx)
	execCtx.SlotCtx.Slot = 1337

	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)

	for _, l := range log.Logs {
		fmt.Printf("log: %s\n", l)
	}
}

func TestInterpreter_Log_Data_Syscall(t *testing.T) {
	// program data account
	programDataPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataPrivKey.PublicKey()
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{Slot: 0, UpgradeAuthorityAddress: nil}}
	validProgramBytes := fixtures.Load(t, "sbpf", "sol_log_data.so")
	programDataStateWriter := new(bytes.Buffer)
	programDataStateEncoder := bin.NewBinEncoder(programDataStateWriter)
	err = programDataAcctState.MarshalWithEncoder(programDataStateEncoder)
	assert.NoError(t, err)
	programDataStateWriter.Write(validProgramBytes)
	programDataStateBytes := make([]byte, len(validProgramBytes)+upgradeableLoaderSizeOfProgramDataMetaData)
	copy(programDataStateBytes, programDataStateWriter.Bytes())
	copy(programDataStateBytes[upgradeableLoaderSizeOfProgramDataMetaData:], validProgramBytes)

	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataStateBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// program account
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: programDataAcct.Key}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programData := make([]byte, 5000)
	copy(programData, programBytes)
	programAcct := accounts.Account{Key: programPubkey, Lamports: 10000, Data: programData, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	instrData := make([]byte, 0)

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct})

	acctMetas := []AccountMeta{{Pubkey: programAcct.Key, IsSigner: false, IsWritable: false}}

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	var log LogRecorder
	execCtx := ExecutionCtx{Log: &log, TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeter(10000000000)}
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.Curve25519SyscallEnabled, 0)
	execCtx.Features = *f

	execCtx.Accounts = accounts.NewMemAccounts()

	pk := [32]byte(programDataAcct.Key)
	err = execCtx.Accounts.SetAccount(&pk, &programDataAcct)
	assert.NoError(t, err)

	execCtx.SlotCtx = new(SlotCtx)
	execCtx.SlotCtx.Slot = 1337

	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)

	for _, l := range log.Logs {
		fmt.Printf("log: %s\n", l)
	}
}

func TestInterpreter_Cpi_C_System_Program_Allocate(t *testing.T) {
	// program data account
	programDataPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataPrivKey.PublicKey()
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{Slot: 0, UpgradeAuthorityAddress: nil}}
	validProgramBytes := fixtures.Load(t, "sbpf", "cpi_c_to_system_program_allocate.so")
	programDataStateWriter := new(bytes.Buffer)
	programDataStateEncoder := bin.NewBinEncoder(programDataStateWriter)
	err = programDataAcctState.MarshalWithEncoder(programDataStateEncoder)
	assert.NoError(t, err)
	programDataStateWriter.Write(validProgramBytes)
	programDataStateBytes := make([]byte, len(validProgramBytes)+upgradeableLoaderSizeOfProgramDataMetaData)
	copy(programDataStateBytes, programDataStateWriter.Bytes())
	copy(programDataStateBytes[upgradeableLoaderSizeOfProgramDataMetaData:], validProgramBytes)

	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataStateBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// program account
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: programDataAcct.Key}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programData := make([]byte, 5000)
	copy(programData, programBytes)
	programAcct := accounts.Account{Key: programPubkey, Lamports: 10000, Data: programData, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	systemAcct := accounts.Account{Key: a.SystemProgramAddr, Lamports: 10000, Data: programData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	seed := []byte{'Y', 'o', 'u', ' ', 'p', 'a', 's', 's',
		' ', 'b', 'u', 't', 't', 'e', 'r'}

	acctToAllocPubKey, bumpSeed, err := solana.FindProgramAddress([][]byte{seed}, programPubkey)
	assert.NoError(t, err)
	acctToAlloc := accounts.Account{Key: acctToAllocPubKey, Lamports: 10000, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 1)
	instrData[0] = bumpSeed

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, systemAcct, acctToAlloc})

	acctMetas := []AccountMeta{{Pubkey: a.SystemProgramAddr, IsSigner: false, IsWritable: false},
		{Pubkey: acctToAlloc.Key, IsSigner: true, IsWritable: true}}

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	var log LogRecorder
	execCtx := ExecutionCtx{Log: &log, TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeter(10000000000)}
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.Curve25519SyscallEnabled, 0)
	execCtx.Features = *f

	execCtx.Accounts = accounts.NewMemAccounts()

	pk := [32]byte(programDataAcct.Key)
	err = execCtx.Accounts.SetAccount(&pk, &programDataAcct)
	assert.NoError(t, err)

	execCtx.SlotCtx = new(SlotCtx)
	execCtx.SlotCtx.Slot = 1337

	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.NoError(t, err)

	// check that the SystemProgram::Allocate instruction worked to resize account data to 1337 bytes
	postAllocAcct, err := execCtx.TransactionContext.Accounts.GetAccount(2)
	assert.NoError(t, err)
	assert.Equal(t, 1337, len(postAllocAcct.Data))

	for _, l := range log.Logs {
		fmt.Printf("log: %s\n", l)
	}
}

func TestInterpreter_Cpi_Rust_System_Program_Allocate(t *testing.T) {
	// program data account
	programDataPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataPrivKey.PublicKey()
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{Slot: 0, UpgradeAuthorityAddress: nil}}
	validProgramBytes := fixtures.Load(t, "sbpf", "cpi_rust_to_system_program_allocate.so")
	programDataStateWriter := new(bytes.Buffer)
	programDataStateEncoder := bin.NewBinEncoder(programDataStateWriter)
	err = programDataAcctState.MarshalWithEncoder(programDataStateEncoder)
	assert.NoError(t, err)
	programDataStateWriter.Write(validProgramBytes)
	programDataStateBytes := make([]byte, len(validProgramBytes)+upgradeableLoaderSizeOfProgramDataMetaData)
	copy(programDataStateBytes, programDataStateWriter.Bytes())
	copy(programDataStateBytes[upgradeableLoaderSizeOfProgramDataMetaData:], validProgramBytes)

	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataStateBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// program account
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: programDataAcct.Key}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programData := make([]byte, 5000)
	copy(programData, programBytes)
	programAcct := accounts.Account{Key: programPubkey, Lamports: 10000, Data: programData, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	systemAcct := accounts.Account{Key: a.SystemProgramAddr, Lamports: 10000, Data: programData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	seed := []byte{'Y', 'o', 'u', ' ', 'p', 'a', 's', 's',
		' ', 'b', 'u', 't', 't', 'e', 'r'}

	acctToAllocPubKey, bumpSeed, err := solana.FindProgramAddress([][]byte{seed}, programPubkey)
	assert.NoError(t, err)
	acctToAlloc := accounts.Account{Key: acctToAllocPubKey, Lamports: 10000, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 1)
	instrData[0] = bumpSeed

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, systemAcct, acctToAlloc})

	acctMetas := []AccountMeta{{Pubkey: a.SystemProgramAddr, IsSigner: false, IsWritable: false},
		{Pubkey: acctToAlloc.Key, IsSigner: true, IsWritable: true}}

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	var log LogRecorder
	execCtx := ExecutionCtx{Log: &log, TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeter(10000000000)}
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.Curve25519SyscallEnabled, 0)
	execCtx.Features = *f

	execCtx.Accounts = accounts.NewMemAccounts()

	pk := [32]byte(programDataAcct.Key)
	err = execCtx.Accounts.SetAccount(&pk, &programDataAcct)
	assert.NoError(t, err)

	execCtx.SlotCtx = new(SlotCtx)
	execCtx.SlotCtx.Slot = 1337

	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.NoError(t, err)

	// check that the SystemProgram::Allocate instruction worked to resize account data to 1337 bytes
	postAllocAcct, err := execCtx.TransactionContext.Accounts.GetAccount(2)
	assert.NoError(t, err)
	assert.Equal(t, 1337, len(postAllocAcct.Data))

	for _, l := range log.Logs {
		fmt.Printf("log: %s\n", l)
	}
}

func TestInterpreter_Cpi_C_Bpf_Program_Call(t *testing.T) {
	// program data account
	programDataPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataPrivKey.PublicKey()
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{Slot: 0, UpgradeAuthorityAddress: nil}}
	validProgramBytes := fixtures.Load(t, "sbpf", "cpi_c_to_bpf.so")
	programDataStateWriter := new(bytes.Buffer)
	programDataStateEncoder := bin.NewBinEncoder(programDataStateWriter)
	err = programDataAcctState.MarshalWithEncoder(programDataStateEncoder)
	assert.NoError(t, err)
	programDataStateWriter.Write(validProgramBytes)
	programDataStateBytes := make([]byte, len(validProgramBytes)+upgradeableLoaderSizeOfProgramDataMetaData)
	copy(programDataStateBytes, programDataStateWriter.Bytes())
	copy(programDataStateBytes[upgradeableLoaderSizeOfProgramDataMetaData:], validProgramBytes)

	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataStateBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// program account
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: programDataAcct.Key}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programData := make([]byte, 5000)
	copy(programData, programBytes)
	programAcct := accounts.Account{Key: programPubkey, Lamports: 10000, Data: programData, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	fromAcctPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	fromAcctPubkey := fromAcctPrivateKey.PublicKey()
	fromAcct := accounts.Account{Key: fromAcctPubkey, Lamports: 10000, Data: make([]byte, 0), Owner: programAcct.Key, Executable: false, RentEpoch: 100}

	toAcctPrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	toAcctPubkey := toAcctPrivateKey.PublicKey()
	toAcct := accounts.Account{Key: toAcctPubkey, Lamports: 10000, Data: make([]byte, 0), Owner: programAcct.Key, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 1)
	instrData[0] = 0

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, fromAcct, toAcct})

	acctMetas := []AccountMeta{{Pubkey: programAcct.Key, IsSigner: false, IsWritable: false},
		{Pubkey: fromAcct.Key, IsSigner: true, IsWritable: true},
		{Pubkey: toAcct.Key, IsSigner: true, IsWritable: true}}

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	var log LogRecorder
	execCtx := ExecutionCtx{Log: &log, TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeter(10000000000)}
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.Curve25519SyscallEnabled, 0)
	execCtx.Features = *f

	execCtx.Accounts = accounts.NewMemAccounts()

	pk := [32]byte(programDataAcct.Key)
	err = execCtx.Accounts.SetAccount(&pk, &programDataAcct)
	assert.NoError(t, err)

	execCtx.SlotCtx = new(SlotCtx)
	execCtx.SlotCtx.Slot = 1337

	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.NoError(t, err)

	// check that the transfer of funds actually succeeded
	postFromAcct, err := execCtx.TransactionContext.Accounts.GetAccount(1)
	assert.NoError(t, err)

	postToAcct, err := execCtx.TransactionContext.Accounts.GetAccount(2)
	assert.NoError(t, err)

	assert.Equal(t, uint64(9000), postFromAcct.Lamports)
	assert.Equal(t, uint64(11000), postToAcct.Lamports)

	for _, l := range log.Logs {
		fmt.Printf("log: %s\n", l)
	}
}

func executeFirstBpfProgramAndReturnExecCtx(t *testing.T, log *LogRecorder, acct1 *accounts.Account, acct2 *accounts.Account, acct3 *accounts.Account) (*ExecutionCtx, *accounts.Account, []byte) {
	// program data account
	programDataPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataPrivKey.PublicKey()
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{Slot: 0, UpgradeAuthorityAddress: nil}}
	validProgramBytes := fixtures.Load(t, "sbpf", "get_stack_height.so")
	programDataStateWriter := new(bytes.Buffer)
	programDataStateEncoder := bin.NewBinEncoder(programDataStateWriter)
	err = programDataAcctState.MarshalWithEncoder(programDataStateEncoder)
	assert.NoError(t, err)
	programDataStateWriter.Write(validProgramBytes)
	programDataStateBytes := make([]byte, len(validProgramBytes)+upgradeableLoaderSizeOfProgramDataMetaData)
	copy(programDataStateBytes, programDataStateWriter.Bytes())
	copy(programDataStateBytes[upgradeableLoaderSizeOfProgramDataMetaData:], validProgramBytes)

	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataStateBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// program account
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: programDataAcct.Key}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programData := make([]byte, 5000)
	copy(programData, programBytes)
	programAcct := accounts.Account{Key: programPubkey, Lamports: 10000, Data: programData, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	instrData := make([]byte, 5)
	instrData[0] = 'x'
	instrData[1] = 'x'
	instrData[2] = 'x'
	instrData[3] = 'x'
	instrData[4] = 'x'

	fmt.Printf("******** first program call is %s\n", programAcct.Key)
	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, *acct1, *acct2, *acct3})

	acctMetas := []AccountMeta{{Pubkey: programAcct.Key, IsSigner: false, IsWritable: false}}

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	execCtx := ExecutionCtx{Log: log, TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeter(10000000000)}

	execCtx.Accounts = accounts.NewMemAccounts()
	var clock SysvarClock
	clock.Slot = 1234
	clockAcct := accounts.Account{}
	clockAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarClockAddr, &clockAcct)
	WriteClockSysvar(&execCtx.Accounts, clock)

	var rent SysvarRent
	rent.LamportsPerUint8Year = 1
	rent.ExemptionThreshold = 1
	rent.BurnPercent = 0

	rentAcct := accounts.Account{}
	rentAcct.Lamports = 1
	execCtx.Accounts.SetAccount(&SysvarRentAddr, &rentAcct)
	WriteRentSysvar(&execCtx.Accounts, rent)

	pk := [32]byte(programDataAcct.Key)
	err = execCtx.Accounts.SetAccount(&pk, &programDataAcct)
	assert.NoError(t, err)

	execCtx.SlotCtx = new(SlotCtx)
	execCtx.SlotCtx.Slot = 1337

	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)

	for _, l := range log.Logs {
		fmt.Printf("log: %s\n", l)
	}

	return &execCtx, &programAcct, instrData
}

func TestInterpreter_Get_Processed_Sibling_Instruction_Test(t *testing.T) {

	// program data account
	programDataPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programDataPubkey := programDataPrivKey.PublicKey()
	programDataAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{Slot: 0, UpgradeAuthorityAddress: nil}}
	validProgramBytes := fixtures.Load(t, "sbpf", "solana_sbf_rust_system_alloc_then_sibling_introspection.so")
	programDataStateWriter := new(bytes.Buffer)
	programDataStateEncoder := bin.NewBinEncoder(programDataStateWriter)
	err = programDataAcctState.MarshalWithEncoder(programDataStateEncoder)
	assert.NoError(t, err)
	programDataStateWriter.Write(validProgramBytes)
	programDataStateBytes := make([]byte, len(validProgramBytes)+upgradeableLoaderSizeOfProgramDataMetaData)
	copy(programDataStateBytes, programDataStateWriter.Bytes())
	copy(programDataStateBytes[upgradeableLoaderSizeOfProgramDataMetaData:], validProgramBytes)

	programDataAcct := accounts.Account{Key: programDataPubkey, Lamports: 0, Data: programDataStateBytes, Owner: a.BpfLoaderUpgradeableAddr, Executable: false, RentEpoch: 100}

	// program account
	programAcctState := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: programDataAcct.Key}}
	programWriter := new(bytes.Buffer)
	programEncoder := bin.NewBinEncoder(programWriter)
	err = programAcctState.MarshalWithEncoder(programEncoder)
	assert.NoError(t, err)
	programBytes := programWriter.Bytes()
	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programData := make([]byte, 5000)
	copy(programData, programBytes)
	programAcct := accounts.Account{Key: programPubkey, Lamports: 10000, Data: programData, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, RentEpoch: 100}

	systemAcct := accounts.Account{Key: a.SystemProgramAddr, Lamports: 10000, Data: programData, Owner: a.NativeLoaderAddr, Executable: true, RentEpoch: 100}

	seed := []byte{'Y', 'o', 'u', ' ', 'p', 'a', 's', 's',
		' ', 'b', 'u', 't', 't', 'e', 'r'}

	acctToAllocPubKey, _, err := solana.FindProgramAddress([][]byte{seed}, programPubkey)
	assert.NoError(t, err)
	acctToAlloc := accounts.Account{Key: acctToAllocPubKey, Lamports: 10000, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: false, RentEpoch: 100}

	instrData := make([]byte, 4)
	instrData[0] = 0x61
	instrData[1] = 0x62
	instrData[2] = 0x61
	instrData[3] = 0x62

	var log LogRecorder
	execCtx, firstProgramAcct, firstInstrData := executeFirstBpfProgramAndReturnExecCtx(t, &log, &programAcct, &systemAcct, &acctToAlloc)

	pk := [32]byte(programDataAcct.Key)
	err = execCtx.Accounts.SetAccount(&pk, &programDataAcct)
	assert.NoError(t, err)

	acctMetas := []AccountMeta{{Pubkey: a.SystemProgramAddr, IsSigner: false, IsWritable: false},
		{Pubkey: acctToAlloc.Key, IsSigner: true, IsWritable: true}}

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, execCtx.TransactionContext.Accounts)
	fmt.Printf("**** num instruction accounts: %d\n", len(instructionAccts))

	fmt.Printf("******** second program call is %s\n", programAcct.Key)

	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{1})
	assert.NoError(t, err)

	// test that the program logs from the CPI'd program (which calls get_processed_sibling_instruction)
	// are as expected
	expected := fmt.Sprintf("Program log: ******** sibling instruction 0 program id: %s", programAcct.Key)
	assert.Equal(t, expected, log.Logs[1])
	expected = fmt.Sprintf("Program log: ******** sibling instruction 0 instruction data: %s", reformatHexBytes(instrData))
	assert.Equal(t, expected, log.Logs[2])

	expected = fmt.Sprintf("Program log: ******** sibling instruction 1 program id: %s", firstProgramAcct.Key)
	assert.Equal(t, expected, log.Logs[3])
	expected = fmt.Sprintf("Program log: ******** sibling instruction 1 instruction data: %s", reformatHexBytes(firstInstrData))
	assert.Equal(t, expected, log.Logs[4])

	for _, l := range log.Logs {
		fmt.Printf("log: %s\n", l)
	}
}

func TestInterpreter_Test_Memo_Program_With_LoaderV2(t *testing.T) {
	// as on mainnet, we set the memo program up such that it is a program owned by the older
	// and non-upgradeable BPFLoader2
	programData := fixtures.Load(t, "sealevel", "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr.so")
	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programAcct := accounts.Account{Key: programPubkey, Lamports: 10000, Data: programData, Owner: a.BpfLoader2Addr, Executable: true, RentEpoch: 100}

	signerPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	signerPubkey := signerPrivKey.PublicKey()
	signerAcct := accounts.Account{Key: signerPubkey, Lamports: 10000, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: true, RentEpoch: 100}

	instrData := []byte("hello world")

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, signerAcct})
	acctMetas := []AccountMeta{{Pubkey: signerAcct.Key, IsSigner: true, IsWritable: false}}
	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	var log LogRecorder
	execCtx := ExecutionCtx{Log: &log, TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeter(10000000000)}
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.Curve25519SyscallEnabled, 0)
	execCtx.Features = *f

	execCtx.Accounts = accounts.NewMemAccounts()

	pk := [32]byte(programAcct.Key)
	err = execCtx.Accounts.SetAccount(&pk, &programAcct)
	assert.NoError(t, err)

	execCtx.SlotCtx = new(SlotCtx)
	execCtx.SlotCtx.Slot = 1337

	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)

	expected := fmt.Sprintf("Program log: Signed by %s", signerPubkey)
	containsExpected := strings.HasPrefix(log.Logs[0], expected)
	assert.Equal(t, true, containsExpected)
	expected = fmt.Sprintf("Program log: Memo (len 11): \"hello world\"")
	assert.Equal(t, expected, log.Logs[1])

	instrData = make([]byte, 2)
	instrData[0] = 0xee
	instrData[1] = 0xff

	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)

	expected = fmt.Sprintf("Program log: Signed by %s", signerPubkey)
	containsExpected = strings.HasPrefix(log.Logs[2], expected)
	assert.Equal(t, true, containsExpected)
	expected = fmt.Sprintf("Program log: Invalid UTF-8, from byte 0")
	assert.Equal(t, expected, log.Logs[3])

	for _, l := range log.Logs {
		fmt.Printf("log: %s\n", l)
	}
}

func TestInterpreter_Test_Deprecated_Loader(t *testing.T) {
	// as on mainnet, we set the memo program up such that it is a program owned by the older
	// and non-upgradeable BPFLoader2
	programData := fixtures.Load(t, "sbpf", "deprecated_loader_simple_program.so")
	programPrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	programPubkey := programPrivKey.PublicKey()
	programAcct := accounts.Account{Key: programPubkey, Lamports: 10000, Data: programData, Owner: a.BpfLoaderDeprecatedAddr, Executable: true, RentEpoch: 100}

	acct1PrivKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	acct1Pubkey := acct1PrivKey.PublicKey()
	acct1 := accounts.Account{Key: acct1Pubkey, Lamports: 10000, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: true, RentEpoch: 100}
	acct1 = accounts.Account{Key: a.SystemProgramAddr, Lamports: 10000, Data: make([]byte, 0), Owner: a.SystemProgramAddr, Executable: true, RentEpoch: 100}

	acct2PrivateKey, err := solana.NewRandomPrivateKey()
	assert.NoError(t, err)
	acct2Pubkey := acct2PrivateKey.PublicKey()
	acct2 := accounts.Account{Key: acct2Pubkey, Lamports: 0x1337, Data: make([]byte, 0), Owner: a.VoteProgramAddr, Executable: true, RentEpoch: 100}
	acct2 = accounts.Account{Key: a.VoteProgramAddr, Lamports: 0x1337, Data: make([]byte, 0), Owner: a.VoteProgramAddr, Executable: true, RentEpoch: 100}

	fmt.Printf("acct1 key: %s\n", acct1.Key)
	fmt.Printf("acct2 key: %s\n", acct2.Key)

	instrData := []byte("hello world")

	transactionAccts := NewTransactionAccounts([]accounts.Account{programAcct, acct1, acct2})
	acctMetas := []AccountMeta{{Pubkey: acct1.Key, IsSigner: true, IsWritable: false},
		{Pubkey: acct2.Key, IsSigner: false, IsWritable: true}}

	instructionAccts := InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)

	txCtx := NewTransactionCtx(*transactionAccts, 5, 64)
	var log LogRecorder
	execCtx := ExecutionCtx{Log: &log, TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeter(10000000000)}
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.Curve25519SyscallEnabled, 0)
	execCtx.Features = *f

	execCtx.Accounts = accounts.NewMemAccounts()

	pk := [32]byte(programAcct.Key)
	err = execCtx.Accounts.SetAccount(&pk, &programAcct)
	assert.NoError(t, err)

	execCtx.SlotCtx = new(SlotCtx)
	execCtx.SlotCtx.Slot = 1337

	err = execCtx.ProcessInstruction(instrData, instructionAccts, []uint64{0})
	assert.Equal(t, nil, err)

	for _, l := range log.Logs {
		fmt.Printf("log: %s\n", l)
	}
}

func TestLoader_Old_Program(t *testing.T) {
	loader, err := loader.NewLoaderFromBytes(fixtures.Load(t, "sbpf", "old_program.so"))
	require.NoError(t, err)
	require.NotNil(t, loader)

	program, err := loader.Load()
	require.NoError(t, err)
	require.NotNil(t, program)

	require.NoError(t, program.Verify())
}

type executeCase struct {
	Name    string
	Program string
	Params  Params
	Logs    []string
}

func (e *executeCase) run(t *testing.T) {
	ld, err := loader.NewLoaderFromBytes(fixtures.Load(t, e.Program))
	require.NoError(t, err)
	require.NotNil(t, ld)

	program, err := ld.Load()
	require.NoError(t, err)
	require.NotNil(t, program)

	require.NoError(t, program.Verify())

	tx := TransactionCtx{}
	tx.PushInstructionCtx(InstructionCtx{})
	opts := tx.newVMOpts(&e.Params)
	opts.Tracer = testLogger{t}

	interpreter := sbpf.NewInterpreter(program, opts)
	require.NotNil(t, interpreter)

	_, _, err = interpreter.Run()
	assert.NoError(t, err)

	logs := opts.Context.(*ExecutionCtx).Log.(*LogRecorder).Logs
	assert.Equal(t, logs, e.Logs)
}

func TestExecute(t *testing.T) {
	// Collect test cases
	var cases []executeCase
	err := filepath.WalkDir(fixtures.Path(t, "sealevel"), func(path string, entry fs.DirEntry, err error) error {
		if !entry.Type().IsRegular() ||
			!strings.HasPrefix(filepath.Base(path), "test_") ||
			filepath.Ext(path) != ".json" {
			return nil
		}

		buf, err := os.ReadFile(path)
		require.NoError(t, err, path)

		var _case executeCase
		require.NoError(t, json.Unmarshal(buf, &_case), path)

		cases = append(cases, _case)
		return nil
	})
	require.NoError(t, err)

	for i := range cases {
		_case := cases[i]
		t.Run(_case.Name, func(t *testing.T) {
			t.Parallel()
			_case.run(t)
		})
	}
}

type testLogger struct {
	t *testing.T
}

func (t testLogger) Printf(format string, args ...any) {
	t.t.Logf(format, args...)
}
