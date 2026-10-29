package sealevel

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/stretchr/testify/require"
)

type firstWriteDetach struct{ oldLen, copied uint64 }

func recordFirstWriteDetaches(tx *TransactionAccounts) *[]firstWriteDetach {
	events := new([]firstWriteDetach)
	tx.OnFirstWriteDetach = func(oldLen, copied uint64) {
		*events = append(*events, firstWriteDetach{oldLen, copied})
	}
	return events
}

func TestAccountDataReplacementIsolation(t *testing.T) {
	for _, size := range []int{0, 2, 5, 9000} {
		ctx := serializationTestContext(t, 1)
		tx := ctx.TransactionContext
		ix, err := tx.CurrentInstructionCtx()
		require.NoError(t, err)
		acct, err := ix.BorrowInstructionAccount(tx, 0)
		require.NoError(t, err)
		original := acct.Account
		before := original.Clone()
		tx.Accounts.Shared[1] = true
		events := recordFirstWriteDetaches(&tx.Accounts)
		replacement := bytes.Repeat([]byte{42}, size)
		require.NoError(t, acct.SetData(ctx.Features, replacement))
		require.NotSame(t, original, acct.Account)
		require.Same(t, acct.Account, tx.Accounts.Accounts[1])
		require.Equal(t, before, original)
		require.False(t, tx.Accounts.Shared[1])
		require.True(t, tx.Accounts.Touched[1])
		require.Equal(t, []firstWriteDetach{{5, 0}}, *events)
		require.EqualValues(t, size-5, tx.AccountsResizeDelta)
		require.Equal(t, replacement, acct.Data())
		// Adoption is intentional: detach at the existing block-end boundary, not
		// at each CPI. This test guards against accidentally changing that lifetime.
		if size > 0 {
			replacement[0] = 43
			require.Equal(t, byte(43), acct.Data()[0])
		}
		first := acct.Account
		require.NoError(t, acct.SetData(ctx.Features, []byte{1, 2, 3}))
		require.Same(t, first, acct.Account)
		require.Equal(t, []firstWriteDetach{{5, 0}}, *events)
		require.EqualValues(t, -2, tx.AccountsResizeDelta)
		acct.Drop()
		// A duplicate instruction reference observes the same replacement.
		duplicate, err := ix.BorrowInstructionAccount(tx, 1)
		require.NoError(t, err)
		require.Same(t, first, duplicate.Account)
		require.Equal(t, []byte{1, 2, 3}, duplicate.Data())
		duplicate.Drop()
		require.Equal(t, before, original)
	}
}

// SetData adopts the caller's slice as is: nil stays nil, an empty slice stays
// non-nil, spare capacity is kept, and byte-identical data still touches.
func TestAccountDataReplacementKeepsSliceShape(t *testing.T) {
	spare := make([]byte, 3, 64)
	copy(spare, []byte{7, 8, 9})
	for _, tc := range []struct {
		name  string
		data  []byte
		delta int64
	}{
		{"nil", nil, -5},
		{"empty", []byte{}, -5},
		{"shrink", []byte{1, 2}, -3},
		{"identical", []byte{1, 2, 3, 4, 5}, 0},
		{"grow", bytes.Repeat([]byte{6}, 70), 65},
		{"spare_capacity", spare, -2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := serializationTestContext(t, 1)
			tx := ctx.TransactionContext
			ix, err := tx.CurrentInstructionCtx()
			require.NoError(t, err)
			acct, err := ix.BorrowInstructionAccount(tx, 0)
			require.NoError(t, err)
			defer acct.Drop()
			original := acct.Account
			tx.Accounts.Shared[1] = true
			require.NoError(t, acct.SetData(ctx.Features, tc.data))
			require.NotSame(t, original, acct.Account)
			require.Equal(t, []byte{1, 2, 3, 4, 5}, original.Data)
			require.True(t, tx.Accounts.Touched[1])
			require.Equal(t, tc.delta, tx.AccountsResizeDelta)
			got := acct.Data()
			require.Equal(t, tc.data == nil, got == nil)
			require.Equal(t, len(tc.data), len(got))
			require.Equal(t, cap(tc.data), cap(got))
			if cap(tc.data) > 0 {
				require.Same(t, &tc.data[:1][0], &got[:1][0], "SetData must not copy the incoming slice")
			}
		})
	}
}

func TestAccountDataReplacementErrorsDoNotTouch(t *testing.T) {
	for _, kind := range []string{"readonly", "external", "external_resize", "executable", "oversized", "invalid_index",
		"readonly_oversized", "readonly_external_resize", "executable_oversized"} {
		t.Run(kind, func(t *testing.T) {
			ctx := serializationTestContext(t, 1)
			tx := ctx.TransactionContext
			ix, err := tx.CurrentInstructionCtx()
			require.NoError(t, err)
			acct, err := ix.BorrowInstructionAccount(tx, 0)
			require.NoError(t, err)
			defer tx.Accounts.Unlock(1)
			data := []byte{9, 8, 7, 6, 5}
			var want error
			// Resize checks run before data-change checks, so a call that fails
			// both reports the resize error.
			switch kind {
			case "readonly":
				ix.InstructionAccounts[0].IsWritable = false
				want = InstrErrReadonlyDataModified
			case "external":
				acct.Account.Owner = [32]byte{77}
				want = InstrErrExternalAccountDataModified
			case "external_resize":
				acct.Account.Owner = [32]byte{77}
				data = nil
				want = InstrErrAccountDataSizeChanged
			case "executable":
				ctx.Features.DisableFeature(features.RemoveAccountsExecutableFlagChecks)
				acct.Account.Executable = true
				want = InstrErrExecutableDataModified
			case "oversized":
				data = make([]byte, MaxPermittedDataLength+1)
				want = InstrErrInvalidRealloc
			case "invalid_index":
				acct.IndexInTransaction = 99
				want = InstrErrNotEnoughAccountKeys
			case "readonly_oversized":
				ix.InstructionAccounts[0].IsWritable = false
				data = make([]byte, MaxPermittedDataLength+1)
				want = InstrErrInvalidRealloc
			case "readonly_external_resize":
				ix.InstructionAccounts[0].IsWritable = false
				acct.Account.Owner = [32]byte{77}
				data = []byte{1}
				want = InstrErrAccountDataSizeChanged
			case "executable_oversized":
				ctx.Features.DisableFeature(features.RemoveAccountsExecutableFlagChecks)
				acct.Account.Executable = true
				data = make([]byte, MaxPermittedDataLength+1)
				want = InstrErrInvalidRealloc
			}
			original, before := acct.Account, acct.Account.Clone()
			tx.Accounts.Shared[1] = true
			tx.AccountsResizeDelta = 123
			tx.Accounts.OnFirstWriteDetach = func(uint64, uint64) { t.Fatal("failed replacement detached") }
			require.ErrorIs(t, acct.SetData(ctx.Features, data), want)
			require.Same(t, original, acct.Account)
			require.Same(t, original, tx.Accounts.Accounts[1])
			require.Equal(t, before, original)
			require.True(t, tx.Accounts.Shared[1])
			require.False(t, tx.Accounts.Touched[1])
			require.EqualValues(t, 123, tx.AccountsResizeDelta)
		})
	}
}

func TestAccountDataReplacementAfterOrdinaryWrite(t *testing.T) {
	ctx := serializationTestContext(t, 1)
	tx := ctx.TransactionContext
	ix, err := tx.CurrentInstructionCtx()
	require.NoError(t, err)
	acct, err := ix.BorrowInstructionAccount(tx, 0)
	require.NoError(t, err)
	defer acct.Drop()
	original := acct.Account
	tx.Accounts.Shared[1] = true
	events := recordFirstWriteDetaches(&tx.Accounts)
	require.NoError(t, acct.SetLamports(2, ctx.Features))
	require.Equal(t, []firstWriteDetach{{5, 5}}, *events)
	require.NoError(t, acct.SetData(ctx.Features, []byte{42}))
	require.Equal(t, []firstWriteDetach{{5, 5}}, *events)
	require.EqualValues(t, 1, original.Lamports)
	require.Equal(t, []byte{1, 2, 3, 4, 5}, original.Data)
}

func TestAccountDataReplacementDoesNotChangeOrdinaryTouch(t *testing.T) {
	original := &accounts.Account{Data: []byte{1, 2, 3}}
	tx := NewTransactionAccountsFromRefs([]*accounts.Account{original}, []bool{true})
	events := recordFirstWriteDetaches(tx)
	clone, err := tx.Touch(0)
	require.NoError(t, err)
	clone.Data[0] = 9
	require.Equal(t, byte(1), original.Data[0])
	require.Equal(t, []firstWriteDetach{{3, 3}}, *events)
}

// An OnFirstWriteClone callback keeps the exact deep-copy sequence: it gets a
// detached copy of the old data, and the resize delta sees its changes.
func TestAccountDataReplacementLegacyCloneCallback(t *testing.T) {
	ctx := serializationTestContext(t, 1)
	tx := ctx.TransactionContext
	ix, err := tx.CurrentInstructionCtx()
	require.NoError(t, err)
	acct, err := ix.BorrowInstructionAccount(tx, 0)
	require.NoError(t, err)
	defer acct.Drop()
	original := acct.Account
	tx.Accounts.Shared[1] = true
	var order []string
	var retained []byte
	tx.Accounts.OnFirstWriteClone = func(clone *accounts.Account) {
		order = append(order, "clone")
		require.NotSame(t, original, clone)
		require.Equal(t, original.Data, clone.Data)
		retained = clone.Data
		clone.Data = clone.Data[:2]
		clone.Lamports = 99
	}
	tx.Accounts.OnFirstWriteDetach = func(oldLen, copied uint64) {
		order = append(order, fmt.Sprintf("detach %d/%d", oldLen, copied))
	}
	replacement := []byte{4, 4, 4, 4, 4, 4}
	require.NoError(t, acct.SetData(ctx.Features, replacement))
	require.Equal(t, []string{"clone", "detach 5/5"}, order)
	require.EqualValues(t, 6-2, tx.AccountsResizeDelta, "delta uses the length after the callback, as before")
	require.EqualValues(t, 99, acct.Lamports())
	require.Same(t, &replacement[0], &acct.Data()[0])
	require.Equal(t, []byte{1, 2, 3, 4, 5}, retained)
	require.Equal(t, []byte{1, 2, 3, 4, 5}, original.Data)
	require.NotSame(t, &retained[0], &original.Data[0])
}

// replacementRun records what a scenario left behind, so the legacy deep-copy
// path and the header-only path can be compared field for field.
type replacementRun struct {
	err      string
	accounts []accounts.Account
	shared   []bool
	touched  []bool
	delta    int64
	events   []firstWriteDetach
}

// runReplacementScenario runs scenario on a context whose instruction accounts
// are shared parent accounts. With legacy set it installs OnFirstWriteClone,
// which forces SetData through the deep-copying Touch.
func runReplacementScenario(t *testing.T, ctx *ExecutionCtx, legacy bool, scenario func() error) replacementRun {
	t.Helper()
	tx := ctx.TransactionContext
	parents := make([]*accounts.Account, len(tx.Accounts.Accounts))
	parentCopies := make([]*accounts.Account, len(tx.Accounts.Accounts))
	for i := 1; i < len(tx.Accounts.Accounts); i++ {
		tx.Accounts.Shared[i] = true
		parents[i] = tx.Accounts.Accounts[i]
		parentCopies[i] = parents[i].Clone()
	}
	if legacy {
		tx.Accounts.OnFirstWriteClone = func(*accounts.Account) {}
	}
	events := recordFirstWriteDetaches(&tx.Accounts)
	var run replacementRun
	if err := scenario(); err != nil {
		run.err = err.Error()
	}
	for i := 1; i < len(parents); i++ {
		require.Equal(t, parentCopies[i], parents[i], "shared parent account %d changed", i)
	}
	for _, acct := range tx.Accounts.Accounts {
		run.accounts = append(run.accounts, *acct.Clone())
	}
	run.shared = append(run.shared, tx.Accounts.Shared...)
	run.touched = append(run.touched, tx.Accounts.Touched...)
	run.delta = tx.AccountsResizeDelta
	run.events = *events
	return run
}

// requireSameReplacement checks that both paths produce identical accounts,
// flags, deltas and errors, and that they detached the same accounts. The
// legacy path copies all old data; the replacement path copies none of it
// when SetData is the account's first write.
func requireSameReplacement(t *testing.T, legacy, replaced replacementRun, wantCopied []uint64) {
	t.Helper()
	require.Equal(t, legacy.err, replaced.err)
	require.Equal(t, legacy.accounts, replaced.accounts)
	require.Equal(t, legacy.shared, replaced.shared)
	require.Equal(t, legacy.touched, replaced.touched)
	require.Equal(t, legacy.delta, replaced.delta)
	require.Len(t, replaced.events, len(legacy.events))
	copied := make([]uint64, len(replaced.events))
	for i, event := range replaced.events {
		require.Equal(t, legacy.events[i].oldLen, event.oldLen)
		require.Equal(t, legacy.events[i].oldLen, legacy.events[i].copied)
		copied[i] = event.copied
	}
	require.Equal(t, wantCopied, copied)
}

// Serialized input offsets of one instruction account's lamports, data length
// and data, for the aligned and deprecated unaligned loader layouts.
type serializedAccountOffsets struct{ lamports, dataLen, data uint64 }

func serializedOffsets(aligned bool, metadatas []serializedAcctMetadata, dupes []bool) []serializedAccountOffsets {
	offsets := make([]serializedAccountOffsets, len(metadatas))
	off := uint64(8)
	for i, meta := range metadatas {
		preLen := meta.originalDataLen
		switch {
		case dupes[i] && aligned:
			off += 8
		case dupes[i]:
			off++
		case aligned:
			offsets[i] = serializedAccountOffsets{off + 72, off + 80, off + 88}
			off += 88 + preLen + MaxPermittedDataIncrease + (-preLen)%8 + 8
		default:
			offsets[i] = serializedAccountOffsets{off + 35, off + 43, off + 51}
			off += 51 + preLen + 32 + 1 + 8
		}
	}
	return offsets
}

// Every loader branch that calls SetData (aligned and unaligned, with and
// without VASA, direct mapping off) yields the legacy result.
func TestAccountDataReplacementLoaderDeserialization(t *testing.T) {
	type scenario struct {
		name        string
		alignedOnly bool
		readonly    bool
		lamports    uint64 // account 0's new lamports, if not zero
		dataLen     uint64 // account 0's new length, if not zero
		data0       byte   // fill for account 0's data, if not zero
		data1       byte   // fill for account 1's data, if not zero
		wantCopied  []uint64
		wantErr     bool
	}
	scenarios := []scenario{
		{name: "unchanged", wantCopied: []uint64{0, 0}},
		{name: "rewrite", data0: 9, data1: 8, wantCopied: []uint64{0, 0}},
		{name: "lamports_first", lamports: 2, data0: 9, wantCopied: []uint64{5, 0}},
		{name: "grow", alignedOnly: true, dataLen: 8, data0: 9, wantCopied: []uint64{0, 0}},
		{name: "shrink", alignedOnly: true, dataLen: 2, wantCopied: []uint64{0, 0}},
		{name: "readonly_unchanged", readonly: true, wantCopied: []uint64{0}},
		{name: "readonly_changed", readonly: true, data1: 8, wantCopied: []uint64{0}},
		{name: "invalid_realloc", alignedOnly: true, dataLen: 5 + MaxPermittedDataIncrease + 1, wantCopied: []uint64{}, wantErr: true},
	}
	for _, layout := range []string{"aligned", "unaligned"} {
		for _, vasa := range []bool{false, true} {
			for _, sc := range scenarios {
				aligned := layout == "aligned"
				if sc.alignedOnly && !aligned {
					continue
				}
				t.Run(fmt.Sprintf("%s/vasa=%t/%s", layout, vasa, sc.name), func(t *testing.T) {
					var runs [2]replacementRun
					for i, legacy := range []bool{true, false} {
						ctx := serializationTestContext(t, 2)
						if vasa {
							ctx.Features.EnableFeature(features.VirtualAddressSpaceAdjustments, 0)
						}
						ix, err := ctx.TransactionContext.CurrentInstructionCtx()
						require.NoError(t, err)
						if sc.readonly {
							ix.InstructionAccounts[1].IsWritable = false
						}
						serialize, deserialize := serializeParametersUnaligned, deserializeParametersUnaligned
						if aligned {
							serialize, deserialize = serializeParametersAligned, deserializeParametersAligned
						}
						params, _, metadatas, _, err := serialize(ctx)
						require.NoError(t, err)
						offsets := serializedOffsets(aligned, metadatas, []bool{false, false, true})
						if sc.lamports != 0 {
							binary.LittleEndian.PutUint64(params[offsets[0].lamports:], sc.lamports)
						}
						if sc.dataLen != 0 {
							binary.LittleEndian.PutUint64(params[offsets[0].dataLen:], sc.dataLen)
						}
						fill := func(acct int, v byte, n uint64) {
							for j := range n {
								params[offsets[acct].data+j] = v
							}
						}
						if sc.data0 != 0 {
							fill(0, sc.data0, max(sc.dataLen, 5))
						}
						if sc.data1 != 0 {
							fill(1, sc.data1, 5)
						}
						runs[i] = runReplacementScenario(t, ctx, legacy, func() error {
							return deserialize(ctx, params, metadatas)
						})
					}
					wantErr := sc.wantErr || (sc.name == "readonly_changed" && !vasa)
					require.Equal(t, wantErr, runs[0].err != "", runs[0].err)
					requireSameReplacement(t, runs[0], runs[1], sc.wantCopied)
				})
			}
		}
	}
}

// Direct mapping resizes through SetDataLength, which still deep-copies.
func TestAccountDataReplacementDirectMappingUsesOrdinaryTouch(t *testing.T) {
	ctx := serializationTestContext(t, 2)
	ctx.Features.EnableFeature(features.VirtualAddressSpaceAdjustments, 0)
	ctx.Features.EnableFeature(features.AccountDataDirectMapping, 0)
	params, _, metadatas, _, err := serializeParametersAligned(ctx)
	require.NoError(t, err)
	// Direct mapping serializes an 8-byte placeholder in place of the data.
	binary.LittleEndian.PutUint64(params[8+80:], 7)
	run := runReplacementScenario(t, ctx, false, func() error {
		return deserializeParametersAligned(ctx, params, metadatas)
	})
	require.Empty(t, run.err)
	require.Equal(t, []firstWriteDetach{{5, 5}}, run.events)
	require.Len(t, run.accounts[1].Data, 7)
}

// cpiReplacementVM serializes ctx's instruction as the loader would and writes
// the caller's account infos for both instruction accounts into the heap.
func cpiReplacementVM(t *testing.T, ctx *ExecutionCtx, rust bool) (*sbpf.Interpreter, []serializedAcctMetadata, func(uint64) uint64) {
	t.Helper()
	params, _, metadatas, regions, err := serializeParametersAligned(ctx)
	require.NoError(t, err)
	ctx.serializedAccountMetadataStack = append(ctx.serializedAccountMetadataStack, metadatas)
	ctx.ComputeMeter = cu.NewComputeMeter(1_000_000)
	vm := sbpf.NewInterpreter(&sbpf.Program{TextVA: sbpf.VaddrProgram, Funcs: map[uint32]int64{}}, &sbpf.VMOpts{
		HeapMax: 32 * 1024, Input: params, InputRegions: regions, Context: ctx, ComputeMeter: &ctx.ComputeMeter,
	})
	t.Cleanup(vm.Finish)

	const infos = 2
	heap, err := vm.Translate(sbpf.VaddrHeap, 1024, true)
	require.NoError(t, err)
	put := func(off, v uint64) { binary.LittleEndian.PutUint64(heap[off:], v) }
	infoSize := uint64(SolAccountInfoCSize)
	if rust {
		infoSize = SolAccountInfoRustSize
	}
	boxes := infos * infoSize
	for i := range uint64(infos) {
		meta := metadatas[i]
		info := i * infoSize
		if rust {
			lamportsBox := boxes + i*(RefCellRustSize+RefCellVecRustSize)
			dataBox := lamportsBox + RefCellRustSize
			put(info, meta.vmKeyAddr)
			put(info+8, sbpf.VaddrHeap+lamportsBox)
			put(info+16, sbpf.VaddrHeap+dataBox)
			put(info+24, meta.vmOwnerAddr)
			heap[info+41] = 1 // writable
			put(lamportsBox+24, meta.vmLamportsAddr)
			put(dataBox+24, meta.vmDataAddr)
			put(dataBox+32, meta.originalDataLen)
		} else {
			put(info, meta.vmKeyAddr)
			put(info+8, meta.vmLamportsAddr)
			put(info+16, meta.originalDataLen)
			put(info+24, meta.vmDataAddr)
			put(info+32, meta.vmOwnerAddr)
			heap[info+49] = 1 // writable
		}
	}
	// The caller's data length, which the callee update reads.
	dataLenAddr := func(i uint64) uint64 {
		if rust {
			return sbpf.VaddrHeap + boxes + i*(RefCellRustSize+RefCellVecRustSize) + RefCellRustSize + solAccountInfoRustDataLenOffset
		}
		return sbpf.VaddrHeap + i*infoSize + solAccountInfoCDataLenOffset
	}
	return vm, metadatas, dataLenAddr
}

// Both CPI ABIs, with and without VASA, give the callee the legacy result, and
// the callee keeps aliasing the caller's serialized data.
func TestAccountDataReplacementCPI(t *testing.T) {
	type scenario struct {
		name       string
		readonly   bool
		preTouch   bool
		lamports   uint64
		dataLen    uint64
		owner      byte
		fill       byte
		wantCopied map[bool][]uint64 // by VASA
		wantErr    map[bool]bool
	}
	same := func(c []uint64) map[bool][]uint64 { return map[bool][]uint64{false: c, true: c} }
	scenarios := []scenario{
		{name: "unchanged", wantCopied: same([]uint64{0, 0})},
		{name: "rewrite", fill: 9, wantCopied: same([]uint64{0, 0})},
		{name: "grow", dataLen: 8, fill: 9, wantCopied: map[bool][]uint64{false: {0, 0}, true: {5, 0}}},
		{name: "shrink", dataLen: 2, wantCopied: map[bool][]uint64{false: {0, 0}, true: {5, 0}}},
		{name: "lamports_first", lamports: 2, fill: 9, wantCopied: same([]uint64{5, 0})},
		{name: "already_touched", preTouch: true, fill: 9, wantCopied: same([]uint64{5, 0})},
		{name: "owner_change", owner: 7, fill: 0xff, wantCopied: same([]uint64{0, 0})},
		{name: "readonly_unchanged", readonly: true, wantCopied: same([]uint64{0})},
		{name: "readonly_changed", readonly: true, fill: 9, wantCopied: same([]uint64{0}),
			wantErr: map[bool]bool{false: true}},
	}
	for _, abi := range []string{"c", "rust"} {
		for _, vasa := range []bool{false, true} {
			for _, sc := range scenarios {
				t.Run(fmt.Sprintf("%s/vasa=%t/%s", abi, vasa, sc.name), func(t *testing.T) {
					var runs [2]replacementRun
					for i, legacy := range []bool{true, false} {
						ctx := serializationTestContext(t, 2)
						if vasa {
							ctx.Features.EnableFeature(features.VirtualAddressSpaceAdjustments, 0)
						}
						tx := ctx.TransactionContext
						ix, err := tx.CurrentInstructionCtx()
						require.NoError(t, err)
						// The account written in the readonly scenarios is the
						// second, so its error follows the first's replacement.
						target := uint64(0)
						if sc.readonly {
							target = 1
							ix.InstructionAccounts[1].IsWritable = false
						}
						vm, metadatas, dataLenAddr := cpiReplacementVM(t, ctx, abi == "rust")
						write := func(addr uint64, b []byte) {
							mem, err := vm.Translate(addr, uint64(len(b)), true)
							require.NoError(t, err)
							copy(mem, b)
						}
						meta := metadatas[target]
						newLen := uint64(5)
						if sc.dataLen != 0 {
							newLen = sc.dataLen
							lenBytes := binary.LittleEndian.AppendUint64(nil, newLen)
							write(dataLenAddr(target), lenBytes)
						}
						if sc.lamports != 0 {
							write(meta.vmLamportsAddr, binary.LittleEndian.AppendUint64(nil, sc.lamports))
						}
						if sc.owner != 0 {
							write(meta.vmOwnerAddr, bytes.Repeat([]byte{sc.owner}, 32))
						}
						if sc.fill != 0 {
							data, err := translateSerializedAccountData(vm, ctx, meta.vmDataAddr, newLen)
							require.NoError(t, err)
							fill := sc.fill
							if sc.owner != 0 {
								fill = 0 // an owner change needs zeroed data
							}
							for j := range data {
								data[j] = fill
							}
						}
						instructionAccts := []InstructionAccount{
							{IndexInTransaction: 1, IndexInCaller: 0, IndexInCallee: 0, IsWritable: !sc.readonly || target != 0},
							{IndexInTransaction: 2, IndexInCaller: 1, IndexInCallee: 1, IsWritable: !sc.readonly},
						}
						runs[i] = runReplacementScenario(t, ctx, legacy, func() error {
							if sc.preTouch {
								if _, err := tx.Accounts.Touch(1); err != nil {
									return err
								}
							}
							if abi == "rust" {
								infos, keys, err := translateAccountInfosRust(vm, sbpf.VaddrHeap, 2)
								if err != nil {
									return err
								}
								_, err = translateAndUpdateAccountsRust(vm, instructionAccts, []uint64{0}, keys, infos, sbpf.VaddrHeap, false)
								return err
							}
							infos, keys, err := translateAccountInfosC(vm, sbpf.VaddrHeap, 2)
							if err != nil {
								return err
							}
							_, err = translateAndUpdateAccountsC(vm, instructionAccts, []uint64{0}, keys, infos, sbpf.VaddrHeap, false)
							return err
						})
						if runs[i].err == "" && !sc.readonly {
							// The callee's data is a live view of the caller's
							// serialized account data.
							write(meta.vmDataAddr, []byte{0x5a})
							require.Equal(t, byte(0x5a), tx.Accounts.Accounts[1].Data[0])
						}
					}
					require.Equal(t, sc.wantErr[vasa], runs[0].err != "", runs[0].err)
					requireSameReplacement(t, runs[0], runs[1], sc.wantCopied[vasa])
				})
			}
		}
	}
}
