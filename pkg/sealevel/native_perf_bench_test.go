package sealevel

import (
	"encoding/binary"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/gagliardetto/solana-go"
)

// Micro-benchmarks for the natively implemented programs that are still on the
// mainnet hot path (System transfer, Vote TowerSync), measured through the same
// ExecutionCtx.ProcessInstruction entry point replay uses, so the per-instruction
// plumbing (instruction context push/pop, lamport-sum checks, timing metrics)
// is included. Run with:
//
//	go test ./pkg/sealevel/ -run XXX -bench 'Native|VoteState|Timing' -benchmem -cpu 1 -count 5

func benchPubkey(b byte) solana.PublicKey {
	var pk solana.PublicKey
	for i := range pk {
		pk[i] = b
	}
	return pk
}

// newBenchExecCtx mirrors newSystemProgramTestExecCtx without testing.T.
func newBenchExecCtx(txAccts *TransactionAccounts, clockSlot uint64, enabled ...features.FeatureGate) *ExecutionCtx {
	txCtx := NewTransactionCtx(*txAccts, 5, 64)
	execCtx := &ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeter(1 << 62)}
	execCtx.Accounts = accounts.NewMemAccounts()

	clockAcct := accounts.Account{Lamports: 1}
	if err := execCtx.Accounts.SetAccount(&SysvarClockAddr, &clockAcct); err != nil {
		panic(err)
	}
	WriteClockSysvar(&execCtx.Accounts, SysvarClock{Slot: clockSlot, Epoch: 0})

	rentAcct := accounts.Account{Lamports: 1}
	if err := execCtx.Accounts.SetAccount(&SysvarRentAddr, &rentAcct); err != nil {
		panic(err)
	}
	WriteRentSysvar(&execCtx.Accounts, SysvarRent{LamportsPerUint8Year: 3480, ExemptionThreshold: 2, BurnPercent: 50})

	f := features.NewFeaturesDefault()
	for _, gate := range enabled {
		f.EnableFeature(gate, 0)
	}
	execCtx.Features = *f
	return execCtx
}

// resetTxCtx gives the execution context a fresh instruction trace/stack for
// the next instruction (each ProcessInstruction consumes one trace slot).
func resetTxCtx(execCtx *ExecutionCtx, txAccts *TransactionAccounts) {
	execCtx.TransactionContext = NewTransactionCtx(*txAccts, 5, 64)
}

// ---------------------------------------------------------------- System

func encodeSystemTransfer(lamports uint64) []byte {
	out := binary.LittleEndian.AppendUint32(nil, uint32(SystemProgramInstrTypeTransfer))
	return binary.LittleEndian.AppendUint64(out, lamports)
}

func benchmarkSystemTransfer(b *testing.B, skipTiming bool) {
	systemProgramAcct := accounts.Account{Key: a.SystemProgramAddr, Lamports: 1, Data: []byte{}, Owner: a.NativeLoaderAddr, Executable: true}
	from := accounts.Account{Key: benchPubkey(0x11), Lamports: 1 << 60, Data: []byte{}, Owner: a.SystemProgramAddr}
	to := accounts.Account{Key: benchPubkey(0x22), Lamports: 1_000_000, Data: []byte{}, Owner: a.SystemProgramAddr}
	txAccts := NewTransactionAccounts([]accounts.Account{systemProgramAcct, from, to})
	metas := []AccountMeta{
		{Pubkey: from.Key, IsSigner: true, IsWritable: true},
		{Pubkey: to.Key, IsSigner: false, IsWritable: true},
	}
	instrAccts := InstructionAcctsFromAccountMetas(metas, *txAccts)
	instr := encodeSystemTransfer(1)

	execCtx := newBenchExecCtx(txAccts, 1234)
	execCtx.SkipTimingMetrics = skipTiming

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resetTxCtx(execCtx, txAccts)
		if err := execCtx.ProcessInstruction(instr, instrAccts, []uint64{0}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if got := txAccts.Accounts[2].Lamports; got != 1_000_000+uint64(b.N) {
		b.Fatalf("destination lamports %d, want %d", got, 1_000_000+uint64(b.N))
	}
}

func BenchmarkNativeSystemTransfer(b *testing.B)         { benchmarkSystemTransfer(b, false) }
func BenchmarkNativeSystemTransferNoTiming(b *testing.B) { benchmarkSystemTransfer(b, true) }

// ---------------------------------------------------------------- Vote

const (
	benchVoteRoot     = uint64(1000)
	benchVoteLastSlot = benchVoteRoot + MaxLockoutHistory // 1031
	benchClockSlot    = benchVoteLastSlot + 2
)

func benchSlotHash(slot uint64) [32]byte {
	var h [32]byte
	binary.LittleEndian.PutUint64(h[:], slot*0x9e3779b97f4a7c15)
	binary.LittleEndian.PutUint64(h[8:], ^slot)
	h[31] = 0xa5
	return h
}

// benchSlotHashes builds a 512-entry SlotHashes sysvar (newest first) that
// covers every slot the benchmark tower refers to.
func benchSlotHashes() SysvarSlotHashes {
	const n = 512
	newest := benchVoteLastSlot + 1
	sh := make(SysvarSlotHashes, 0, n)
	for i := uint64(0); i < n; i++ {
		s := newest - i
		sh = append(sh, SlotHash{Slot: s, Hash: benchSlotHash(s)})
	}
	return sh
}

// benchInitialVoteState is a fully populated current-version vote state with a
// full 31-entry tower ending at benchVoteLastSlot.
func benchInitialVoteState(voter solana.PublicKey) *VoteState {
	vs := &VoteState{
		NodePubkey:           voter,
		AuthorizedWithdrawer: voter,
		Commission:           10,
		PriorVoters:          PriorVoters{Index: 31, IsEmpty: true},
		EpochCredits:         []EpochCredits{{Epoch: 0, Credits: 1000, PrevCredits: 0}},
		LastTimestamp:        BlockTimestamp{Slot: benchVoteLastSlot, Timestamp: 1_700_000_000},
	}
	vs.AuthorizedVoters.AuthorizedVoters.Set(0, voter)
	root := benchVoteRoot
	vs.RootSlot = &root
	for i := uint64(0); i < MaxLockoutHistory; i++ {
		vs.Votes.PushBack(LandedVote{
			Latency: 1,
			Lockout: VoteLockout{Slot: benchVoteRoot + 1 + i, ConfirmationCount: uint32(MaxLockoutHistory - i)},
		})
	}
	return vs
}

func benchSerializedVoteState(vs *VoteState) []byte {
	versioned := &VoteStateVersions{Type: VoteStateVersionCurrent, Current: *vs}
	data := make([]byte, VoteStateV3Size)
	if err := WriteVersionedVoteStateInPlace(data, versioned); err != nil {
		panic(err)
	}
	return data
}

// encodeTowerSync encodes a TowerSync that advances the tower by one slot:
// root = old root + 1, lockouts = old lockouts shifted by one slot plus the
// new slot, i.e. exactly what a validator sends every slot.
func encodeTowerSync() []byte {
	root := benchVoteRoot + 1
	out := binary.LittleEndian.AppendUint32(nil, uint32(VoteProgramInstrTypeTowerSync))
	out = binary.LittleEndian.AppendUint64(out, root)
	out = append(out, byte(MaxLockoutHistory)) // compact-u16, < 0x80
	prev := root
	for i := uint64(0); i < MaxLockoutHistory; i++ {
		slot := root + 1 + i
		out = binary.AppendUvarint(out, slot-prev)
		out = append(out, byte(MaxLockoutHistory-i))
		prev = slot
	}
	last := root + MaxLockoutHistory // benchVoteLastSlot + 1
	h := benchSlotHash(last)
	out = append(out, h[:]...)
	out = append(out, 1) // Some(timestamp)
	out = binary.LittleEndian.AppendUint64(out, uint64(1_700_000_001))
	var blockID [32]byte
	out = append(out, blockID[:]...)
	return out
}

type voteBench struct {
	execCtx    *ExecutionCtx
	txAccts    *TransactionAccounts
	instr      []byte
	instrAccts []InstructionAccount
	initial    []byte
}

func newVoteBench(skipTiming bool) *voteBench {
	voter := benchPubkey(0x33)
	votePk := benchPubkey(0x44)
	initial := benchSerializedVoteState(benchInitialVoteState(voter))

	voteProgramAcct := accounts.Account{Key: a.VoteProgramAddr, Lamports: 1, Data: []byte{}, Owner: a.NativeLoaderAddr, Executable: true}
	voteAcct := accounts.Account{Key: votePk, Lamports: 1_000_000_000, Data: append([]byte(nil), initial...), Owner: a.VoteProgramAddr}
	voterAcct := accounts.Account{Key: voter, Lamports: 1_000_000_000, Data: []byte{}, Owner: a.SystemProgramAddr}
	txAccts := NewTransactionAccounts([]accounts.Account{voteProgramAcct, voteAcct, voterAcct})
	metas := []AccountMeta{
		{Pubkey: votePk, IsSigner: false, IsWritable: true},
		{Pubkey: voter, IsSigner: true, IsWritable: false},
	}
	instrAccts := InstructionAcctsFromAccountMetas(metas, *txAccts)

	execCtx := newBenchExecCtx(txAccts, benchClockSlot,
		features.EnableTowerSyncIx,
		features.VoteStateAddVoteLatency,
		features.TimelyVoteCredits,
		features.DeprecateUnusedLegacyVotePlumbing,
	)
	execCtx.SkipTimingMetrics = skipTiming
	shAcct := accounts.Account{Lamports: 1}
	if err := execCtx.Accounts.SetAccount(&SysvarSlotHashesAddr, &shAcct); err != nil {
		panic(err)
	}
	WriteSlotHashesSysvar(&execCtx.Accounts, benchSlotHashes())

	return &voteBench{execCtx: execCtx, txAccts: txAccts, instr: encodeTowerSync(), instrAccts: instrAccts, initial: initial}
}

// step runs one TowerSync against the initial vote state (the account data is
// rewound to the initial state first so every iteration performs identical work).
func (vb *voteBench) step() error {
	copy(vb.txAccts.Accounts[1].Data, vb.initial)
	resetTxCtx(vb.execCtx, vb.txAccts)
	return vb.execCtx.ProcessInstruction(vb.instr, vb.instrAccts, []uint64{0})
}

func TestNativeVoteTowerSyncBenchSetup(t *testing.T) {
	vb := newVoteBench(false)
	if err := vb.step(); err != nil {
		t.Fatalf("TowerSync failed: %v", err)
	}
	versioned, err := UnmarshalVersionedVoteState(vb.txAccts.Accounts[1].Data)
	if err != nil {
		t.Fatal(err)
	}
	vs := versioned.ConvertToCurrent()
	if vs.Votes.Len() != MaxLockoutHistory {
		t.Fatalf("tower length %d, want %d", vs.Votes.Len(), MaxLockoutHistory)
	}
	if last := vs.Votes.Back().Lockout.Slot; last != benchVoteLastSlot+1 {
		t.Fatalf("last voted slot %d, want %d", last, benchVoteLastSlot+1)
	}
	if vs.RootSlot == nil || *vs.RootSlot != benchVoteRoot+1 {
		t.Fatalf("root %v, want %d", vs.RootSlot, benchVoteRoot+1)
	}
}

func benchmarkVoteTowerSync(b *testing.B, skipTiming bool) {
	vb := newVoteBench(skipTiming)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := vb.step(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNativeVoteTowerSync(b *testing.B)         { benchmarkVoteTowerSync(b, false) }
func BenchmarkNativeVoteTowerSyncNoTiming(b *testing.B) { benchmarkVoteTowerSync(b, true) }

// BenchmarkVoteStateRoundTrip isolates vote-state (de)serialization: decode
// the account, convert to current, re-encode — the fixed cost of every vote.
func BenchmarkVoteStateRoundTrip(b *testing.B) {
	data := benchSerializedVoteState(benchInitialVoteState(benchPubkey(0x33)))
	out := make([]byte, VoteStateV3Size)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		versioned, err := UnmarshalVersionedVoteState(data)
		if err != nil {
			b.Fatal(err)
		}
		vs := versioned.ConvertToCurrent()
		cur := &VoteStateVersions{Type: VoteStateVersionCurrent, Current: *vs}
		if err := WriteVersionedVoteStateInPlace(out, cur); err != nil {
			b.Fatal(err)
		}
	}
}
