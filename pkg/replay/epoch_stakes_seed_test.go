package replay

import (
	"encoding/json"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestStartupEpochStakeSyscallUsesCurrentEffectiveStakes(t *testing.T) {
	// Testnet slot 446988313 copied the total into a program account. Seeding
	// execution from key E returned the previous total and diverged despite
	// identical transaction outcomes, fees and compute units.
	const previousTotal = uint64(361_336_206_088_827_916)
	const currentTotal = uint64(383_027_894_827_581_045)
	const epoch = uint64(91_001)
	shared, previousOnly, currentOnly := solana.PublicKey{1}, solana.PublicKey{2}, solana.PublicKey{3}
	encode := func(key, total uint64, stakes map[string]uint64) string {
		data, err := json.Marshal(epochstakes.PersistedEpochStakes{Epoch: key, TotalStake: total, Stakes: stakes})
		require.NoError(t, err)
		return string(data)
	}
	seeds := map[uint64]string{
		epoch:     encode(epoch, previousTotal, map[string]uint64{shared.String(): 10, previousOnly.String(): previousTotal - 10}),
		epoch + 1: encode(epoch+1, currentTotal, map[string]uint64{shared.String(): 20, currentOnly.String(): currentTotal - 20}),
	}
	for _, mode := range []string{"snapshot", "same epoch resume", "resume after epoch boundary"} {
		t.Run(mode, func(t *testing.T) {
			for _, key := range []uint64{epoch, epoch + 1} {
				global.ClearEpochStakes(key)
				t.Cleanup(func() { global.ClearEpochStakes(key) })
			}
			ms := &state.MithrilState{ManifestEpochStakes: seeds}
			var resume *ResumeState
			snapshotEpoch := epoch
			if mode == "same epoch resume" {
				resume = &ResumeState{}
			} else if mode == "resume after epoch boundary" {
				ms.ManifestEpochStakes = nil
				snapshotEpoch--
				resume = &ResumeState{ComputedEpochStakes: map[uint64][]byte{
					epoch: []byte(seeds[epoch]), epoch + 1: []byte(seeds[epoch+1]),
				}}
			}
			require.NoError(t, LoadInitialEpochStakesCache(ms, resume, epoch, snapshotEpoch))
			block := &b.Block{Epoch: epoch}
			require.NoError(t, seedEpochStakesForExecution(block, block.Epoch))
			slotCtx := newSlotCtx(block, accounts.NewMemAccounts(), accounts.NewMemAccounts(), nil, nil, 0)
			execCtx := &sealevel.ExecutionCtx{SlotCtx: slotCtx, ComputeMeter: cu.NewComputeMeter(10_000)}
			input := append(append(append([]byte(nil), shared[:]...), previousOnly[:]...), currentOnly[:]...)
			vm := sbpf.NewInterpreter(&sbpf.Program{TextVA: sbpf.VaddrProgram, Funcs: map[uint32]int64{}}, &sbpf.VMOpts{
				Input: input, Context: execCtx, ComputeMeter: &execCtx.ComputeMeter,
			})
			t.Cleanup(vm.Finish)
			for _, query := range []struct{ address, want uint64 }{
				{0, currentTotal},
				{sbpf.VaddrInput, 20},
				{sbpf.VaddrInput + 32, 0},
				{sbpf.VaddrInput + 64, currentTotal - 20},
			} {
				got, err := sealevel.SyscallGetEpochStakeImpl(vm, query.address)
				require.NoError(t, err)
				require.Equal(t, query.want, got)
			}
			// Leader/consensus stakes retain the preceding generation under E.
			require.Equal(t, previousTotal, global.EpochTotalStake(epoch))
			require.Equal(t, uint64(10), global.EpochStakes(epoch)[shared])
		})
	}
}

func TestStartupEpochStakesRejectMissingCurrentGeneration(t *testing.T) {
	const epoch = uint64(91_003)
	global.PutEpochStakes(epoch, map[solana.PublicKey]uint64{{1}: 100}, nil, 100)
	global.ClearEpochStakes(epoch + 1)
	t.Cleanup(func() { global.ClearEpochStakes(epoch) })
	block := &b.Block{Epoch: epoch}
	require.ErrorContains(t, seedEpochStakesForExecution(block, block.Epoch), "cache key 91004")
	require.Nil(t, block.EpochStakesPerVoteAcct, "must not substitute the previous generation")
}

func TestPrepareManifestEpochStakesForRuntimeRepairsPreviouslyRebasedDevnetFrame(t *testing.T) {
	const (
		parentSlot    = 463538376
		snapshotEpoch = 1073
		currentEpoch  = 1073
	)

	mithrilState := &state.MithrilState{
		ManifestParentSlot:  parentSlot,
		ManifestEpochStakes: make(map[uint64]string),
	}
	for _, epoch := range []uint64{56581, 56582, 56583, 56584, 56585} {
		mithrilState.ManifestEpochStakes[epoch] = persistedEpochStakeJSON(t, epoch)
	}

	seeds, rebased, err := prepareManifestEpochStakesForRuntime(mithrilState, currentEpoch, snapshotEpoch)
	if err != nil {
		t.Fatalf("prepareManifestEpochStakesForRuntime returned error: %v", err)
	}
	if !rebased {
		t.Fatalf("expected manifest epoch stakes to be rebased")
	}

	wantEpochs := []uint64{1070, 1071, 1072, 1073, 1074}
	if len(seeds) != len(wantEpochs) {
		t.Fatalf("expected %d seeds, got %d", len(wantEpochs), len(seeds))
	}
	for i, wantEpoch := range wantEpochs {
		if seeds[i].runtimeEpoch != wantEpoch {
			t.Fatalf("seed %d runtime epoch = %d, want %d", i, seeds[i].runtimeEpoch, wantEpoch)
		}
		var persisted epochstakes.PersistedEpochStakes
		if err := json.Unmarshal(seeds[i].data, &persisted); err != nil {
			t.Fatalf("failed to decode seed %d: %v", i, err)
		}
		if persisted.Epoch != wantEpoch {
			t.Fatalf("seed %d payload epoch = %d, want %d", i, persisted.Epoch, wantEpoch)
		}
	}
}

func TestPrepareManifestEpochStakesForRuntimeKeepsRuntimeFrame(t *testing.T) {
	mithrilState := &state.MithrilState{
		ManifestParentSlot: 463538376,
		ManifestEpochStakes: map[uint64]string{
			1073: persistedEpochStakeJSON(t, 1073),
			1074: persistedEpochStakeJSON(t, 1074),
		},
	}

	seeds, rebased, err := prepareManifestEpochStakesForRuntime(mithrilState, 1073, 1073)
	if err != nil {
		t.Fatalf("prepareManifestEpochStakesForRuntime returned error: %v", err)
	}
	if rebased {
		t.Fatalf("did not expect manifest epoch stakes to be rebased")
	}
	if len(seeds) != 2 || seeds[0].runtimeEpoch != 1073 || seeds[1].runtimeEpoch != 1074 {
		t.Fatalf("unexpected seeds: %#v", seeds)
	}
}

func TestLoadInitialEpochStakesCacheUsesManifestOnSameEpochResume(t *testing.T) {
	const epoch = uint64(84001)
	global.ClearEpochStakes(epoch)
	t.Cleanup(func() { global.ClearEpochStakes(epoch) })

	mithrilState := &state.MithrilState{
		ManifestEpochStakes: map[uint64]string{epoch: persistedEpochStakeJSON(t, epoch)},
	}
	restart := &ResumeState{}
	requireNoError(t, LoadInitialEpochStakesCache(mithrilState, restart, epoch, epoch))
	if !global.HasEpochStakes(epoch) {
		t.Fatalf("same-epoch resume did not load manifest stakes for epoch %d", epoch)
	}
}

func TestLoadInitialEpochStakesCacheRequiresComputedStakesAfterBoundary(t *testing.T) {
	err := LoadInitialEpochStakesCache(&state.MithrilState{}, &ResumeState{}, 84002, 84001)
	if err == nil {
		t.Fatal("cross-epoch resume without computed stakes unexpectedly succeeded")
	}
}

func TestLoadInitialEpochStakesCacheUsesPersistedStakesAfterBoundary(t *testing.T) {
	const epoch = uint64(84003)
	global.ClearEpochStakes(epoch)
	t.Cleanup(func() { global.ClearEpochStakes(epoch) })

	restart := &ResumeState{ComputedEpochStakes: map[uint64][]byte{
		epoch: []byte(persistedEpochStakeJSON(t, epoch)),
	}}
	requireNoError(t, LoadInitialEpochStakesCache(&state.MithrilState{}, restart, epoch, epoch-1))
	if !global.HasEpochStakes(epoch) {
		t.Fatalf("cross-epoch resume did not load persisted stakes for epoch %d", epoch)
	}
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func persistedEpochStakeJSON(t *testing.T, epoch uint64) string {
	t.Helper()

	data, err := json.Marshal(epochstakes.PersistedEpochStakes{
		Epoch:      epoch,
		TotalStake: 42,
		Stakes:     map[string]uint64{},
		VoteAccts:  map[string]*epochstakes.VoteAccountJSON{},
	})
	if err != nil {
		t.Fatalf("failed to marshal epoch stakes: %v", err)
	}
	return string(data)
}
