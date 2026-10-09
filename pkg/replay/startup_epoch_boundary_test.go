package replay

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestStartupAcrossEpochBoundary(t *testing.T) {
	// Startup can call os.Exit, and its caches are process-wide. Exercise the
	// real configuration and transition paths in isolated processes so an exit
	// is reported as a failed case rather than terminating the whole suite.
	const caseEnv = "MITHRIL_TEST_STARTUP_EPOCH_CASE"
	if selected := os.Getenv(caseEnv); selected != "" {
		var mode string
		var offset uint64
		_, err := fmt.Sscanf(selected, "%s %d", &mode, &offset)
		require.NoError(t, err)
		testStartupAcrossEpochBoundary(t, mode, offset)
		return
	}
	for _, mode := range []string{"snapshot", "resume", "persisted", "cached"} {
		for _, offset := range []uint64{0, 3} {
			t.Run(fmt.Sprintf("%s/skipped_%d", mode, offset), func(t *testing.T) {
				cmd := exec.Command(os.Args[0], "-test.run=^TestStartupAcrossEpochBoundary$", "-test.v")
				cmd.Env = append(os.Environ(), fmt.Sprintf("%s=%s %d", caseEnv, mode, offset))
				output, err := cmd.CombinedOutput()
				require.NoError(t, err, "%s", output)
			})
		}
	}
}

func testStartupAcrossEpochBoundary(t *testing.T, mode string, offset uint64) {
	const parentEpoch, enteringEpoch = uint64(33), uint64(34)
	const parentSlot = enteringEpoch*100 - 1
	const parentStake, enteringStake = uint64(222), uint64(1000)
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100, LeaderScheduleSlotOffset: 100}
	voteKey := solana.PublicKey{0xc1}
	db := epochBoundaryStakeDB(t, parentSlot, []sealevel.Delegation{{
		VoterPubkey: voteKey, StakeLamports: enteringStake,
		ActivationEpoch: math.MaxUint64, DeactivationEpoch: math.MaxUint64,
	}})
	cfg := mlog.DefaultConfig()
	cfg.Dir = t.TempDir()
	require.NoError(t, mlog.Initialize(cfg, "startup-epoch-test"))
	t.Cleanup(mlog.Shutdown)
	global.SetManageLeaderSchedule(true)
	global.SetBlockHeight(1000)

	var rewardData bytes.Buffer
	epochRewards := &sealevel.SysvarEpochRewards{}
	require.NoError(t, epochRewards.MarshalWithEncoder(bin.NewBinEncoder(&rewardData)))
	done := make(chan struct{})
	require.NoError(t, db.StoreAccounts([]*accounts.Account{
		{Key: sealevel.SysvarStakeHistoryAddr, Lamports: 1, Data: make([]byte, 16392)},
		{Key: sealevel.SysvarEpochRewardsAddr, Lamports: 1, Data: rewardData.Bytes()},
	}, parentSlot, func() { close(done) }))
	<-done

	seeds := make(map[uint64]string)
	for epoch, stake := range map[uint64]uint64{parentEpoch: 111, enteringEpoch: parentStake} {
		data, err := json.Marshal(epochstakes.PersistedEpochStakes{
			Epoch: epoch, TotalStake: stake, Stakes: map[string]uint64{voteKey.String(): stake},
			VoteAccts: map[string]*epochstakes.VoteAccountJSON{voteKey.String(): {NodePubkey: voteKey.String(), Owner: solana.PublicKey(addresses.VoteProgramAddr).String()}},
		})
		require.NoError(t, err)
		seeds[epoch] = string(data)
	}
	parentHash, evictedHash := solana.Hash{1}, solana.Hash{2}
	ms := &state.MithrilState{
		ManifestParentSlot: parentSlot, ManifestParentBankhash: parentHash.String(),
		ManifestEvictedBlockhash: evictedHash.String(), ManifestEpochStakes: seeds,
		ManifestFeeRateGovernor: &state.ManifestFeeRateGovernorSeed{},
	}
	var resume *ResumeState
	if mode != "snapshot" {
		recent := sealevel.SysvarRecentBlockhashes{{Blockhash: parentHash}}
		resume = &ResumeState{ParentSlot: parentSlot, ParentBankhash: parentHash[:],
			RecentBlockhashes: &recent, LastBlockhash: parentHash, EvictedBlockhash: evictedHash}
		if mode == "persisted" || mode == "cached" {
			// A checkpoint whose original snapshot predates its rooted parent.
			ms.ManifestParentSlot -= 100
			resume.ComputedEpochStakes = make(map[uint64][]byte)
			for epoch, seed := range seeds {
				resume.ComputedEpochStakes[epoch] = []byte(seed)
			}
			if mode == "cached" {
				data, err := json.Marshal(epochstakes.PersistedEpochStakes{
					Epoch: enteringEpoch + 1, TotalStake: enteringStake,
					Stakes:    map[string]uint64{voteKey.String(): enteringStake},
					VoteAccts: map[string]*epochstakes.VoteAccountJSON{voteKey.String(): {NodePubkey: voteKey.String(), Owner: solana.PublicKey(addresses.VoteProgramAddr).String()}},
				})
				require.NoError(t, err)
				resume.ComputedEpochStakes[enteringEpoch+1] = data
			}
		}
	}
	parentBankEpoch := initialReplayEpoch(schedule, parentSlot+1, ms.ManifestParentSlot, resume)
	require.Equal(t, parentEpoch, parentBankEpoch)
	require.NoError(t, LoadInitialEpochStakesCache(ms, resume, parentBankEpoch, schedule.GetEpoch(ms.ManifestParentSlot)))
	blk := &b.Block{Slot: parentSlot + 1 + offset, Epoch: enteringEpoch, LastBlockhash: parentHash}
	if resume == nil {
		require.NoError(t, configureInitialBlock(db, blk, ms, schedule))
	} else {
		require.NoError(t, configureInitialBlockFromResume(db, blk, resume, ms, schedule))
	}
	require.Equal(t, voteKey, blk.Leader, "leader preparation must succeed before the epoch transition")
	require.Equal(t, parentStake, blk.TotalEpochStake, "initial configuration inherits the parent's execution stakes")
	require.Equal(t, parentStake, blk.EpochStakesPerVoteAcct[voteKey])
	require.Equal(t, mode == "cached", global.HasEpochStakes(enteringEpoch+1))

	f := features.NewFeaturesDefault()
	blk.Features = f
	parent := epochBoundaryParentCtx(db, blk, parentBankEpoch, f)
	replayCtx := &ReplayCtx{CurrentFeatures: f, Capitalization: 1_000_000_000, SlotsPerYear: 78_892_314.984,
		Inflation: rewards.Inflation{Initial: 0.08, Terminal: 0.015, Taper: 0.15}}
	dbg, err := NewDebugOptions(nil, nil, false)
	require.NoError(t, err)
	require.NotNil(t, handleEpochTransition(db, true, parent, replayCtx, schedule, f, blk, parentBankEpoch, nil, dbg))
	require.Equal(t, enteringStake, blk.TotalEpochStake)
	require.Equal(t, enteringStake, blk.EpochStakesPerVoteAcct[voteKey])
	require.Equal(t, parentStake, global.EpochTotalStake(enteringEpoch), "consensus must retain its own generation")

	slotCtx := newSlotCtx(blk, accounts.NewMemAccounts(), accounts.NewMemAccounts(), nil, nil, 0)
	execCtx := &sealevel.ExecutionCtx{SlotCtx: slotCtx, ComputeMeter: cu.NewComputeMeter(10_000)}
	vm := sbpf.NewInterpreter(&sbpf.Program{TextVA: sbpf.VaddrProgram, Funcs: map[uint32]int64{}}, &sbpf.VMOpts{
		Input: voteKey[:], Context: execCtx, ComputeMeter: &execCtx.ComputeMeter,
	})
	defer vm.Finish()
	for _, addr := range []uint64{0, sbpf.VaddrInput} {
		got, err := sealevel.SyscallGetEpochStakeImpl(vm, addr)
		require.NoError(t, err)
		require.Equal(t, enteringStake, got, "the first block must execute with the entered epoch's stakes")
	}
	blk.EpochStakesPerVoteAcct[voteKey]++
	require.Equal(t, enteringStake, global.EpochStakes(enteringEpoch + 1)[voteKey], "the cached generation must stay immutable")
}
