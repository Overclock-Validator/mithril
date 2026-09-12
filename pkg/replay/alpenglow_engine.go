package replay

import (
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	consensusengine "github.com/Overclock-Validator/mithril/pkg/consensus"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

// alpenglowRootedSlot returns the highest slot the engine has seen a finalization
// certificate for — the Alpenglow finality watermark that drives promotion, the
// certificate-based counterpart to TowerBFT's HighestRootedSlot.
func alpenglowRootedSlot(consensusEngine consensusengine.Engine) (uint64, bool) {
	if consensusEngine == nil {
		return 0, false
	}
	snap := consensusEngine.Snapshot()
	if snap.AlpenglowChain == nil {
		return 0, false
	}
	slot := snap.AlpenglowChain.LatestDirectFinalizedBlock.Slot
	return slot, slot > 0
}

// installAlpenglowValidatorSet builds and installs the BLS validator set for one
// epoch into the engine (no-op if the engine isn't a validator-set sink).
func installAlpenglowValidatorSet(consensusEngine consensusengine.Engine, epoch uint64) bool {
	sink, ok := consensusEngine.(consensusengine.AlpenglowValidatorSetSink)
	if !ok {
		return false
	}

	stakes := global.EpochStakes(epoch)
	voteAccts := alpenglowVoteAccountsForEpoch(epoch, stakes)
	set, err := alpenglow.BuildValidatorSet(epoch, stakes, voteAccts, global.EpochTotalStake(epoch))
	if err != nil {
		mlog.Log.FileOnlyf("ALPENGLOW observer: validator set unavailable for epoch %d: %v", epoch, err)
		return false
	}
	if err := sink.SetAlpenglowValidatorSet(set); err != nil {
		mlog.Log.FileOnlyf("ALPENGLOW observer: failed to install validator set for epoch %d: %v", epoch, err)
		return false
	}
	return true
}

// installEpochTransitionAlpenglowValidatorSets installs the validator sets
// needed both to execute the newly entered epoch and to verify the future epoch
// whose leader schedule was prepared at the boundary.
func installEpochTransitionAlpenglowValidatorSets(consensusEngine consensusengine.Engine, newEpoch, leaderScheduleEpoch uint64) {
	for _, targetEpoch := range epochTransitionTargetEpochs(newEpoch, leaderScheduleEpoch) {
		installAlpenglowValidatorSet(consensusEngine, targetEpoch)
	}
}

// InstallCachedAlpenglowValidatorSets installs validator sets for every cached
// epoch (so certs spanning an epoch boundary verify), plus the current epoch.
// It returns the number of sets successfully published and whether the
// requested root/current epoch was among them. Node startup requires the
// latter before advertising Votor; replay calls it idempotently.
func InstallCachedAlpenglowValidatorSets(consensusEngine consensusengine.Engine, currentEpoch uint64) (int, bool) {
	sink, ok := consensusEngine.(consensusengine.AlpenglowValidatorSetSink)
	if !ok {
		return 0, false
	}

	epochs := global.GetAllCachedEpochs()
	if len(epochs) == 0 {
		if installAlpenglowValidatorSet(consensusEngine, currentEpoch) {
			return 1, true
		}
		return 0, false
	}
	sort.Slice(epochs, func(i, j int) bool { return epochs[i] < epochs[j] })
	sawCurrent := false
	installedCurrent := false
	installed := 0
	for _, epoch := range epochs {
		if epoch == currentEpoch {
			sawCurrent = true
		}
		stakes := global.EpochStakes(epoch)
		voteAccts := alpenglowVoteAccountsForEpoch(epoch, stakes)
		set, err := alpenglow.BuildValidatorSet(epoch, stakes, voteAccts, global.EpochTotalStake(epoch))
		if err != nil {
			mlog.Log.FileOnlyf("ALPENGLOW observer: validator set unavailable for cached epoch %d: %v", epoch, err)
			continue
		}
		if err := sink.SetAlpenglowValidatorSet(set); err != nil {
			mlog.Log.FileOnlyf("ALPENGLOW observer: failed to install cached validator set for epoch %d: %v", epoch, err)
			continue
		}
		installed++
		if epoch == currentEpoch {
			installedCurrent = true
		}
	}
	if !sawCurrent {
		if installAlpenglowValidatorSet(consensusEngine, currentEpoch) {
			installed++
			installedCurrent = true
		}
	}
	return installed, installedCurrent
}

func alpenglowVoteAccountsForEpoch(epoch uint64, stakes map[solana.PublicKey]uint64) map[solana.PublicKey]*epochstakes.VoteAccount {
	voteAccts := make(map[solana.PublicKey]*epochstakes.VoteAccount, len(stakes))
	for voteAcct, meta := range global.EpochStakesVoteAccts(epoch) {
		if meta == nil {
			continue
		}
		copied := *meta
		copied.BlsPubkeyCompressed = cloneBLSCompressed(meta.BlsPubkeyCompressed)
		voteAccts[voteAcct] = &copied
	}
	return voteAccts
}

func cloneBLSCompressed(src *[48]byte) *[48]byte {
	if src == nil {
		return nil
	}
	copied := *src
	return &copied
}

// applyAlpenglowFooterClock rewrites the Clock sysvar from the block footer's
// timestamp (Alpenglow uses the footer time, not the estimated PoH time) and
// publishes the accepted value to replay's process-global sysvar cache.
func applyAlpenglowFooterClock(slotCtx *sealevel.SlotCtx, block *b.Block, epochSchedule *sealevel.SysvarEpochSchedule) error {
	return applyAlpenglowFooterClockWithCache(slotCtx, block, epochSchedule, true)
}

// applyAlpenglowFooterClockLocal performs the same deterministic slot-state
// update without publishing it globally. Block production freezes a candidate
// before ordered adopt publishes the cache; updating SysvarCache at freeze
// would make the next load mistake the candidate's Clock for its parent Clock
// and omit the Clock delta from AccountsLtHash.
func applyAlpenglowFooterClockLocal(slotCtx *sealevel.SlotCtx, block *b.Block, epochSchedule *sealevel.SysvarEpochSchedule) error {
	return applyAlpenglowFooterClockWithCache(slotCtx, block, epochSchedule, false)
}

func applyAlpenglowFooterClockWithCache(slotCtx *sealevel.SlotCtx, block *b.Block, epochSchedule *sealevel.SysvarEpochSchedule, updateCache bool) error {
	if _, ok, err := alpenglowFooterUnixTimestamp(block); err != nil {
		return err
	} else if !ok {
		return nil
	}

	clockAcct, err := slotCtx.GetAccount(sealevel.SysvarClockAddr)
	if err != nil {
		return fmt.Errorf("unable to get clock sysvar for Alpenglow footer update: %w", err)
	}

	decoder := bin.NewBinDecoder(clockAcct.Data)
	var clock sealevel.SysvarClock
	if err := clock.UnmarshalWithDecoder(decoder); err != nil {
		return fmt.Errorf("unable to unmarshal clock sysvar for Alpenglow footer update: %w", err)
	}
	if err := updateClockSysvarFromAlpenglowFooter(&clock, block, epochSchedule); err != nil {
		return err
	}

	newClockBytes := clock.MustMarshal()
	copy(clockAcct.Data, newClockBytes)
	// slotCtx.GetAccount returns a clone, so write the footer-updated clock back
	// into slot state — otherwise the timestamp is dropped from the bankhash.
	if err := slotCtx.SetAccount(sealevel.SysvarClockAddr, clockAcct); err != nil {
		return fmt.Errorf("unable to write Alpenglow footer clock back to slot state: %w", err)
	}
	bankSysvars := slotCtx.BankSysvars()
	if bankSysvars == nil {
		bankSysvars, err = sealevel.NewBankSysvars(slotCtx.Slot, clockAcct)
	} else {
		bankSysvars, err = bankSysvars.WithAccounts(clockAcct)
	}
	if err != nil {
		return fmt.Errorf("update bank-local Clock snapshot: %w", err)
	}
	if err := slotCtx.PublishBankSysvars(bankSysvars); err != nil {
		return err
	}
	if updateCache {
		sealevel.SysvarCache.Clock.Sysvar = &clock
		sealevel.SysvarCache.Clock.Acct = clockAcct
	}
	return nil
}

// ingestAlpenglowFooterCertificate decodes a block-footer final_cert and feeds the
// verified certificate(s) to the observer engine's chain tracker (the unstaked
// finality path). No-op unless the engine accepts footer certificates.
func ingestAlpenglowFooterCertificate(consensusEngine consensusengine.Engine, raw []byte) []alpenglow.BlockID {
	sink, ok := consensusEngine.(consensusengine.AlpenglowFooterCertificateSink)
	if !ok {
		return nil
	}
	fc, err := turbine.UnmarshalFinalCertificate(raw)
	if err != nil {
		mlog.Log.FileOnlyf("ALPENGLOW footer cert decode: %v", err)
		return nil
	}
	var notarSig, notarBitmap []byte
	if fc.NotarAggregate != nil {
		notarSig = fc.NotarAggregate.Signature[:]
		notarBitmap = fc.NotarAggregate.Bitmap
	}
	certs, err := alpenglow.FinalCertToCertificates(fc.Slot, fc.BlockID,
		fc.FinalAggregate.Signature[:], fc.FinalAggregate.Bitmap, notarSig, notarBitmap)
	if err != nil {
		mlog.Log.FileOnlyf("ALPENGLOW footer cert convert (slot %d): %v", fc.Slot, err)
		return nil
	}
	return sink.ObserveFooterCertificates(certs)
}

// AlpenglowFinalityMismatch reports an executed block that contradicts the
// certificate-finalized block for its slot — equivocation evidence; the executed
// branch must not reach durable state.
type AlpenglowFinalityMismatch struct {
	Slot      uint64
	Executed  solana.Hash
	Finalized solana.Hash
	Skip      bool // finalized ancestry proves this slot was omitted
	Conflict  bool // recorded safety violation at the slot, not a wrong local block
}

func (m *AlpenglowFinalityMismatch) Error() string {
	if m.Conflict {
		return fmt.Sprintf("alpenglow finality conflict at slot %d", m.Slot)
	}
	if m.Skip {
		return fmt.Sprintf("alpenglow finality mismatch at slot %d: executed block %s, finalized outcome skipped", m.Slot, m.Executed)
	}
	return fmt.Sprintf("alpenglow finality mismatch at slot %d: executed block %s, finalized block %s",
		m.Slot, m.Executed, m.Finalized)
}

// recordAlpenglowEvidence persists a gate violation into the state file (written on
// halt/shutdown) so a restart cannot fold the disputed slot under delegated trust.
// If a fresh bootstrap halts before any slot is rooted the state file is not written,
// but re-replay from the durable base re-observes the same certs and re-derives the
// violation, so the forced-match protection re-arms regardless.
func recordAlpenglowEvidence(mithrilState *state.MithrilState, m *AlpenglowFinalityMismatch) {
	if mithrilState == nil || m == nil {
		return
	}
	for _, ev := range mithrilState.AlpenglowEvidence {
		if ev.Slot == m.Slot {
			return
		}
	}
	ev := state.AlpenglowFinalityEvidence{Slot: m.Slot, Skip: m.Skip, Conflict: m.Conflict}
	if !m.Conflict {
		ev.Executed = hex.EncodeToString(m.Executed[:])
		ev.Finalized = hex.EncodeToString(m.Finalized[:])
	}
	mithrilState.AlpenglowEvidence = append(mithrilState.AlpenglowEvidence, ev)
}

func clearAlpenglowEvidence(mithrilState *state.MithrilState, slot uint64) {
	if mithrilState == nil {
		return
	}
	kept := mithrilState.AlpenglowEvidence[:0]
	for _, ev := range mithrilState.AlpenglowEvidence {
		if ev.Slot != slot {
			kept = append(kept, ev)
		}
	}
	mithrilState.AlpenglowEvidence = kept
}

// alpenglowGateStats counts gate outcomes so an operator can tell exact-match
// verification from delegated-trust passes.
type alpenglowGateStats struct {
	checked, matched, noFinality, noLocalID uint64
}

// alpenglowFinalityExpectation preserves the distinction between a conflict
// and a finalized skip. Both have no finalized block hash, but a skip is a
// valid local outcome while a conflict must never promote.
type alpenglowFinalityExpectation struct {
	Block    solana.Hash
	Skip     bool
	Conflict bool
}

// alpenglowPromotionGate caps promotion at the last slot whose executed identity is
// consistent with certificate finality. It walks (lastRooted, through] in order and
// stops at the first violation (prefix-stop: descendants of an unpromoted block must
// not be promoted either). Slots with no executed alpenglow block-id (RPC fallback)
// or no known finality (pruned/absent) pass under the delegated-trust regime.
// footerFinalized is the replay-local capture, immune to tracker pruning; the engine
// index is the near-tip fallback. forced holds persisted evidence expectations: a
// slot present there promotes ONLY on an exact executed outcome; delegated
// trust does not apply to a disputed slot.
func alpenglowPromotionGate(
	consensusEngine consensusengine.Engine,
	footerFinalized map[uint64]solana.Hash,
	executed map[uint64]solana.Hash,
	forced map[uint64]alpenglowFinalityExpectation,
	lastRooted, through, executedTip uint64,
	stats *alpenglowGateStats,
) (uint64, error) {
	if through <= lastRooted {
		return through, nil
	}
	// Only walk slots replay has actually executed: during catchup the watermark can
	// sit millions of slots ahead, and slots beyond the executed tail never fold
	// anyway (promote bounds there). Passing `through` back unclamped keeps the
	// promotion bound itself intact; the walk stays bounded by the tail.
	walkTop := through
	if executedTip < walkTop {
		walkTop = executedTip
	}
	// Nothing below the in-RAM tail can fold, so never walk it: on a fresh bootstrap
	// lastRooted starts at 0 and an unfloored walk would span the whole chain.
	if executedTip > unrootedTailHaltCap && lastRooted+1 < executedTip-unrootedTailHaltCap {
		lastRooted = executedTip - unrootedTailHaltCap
	}
	index, _ := consensusEngine.(consensusengine.AlpenglowFinalityIndex)
	for slot := lastRooted + 1; slot <= walkTop; slot++ {
		if want, disputed := forced[slot]; disputed {
			if want.Conflict {
				return slot - 1, &AlpenglowFinalityMismatch{Slot: slot, Conflict: true}
			}
			if got, ok := executed[slot]; !ok || got != want.Block {
				return slot - 1, &AlpenglowFinalityMismatch{
					Slot: slot, Executed: got, Finalized: want.Block, Skip: want.Skip,
				}
			}
		}
		if index != nil && index.FinalityConflictAt(slot) {
			return slot - 1, &AlpenglowFinalityMismatch{Slot: slot, Conflict: true}
		}
		if index != nil {
			if _, finalizedSkip := index.FinalizedSkipAt(slot); finalizedSkip {
				got, haveExecuted := executed[slot]
				if stats != nil {
					stats.checked++
					if !haveExecuted {
						stats.noLocalID++
					} else if got.IsZero() {
						stats.matched++
					}
				}
				if haveExecuted && !got.IsZero() {
					return slot - 1, &AlpenglowFinalityMismatch{Slot: slot, Executed: got, Skip: true}
				}
				continue
			}
		}
		expected, haveExpected := footerFinalized[slot]
		if !haveExpected && index != nil {
			if id, ok := index.FinalizedBlockAt(slot); ok {
				expected, haveExpected = id.Hash, true
			}
		}
		got, haveExecuted := executed[slot]
		if stats != nil {
			stats.checked++
			switch {
			case haveExpected && haveExecuted:
				if expected == got {
					stats.matched++
				}
			case !haveExpected:
				stats.noFinality++
			default:
				stats.noLocalID++
			}
		}
		if haveExpected && haveExecuted && expected != got {
			return slot - 1, &AlpenglowFinalityMismatch{Slot: slot, Executed: got, Finalized: expected}
		}
	}
	return through, nil
}
