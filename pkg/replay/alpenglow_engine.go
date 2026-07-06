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
func installAlpenglowValidatorSet(consensusEngine consensusengine.Engine, epoch uint64) {
	sink, ok := consensusEngine.(consensusengine.AlpenglowValidatorSetSink)
	if !ok {
		return
	}

	stakes := global.EpochStakes(epoch)
	voteAccts := alpenglowVoteAccountsForEpoch(epoch, stakes)
	set, err := alpenglow.BuildValidatorSet(epoch, stakes, voteAccts, global.EpochTotalStake(epoch))
	if err != nil {
		mlog.Log.FileOnlyf("ALPENGLOW observer: validator set unavailable for epoch %d: %v", epoch, err)
		return
	}
	if err := sink.SetAlpenglowValidatorSet(set); err != nil {
		mlog.Log.FileOnlyf("ALPENGLOW observer: failed to install validator set for epoch %d: %v", epoch, err)
	}
}

// installCachedAlpenglowValidatorSets installs validator sets for every cached
// epoch (so certs spanning an epoch boundary verify), plus the current epoch.
func installCachedAlpenglowValidatorSets(consensusEngine consensusengine.Engine, currentEpoch uint64) {
	sink, ok := consensusEngine.(consensusengine.AlpenglowValidatorSetSink)
	if !ok {
		return
	}

	epochs := global.GetAllCachedEpochs()
	if len(epochs) == 0 {
		installAlpenglowValidatorSet(consensusEngine, currentEpoch)
		return
	}
	sort.Slice(epochs, func(i, j int) bool { return epochs[i] < epochs[j] })
	installedCurrent := false
	for _, epoch := range epochs {
		if epoch == currentEpoch {
			installedCurrent = true
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
		}
	}
	if !installedCurrent {
		installAlpenglowValidatorSet(consensusEngine, currentEpoch)
	}
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
// timestamp (Alpenglow uses the footer time, not the estimated PoH time).
func applyAlpenglowFooterClock(slotCtx *sealevel.SlotCtx, block *b.Block, epochSchedule *sealevel.SysvarEpochSchedule) error {
	if block.UnixTimestamp == 0 {
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
	sealevel.SysvarCache.Clock.Sysvar = &clock
	sealevel.SysvarCache.Clock.Acct = clockAcct
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
	Conflict  bool // recorded safety violation at the slot, not a wrong local block
}

func (m *AlpenglowFinalityMismatch) Error() string {
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
	ev := state.AlpenglowFinalityEvidence{Slot: m.Slot, Conflict: m.Conflict}
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

// alpenglowPromotionGate caps promotion at the last slot whose executed identity is
// consistent with certificate finality. It walks (lastRooted, through] in order and
// stops at the first violation (prefix-stop: descendants of an unpromoted block must
// not be promoted either). Slots with no executed alpenglow block-id (RPC fallback)
// or no known finality (pruned/absent) pass under the delegated-trust regime.
// footerFinalized is the replay-local capture, immune to tracker pruning; the engine
// index is the near-tip fallback. forced holds persisted evidence expectations: a
// slot present there promotes ONLY on an exact executed match (zero hash = conflict
// evidence, never promotes) — delegated trust does not apply to a disputed slot.
func alpenglowPromotionGate(
	consensusEngine consensusengine.Engine,
	footerFinalized map[uint64]solana.Hash,
	executed map[uint64]solana.Hash,
	forced map[uint64]solana.Hash,
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
			if want.IsZero() {
				return slot - 1, &AlpenglowFinalityMismatch{Slot: slot, Conflict: true}
			}
			if got, ok := executed[slot]; !ok || got != want {
				return slot - 1, &AlpenglowFinalityMismatch{Slot: slot, Executed: got, Finalized: want}
			}
		}
		if index != nil && index.FinalityConflictAt(slot) {
			return slot - 1, &AlpenglowFinalityMismatch{Slot: slot, Conflict: true}
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
