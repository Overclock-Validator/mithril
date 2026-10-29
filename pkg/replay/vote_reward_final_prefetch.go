package replay

import (
	"bytes"
	"sync/atomic"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/rewardcerts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
)

// A block's final certificate costs about 1.4 ms of BLS verification in
// ApplyAlpenglowVoteRewards, after the block's transactions. The pre-consensus
// preflight already holds the footer, so it starts that validation in the
// background. The reward step uses the result only if it was computed for the
// same slot, certificate bytes, epoch and verifier material it would use itself,
// and succeeded; anything else validates synchronously as before, so results
// and errors are unchanged. At most one validation runs at a time: a candidate
// that arrives while one is still running (a flood of candidates, most of them
// rejected) gets no prefetch and validates synchronously if it reaches the
// reward step; the running validation stays available to its own block.
type finalCertPrefetch struct {
	slot  uint64
	raw   []byte
	epoch uint64
	done  chan struct{}
	// Set before done closes.
	material  *voteRewardVerifierMaterial
	validated *rewardcerts.ValidatedFinalCert
	err       error
}

var pendingFinalCert atomic.Pointer[finalCertPrefetch]

// finalCertPrefetchRunning is set while a background validation runs.
var finalCertPrefetchRunning atomic.Bool

func startFinalCertPrefetch(block *b.Block, epochSchedule *sealevel.SysvarEpochSchedule, shredVersion uint16) {
	if block == nil || len(block.BlockFinalCert) == 0 || epochSchedule == nil {
		pendingFinalCert.Store(nil)
		return
	}
	fc, err := rewardcerts.DecodeFinalCertificate(block.BlockFinalCert)
	if err != nil {
		pendingFinalCert.Store(nil) // the reward step reports decode errors itself
		return
	}
	if !finalCertPrefetchRunning.CompareAndSwap(false, true) {
		// Keep the running validation: its own candidate can still use it, and
		// any other block fails the exact-input match.
		return
	}
	p := &finalCertPrefetch{
		slot:  block.Slot,
		raw:   bytes.Clone(block.BlockFinalCert),
		epoch: epochSchedule.GetEpoch(fc.Slot),
		done:  make(chan struct{}),
	}
	pendingFinalCert.Store(p)
	go func() {
		defer finalCertPrefetchRunning.Store(false)
		defer close(p.done)
		// No metrics here: the reward step still loads (and counts) its own material.
		p.material, p.err = loadVoteRewardVerifierMaterial(p.epoch, shredVersion, nil)
		if p.err != nil {
			return
		}
		p.validated, p.err = rewardcerts.ValidateDecodedBlockFinalCertificateWithVerifier(fc, p.epoch, p.material.verifier)
	}()
}

// takeFinalCertPrefetch returns a successful background validation of exactly
// these inputs, waiting for it to finish, or false.
func takeFinalCertPrefetch(slot uint64, raw []byte, epoch uint64, material *voteRewardVerifierMaterial) (*rewardcerts.ValidatedFinalCert, bool) {
	p := pendingFinalCert.Load()
	if p == nil || p.slot != slot || p.epoch != epoch || !bytes.Equal(p.raw, raw) {
		return nil, false
	}
	pendingFinalCert.CompareAndSwap(p, nil)
	<-p.done
	if p.err != nil || p.material != material {
		return nil, false
	}
	return p.validated, true
}
