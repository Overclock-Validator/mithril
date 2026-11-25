package replay

import (
	"bytes"
	"os"
	"sync"
	"sync/atomic"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/rewardcerts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
)

// A block's skip and notar reward certificates cost about 1.5 ms of BLS
// verification in the pre-consensus preflight, which runs on the replay loop
// between two blocks. The certificates are in the footer, so they are known as
// soon as the block is queued for replay, usually while its parent is still
// executing. offerRewardCertPrefetch starts that validation then, on one
// background goroutine.
//
// The preflight and the reward step use a background result only if it was
// computed for the same slot, certificate bytes and verifier material they
// would use themselves, and it succeeded. Anything else (a different candidate
// for the slot, an epoch whose stakes were not ready, an invalid certificate, a
// validation that has not started yet) validates synchronously as before, so
// results and errors are unchanged.
//
// MITHRIL_REWARD_CERT_PREFETCH=0 turns it off.
var rewardCertPrefetchEnabled = os.Getenv("MITHRIL_REWARD_CERT_PREFETCH") != "0"

// rewardCertPrefetchKeep bounds queued and finished validations. It covers the
// blocks that can be queued ahead of replay; older entries (blocks that were
// discarded before replay reached them) are dropped first.
const rewardCertPrefetchKeep = 256

const (
	rewardCertPrefetchQueued int32 = iota
	rewardCertPrefetchStarted
	rewardCertPrefetchAbandoned
)

type rewardCertPrefetch struct {
	slot              uint64
	skipRaw, notarRaw []byte
	epoch             uint64
	shredVersion      uint16
	state             atomic.Int32
	done              chan struct{}
	// Set before done closes.
	material  *voteRewardVerifierMaterial
	validated *rewardcerts.ValidatedRewardCert
	err       error
}

type rewardCertPrefetcher struct {
	mu     sync.Mutex
	bySlot map[uint64]*rewardCertPrefetch
	order  []uint64
	queue  chan *rewardCertPrefetch
	start  sync.Once
	// validate is replaced by tests.
	validate func(*rewardCertPrefetch)
}

var rewardCertPrefetches = newRewardCertPrefetcher()

func newRewardCertPrefetcher() *rewardCertPrefetcher {
	return &rewardCertPrefetcher{
		bySlot:   make(map[uint64]*rewardCertPrefetch),
		queue:    make(chan *rewardCertPrefetch, rewardCertPrefetchKeep),
		validate: validateRewardCertPrefetch,
	}
}

func validateRewardCertPrefetch(p *rewardCertPrefetch) {
	// No metrics here: the preflight still loads (and counts) its own material.
	p.material, p.err = loadVoteRewardVerifierMaterial(p.epoch, p.shredVersion, nil)
	if p.err != nil {
		return
	}
	p.validated, _, p.err = rewardcerts.ValidateRewardCertificatesWithVerifier(
		p.slot, p.skipRaw, p.notarRaw, p.material.snapshot.Epoch, p.material.verifier, false,
	)
}

// offerRewardCertPrefetch queues block's reward certificates for background
// validation. It does not keep block and never blocks.
func offerRewardCertPrefetch(block *b.Block, epochSchedule *sealevel.SysvarEpochSchedule) {
	if !rewardCertPrefetchEnabled {
		return
	}
	rewardCertPrefetches.offer(block, epochSchedule)
}

func (c *rewardCertPrefetcher) offer(block *b.Block, epochSchedule *sealevel.SysvarEpochSchedule) {
	if block == nil || block.IsSkipped || epochSchedule == nil ||
		(len(block.SkipRewardCert) == 0 && len(block.NotarRewardCert) == 0) {
		return
	}
	rewardSlot, ok := rewardcerts.RewardSlotForLeader(block.Slot)
	if !ok {
		return // the preflight reports this itself
	}
	p := &rewardCertPrefetch{
		slot:         block.Slot,
		skipRaw:      bytes.Clone(block.SkipRewardCert),
		notarRaw:     bytes.Clone(block.NotarRewardCert),
		epoch:        epochSchedule.GetEpoch(rewardSlot),
		shredVersion: block.AlpenglowShredVersion,
		done:         make(chan struct{}),
	}
	c.start.Do(func() { go c.run() })
	select {
	case c.queue <- p:
	default:
		return // far behind: these blocks validate synchronously
	}
	c.mu.Lock()
	if _, replaced := c.bySlot[p.slot]; !replaced {
		c.order = append(c.order, p.slot)
	}
	c.bySlot[p.slot] = p // a later candidate for the slot replaces an earlier one
	for len(c.order) > rewardCertPrefetchKeep {
		delete(c.bySlot, c.order[0])
		c.order = c.order[1:]
	}
	c.mu.Unlock()
}

func (c *rewardCertPrefetcher) run() {
	for p := range c.queue {
		if p.state.CompareAndSwap(rewardCertPrefetchQueued, rewardCertPrefetchStarted) {
			c.validate(p)
		}
		close(p.done)
	}
}

// take returns a successful background validation of exactly these inputs, or
// false. It waits only for a validation that is already running, which takes no
// longer than validating here would; one still waiting in the queue is
// abandoned, so the caller never waits behind other blocks.
func (c *rewardCertPrefetcher) take(slot uint64, skipRaw, notarRaw []byte, material *voteRewardVerifierMaterial) (*rewardcerts.ValidatedRewardCert, bool) {
	c.mu.Lock()
	p := c.bySlot[slot]
	if p == nil || !bytes.Equal(p.skipRaw, skipRaw) || !bytes.Equal(p.notarRaw, notarRaw) {
		c.mu.Unlock()
		return nil, false
	}
	delete(c.bySlot, slot)
	for i, queued := range c.order {
		if queued == slot {
			c.order = append(c.order[:i], c.order[i+1:]...)
			break
		}
	}
	c.mu.Unlock()
	if p.state.CompareAndSwap(rewardCertPrefetchQueued, rewardCertPrefetchAbandoned) {
		return nil, false
	}
	<-p.done
	if p.err != nil || p.validated == nil || p.material != material {
		return nil, false
	}
	return p.validated, true
}
