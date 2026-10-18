package harmonic

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	blockpb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/block"
	"github.com/Overclock-Validator/mithril/pkg/tpu/packet"
	"github.com/Overclock-Validator/mithril/pkg/tpu/sigverify"
	"github.com/Overclock-Validator/mithril/pkg/txverify"
	"github.com/gagliardetto/solana-go"
)

// MaxBundleTransactions is the most transactions a block bundle may hold.
const MaxBundleTransactions = 5

// BlockEvent is what the block pipeline hands the executor, in stream order.
type BlockEvent interface {
	isBlockEvent()
}

// BundleEvent is a validated bundle of the current slot's block.
type BundleEvent struct {
	Slot uint64
	// Seq is the bundle's 1-based position in the slot's bundle sequence.
	Seq             uint64
	Arrival         time.Time
	RevertProtected bool
	Transactions    []*solana.Transaction
}

// InvalidBundleEvent is a bundle that failed validation. It holds its place
// in the sequence, and under the strict profile ends the slot's block.
type InvalidBundleEvent struct {
	Slot    uint64
	Seq     uint64
	Arrival time.Time
	Reason  error
}

// StreamLostEvent reports that the rest of slot's block was lost.
type StreamLostEvent struct {
	Slot uint64
}

func (BundleEvent) isBlockEvent()        {}
func (InvalidBundleEvent) isBlockEvent() {}
func (StreamLostEvent) isBlockEvent()    {}

// BlockPipelineStats counts block pipeline activity.
type BlockPipelineStats struct {
	Messages       uint64
	WrongSlot      uint64
	LostSlot       uint64
	Bundles        uint64
	InvalidBundles uint64
	// Transactions counts the transactions of valid bundles.
	Transactions uint64
}

// BlockPipeline turns the block stream into executor events. It admits only
// messages for the current block slot, splits each into bundles, validates
// them, and emits them in order. It is the session's BlockHandler.
type BlockPipeline struct {
	events      chan<- BlockEvent
	done        chan struct{}
	closeOnce   sync.Once
	txV1Enabled func() bool
	current     atomic.Uint64
	inFlight    atomic.Int32
	verifier    txverify.BatchVerifier
	stats       struct {
		messages, wrongSlot, lostSlot, bundles, invalid, transactions atomic.Uint64
	}

	mu      sync.Mutex
	lost    map[uint64]bool
	seqSlot uint64
	nextSeq uint64
}

// NewBlockPipeline sends events on events, blocking rather than dropping
// when the executor falls behind, until Close. txV1Enabled reports whether the
// current bank accepts v1 transactions; nil rejects them.
func NewBlockPipeline(events chan<- BlockEvent, txV1Enabled func() bool) *BlockPipeline {
	return &BlockPipeline{events: events, done: make(chan struct{}), txV1Enabled: txV1Enabled, lost: make(map[uint64]bool)}
}

// Close drops every later event, so a stream goroutine blocked on an
// executor that has stopped returns.
func (p *BlockPipeline) Close() {
	p.closeOnce.Do(func() { close(p.done) })
}

func (p *BlockPipeline) emit(e BlockEvent) {
	select {
	case p.events <- e:
	case <-p.done:
	}
}

func (p *BlockPipeline) Stats() BlockPipelineStats {
	return BlockPipelineStats{
		Messages:       p.stats.messages.Load(),
		WrongSlot:      p.stats.wrongSlot.Load(),
		LostSlot:       p.stats.lostSlot.Load(),
		Bundles:        p.stats.bundles.Load(),
		InvalidBundles: p.stats.invalid.Load(),
		Transactions:   p.stats.transactions.Load(),
	}
}

// SetCurrentSlot makes slot the current block slot, or clears it with 0. Call
// it before sending slot's leader notice: the block engine streams the slot
// only after the notice, so none of it can arrive earlier.
func (p *BlockPipeline) SetCurrentSlot(slot uint64) {
	p.current.Store(slot)
	p.mu.Lock()
	defer p.mu.Unlock()
	for lost := range p.lost {
		if lost < slot {
			delete(p.lost, lost)
		}
	}
}

// InFlight reports whether a current-slot message is being split into
// bundles whose events are not all sent yet.
func (p *BlockPipeline) InFlight() bool {
	return p.inFlight.Load() > 0
}

// StreamLost marks slot lost: the rest of its block is discarded even after
// the session reconnects, since nothing says where a new stream resumes.
func (p *BlockPipeline) StreamLost(slot uint64) {
	p.mu.Lock()
	p.lost[slot] = true
	p.mu.Unlock()
	p.emit(StreamLostEvent{Slot: slot})
}

// HandleBlock admits, segments and validates one block message.
func (p *BlockPipeline) HandleBlock(block *blockpb.Block, arrival time.Time) {
	// Until its last event is sent, a message counts as buffered.
	p.inFlight.Add(1)
	defer p.inFlight.Add(-1)
	p.stats.messages.Add(1)
	slot := block.GetSlot()
	if current := p.current.Load(); current == 0 || slot != current {
		p.stats.wrongSlot.Add(1)
		return
	}
	p.mu.Lock()
	lost := p.lost[slot]
	if !lost && p.seqSlot != slot {
		p.seqSlot, p.nextSeq = slot, 1
	}
	p.mu.Unlock()
	if lost {
		p.stats.lostSlot.Add(1)
		return
	}

	txs := block.GetTransactions()
	parsed, parseErrs := p.parse(txs)
	for _, bundle := range segment(txs) {
		seq := p.takeSeq()
		if err := validateBundle(txs, parsed, parseErrs, bundle); err != nil {
			p.stats.invalid.Add(1)
			p.emit(InvalidBundleEvent{Slot: slot, Seq: seq, Arrival: arrival, Reason: err})
			continue
		}
		p.stats.bundles.Add(1)
		p.stats.transactions.Add(uint64(bundle.end - bundle.start))
		p.emit(BundleEvent{
			Slot:            slot,
			Seq:             seq,
			Arrival:         arrival,
			RevertProtected: txs[bundle.start].GetBundleId() != 0,
			Transactions:    append([]*solana.Transaction(nil), parsed[bundle.start:bundle.end]...),
		})
	}
}

func (p *BlockPipeline) takeSeq() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	seq := p.nextSeq
	p.nextSeq++
	return seq
}

// parse parses every transaction in a message and verifies all their
// signatures as one batch. errs[i] is set for a transaction that is unusable.
func (p *BlockPipeline) parse(txs []*blockpb.Transaction) ([]*solana.Transaction, []error) {
	parsed := make([]*solana.Transaction, len(txs))
	errs := make([]error, len(txs))
	verify := make([]*solana.Transaction, 0, len(txs))
	verifyAt := make([]int, 0, len(txs))
	for i, t := range txs {
		data := t.GetTransaction()
		if len(data) == 0 || len(data) > packet.DataSize {
			errs[i] = fmt.Errorf("transaction %d has %d bytes", i, len(data))
			continue
		}
		tx, err := sigverify.ParseTx(data)
		if err != nil {
			errs[i] = fmt.Errorf("transaction %d: %w", i, err)
			continue
		}
		if tx.Message.GetVersion() == solana.MessageVersionV1 && (p.txV1Enabled == nil || !p.txV1Enabled()) {
			errs[i] = fmt.Errorf("transaction %d: v1 transactions are not enabled", i)
			continue
		}
		parsed[i] = tx
		verify = append(verify, tx)
		verifyAt = append(verifyAt, i)
	}
	verifyErrs := make([]error, len(verify))
	p.verifier.Verify(verify, verifyErrs)
	for j, err := range verifyErrs {
		if err != nil {
			errs[verifyAt[j]] = fmt.Errorf("transaction %d: %w", verifyAt[j], err)
		}
	}
	return parsed, errs
}

type bundleSpan struct {
	start, end int
}

// segment splits one message into bundles: each zero bundle_id transaction is
// its own standalone bundle, and each run of equal nonzero ids is one
// revert-protected bundle. Segmentation never spans messages.
func segment(txs []*blockpb.Transaction) []bundleSpan {
	var spans []bundleSpan
	for i := 0; i < len(txs); {
		id := txs[i].GetBundleId()
		j := i + 1
		if id != 0 {
			for j < len(txs) && txs[j].GetBundleId() == id {
				j++
			}
		}
		spans = append(spans, bundleSpan{start: i, end: j})
		i = j
	}
	return spans
}

func validateBundle(txs []*blockpb.Transaction, parsed []*solana.Transaction, errs []error, bundle bundleSpan) error {
	if n := bundle.end - bundle.start; n == 0 || n > MaxBundleTransactions {
		return fmt.Errorf("bundle has %d transactions", n)
	}
	seen := make(map[solana.Signature]struct{}, bundle.end-bundle.start)
	for i := bundle.start; i < bundle.end; i++ {
		if errs[i] != nil {
			return errs[i]
		}
		signature := parsed[i].Signatures[0]
		if _, dup := seen[signature]; dup {
			return errors.New("bundle repeats a transaction signature")
		}
		seen[signature] = struct{}{}
	}
	return nil
}
