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

// pipelineBatch is the most messages a worker parses and verifies together,
// so short messages still fill signature verification batches.
const pipelineBatch = 16

// BlockPipeline turns the block stream into executor events. It admits only
// messages for the current block slot, splits each into bundles, validates
// them, and emits them in stream order. Workers parse and verify messages in
// parallel; their results are put back in stream order before anything is
// emitted. It is the session's BlockHandler.
type BlockPipeline struct {
	events      chan<- BlockEvent
	done        chan struct{}
	closeOnce   sync.Once
	txV1Enabled func() bool
	current     atomic.Uint64
	inFlight    atomic.Int32
	jobs        chan *pipelineJob
	results     chan *pipelineJob
	stats       struct {
		messages, wrongSlot, lostSlot, bundles, invalid, transactions atomic.Uint64
	}

	mu   sync.Mutex
	lost map[uint64]bool
	// nextOrder is the stream position of the next admitted message.
	nextOrder uint64
}

// pipelineJob is an admitted message, or a lost-stream marker, at its stream
// position.
type pipelineJob struct {
	order   uint64
	slot    uint64
	arrival time.Time
	txs     []*blockpb.Transaction
	lost    bool
	parsed  []*solana.Transaction
	errs    []error
}

// NewBlockPipeline sends events on events, blocking rather than dropping
// when the executor falls behind, until Close. txV1Enabled reports whether the
// current bank accepts v1 transactions; nil rejects them. workers parse and
// verify messages in parallel.
func NewBlockPipeline(events chan<- BlockEvent, txV1Enabled func() bool, workers int) *BlockPipeline {
	workers = max(workers, 1)
	p := &BlockPipeline{
		events:      events,
		done:        make(chan struct{}),
		txV1Enabled: txV1Enabled,
		jobs:        make(chan *pipelineJob, workers*pipelineBatch),
		results:     make(chan *pipelineJob, workers*pipelineBatch),
		lost:        make(map[uint64]bool),
		nextOrder:   1,
	}
	for range workers {
		go p.work()
	}
	go p.mux()
	return p
}

// Close stops the pipeline and drops every later event, so a stream
// goroutine blocked on an executor that has stopped returns.
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

// InFlight reports whether an admitted message has events not yet sent.
func (p *BlockPipeline) InFlight() bool {
	return p.inFlight.Load() > 0
}

// StreamLost marks slot lost: the rest of its block is discarded even after
// the session reconnects, since nothing says where a new stream resumes. The
// loss is reported after every message admitted before it.
func (p *BlockPipeline) StreamLost(slot uint64) {
	p.mu.Lock()
	p.lost[slot] = true
	job := &pipelineJob{order: p.nextOrder, slot: slot, lost: true}
	p.nextOrder++
	p.mu.Unlock()
	p.inFlight.Add(1)
	p.queue(job)
}

// HandleBlock admits one block message and queues it for parsing. It blocks
// while the pipeline is full, holding back the stream.
func (p *BlockPipeline) HandleBlock(block *blockpb.Block, arrival time.Time) {
	p.stats.messages.Add(1)
	slot := block.GetSlot()
	if current := p.current.Load(); current == 0 || slot != current {
		p.stats.wrongSlot.Add(1)
		return
	}
	p.mu.Lock()
	if p.lost[slot] {
		p.mu.Unlock()
		p.stats.lostSlot.Add(1)
		return
	}
	job := &pipelineJob{order: p.nextOrder, slot: slot, arrival: arrival, txs: block.GetTransactions()}
	p.nextOrder++
	// Until its last event is sent, a message counts as buffered.
	p.inFlight.Add(1)
	p.mu.Unlock()
	p.queue(job)
}

func (p *BlockPipeline) queue(job *pipelineJob) {
	select {
	case p.jobs <- job:
	case <-p.done:
	}
}

// work parses and verifies queued messages, several at a time.
func (p *BlockPipeline) work() {
	batch := make([]*pipelineJob, 0, pipelineBatch)
	var verifier txverify.BatchVerifier
	for {
		select {
		case <-p.done:
			return
		case job := <-p.jobs:
			batch = append(batch[:0], job)
		}
	gather:
		for len(batch) < pipelineBatch {
			select {
			case job := <-p.jobs:
				batch = append(batch, job)
			default:
				break gather
			}
		}
		p.parse(batch, &verifier)
		for _, job := range batch {
			select {
			case p.results <- job:
			case <-p.done:
				return
			}
		}
	}
}

// mux emits results in stream order, holding each until every earlier one is
// out. It alone assigns bundle sequence numbers.
func (p *BlockPipeline) mux() {
	held := make(map[uint64]*pipelineJob)
	next := uint64(1)
	var seqSlot, nextSeq uint64
	for {
		select {
		case <-p.done:
			return
		case job := <-p.results:
			held[job.order] = job
		}
		for job, ok := held[next]; ok; job, ok = held[next] {
			delete(held, next)
			next++
			if job.lost {
				p.emit(StreamLostEvent{Slot: job.slot})
			} else {
				if seqSlot != job.slot {
					seqSlot, nextSeq = job.slot, 1
				}
				nextSeq = p.emitMessage(job, nextSeq)
			}
			p.inFlight.Add(-1)
		}
	}
}

// emitMessage splits a parsed message into bundles and emits them, numbered
// from seq. It returns the next sequence number.
func (p *BlockPipeline) emitMessage(job *pipelineJob, seq uint64) uint64 {
	for _, bundle := range segment(job.txs) {
		if err := validateBundle(job.txs, job.parsed, job.errs, bundle); err != nil {
			p.stats.invalid.Add(1)
			p.emit(InvalidBundleEvent{Slot: job.slot, Seq: seq, Arrival: job.arrival, Reason: err})
		} else {
			p.stats.bundles.Add(1)
			p.stats.transactions.Add(uint64(bundle.end - bundle.start))
			p.emit(BundleEvent{
				Slot:            job.slot,
				Seq:             seq,
				Arrival:         job.arrival,
				RevertProtected: job.txs[bundle.start].GetBundleId() != 0,
				Transactions:    append([]*solana.Transaction(nil), job.parsed[bundle.start:bundle.end]...),
			})
		}
		seq++
	}
	return seq
}

// parse parses every transaction of the batch's messages and verifies all
// their signatures together. errs[i] is set for a transaction that is
// unusable.
func (p *BlockPipeline) parse(batch []*pipelineJob, verifier *txverify.BatchVerifier) {
	var verify []*solana.Transaction
	type at struct{ job, tx int }
	var verifyAt []at
	for j, job := range batch {
		if job.lost {
			continue
		}
		job.parsed = make([]*solana.Transaction, len(job.txs))
		job.errs = make([]error, len(job.txs))
		for i, t := range job.txs {
			data := t.GetTransaction()
			if len(data) == 0 || len(data) > packet.DataSize {
				job.errs[i] = fmt.Errorf("transaction %d has %d bytes", i, len(data))
				continue
			}
			tx, err := sigverify.ParseTx(data)
			if err != nil {
				job.errs[i] = fmt.Errorf("transaction %d: %w", i, err)
				continue
			}
			if tx.Message.GetVersion() == solana.MessageVersionV1 && (p.txV1Enabled == nil || !p.txV1Enabled()) {
				job.errs[i] = fmt.Errorf("transaction %d: v1 transactions are not enabled", i)
				continue
			}
			job.parsed[i] = tx
			verify = append(verify, tx)
			verifyAt = append(verifyAt, at{j, i})
		}
	}
	verifyErrs := make([]error, len(verify))
	verifier.Verify(verify, verifyErrs)
	for k, err := range verifyErrs {
		if err != nil {
			a := verifyAt[k]
			batch[a.job].errs[a.tx] = fmt.Errorf("transaction %d: %w", a.tx, err)
		}
	}
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
