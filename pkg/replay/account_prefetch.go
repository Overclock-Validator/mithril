package replay

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
)

// Account prefetch (on by default; MITHRIL_ACCOUNT_PREFETCH=0 turns it off)
// reads the accounts of the block queued after the one replay is executing, and loads and verifies the programs
// it invokes, through the same unrooted-tail source replay loads from. Results
// are discarded: durable reads publish into AccountsDB's read caches and the
// program cache, so the block's own load finds them there. It only uses the
// ordinary read paths and every cached program is still checked against its
// exact source and features before use, so it cannot change what replay
// reads or executes.
//
// MITHRIL_TIP_WARM=1 (with prefetch) also warms each decoded turbine batch of
// the executing and next slot as soon as it is decoded: near the tip a block
// completes only at its last shred, while the next block's batches arrive
// during a long block.
//
// Transactions are never read once the block can be executing: streaming
// execution resolves address tables into the same messages. Turbine summarizes
// every decoded batch (turbine.StreamAccountRefs) before publishing it, and
// whole-block jobs for such blocks are built from those summaries.
var (
	accountPrefetchEnabled = os.Getenv("MITHRIL_ACCOUNT_PREFETCH") != "0"
	tipWarmEnabled         = os.Getenv("MITHRIL_TIP_WARM") == "1"
)

const (
	// accountPrefetchAhead is how many slots past the executing one are warmed.
	accountPrefetchAhead = 1
	// accountPrefetchWorkers bounds the storage goroutines of one warm-up read,
	// so it takes one or two cores from the transaction loop, not all of them.
	accountPrefetchWorkers      = 2
	accountPrefetchQueue        = 128
	accountPrefetchPartialQueue = 1024
	accountPrefetchMergeMax     = 64 // queued batches of one slot merged into one read
	accountPrefetchLogEvery     = 500
	// accountPrefetchStreamedKeep is how many slots a streamed mark outlives
	// the executing slot, and how far past it marks are recorded (before
	// replay begins, how many marks are recorded at most).
	accountPrefetchStreamedKeep = 4096
	// accountPrefetchRefsWindow bounds how far past the executing slot batch
	// summaries are kept (catch-up delivers blocks up to 256 slots ahead).
	// During a long catch-up the live edge keeps decoding blocks far ahead of
	// replay; without the bound their summaries would accumulate until replay
	// reached them.
	accountPrefetchRefsWindow = 256
)

type accountPrefetcher struct {
	src     blockAccountSource
	db      *accountsdb.AccountsDb // program cache, for preloading; nil disables it
	ahead   uint64
	workers int
	tipWarm bool

	features atomic.Pointer[features.Features] // of the bank replay last began
	started  atomic.Uint64                     // slot replay most recently began

	jobs    chan accountPrefetchJob
	partial chan partialPrefetch // tip warming, served before jobs
	turn    chan struct{}        // replay began a slot
	ctx     context.Context      // cancelled by Close, which waits for exited
	cancel  context.CancelFunc
	exited  chan struct{}

	// refs holds each slot's batch summaries from the decode workers until the
	// block is offered (decode workers and the block source). streamed marks
	// slots that had a decoded batch, even after Started dropped their
	// summaries. The block's FromLiveStream flag remains authoritative when
	// these bounded, consumable marks are gone. startedLocked
	// is started as seen under refsMu, so Offer and Started agree on it.
	refsMu        sync.Mutex
	refs          map[uint64][]*turbine.StreamAccountRefs
	streamed      map[uint64]struct{}
	startedLocked uint64
	// streamedFloor: no streamed mark is below it (Started prunes up to it).
	streamedFloor uint64

	// Keys and programs tip warming already warmed per slot (worker only).
	partialSeen, partialPrograms map[uint64]map[solana.PublicKey]struct{}

	offered, dropped, late, warmed                                                        atomic.Uint64
	partialOffered, partialDropped, partialStale, partialWarmed, partialKeys, tipFiltered atomic.Uint64
	keys, durable, storage, nanos                                                         atomic.Uint64
	programs, preloaded                                                                   atomic.Uint64
}

// accountPrefetchJob is a block's account references, copied from data
// nothing else mutates.
type accountPrefetchJob struct {
	slot    uint64
	keys    []solana.PublicKey
	tables  []solana.PublicKey
	lookups []accountPrefetchLookup
	// programs are the block's top-level program ids, always static keys.
	programs []solana.PublicKey
}

// partialPrefetch is one decoded turbine batch's references (tip warming).
type partialPrefetch struct {
	slot uint64
	refs *turbine.StreamAccountRefs
}

type accountPrefetchLookup struct {
	table   int
	indexes []uint8
}

func newAccountPrefetcher(src blockAccountSource, db *accountsdb.AccountsDb, ahead uint64, workers int, tipWarm bool) *accountPrefetcher {
	ctx, cancel := context.WithCancel(context.Background())
	p := &accountPrefetcher{
		src:             src,
		db:              db,
		tipWarm:         tipWarm,
		ahead:           max(ahead, 1),
		workers:         workers,
		jobs:            make(chan accountPrefetchJob, accountPrefetchQueue),
		partial:         make(chan partialPrefetch, accountPrefetchPartialQueue),
		turn:            make(chan struct{}, 1),
		ctx:             ctx,
		cancel:          cancel,
		exited:          make(chan struct{}),
		refs:            make(map[uint64][]*turbine.StreamAccountRefs),
		streamed:        make(map[uint64]struct{}),
		partialSeen:     make(map[uint64]map[solana.PublicKey]struct{}),
		partialPrograms: make(map[uint64]map[solana.PublicKey]struct{}),
	}
	go p.run()
	return p
}

// installRefsHook routes decoded turbine batches to OfferStreamRefs and returns
// the function that removes the hook.
func (p *accountPrefetcher) installRefsHook() func() {
	turbine.SetStreamRefsHook(p.OfferStreamRefs)
	return func() { turbine.SetStreamRefsHook(nil) }
}

// OfferStreamRefs is the decode workers' hook, called before a batch is
// published. It records the batch for its block's whole-block job and, with
// tip warming, queues it for warming. It never blocks.
func (p *accountPrefetcher) OfferStreamRefs(slot uint64, refs *turbine.StreamAccountRefs) {
	if refs == nil {
		return
	}
	p.refsMu.Lock()
	// Marks are bounded like the summaries, so decode far ahead of replay
	// (catch-up) cannot grow the map. A block decoded beyond the bound still
	// cannot be read from its transactions: the assembler attaches its
	// completion summary, which Offer uses first, and a live block carries
	// FromLiveStream.
	if slot >= p.streamedFloor && (p.startedLocked == 0 && len(p.streamed) < accountPrefetchStreamedKeep ||
		p.startedLocked != 0 && slot <= p.startedLocked+accountPrefetchStreamedKeep) {
		p.streamed[slot] = struct{}{}
	}
	if slot > p.startedLocked && (p.startedLocked == 0 || slot <= p.startedLocked+accountPrefetchRefsWindow) {
		p.refs[slot] = append(p.refs[slot], refs)
	}
	p.refsMu.Unlock()
	if !p.tipWarm {
		return
	}
	p.partialOffered.Add(1)
	select {
	case p.partial <- partialPrefetch{slot: slot, refs: refs}:
	default:
		p.partialDropped.Add(1)
	}
}

// Offer is the block source's emit hook. It queues the block's references,
// dropping the block if the queue is full.
func (p *accountPrefetcher) Offer(blk *b.Block) {
	if len(blk.Transactions) == 0 {
		return
	}
	p.refsMu.Lock()
	refs := p.refs[blk.Slot]
	delete(p.refs, blk.Slot)
	_, streamed := p.streamed[blk.Slot]
	delete(p.streamed, blk.Slot)
	late := blk.Slot <= p.startedLocked
	p.refsMu.Unlock()
	p.offered.Add(1)
	if late {
		// Replay (or a stream) already began it: not worth warming, and its
		// transactions may be in use.
		p.late.Add(1)
		return
	}
	if len(p.jobs) == cap(p.jobs) {
		p.dropped.Add(1)
		return
	}
	var job accountPrefetchJob
	switch {
	case blk.AccountPrefetchRefs() != nil:
		// Completion owns this immutable copy even when decode ran beyond
		// the live-batch retention window. Never reread shared messages.
		job = mergeRefs(blk.Slot, []*turbine.StreamAccountRefs{blk.AccountPrefetchRefs()}, nil, nil)
	case len(refs) > 0:
		job = mergeRefs(blk.Slot, refs, nil, nil)
	case streamed || blk.FromLiveStream:
		// Its summaries are gone; its transactions are a stream's.
		p.late.Add(1)
		return
	default:
		// The block is not from the live stream and has no decoded-batch
		// marker. Copy its references before the block source hands it to
		// replay; missing summaries alone do not establish ownership.
		job = snapshotAccountPrefetchJob(blk)
	}
	select {
	case p.jobs <- job:
	default:
		p.dropped.Add(1)
	}
}

// Started records that replay began executing slot: blocks at or below it are
// no longer worth warming, and blocks up to slot+ahead may now be warmed.
func (p *accountPrefetcher) Started(slot uint64) {
	p.refsMu.Lock()
	p.started.Store(slot)
	p.startedLocked = slot
	for s := range p.refs {
		if s <= slot {
			delete(p.refs, s)
		}
	}
	// Marks are dropped only long after their slot, so one that replay passed
	// and then returned to (a rewind) is still known as streamed. Only the
	// slots replay advanced past are visited, not every mark.
	var floor uint64
	if slot > accountPrefetchStreamedKeep {
		floor = slot - accountPrefetchStreamedKeep
	}
	if floor > p.streamedFloor && floor-p.streamedFloor > uint64(len(p.streamed)) {
		for s := range p.streamed {
			if s < floor {
				delete(p.streamed, s)
			}
		}
	} else {
		for s := p.streamedFloor; s < floor; s++ {
			delete(p.streamed, s)
		}
	}
	p.streamedFloor = floor
	p.refsMu.Unlock()
	select {
	case p.turn <- struct{}{}:
	default:
	}
}

// Close stops the worker, cancelling any read in flight, and waits for it, so
// AccountsDB never closes under a prefetch read.
func (p *accountPrefetcher) Close() {
	p.cancel()
	<-p.exited
}

// SetFeatures records the feature set of the bank replay is beginning, for
// program preloading. It keeps a clone, because the caller's map can be
// mutated in place at an epoch boundary.
func (p *accountPrefetcher) SetFeatures(f *features.Features) {
	if p.db == nil || f == nil {
		return
	}
	p.features.Store(f.Clone())
}

// snapshotAccountPrefetchJob copies a block's references from its transactions.
// Only for blocks no stream shares (see Offer).
func snapshotAccountPrefetchJob(blk *b.Block) accountPrefetchJob {
	txs := make([]*solana.Transaction, 0, len(blk.Transactions))
	for _, tx := range blk.Transactions {
		if tx != nil {
			txs = append(txs, tx)
		}
	}
	return mergeRefs(blk.Slot, []*turbine.StreamAccountRefs{turbine.SummarizeAccountRefs(txs)}, nil, nil)
}

// mergeRefs builds one job from batch summaries, deduplicating keys, programs
// and tables, and skipping keys and programs already in seen and seenPrograms
// (which it extends when non-nil).
func mergeRefs(slot uint64, refs []*turbine.StreamAccountRefs, seen, seenPrograms map[solana.PublicKey]struct{}) accountPrefetchJob {
	if seen == nil {
		seen = make(map[solana.PublicKey]struct{})
	}
	if seenPrograms == nil {
		seenPrograms = make(map[solana.PublicKey]struct{})
	}
	job := accountPrefetchJob{slot: slot}
	tableIdx := make(map[solana.PublicKey]int)
	for _, r := range refs {
		for _, key := range r.Keys {
			if _, ok := seen[key]; !ok {
				seen[key] = struct{}{}
				job.keys = append(job.keys, key)
			}
		}
		for _, program := range r.Programs {
			if _, ok := seenPrograms[program]; !ok {
				seenPrograms[program] = struct{}{}
				job.programs = append(job.programs, program)
			}
		}
		for _, lookup := range r.Lookups {
			if lookup.Table >= len(r.Tables) {
				continue
			}
			table := r.Tables[lookup.Table]
			idx, ok := tableIdx[table]
			if !ok {
				idx = len(job.tables)
				tableIdx[table] = idx
				job.tables = append(job.tables, table)
			}
			job.lookups = append(job.lookups, accountPrefetchLookup{table: idx, indexes: lookup.Indexes})
		}
	}
	return job
}

// run serves tip-warming batches first, then whole-block jobs once their slot
// is within the lookahead. A job waiting for its turn never blocks batches.
func (p *accountPrefetcher) run() {
	defer close(p.exited)
	var pending *accountPrefetchJob
	for {
		select {
		case part := <-p.partial:
			p.warmPartial(part)
			continue
		default:
		}
		if pending != nil {
			started := p.started.Load()
			switch {
			case pending.slot <= started:
				p.late.Add(1)
				pending = nil
				continue
			case pending.slot <= started+p.ahead:
				p.warmJob(p.withoutTipWarmed(*pending), true)
				pending = nil
				continue
			}
		}
		var jobs <-chan accountPrefetchJob
		if pending == nil {
			jobs = p.jobs
		}
		select {
		case <-p.ctx.Done():
			return
		case part := <-p.partial:
			p.warmPartial(part)
		case job := <-jobs:
			pending = &job
		case <-p.turn:
		}
	}
}

// withoutTipWarmed drops from a whole-block job the keys and programs tip
// warming already warmed for its slot. Worker goroutine only.
func (p *accountPrefetcher) withoutTipWarmed(job accountPrefetchJob) accountPrefetchJob {
	seen, seenPrograms := p.partialSeen[job.slot], p.partialPrograms[job.slot]
	if len(seen) == 0 && len(seenPrograms) == 0 {
		return job
	}
	keys := make([]solana.PublicKey, 0, len(job.keys))
	for _, key := range job.keys {
		if _, ok := seen[key]; !ok {
			keys = append(keys, key)
		}
	}
	programs := make([]solana.PublicKey, 0, len(job.programs))
	for _, program := range job.programs {
		if _, ok := seenPrograms[program]; !ok {
			programs = append(programs, program)
		}
	}
	p.tipFiltered.Add(uint64(len(job.keys) - len(keys)))
	job.keys, job.programs = keys, programs
	return job
}

// warmPartial warms one decoded batch, together with any other queued batches
// of the same slot (one read instead of one per batch), if the slot is the one
// replay is executing or within the lookahead.
func (p *accountPrefetcher) warmPartial(part partialPrefetch) {
	parts := []partialPrefetch{part}
	for len(parts) < accountPrefetchMergeMax {
		select {
		case next := <-p.partial:
			if next.slot == part.slot {
				parts = append(parts, next)
				continue
			}
			p.warmPartialBatches(parts)
			parts, part = []partialPrefetch{next}, next
			continue
		default:
		}
		break
	}
	p.warmPartialBatches(parts)
}

// warmPartialBatches warms the not yet warmed references of batches of one slot.
func (p *accountPrefetcher) warmPartialBatches(parts []partialPrefetch) {
	slot, started := parts[0].slot, p.started.Load()
	if p.ctx.Err() != nil || slot < started || slot > started+p.ahead {
		p.partialStale.Add(uint64(len(parts)))
		return
	}
	refs := make([]*turbine.StreamAccountRefs, len(parts))
	for i, pp := range parts {
		refs[i] = pp.refs
	}
	job := mergeRefs(slot, refs, partialSet(p.partialSeen, slot, started), partialSet(p.partialPrograms, slot, started))
	if len(job.keys) == 0 && len(job.lookups) == 0 && len(job.programs) == 0 {
		return
	}
	p.partialWarmed.Add(uint64(len(parts)))
	p.partialKeys.Add(uint64(len(job.keys)))
	p.warmJob(job, false)
}

// partialSet returns sets[slot], creating it, after dropping the sets of slots
// before started.
func partialSet(sets map[uint64]map[solana.PublicKey]struct{}, slot, started uint64) map[solana.PublicKey]struct{} {
	for s := range sets {
		if s < started {
			delete(sets, s)
		}
	}
	set := sets[slot]
	if set == nil {
		set = make(map[solana.PublicKey]struct{})
		sets[slot] = set
	}
	return set
}

// warmJob reads job's accounts (and address-table selections) and preloads
// its programs; whole marks a whole-block job, which the statistics count.
func (p *accountPrefetcher) warmJob(job accountPrefetchJob, whole bool) {
	start := time.Now()
	ctx := accountsdb.WithBatchWorkerLimit(p.ctx, p.workers)
	keys := job.keys
	if len(job.tables) > 0 {
		keys = append(keys[:len(keys):len(keys)], p.lookupTableKeys(ctx, job)...)
	}
	_, stats, err := getAccountsBatchSharedWithStats(ctx, p.src, job.slot, keys)
	if err != nil {
		return
	}
	if p.db != nil && len(job.programs) > 0 {
		p.preloadPrograms(ctx, job)
	}
	if !whole {
		return
	}
	p.keys.Add(uint64(len(keys)))
	p.durable.Add(stats.DurableKeys)
	p.storage.Add(stats.IndexHits + stats.IndexMisses)
	p.nanos.Add(uint64(time.Since(start).Nanoseconds()))
	if n := p.warmed.Add(1); n%accountPrefetchLogEvery == 0 {
		mlog.Log.Infof("account prefetch: %d blocks warmed, %d late, %d dropped of %d offered | per block %.0f keys, %.0f past the working set, %.0f to storage, %.2f ms | programs %.1f per block, %.2f preloaded | tip warming: %d batches warmed (%d keys), %d stale, %d dropped of %d offered, %d whole-block keys skipped",
			n, p.late.Load(), p.dropped.Load(), p.offered.Load(),
			float64(p.keys.Load())/float64(n), float64(p.durable.Load())/float64(n),
			float64(p.storage.Load())/float64(n), float64(p.nanos.Load())/float64(n)/1e6,
			float64(p.programs.Load())/float64(n), float64(p.preloaded.Load())/float64(n),
			p.partialWarmed.Load(), p.partialKeys.Load(), p.partialStale.Load(), p.partialDropped.Load(), p.partialOffered.Load(), p.tipFiltered.Load())
	}
}

// preloadPrograms compiles the executables of the block's top-level programs
// that the program cache does not already hold for their current source.
func (p *accountPrefetcher) preloadPrograms(ctx context.Context, job accountPrefetchJob) {
	f := p.features.Load()
	if f == nil {
		return
	}
	accts, _, err := getAccountsBatchSharedWithStats(ctx, p.src, job.slot, job.programs)
	if err != nil || len(accts) != len(job.programs) {
		return
	}
	readProgramData := func(key solana.PublicKey) *accounts.Account {
		out, _, err := getAccountsBatchSharedWithStats(ctx, p.src, job.slot, []solana.PublicKey{key})
		if err != nil || len(out) != 1 {
			return nil
		}
		return out[0]
	}
	for i, acct := range accts {
		cacheKey, source, ok := sealevel.PreloadSource(job.programs[i], acct, job.slot, readProgramData)
		if !ok {
			continue
		}
		p.programs.Add(1)
		if sealevel.PreloadProgram(p.db, cacheKey, source, f) {
			p.preloaded.Add(1)
		}
	}
}

// lookupTableKeys returns the addresses the block's lookups select from the
// current table contents, without the activity and bounds rules replay
// applies: a table the next block extends or closes only makes the warm-up
// less useful, never replay less correct. Keys already in job.keys are kept
// out, so the batch stays deduplicated.
func (p *accountPrefetcher) lookupTableKeys(ctx context.Context, job accountPrefetchJob) []solana.PublicKey {
	accts, _, err := getAccountsBatchSharedWithStats(ctx, p.src, job.slot, job.tables)
	if err != nil || len(accts) != len(job.tables) {
		return nil
	}
	tables := make([]solana.PublicKeySlice, len(job.tables))
	for i, acct := range accts {
		if acct == nil || acct.Lamports == 0 || acct.Owner != addresses.AddressLookupTableAddr {
			continue
		}
		if table, err := sealevel.UnmarshalAddressLookupTable(acct.Data); err == nil {
			tables[i] = table.Addresses
		}
	}
	seen := make(map[solana.PublicKey]struct{}, len(job.keys))
	for _, key := range job.keys {
		seen[key] = struct{}{}
	}
	var out []solana.PublicKey
	for _, lookup := range job.lookups {
		addrs := tables[lookup.table]
		for _, idx := range lookup.indexes {
			if int(idx) >= len(addrs) {
				continue
			}
			if _, ok := seen[addrs[idx]]; !ok {
				seen[addrs[idx]] = struct{}{}
				out = append(out, addrs[idx])
			}
		}
	}
	return out
}
