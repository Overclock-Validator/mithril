package replay

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/blockstream"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/Overclock-Validator/mithril/pkg/txverify"
	"github.com/gagliardetto/solana-go"
)

// Streaming execution executes a turbine block's entry batches while the rest
// of its shreds are still arriving, so that only the last batch and the
// end-of-block tail remain after the final shred. The complete block from the
// ordinary emission path stays the authority: the executor only pre-computes
// the bank overlay for a prefix of it, proves at finalize that the prefix is
// the block (pointer identity of every executed transaction, same parent slot
// and ID, same generation, same features), and otherwise throws the prefix
// away and lets the whole-block path execute the block from scratch.
//
// Nothing a stream does reaches process-global state until finalize: the
// overlay lives in the SlotCtx, vote-cache publication is deferred on the
// SlotCtx, program-cache insertions are recorded for undo, pending stake index
// entries are slot-keyed and dropped, the parent's VoteTimestamps map is
// cloned at open, the process-wide current slot is not published for the
// shell, and the legacy sysvar cache written by bank open is snapshotted and
// restored. Discard therefore restores the world to the state before the
// stream opened.

// StreamingExecutionConfig is set from the node flags before replay starts.
type StreamingExecutionConfig struct {
	// Enabled turns streaming execution on for turbine-sourced blocks.
	Enabled bool
	// Workers bounds the executor goroutines per group (0 = min(txpar, 4)).
	Workers int
	// MinGroupBatches delays a group until this many contiguous batches are
	// ready, unless the slot is already complete (0 or 1 = execute as soon as
	// one batch is ready).
	MinGroupBatches int
	// MaxOpenAge discards a stream that has been open this long without its
	// block completing (0 = 2 s).
	MaxOpenAge time.Duration
}

// StreamingExecutionCfg is the process-wide streaming configuration.
var StreamingExecutionCfg StreamingExecutionConfig

const (
	defaultStreamingWorkers = 4
	defaultStreamingMaxAge  = 2 * time.Second
	streamingPollInterval   = 5 * time.Millisecond
	// streamingHardOpenAgeFactor bounds a completed-but-not-yet-emitted stream
	// to this multiple of MaxOpenAge.
	streamingHardOpenAgeFactor = 10
)

func (cfg StreamingExecutionConfig) workers(txParallelism int) int {
	workers := cfg.Workers
	if workers <= 0 {
		workers = defaultStreamingWorkers
	}
	if txParallelism > 0 && workers > txParallelism {
		workers = txParallelism
	}
	return workers
}

func (cfg StreamingExecutionConfig) maxOpenAge() time.Duration {
	if cfg.MaxOpenAge <= 0 {
		return defaultStreamingMaxAge
	}
	return cfg.MaxOpenAge
}

// streamingFeed is the block source's view of the turbine feed
// (*blockstream.BlockSource implements it; tests substitute a fake).
type streamingFeed interface {
	StreamEvents() <-chan turbine.StreamEvent
	StreamStatusOf(turbine.StreamGeneration) turbine.StreamStatus
	PendingStreamBatches(turbine.StreamGeneration, uint32) []*turbine.StreamBatch
	PrioritizeStreamRepair(uint64)
}

var _ streamingFeed = (*blockstream.BlockSource)(nil)

// streamingDeps is what the executor needs from the replay loop. The closures
// read loop-local state (last slot context, frontier, features, switch
// status) at call time so the executor never caches a stale view.
type streamingDeps struct {
	acctsDb             *accountsdb.AccountsDb
	feed                streamingFeed
	epochSchedule       *sealevel.SysvarEpochSchedule
	txParallelism       int
	dbgOpts             *DebugOptions
	persistedHashes     *persistedTracker
	tail                unrootedState
	transactionStatuses *TransactionStatusCache
	alpenglowClock      bool

	lastSlotCtx      func() *sealevel.SlotCtx
	frontier         func() uint64
	currentFeatures  func() *features.Features
	currentEpoch     func() uint64
	rewardsInFlight  func() bool
	switchPending    func() bool
	executedBlockID  func(slot uint64) (solana.Hash, bool)
	alpenglowMode    bool
	unrootedTailUsed bool
}

type streamingGroup struct {
	startedAt, finishedAt time.Time
	transactions          int
}

// streamingSlot is one in-progress stream.
type streamingSlot struct {
	slot       uint64
	generation turbine.StreamGeneration
	parentSlot uint64
	parentID   solana.Hash
	exec       *blockExecution
	// origin holds the block's own transaction objects in executed order;
	// the bank executes stream-owned copies (see executionCopy), and the
	// handshake proves the block by these originals.
	origin     []*solana.Transaction
	nextStart  uint32
	pending    map[uint32]*turbine.StreamBatch
	footerSeen bool
	completed  bool
	openedAt   time.Time
	headerAt   time.Time
	groups     []streamingGroup
	// restoreSysvarCache puts the legacy process-global sysvar cache back to
	// its state before the bank opened; nil when nothing was published.
	restoreSysvarCache func()
}

// streamingExecutor is owned by the replay loop and driven from its select.
type streamingExecutor struct {
	deps    streamingDeps
	current *streamingSlot
	// headers remembers header batches for slots ahead of the frontier so the
	// next slot can open as soon as its parent finishes, even when its header
	// wake-up arrived earlier.
	headers map[uint64]*turbine.StreamBatch
	ticker  *time.Ticker
	// executeFn runs one group on the open execution; tests substitute it.
	executeFn func(exec *blockExecution, txs []*solana.Transaction, identities *b.PreparedTransactionMessageIdentities, shouldVerifySignatures bool) error
}

func newStreamingExecutor(deps streamingDeps) *streamingExecutor {
	return &streamingExecutor{
		deps:      deps,
		headers:   make(map[uint64]*turbine.StreamBatch),
		executeFn: (*blockExecution).executeTransactionGroup,
	}
}

// tick returns the polling channel, which is nil (never fires) while no
// stream is open, so the replay loop's select stays quiet when idle.
func (s *streamingExecutor) tick() <-chan time.Time {
	if s == nil || s.current == nil {
		return nil
	}
	if s.ticker == nil {
		s.ticker = time.NewTicker(streamingPollInterval)
	}
	return s.ticker.C
}

func (s *streamingExecutor) stopTicker() {
	if s.ticker != nil {
		s.ticker.Stop()
		s.ticker = nil
	}
}

// events is the feed channel the wait loop selects on; nil when the feed is
// off, which never fires.
func (s *streamingExecutor) events() <-chan turbine.StreamEvent {
	if s == nil || s.deps.feed == nil {
		return nil
	}
	return s.deps.feed.StreamEvents()
}

// matches reports whether a stream is open for slot.
func (s *streamingExecutor) matches(slot uint64) bool {
	return s != nil && s.current != nil && s.current.slot == slot
}

// shutdown discards any open stream; the replay loop defers it so an exiting
// attempt never leaves a speculative bank (and its watchdog) behind.
func (s *streamingExecutor) shutdown() {
	if s == nil {
		return
	}
	s.discard("shutdown")
	s.stopTicker()
	s.headers = make(map[uint64]*turbine.StreamBatch)
}

// handleEvent consumes one feed wake-up.
func (s *streamingExecutor) handleEvent(event turbine.StreamEvent) {
	if s == nil {
		return
	}
	switch event.Kind {
	case turbine.StreamBatchReady:
		if event.Batch == nil {
			return
		}
		if s.current != nil && event.Generation == s.current.generation {
			s.offer(event.Batch)
			s.consume()
			return
		}
		if event.Batch.Marker == turbine.StreamMarkerHeader && event.Batch.Start == 0 {
			s.rememberHeader(event.Batch)
		}
		s.tryOpen()
	case turbine.StreamCancelled:
		if s.current != nil && event.Generation == s.current.generation {
			s.discard("cancelled:" + event.Reason)
		}
		if header, ok := s.headers[event.Slot]; ok && header.Generation == event.Generation {
			delete(s.headers, event.Slot)
		}
		s.tryOpen()
	case turbine.StreamCompleted:
		if s.current != nil && event.Generation == s.current.generation {
			s.current.completed = true
			// Wake-ups for the slot's batches precede this event in the
			// channel, so everything decoded is already pending; run it now
			// (the group minimum no longer applies). Anything a dropped
			// wake-up missed runs in the finalize suffix: the prefetch state
			// is released at completion, so there is nothing left to pull.
			s.consume()
			return
		}
		if header, ok := s.headers[event.Slot]; ok && header.Generation == event.Generation {
			delete(s.headers, event.Slot)
		}
	}
}

// handleTick polls the assembler for batches (recovery after dropped
// wake-ups), enforces the open-age bound, and opens the next slot if idle.
func (s *streamingExecutor) handleTick() {
	if s == nil {
		return
	}
	if s.current == nil {
		s.tryOpen()
		return
	}
	cur := s.current
	switch s.deps.feed.StreamStatusOf(cur.generation) {
	case turbine.StreamGone:
		s.discard("gone")
		s.tryOpen()
		return
	case turbine.StreamDone:
		cur.completed = true
	}
	// An incomplete slot is bounded by MaxOpenAge (its shreds stopped
	// arriving); a completed one may legitimately wait longer in the emitter
	// (ancestry decisions) and is only bounded to cap the overlay's lifetime.
	age := time.Since(cur.openedAt)
	if (!cur.completed && age > StreamingExecutionCfg.maxOpenAge()) || age > streamingHardOpenAgeFactor*StreamingExecutionCfg.maxOpenAge() {
		s.discard("timeout")
		s.tryOpen()
		return
	}
	s.pull()
	s.consume()
}

func (s *streamingExecutor) rememberHeader(header *turbine.StreamBatch) {
	frontier := s.deps.frontier()
	if header.Slot <= frontier {
		return
	}
	s.headers[header.Slot] = header
	s.pruneHeaders(frontier)
}

// pruneHeaders bounds the header map: anything at or below the frontier can
// never open.
func (s *streamingExecutor) pruneHeaders(frontier uint64) {
	for slot := range s.headers {
		if slot <= frontier {
			delete(s.headers, slot)
		}
	}
}

// tryOpen opens a stream for frontier+1 when its header is known and every
// eligibility condition holds.
func (s *streamingExecutor) tryOpen() {
	if s == nil || s.current != nil || !StreamingExecutionCfg.Enabled {
		return
	}
	frontier := s.deps.frontier()
	s.pruneHeaders(frontier)
	next := frontier + 1
	header, ok := s.headers[next]
	if !ok {
		return
	}
	if reason := s.eligibility(header); reason != "" {
		mlog.Log.FileOnlyf("streaming: slot %d not opened (%s)", next, reason)
		delete(s.headers, next)
		return
	}
	delete(s.headers, next)
	s.openStream(header)
}

// eligibility returns an empty string when a stream may open on header, or
// the reason it may not.
func (s *streamingExecutor) eligibility(header *turbine.StreamBatch) string {
	d := s.deps
	if !d.alpenglowMode || !d.unrootedTailUsed || d.tail == nil {
		return "requires alpenglow rooted-durable replay"
	}
	if d.feed.StreamStatusOf(header.Generation) != turbine.StreamActive {
		return "generation no longer active"
	}
	last := d.lastSlotCtx()
	if last == nil {
		return "no executed parent context"
	}
	if frontier := d.frontier(); header.Slot != frontier+1 || header.ParentSlot != last.Slot {
		return fmt.Sprintf("slot %d on parent %d does not extend the executed frontier %d (parent context %d)", header.Slot, header.ParentSlot, frontier, last.Slot)
	}
	executedID, ok := d.executedBlockID(last.Slot)
	if !ok || executedID == (solana.Hash{}) || executedID != header.ParentBlockID {
		return "parent block id does not match the executed parent"
	}
	if d.switchPending() {
		return "fork switch pending"
	}
	if d.epochSchedule == nil || d.epochSchedule.GetEpoch(header.Slot) != d.currentEpoch() {
		return "epoch boundary"
	}
	if d.rewardsInFlight() {
		return "partitioned rewards in flight"
	}
	if d.currentFeatures() == nil {
		return "no feature set"
	}
	return ""
}

func (s *streamingExecutor) openStream(header *turbine.StreamBatch) {
	d := s.deps
	last := d.lastSlotCtx()
	shell := &b.Block{
		Slot:                      header.Slot,
		SourceParentSlot:          header.ParentSlot,
		FromLiveStream:            true,
		AlpenglowParentBlockID:    header.ParentBlockID,
		HasAlpenglowParentBlockID: true,
	}
	shell.Epoch = d.epochSchedule.GetEpoch(shell.Slot)
	// Same derivation the loop applies to the complete block, minus the
	// process-global "current slot" publication, which stays at the frontier
	// until the complete block is configured.
	if err := configureBlockFromParent(shell, last, d.epochSchedule, false); err != nil {
		mlog.Log.Warnf("streaming: slot %d not opened: %v", shell.Slot, err)
		return
	}
	// The parent's VoteTimestamps map is shared by reference through
	// configureBlock; a speculative bank must mutate its own copy.
	shell.VoteTimestamps = maps.Clone(last.VoteTimestamps)
	shell.Features = d.currentFeatures()

	// Bank open publishes the child's derived Clock/SlotHashes to the legacy
	// process-global sysvar cache (Alpenglow banks never read it back — they
	// pin from parentBankSysvars — but RPC simulation may). Snapshot it so a
	// discard restores the parent's view; the accepted bank leaves it as a
	// whole-block open would have.
	sysvarCacheAtOpen := sealevel.SysvarCache
	exec := newBlockExecution(d.acctsDb, shell, d.epochSchedule, StreamingExecutionCfg.workers(d.txParallelism), d.dbgOpts, d.persistedHashes, d.tail, d.transactionStatuses, d.alpenglowClock, last.BankSysvars())
	if err := exec.open(); err != nil {
		exec.close()
		sealevel.SysvarCache = sysvarCacheAtOpen
		mlog.Log.Warnf("streaming: slot %d not opened: %v", shell.Slot, err)
		return
	}
	exec.slotCtx.DeferVoteCachePublication = true
	exec.slotCtx.TrackProgramCacheAdds = true
	exec.setReplayStage("streaming_wait")

	s.current = &streamingSlot{
		slot:               shell.Slot,
		generation:         header.Generation,
		parentSlot:         header.ParentSlot,
		parentID:           header.ParentBlockID,
		exec:               exec,
		pending:            make(map[uint32]*turbine.StreamBatch),
		openedAt:           time.Now(),
		headerAt:           header.ReadyAt,
		restoreSysvarCache: func() { sealevel.SysvarCache = sysvarCacheAtOpen },
	}
	metrics.GlobalBlockReplay.StreamingExecution.Opened = 1
	d.feed.PrioritizeStreamRepair(shell.Slot)
	mlog.Log.FileOnlyf("streaming: opened slot %d on parent %d", shell.Slot, header.ParentSlot)
	s.offer(header)
	s.pull()
	s.consume()
}

func (s *streamingExecutor) offer(batch *turbine.StreamBatch) {
	cur := s.current
	if cur == nil || batch == nil || batch.Start < cur.nextStart {
		return
	}
	if _, seen := cur.pending[batch.Start]; !seen {
		cur.pending[batch.Start] = batch
	}
}

// pull asks the assembler for everything decoded since nextStart; it is the
// authoritative path after a dropped wake-up.
func (s *streamingExecutor) pull() {
	cur := s.current
	if cur == nil {
		return
	}
	for _, batch := range s.deps.feed.PendingStreamBatches(cur.generation, cur.nextStart) {
		s.offer(batch)
	}
}

// consume executes every contiguous ready batch from nextStart as one group.
// Nothing is removed from pending until the group is committed, so holding
// for the group minimum leaves markers and batches exactly where they were.
func (s *streamingExecutor) consume() {
	cur := s.current
	if cur == nil {
		return
	}
	var group []*turbine.StreamBatch
	next := cur.nextStart
	footer := false
	for {
		batch, ok := cur.pending[next]
		if !ok {
			break
		}
		if batch.Err != nil {
			s.discard("decode_error")
			return
		}
		switch batch.Marker {
		case turbine.StreamMarkerHeader:
			// The header opened the stream; nothing to execute.
		case turbine.StreamMarkerUpdateParent:
			// The leader abandoned the optimistic prefix we executed.
			s.discard("update_parent")
			return
		case turbine.StreamMarkerFooter:
			footer = true
		default:
			group = append(group, batch)
		}
		next = batch.End + 1
	}
	if minBatches := StreamingExecutionCfg.MinGroupBatches; minBatches > 1 && len(group) > 0 && len(group) < minBatches && !cur.completed {
		return // not enough ready work yet; everything stays pending
	}
	for start := cur.nextStart; start < next; {
		batch := cur.pending[start]
		delete(cur.pending, start)
		start = batch.End + 1
	}
	cur.nextStart = next
	if footer {
		cur.footerSeen = true
	}
	if len(group) == 0 {
		return
	}
	if err := s.executeGroup(group); err != nil {
		s.discard(err.Error())
	}
}

// executeGroup joins verification for every batch in the group and executes
// the group's transactions as one unit.
func (s *streamingExecutor) executeGroup(group []*turbine.StreamBatch) error {
	cur := s.current
	var txs []*solana.Transaction
	var verified []txverify.VerifiedMessageIdentity
	allVerified := true
	for _, batch := range group {
		identities, ok, err := batch.WaitVerification(context.Background())
		if errors.Is(err, turbine.ErrStreamBatchUnverified) {
			ok, err = false, nil
		}
		if err != nil {
			return fmt.Errorf("sigverify: %w", err)
		}
		if !ok {
			allVerified = false
		}
		txs = append(txs, batch.Transactions...)
		verified = append(verified, identities...)
	}
	if len(txs) == 0 {
		return nil
	}
	if !allVerified || len(verified) != len(txs) {
		// The assembler's verifier refused the batch (admission), so the
		// block-level verification at completion will cover it. Verifying here
		// through ProcessTransaction is not an option: that path halts the
		// process on an invalid signature, which a speculative bank on an
		// unauthenticated prefix must never do.
		return errors.New("unverified_batch")
	}
	// The identities are bound to the block's objects by the verifier; that
	// binding is checked here, on the originals, before the copies inherit it.
	preparedForOriginals, err := b.PrepareVerifiedTransactionMessageIdentities(txs, verified)
	if err != nil {
		return fmt.Errorf("identities: %w", err)
	}
	copies, err := executionCopies(txs)
	if err != nil {
		return fmt.Errorf("copies: %w", err)
	}
	prepared, err := preparedForOriginals.Rebind(copies)
	if err != nil {
		return fmt.Errorf("identities: %w", err)
	}
	started := time.Now()
	err = s.executeFn(cur.exec, copies, prepared, false)
	cur.exec.setReplayStage("streaming_wait")
	if err != nil {
		var duplicates *DuplicateTransactionMessagesError
		if errors.As(err, &duplicates) {
			return errors.New("duplicate_message")
		}
		if IsAlreadyProcessedTransactionError(err) {
			return errors.New("already_processed")
		}
		return fmt.Errorf("group: %w", err)
	}
	cur.origin = append(cur.origin, txs...)
	cur.groups = append(cur.groups, streamingGroup{startedAt: started, finishedAt: time.Now(), transactions: len(txs)})
	return nil
}

// errStreamInputResolved reports a batch whose transactions already carry
// address-table resolution; the assembler never produces one, and a stream
// must not execute an object whose account keys were derived elsewhere.
var errStreamInputResolved = errors.New("stream input is already resolved")

// executionCopy returns the object the stream executes in place of a block's
// own transaction. Execution resolves address-table lookups in place
// (SetAddressTables refuses a second call; ResolveLookups appends to
// AccountKeys), so running the block's object would leave it resolved
// against the stream's parent — and unusable, or worse, wrong, for the
// whole-block path after a discard. The copy takes the message by value with
// its own account-key slice; signatures and instructions are shared and never
// mutated by execution.
func executionCopy(tx *solana.Transaction) (*solana.Transaction, error) {
	if tx == nil {
		return nil, errors.New("nil transaction")
	}
	if tx.Message.GetVersion() == solana.MessageVersionV0 && tx.Message.IsResolved() {
		return nil, errStreamInputResolved
	}
	message := tx.Message
	message.AccountKeys = append(solana.PublicKeySlice(nil), tx.Message.AccountKeys...)
	return &solana.Transaction{Signatures: tx.Signatures, Message: message}, nil
}

func executionCopies(txs []*solana.Transaction) ([]*solana.Transaction, error) {
	copies := make([]*solana.Transaction, len(txs))
	for i, tx := range txs {
		dup, err := executionCopy(tx)
		if err != nil {
			return nil, fmt.Errorf("transaction %d: %w", i, err)
		}
		copies[i] = dup
	}
	return copies, nil
}

// discard throws the in-progress stream away and undoes every side effect it
// may have had outside its own SlotCtx.
func (s *streamingExecutor) discard(reason string) {
	if s == nil || s.current == nil {
		return
	}
	cur := s.current
	s.current = nil
	s.stopTicker()
	exec := cur.exec
	if exec != nil {
		exec.close()
		if exec.slotCtx != nil {
			exec.slotCtx.TrackProgramCacheAdds = false
			for _, key := range exec.slotCtx.TakeProgramCacheAdds() {
				if s.deps.acctsDb != nil {
					s.deps.acctsDb.RemoveProgramFromCache(key)
				}
			}
			// Deferred vote-cache changes die with the SlotCtx; the stake index
			// entries are keyed by slot and the stream is the only bank above
			// the frontier.
			exec.slotCtx.PendingVoteCache = nil
			exec.slotCtx.PendingVoteCacheDeletes = nil
			exec.slotCtx.VoteStakeDirty = false
		}
	}
	global.DropPendingStakePubkeysFrom(cur.slot)
	if cur.restoreSysvarCache != nil {
		cur.restoreSysvarCache()
	}
	metrics.GlobalBlockReplay.StreamingExecution.Discarded = 1
	metrics.GlobalBlockReplay.StreamingExecution.DiscardReason = reason
	mlog.Log.FileOnlyf("streaming: discarded slot %d after %d groups (%s)", cur.slot, len(cur.groups), reason)
}

// discardSlot discards the stream if it is open for slot.
func (s *streamingExecutor) discardSlot(slot uint64, reason string) {
	if s.matches(slot) {
		s.discard(reason)
	}
}

// streamingFinalizeError marks a failure after the handshake passed; the
// block is as invalid as it would have been for the whole-block path.
type streamingFinalizeError struct{ err error }

func (e *streamingFinalizeError) Error() string { return e.err.Error() }
func (e *streamingFinalizeError) Unwrap() error { return e.err }

// finalize completes execution of block on the open stream. ok reports
// whether the stream matched the block; when it did not, the stream has been
// discarded and the caller must execute the block whole. A non-nil error with
// ok == true is a failure after the handshake and is final for the block,
// exactly as a ProcessBlock error is.
//
// Ownership: the stream keeps owning its bank (s.current) until the tail has
// committed, so every failure path after the handshake goes through the same
// discard as a pre-handshake mismatch — program-cache insertions evicted,
// unpublished vote-cache entries dropped, slot-keyed stake entries dropped,
// the legacy sysvar cache restored, the execution closed. The one publication
// that precedes the tail is the deferred vote cache, applied at the point
// where whole-block execution would already have written it (before fees,
// rent, footer and bank hash); a failure inside the tail therefore leaves the
// same footprint a whole-block tail failure leaves, and the dirty marker it
// sets is what forces the rooted-checkpoint re-replay on recovery.
func (s *streamingExecutor) finalize(block *b.Block, parentBankSysvars *sealevel.BankSysvars) (slotCtx *sealevel.SlotCtx, ok bool, err error) {
	if s == nil || s.current == nil || block == nil {
		return nil, false, nil
	}
	cur := s.current
	if reason := s.handshake(block, parentBankSysvars); reason != "" {
		s.discard("prefix_mismatch:" + reason)
		return nil, false, nil
	}
	exec := cur.exec
	fullAt := time.Time{}
	if block.ShredFullNanos > 0 {
		fullAt = time.Unix(0, block.ShredFullNanos)
	}

	// Whole-block plan and status validation, exactly as ProcessBlock does
	// them, now that the authoritative block exists. A failure here is not yet
	// a verdict on the block: the whole-block path re-derives it.
	if err := validateBlockTransactionVersions(block); err != nil {
		s.discard("versions")
		return nil, false, nil
	}
	executionPlanStart := time.Now()
	executionPlan, err := planBlockTransactionExecution(block)
	metrics.GlobalBlockReplay.TransactionExecutionPlan.AddTimingSince(executionPlanStart)
	if err != nil {
		s.discard("plan")
		return nil, false, nil
	}
	executed := len(cur.origin)
	if len(exec.transactions) != executed {
		s.discard("prefix_bookkeeping")
		return nil, false, nil
	}
	for i := 0; i < executed; i++ {
		if executionPlan.execute[i] != exec.execute[i] {
			s.discard("execution_mask")
			return nil, false, nil
		}
	}
	statusValidationStart := time.Now()
	statusValidation, statusValidationErr := s.deps.transactionStatuses.validateBlockForPublication(block, executionPlan)
	metrics.GlobalBlockReplay.TransactionStatusValidation.AddTimingSince(statusValidationStart)
	if statusValidationErr != nil {
		s.discard("status_validation")
		return nil, false, nil
	}
	statusPreparation := s.deps.transactionStatuses.startStatusPreparation(executionPlan)
	defer func() {
		statusPreparation.wait()
		if statusPreparation != nil {
			metrics.GlobalBlockReplay.TransactionStatusPreparation.AddTiming(statusPreparation.duration)
		}
	}()

	// From here on the stream is committed to this block: any failure is the
	// block's failure. fail undoes the stream's side effects and reports it.
	s.stopTicker()
	fail := func(reason string, err error) (*sealevel.SlotCtx, bool, error) {
		s.discard("finalize:" + reason)
		return nil, true, &streamingFinalizeError{err: err}
	}
	block.FeeRateGovernor = exec.block.FeeRateGovernor
	block.VoteTimestamps = exec.slotCtx.VoteTimestamps
	exec.block = block
	exec.slotCtx.Blockhash = block.Blockhash
	exec.slotCtx.Epoch = block.Epoch
	if requireAlpenglowBlockFooter(block, exec.slotCtx, s.deps.alpenglowClock) {
		if err := validateAlpenglowFooterNanosecondClock(exec.slotCtx, block); err != nil {
			return fail("footer_clock", err)
		}
	}
	if suffix := block.Transactions[executed:]; len(suffix) > 0 {
		started := time.Now()
		err := s.executeFn(exec, suffix, executionPlan.messageIdentities.Slice(executed, len(block.Transactions)), !block.TransactionSignaturesVerified())
		if err != nil {
			return fail("suffix", fmt.Errorf("execute block suffix at slot %d: %w", block.Slot, err))
		}
		cur.groups = append(cur.groups, streamingGroup{startedAt: started, finishedAt: time.Now(), transactions: len(suffix)})
	}
	if exec.processedSignatures != executionPlan.processedSignatures || exec.processedTxCount != executionPlan.processedTxCount {
		return fail("counts", fmt.Errorf("streaming execution at slot %d processed %d transactions/%d signatures, block plan has %d/%d",
			block.Slot, exec.processedTxCount, exec.processedSignatures, executionPlan.processedTxCount, executionPlan.processedSignatures))
	}
	exec.slotCtx.NumSignatures = executionPlan.processedSignatures

	// Acceptance of the executed transactions: publish what execution would
	// have published as it ran, then run the unchanged tail. Program-cache
	// insertions stay tracked until the tail commits so a tail failure can
	// still evict them.
	publishDeferredVoteCache(exec.slotCtx)
	exec.executionPlan = executionPlan
	exec.statusPreparation = statusPreparation
	exec.statusValidation = statusValidation
	slotCtx, err = exec.finalize()
	if err != nil {
		return fail("tail", err)
	}
	exec.slotCtx.TrackProgramCacheAdds = false
	exec.slotCtx.TakeProgramCacheAdds()
	s.current = nil
	exec.close()

	// The per-block record is rebuilt from the stream's own bookkeeping: the
	// loop resets the collector between waits, so counters accumulated while
	// executing groups may or may not have survived to this point.
	record := &metrics.GlobalBlockReplay.StreamingExecution
	discarded, discardReason := record.Discarded, record.DiscardReason
	*record = metrics.StreamingExecution{Opened: 1, Discarded: discarded, DiscardReason: discardReason}
	record.Groups = uint64(len(cur.groups))
	for _, group := range cur.groups {
		record.Transactions += uint64(group.transactions)
		// Work that finished before the last shred arrived is the latency the
		// stream took off the vote path.
		if !fullAt.IsZero() && group.finishedAt.Before(fullAt) {
			record.TxLoopBeforeFull.AddTiming(group.finishedAt.Sub(group.startedAt))
		}
	}
	record.OpenDelay.AddTiming(cur.openedAt.Sub(cur.headerAt))
	return slotCtx, true, nil
}

// handshake proves the executed prefix is the block. It returns the mismatch
// reason, or "" when every binding holds.
func (s *streamingExecutor) handshake(block *b.Block, parentBankSysvars *sealevel.BankSysvars) string {
	cur := s.current
	exec := cur.exec
	switch {
	case block.Slot != cur.slot:
		return "slot"
	case block.IsSkipped || !block.FromLiveStream:
		return "not a live block"
	case !block.HasAlpenglowParentBlockID || block.AlpenglowParentBlockID != cur.parentID || block.SourceParentSlot != cur.parentSlot:
		return "parent"
	case block.ParentSlot != exec.block.ParentSlot || block.ParentBankhash != exec.block.ParentBankhash:
		return "configured parent"
	case block.Features != exec.block.Features:
		return "features"
	case parentBankSysvars == nil || parentBankSysvars != exec.parentBankSysvars:
		return "parent sysvars"
	case block.Epoch != exec.block.Epoch:
		return "epoch"
	case len(block.EpochUpdatedAccts) != 0:
		return "epoch account updates"
	case len(block.Transactions) < len(cur.origin):
		return "shorter than executed prefix"
	}
	status := s.deps.feed.StreamStatusOf(cur.generation)
	if status == turbine.StreamGone {
		return "generation gone"
	}
	for i, tx := range cur.origin {
		if block.Transactions[i] != tx {
			return fmt.Sprintf("transaction %d identity", i)
		}
	}
	return ""
}
