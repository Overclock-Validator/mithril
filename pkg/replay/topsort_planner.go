package replay

import (
	"fmt"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/util"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

// Type wrappers around indices.
type tx int

func canDeriveAccountsFromMessage(t *solana.Transaction) bool {
	if t.Message.IsResolved() {
		return true
	}
	if len(t.Message.AccountKeys) == 0 {
		return false
	}
	return !t.Message.IsVersioned() || t.Message.AddressTableLookups.NumLookups() == 0
}

func messageAccountLayout(msg *solana.Message) (numStaticAccounts, numWritableLookupAccounts int) {
	numStaticAccounts = len(msg.AccountKeys)
	if msg.IsResolved() {
		numStaticAccounts -= msg.NumLookups()
		numWritableLookupAccounts = msg.GetAddressTableLookups().NumWritableLookups()
	}
	return numStaticAccounts, numWritableLookupAccounts
}

func messageAccountIsWritable(msg *solana.Message, idx, numStaticAccounts, numWritableLookupAccounts int) bool {
	switch {
	case idx >= numStaticAccounts:
		return idx-numStaticAccounts < numWritableLookupAccounts
	case idx >= int(msg.Header.NumRequiredSignatures):
		numUnsignedWritable := (numStaticAccounts - int(msg.Header.NumRequiredSignatures)) - int(msg.Header.NumReadonlyUnsignedAccounts)
		return idx-int(msg.Header.NumRequiredSignatures) < numUnsignedWritable
	default:
		return idx < int(msg.Header.NumRequiredSignatures-msg.Header.NumReadonlySignedAccounts)
	}
}

func messageWritableAccounts(msg *solana.Message) []solana.PublicKey {
	numStaticAccounts, numWritableLookupAccounts := messageAccountLayout(msg)
	accounts := make([]solana.PublicKey, 0, len(msg.AccountKeys))

	for idx, account := range msg.AccountKeys {
		if messageAccountIsWritable(msg, idx, numStaticAccounts, numWritableLookupAccounts) {
			accounts = append(accounts, account)
		}
	}

	return util.DedupePubkeys(accounts)
}

func messageReadonlyAccounts(msg *solana.Message) []solana.PublicKey {
	writable := make(map[solana.PublicKey]struct{}, len(msg.AccountKeys))
	for _, account := range messageWritableAccounts(msg) {
		writable[account] = struct{}{}
	}

	accounts := make([]solana.PublicKey, 0, len(msg.AccountKeys))
	for _, account := range msg.AccountKeys {
		if _, isWritable := writable[account]; isWritable {
			continue
		}
		accounts = append(accounts, account)
	}

	return util.DedupePubkeys(accounts)
}

func canUseDependencyPlanner(b *block.Block) bool {
	for i, tx := range b.Transactions {
		if i < len(b.TxMetas) && plannerShouldUseMeta(tx, b.TxMetas[i]) {
			continue
		}
		if canDeriveAccountsFromMessage(tx) {
			continue
		}
		return false
	}
	return true
}

// plannerTransactionAccounts is the planner's immutable, compact view of one
// transaction. Accounts before readonlyEnd are read-only; the remainder are
// writable. Keeping only public keys and one boundary avoids preserving a full
// transaction (and its metadata) while address-table resolution mutates the
// execution block.
type plannerTransactionAccounts struct {
	accounts    []plannerAccountAccess
	readonlyEnd int
}

type plannerAccountAccess struct {
	publicKey solana.PublicKey
	writable  bool
}

func (a plannerTransactionAccounts) readonly() []plannerAccountAccess {
	return a.accounts[:a.readonlyEnd]
}

func (a plannerTransactionAccounts) writable() []plannerAccountAccess {
	return a.accounts[a.readonlyEnd:]
}

type plannerTransactionAccountBuilder struct {
	accounts []plannerAccountAccess
	seen     map[solana.PublicKey]int
}

const plannerLinearDedupeLimit = 32

func newPlannerTransactionAccountBuilder(capacity int) plannerTransactionAccountBuilder {
	return plannerTransactionAccountBuilder{
		accounts: make([]plannerAccountAccess, 0, capacity),
	}
}

func plannerTransactionAccountBuilderFromStorage(storage []plannerAccountAccess) plannerTransactionAccountBuilder {
	return plannerTransactionAccountBuilder{accounts: storage[:0]}
}

// add deduplicates accounts and promotes any key encountered as both read-only
// and writable to writable. A linear scan is intentionally cheaper than one
// hash map allocation per transaction for Solana's small sanitized key lists.
func (b *plannerTransactionAccountBuilder) add(account solana.PublicKey, writable bool) {
	if b.seen != nil {
		if idx, exists := b.seen[account]; exists {
			b.accounts[idx].writable = b.accounts[idx].writable || writable
			return
		}
		b.seen[account] = len(b.accounts)
		b.accounts = append(b.accounts, plannerAccountAccess{
			publicKey: account,
			writable:  writable,
		})
		return
	}
	for idx := range b.accounts {
		if b.accounts[idx].publicKey != account {
			continue
		}
		b.accounts[idx].writable = b.accounts[idx].writable || writable
		return
	}
	if len(b.accounts) == plannerLinearDedupeLimit {
		b.seen = make(map[solana.PublicKey]int, cap(b.accounts))
		for idx := range b.accounts {
			b.seen[b.accounts[idx].publicKey] = idx
		}
		b.seen[account] = len(b.accounts)
	}
	b.accounts = append(b.accounts, plannerAccountAccess{
		publicKey: account,
		writable:  writable,
	})
}

func (b *plannerTransactionAccountBuilder) finish() plannerTransactionAccounts {
	// Partition in place so the compact view needs only one backing array.
	// Account order does not affect the dependency relation.
	readonlyEnd := 0
	for idx := range b.accounts {
		if b.accounts[idx].writable {
			continue
		}
		b.accounts[readonlyEnd], b.accounts[idx] = b.accounts[idx], b.accounts[readonlyEnd]
		readonlyEnd++
	}
	return plannerTransactionAccounts{
		accounts:    b.accounts,
		readonlyEnd: readonlyEnd,
	}
}

// newPlannerTransactionAccounts is the compact test/convenience constructor.
func newPlannerTransactionAccounts(
	capacity int,
	addAccounts func(add func(solana.PublicKey, bool)),
) plannerTransactionAccounts {
	builder := newPlannerTransactionAccountBuilder(capacity)
	addAccounts(builder.add)
	return builder.finish()
}

func addPlannerAccountsFromMessage(builder *plannerTransactionAccountBuilder, msg *solana.Message) {
	numStaticAccounts, numWritableLookupAccounts := messageAccountLayout(msg)
	for idx, account := range msg.AccountKeys {
		builder.add(account, messageAccountIsWritable(msg, idx, numStaticAccounts, numWritableLookupAccounts))
	}
}

func plannerAccountsFromMessage(msg *solana.Message) plannerTransactionAccounts {
	builder := newPlannerTransactionAccountBuilder(len(msg.AccountKeys))
	addPlannerAccountsFromMessage(&builder, msg)
	return builder.finish()
}

func plannerStaticAccountCount(msg *solana.Message) int {
	numStaticAccounts := len(msg.AccountKeys)
	if msg.IsResolved() {
		numStaticAccounts -= msg.NumLookups()
	}
	if numStaticAccounts < 0 {
		panic("resolved transaction has more lookup accounts than account keys")
	}
	return numStaticAccounts
}

func addPlannerAccountsFromMeta(
	builder *plannerTransactionAccountBuilder,
	t *solana.Transaction,
	tm *rpc.TransactionMeta,
) {
	msg := &t.Message
	numStaticAccounts := plannerStaticAccountCount(msg)
	requiredSignatures := int(msg.Header.NumRequiredSignatures)
	signedWritableEnd := requiredSignatures - int(msg.Header.NumReadonlySignedAccounts)
	unsignedWritableEnd := numStaticAccounts - int(msg.Header.NumReadonlyUnsignedAccounts)
	if signedWritableEnd < 0 || requiredSignatures > numStaticAccounts ||
		unsignedWritableEnd < requiredSignatures {
		panic("transaction message header account ranges are invalid")
	}

	for idx, account := range msg.AccountKeys[:numStaticAccounts] {
		isWritable := idx < signedWritableEnd ||
			(idx >= requiredSignatures && idx < unsignedWritableEnd)
		builder.add(account, isWritable)
	}
	for _, account := range tm.LoadedAddresses.ReadOnly {
		builder.add(account, false)
	}
	for _, account := range tm.LoadedAddresses.Writable {
		builder.add(account, true)
	}
}

func plannerAccountsFromMeta(t *solana.Transaction, tm *rpc.TransactionMeta) plannerTransactionAccounts {
	capacity := plannerStaticAccountCount(&t.Message) + len(tm.LoadedAddresses.ReadOnly) + len(tm.LoadedAddresses.Writable)
	builder := newPlannerTransactionAccountBuilder(capacity)
	addPlannerAccountsFromMeta(&builder, t, tm)
	return builder.finish()
}

func plannerShouldUseMeta(t *solana.Transaction, tm *rpc.TransactionMeta) bool {
	// Once lookup resolution has populated the message, it is the canonical
	// complete account layout. Metadata is needed only to snapshot unresolved
	// v0 lookup accounts before resolution mutates their messages. A nonnil but
	// incomplete metadata object is not sufficient: defer preparation until
	// the message has been resolved instead of silently omitting account locks.
	if tm == nil || t.Message.IsResolved() {
		return false
	}
	lookupCount := t.Message.NumLookups()
	if !t.Message.IsVersioned() || lookupCount == 0 {
		return true
	}
	writableLookupCount := t.Message.GetAddressTableLookups().NumWritableLookups()
	readonlyLookupCount := lookupCount - writableLookupCount
	return len(tm.LoadedAddresses.Writable) == writableLookupCount &&
		len(tm.LoadedAddresses.ReadOnly) == readonlyLookupCount
}

func plannerAccountsForBlock(b *block.Block) ([]plannerTransactionAccounts, bool) {
	if b == nil || !canUseDependencyPlanner(b) {
		return nil, false
	}

	// One shared backing allocation replaces one account-slice allocation per
	// transaction. Each transaction receives a disjoint capacity-bounded window
	// and is compacted independently within it.
	totalCapacity := 0
	for idx, transaction := range b.Transactions {
		capacity := len(transaction.Message.AccountKeys)
		if idx < len(b.TxMetas) && plannerShouldUseMeta(transaction, b.TxMetas[idx]) {
			txMeta := b.TxMetas[idx]
			capacity = plannerStaticAccountCount(&transaction.Message) +
				len(txMeta.LoadedAddresses.ReadOnly) + len(txMeta.LoadedAddresses.Writable)
		}
		totalCapacity += capacity
	}
	accountStorage := make([]plannerAccountAccess, totalCapacity)
	accounts := make([]plannerTransactionAccounts, len(b.Transactions))
	storageOffset := 0
	for idx, transaction := range b.Transactions {
		var txMeta *rpc.TransactionMeta
		if idx < len(b.TxMetas) {
			txMeta = b.TxMetas[idx]
		}
		if !plannerShouldUseMeta(transaction, txMeta) {
			txMeta = nil
		}
		capacity := len(transaction.Message.AccountKeys)
		if txMeta != nil {
			capacity = plannerStaticAccountCount(&transaction.Message) +
				len(txMeta.LoadedAddresses.ReadOnly) + len(txMeta.LoadedAddresses.Writable)
		}
		builder := plannerTransactionAccountBuilderFromStorage(
			accountStorage[storageOffset : storageOffset : storageOffset+capacity],
		)
		if txMeta != nil {
			addPlannerAccountsFromMeta(&builder, transaction, txMeta)
		} else {
			addPlannerAccountsFromMessage(&builder, &transaction.Message)
		}
		accounts[idx] = builder.finish()
		storageOffset += capacity
	}
	return accounts, true
}

type dependencyPlan struct {
	// offsets/dependents are a compact sparse-row adjacency list. Transaction
	// i's dependents are dependents[offsets[i]:offsets[i+1]].
	offsets    []uint32
	dependents []uint32
	inDegree   []uint32
}

type dependencyAccountState struct {
	lastWriterPlusOne  uint32
	firstReaderPlusOne uint32
	moreReaders        []uint32
}

type dependencyEdge struct {
	prerequisite uint32
	dependent    uint32
}

func addDependency(
	edges *[]dependencyEdge,
	inDegree, outDegree, seen []uint32,
	dependent, prerequisite uint32,
) {
	generation := dependent + 1
	if seen[prerequisite] == generation {
		return
	}
	seen[prerequisite] = generation
	*edges = append(*edges, dependencyEdge{prerequisite: prerequisite, dependent: dependent})
	inDegree[dependent]++
	outDegree[prerequisite]++
}

// buildDependencyPlan creates a reachability-equivalent reduction of the old
// all-conflicts graph. A read depends only on the latest writer. A write
// depends on that writer and every reader since it. Older conflicts remain
// ordered transitively, which preserves sequential block semantics while
// avoiding quadratic writer chains and retaining far fewer edges.
func buildDependencyPlan(transactions []plannerTransactionAccounts) *dependencyPlan {
	if uint64(len(transactions)) > uint64(^uint32(0)) {
		panic("dependency planner transaction count exceeds uint32")
	}
	inDegree := make([]uint32, len(transactions))
	outDegree := make([]uint32, len(transactions))
	edges := make([]dependencyEdge, 0, len(transactions)*4)
	accountToState := make(map[solana.PublicKey]int, len(transactions)*4)
	accountStates := make([]dependencyAccountState, 0, len(transactions)*4)
	seenDependencies := make([]uint32, len(transactions))

	stateFor := func(account solana.PublicKey) *dependencyAccountState {
		stateIdx, exists := accountToState[account]
		if !exists {
			stateIdx = len(accountStates)
			accountToState[account] = stateIdx
			accountStates = append(accountStates, dependencyAccountState{})
		}
		return &accountStates[stateIdx]
	}

	for txIdx, transaction := range transactions {
		currentTx := uint32(txIdx)
		for _, account := range transaction.readonly() {
			state := stateFor(account.publicKey)
			if state.lastWriterPlusOne != 0 {
				addDependency(&edges, inDegree, outDegree, seenDependencies, currentTx, state.lastWriterPlusOne-1)
			}
			if state.firstReaderPlusOne == 0 {
				state.firstReaderPlusOne = currentTx + 1
			} else {
				state.moreReaders = append(state.moreReaders, currentTx)
			}
		}
		for _, account := range transaction.writable() {
			state := stateFor(account.publicKey)
			if state.lastWriterPlusOne != 0 {
				addDependency(&edges, inDegree, outDegree, seenDependencies, currentTx, state.lastWriterPlusOne-1)
			}
			if state.firstReaderPlusOne != 0 {
				addDependency(&edges, inDegree, outDegree, seenDependencies, currentTx, state.firstReaderPlusOne-1)
			}
			for _, reader := range state.moreReaders {
				addDependency(&edges, inDegree, outDegree, seenDependencies, currentTx, reader)
			}
			state.firstReaderPlusOne = 0
			state.moreReaders = state.moreReaders[:0]
			state.lastWriterPlusOne = currentTx + 1
		}
	}

	offsets := make([]uint32, len(transactions)+1)
	var edgeCount uint64
	for idx, count := range outDegree {
		edgeCount += uint64(count)
		if edgeCount > uint64(^uint32(0)) {
			panic("dependency planner edge count exceeds uint32")
		}
		offsets[idx+1] = uint32(edgeCount)
	}
	dependents := make([]uint32, len(edges))
	nextOffset := append([]uint32(nil), offsets[:len(transactions)]...)
	for _, edge := range edges {
		offset := nextOffset[edge.prerequisite]
		dependents[offset] = edge.dependent
		nextOffset[edge.prerequisite]++
	}
	return &dependencyPlan{
		offsets:    offsets,
		dependents: dependents,
		inDegree:   inDegree,
	}
}

func dependencyPlanForBlock(b *block.Block) (*dependencyPlan, bool) {
	accounts, available := plannerAccountsForBlock(b)
	if !available {
		return nil, false
	}
	return buildDependencyPlan(accounts), true
}

// preparedDependencyPlanner owns a plan built ahead of TxLoop. Before lookup
// resolution it snapshots compact account accesses synchronously; afterward it
// can safely overlap extraction and graph construction with account loading.
type preparedDependencyPlanner struct {
	started bool
	result  chan preparedDependencyPlanResult
	ready   *preparedDependencyPlanResult
}

type preparedDependencyPlanResult struct {
	plan          *dependencyPlan
	buildDuration time.Duration
	available     bool
}

func newPreparedDependencyPlanner() *preparedDependencyPlanner {
	return &preparedDependencyPlanner{}
}

func plannerBlockHasAddressTableLookups(b *block.Block) bool {
	for _, transaction := range b.Transactions {
		if transaction.Message.IsVersioned() && transaction.Message.NumLookups() > 0 {
			return true
		}
	}
	return false
}

func (p *preparedDependencyPlanner) tryStart(b *block.Block) bool {
	if p == nil || p.started {
		return p != nil && p.started
	}
	if b == nil || !canUseDependencyPlanner(b) {
		return false
	}
	if !plannerBlockHasAddressTableLookups(b) {
		// Lookup resolution cannot mutate these messages, so both compact
		// extraction and graph construction may overlap account loading.
		p.tryStartResolved(b)
		return true
	}
	accountExtractionStart := time.Now()
	accounts, available := plannerAccountsForBlock(b)
	if !available {
		return false
	}
	accountExtractionDuration := time.Since(accountExtractionStart)
	p.started = true
	p.result = make(chan preparedDependencyPlanResult, 1)
	go func() {
		graphBuildStart := time.Now()
		plan := buildDependencyPlan(accounts)
		p.result <- preparedDependencyPlanResult{
			plan:          plan,
			buildDuration: accountExtractionDuration + time.Since(graphBuildStart),
			available:     true,
		}
	}()
	return true
}

// tryStartResolved may read the block in the builder goroutine because lookup
// resolution has completed and transaction messages are immutable from this
// point onward. This overlaps both compact account extraction and graph
// construction with the rest of account loading.
func (p *preparedDependencyPlanner) tryStartResolved(b *block.Block) {
	if p == nil || p.started {
		return
	}
	p.started = true
	p.result = make(chan preparedDependencyPlanResult, 1)
	go func() {
		buildStart := time.Now()
		accounts, available := plannerAccountsForBlock(b)
		var plan *dependencyPlan
		if available {
			plan = buildDependencyPlan(accounts)
		}
		p.result <- preparedDependencyPlanResult{
			plan:          plan,
			buildDuration: time.Since(buildStart),
			available:     available,
		}
	}()
}

func (p *preparedDependencyPlanner) wait() (*dependencyPlan, time.Duration, bool) {
	if p == nil || !p.started {
		return nil, 0, false
	}
	if p.ready == nil {
		result := <-p.result
		p.ready = &result
	}
	return p.ready.plan, p.ready.buildDuration, p.ready.available
}

func blockToDependencyGraph(b *block.Block) (adjacencyList [][]tx, inDegree []int) {
	plan, available := dependencyPlanForBlock(b)
	if !available {
		panic(fmt.Sprintf("dependency planner cannot derive accounts for block at slot %d", b.Slot))
	}
	adjacencyList = make([][]tx, len(plan.inDegree))
	inDegree = make([]int, len(plan.inDegree))
	for idx := range plan.inDegree {
		inDegree[idx] = int(plan.inDegree[idx])
		start, end := plan.offsets[idx], plan.offsets[idx+1]
		adjacencyList[idx] = make([]tx, end-start)
		for edgeIdx, dependent := range plan.dependents[start:end] {
			adjacencyList[idx][edgeIdx] = tx(dependent)
		}
	}
	return adjacencyList, inDegree
}

// TopsortPlanner returns a list of list of ints.
// The ints are indices into the b.Transactions slices.
// Each list of indices do not have write-after-write or read-after-write conflicts.
func TopsortPlanner(b *block.Block) [][]int {
	plan, available := dependencyPlanForBlock(b)
	if !available {
		panic(fmt.Sprintf("dependency planner cannot derive accounts for block at slot %d", b.Slot))
	}

	// Output a topological sorting of the transactions
	topSorted := 0
	var topSortLevels [][]int
	var roots []int
	for t, deg := range plan.inDegree {
		if deg == 0 {
			roots = append(roots, t)
		}
	}
	for topSorted < len(b.Transactions) {
		topSortLevels = append(topSortLevels, roots)
		topSorted += len(roots)
		// Remove roots from graph.
		var nextRoots []int
		for _, root := range roots {
			start, end := plan.offsets[root], plan.offsets[root+1]
			for _, dependentTx := range plan.dependents[start:end] {
				plan.inDegree[dependentTx]--
				if plan.inDegree[dependentTx] == 0 {
					nextRoots = append(nextRoots, int(dependentTx))
				}
			}
		}
		roots = nextRoots
	}

	//mlog.Log.Infof("planner finished in %s", time.Since(start))
	return topSortLevels
}

// TopsortPlanner outputs ints on out channel which have had their dependencies satisfied and can be run. On completion, return the int to the done channel.
func TopsortPlannerStream(b *block.Block, out chan int, done chan int) {
	topsortPlannerStream(b, out, done, nil)
}

// topsortPlannerStream exposes the graph-build boundary to replay metrics
// without coupling the generally useful planner to the global collector.
func topsortPlannerStream(b *block.Block, out chan int, done chan int, onGraphBuilt func()) {
	plan, available := dependencyPlanForBlock(b)
	if !available {
		panic(fmt.Sprintf("dependency planner cannot derive accounts for block at slot %d", b.Slot))
	}
	if onGraphBuilt != nil {
		onGraphBuilt()
	}
	dispatchDependencyPlan(plan, out, done)
}

// dispatchDependencyPlan consumes a one-shot prebuilt plan. It mutates only
// the plan's private in-degree counters while transaction workers report
// completion through done.
func dispatchDependencyPlan(plan *dependencyPlan, out chan int, done chan int) {
	sent := 0
	for transaction, degree := range plan.inDegree {
		if degree == 0 {
			out <- transaction
			sent++
		}
	}

	for sent < len(plan.inDegree) {
		completed := <-done
		start, end := plan.offsets[completed], plan.offsets[completed+1]
		for _, dependentTx := range plan.dependents[start:end] {
			plan.inDegree[dependentTx]--
			if plan.inDegree[dependentTx] == 0 {
				out <- int(dependentTx)
				sent++
			}
		}
	}
	close(out)
}
