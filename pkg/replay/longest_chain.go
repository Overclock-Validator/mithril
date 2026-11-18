package replay

import (
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// chainPath is a dependency chain ending at one transaction: the CU consumed
// along it and the number of transactions on it.
type chainPath struct {
	cu  uint64
	txs uint32
}

// exceeds orders chains by CU, then by length, so equal-CU chains resolve the
// same way on every machine.
func (p chainPath) exceeds(q chainPath) bool {
	return p.cu > q.cu || (p.cu == q.cu && p.txs > q.txs)
}

// chainAccount is the longest chain reaching an account so far: lastWrite ends
// at the latest transaction that had the account writable, readMax at the
// heaviest transaction that has read it since that write.
type chainAccount struct {
	lastWrite chainPath
	readMax   chainPath
}

// longestChainMeter measures the longest dependency chain of one block at a
// time. It keeps its account table between blocks, so a measurement allocates
// nothing once the table has grown to the size of a block.
type longestChainMeter struct {
	index    map[solana.PublicKey]uint32
	accounts []chainAccount
	touched  []uint32
}

// measure returns the largest total consumed CU along any chain of
// transactions that must run one after another, and how many transactions are
// on that chain. Two transactions are linked when both use an account and at
// least one has it writable; the earlier one in block order runs first. Each
// transaction's chain is its own CU plus the heaviest chain of any earlier
// transaction it is linked to, and the block's value is the heaviest of those.
// It is the least CU that has to execute serially however many workers there
// are, so a block whose chain is close to its total CU cannot replay in
// parallel.
//
// transactions are the block's transactions in block order, as executed, with
// address-table accounts resolved. computeUnits[i] is what transaction i
// consumed, the same figure summed into the block's total, so a failed
// transaction counts what it used. Accounts and writability are read the way
// the dependency planner reads them (messageAccountLayout and
// messageAccountIsWritable), so the chain is over the conflicts replay itself
// schedules around.
//
// Only the latest writer and the readers since it are kept per account: an
// older conflict on the same account is already behind one of those, and a
// chain never gets lighter as it extends. That makes the pass linear in the
// number of account references. ok is false, with zero results, when a
// message's accounts cannot be derived (unresolved address tables) or the two
// slices disagree in length.
func (m *longestChainMeter) measure(transactions []*solana.Transaction, computeUnits []uint64) (cu uint64, txs uint32, ok bool) {
	if len(transactions) != len(computeUnits) {
		return 0, 0, false
	}
	for _, transaction := range transactions {
		if transaction == nil || !canDeriveAccountsFromMessage(transaction) {
			return 0, 0, false
		}
	}
	if m.index == nil {
		m.index = make(map[solana.PublicKey]uint32)
	}
	clear(m.index)
	accounts := m.accounts[:0]
	touched := m.touched
	var longest chainPath

	for txIdx, transaction := range transactions {
		msg := &transaction.Message
		numStaticAccounts, numWritableLookupAccounts := messageAccountLayout(msg)

		// The heaviest chain this transaction has to wait for. A duplicated key
		// needs no special case: every occurrence is consulted here before any
		// is updated below.
		touched = touched[:0]
		var before chainPath
		for idx, key := range msg.AccountKeys {
			accountIdx, seen := m.index[key]
			if !seen {
				accountIdx = uint32(len(accounts))
				m.index[key] = accountIdx
				accounts = append(accounts, chainAccount{})
			}
			touched = append(touched, accountIdx)
			account := &accounts[accountIdx]
			if account.lastWrite.exceeds(before) {
				before = account.lastWrite
			}
			if messageAccountIsWritable(msg, idx, numStaticAccounts, numWritableLookupAccounts) && account.readMax.exceeds(before) {
				before = account.readMax
			}
		}

		path := chainPath{cu: before.cu + computeUnits[txIdx], txs: before.txs + 1}
		if path.exceeds(longest) {
			longest = path
		}

		for idx, accountIdx := range touched {
			account := &accounts[accountIdx]
			if messageAccountIsWritable(msg, idx, numStaticAccounts, numWritableLookupAccounts) {
				account.lastWrite = path
				account.readMax = chainPath{}
			} else if path.exceeds(account.readMax) {
				account.readMax = path
			}
		}
	}
	m.accounts, m.touched = accounts, touched
	return longest.cu, longest.txs, true
}

// longestChainPass is one bank's measurement, running on its own goroutine.
// The result fields are written by that goroutine and may be read only after
// done is closed.
type longestChainPass struct {
	bank *sealevel.SlotCtx // identifies the bank; never dereferenced
	done chan struct{}

	cu       uint64
	txs      uint32
	ok       bool
	duration time.Duration // the measurement alone, excluding any wait to start
}

// longestChainRunner measures each finalizing bank's longest dependency chain
// beside the end-of-block work instead of in line with it. The pass takes a
// fraction of a millisecond per block, which the block's tail (fees, rent,
// bank hash, commit) more than covers, so the replay loop normally finds the
// result ready when it writes the block's timing row.
//
// There is one meter and its table is reused by every pass, so passes run
// strictly one after another: each pass first waits for the pass started
// before it. Banks finalize one at a time on the replay goroutine and each is
// recorded or abandoned before the next finalizes, so in replay that wait is
// already over; it only matters to a caller that finalizes banks without
// recording them. A pass holds its own references to the executed
// transactions and their CU, reads them without writing, and is the only
// thing that touches the meter while it runs.
type longestChainRunner struct {
	mu    sync.Mutex
	meter longestChainMeter // touched only by the running pass
	// last is the most recently started pass that has not been joined. Every
	// earlier unjoined pass is ahead of it in line, so last being done means
	// all of them are.
	last *longestChainPass
}

// blockLongestChain serves every bank replay finalizes.
var blockLongestChain longestChainRunner

// start begins measuring a bank whose transactions have all executed and
// returns at once. transactions and computeUnits must not be modified from
// here on; nothing in replay writes them after execution.
func (r *longestChainRunner) start(bank *sealevel.SlotCtx, transactions []*solana.Transaction, computeUnits []uint64) *longestChainPass {
	pass := &longestChainPass{bank: bank, done: make(chan struct{})}
	r.mu.Lock()
	previous := r.last
	r.last = pass
	r.mu.Unlock()
	go func() {
		defer close(pass.done)
		if previous != nil {
			<-previous.done
		}
		start := time.Now()
		pass.cu, pass.txs, pass.ok = r.meter.measure(transactions, computeUnits)
		pass.duration = time.Since(start)
	}()
	return pass
}

// join waits for pass to finish and forgets it.
func (r *longestChainRunner) join(pass *longestChainPass) {
	<-pass.done
	r.mu.Lock()
	if r.last == pass {
		r.last = nil
	}
	r.mu.Unlock()
}

// abandon joins the pass of a bank that will not be recorded, so the pass is
// over before the caller discards or reuses anything the bank owned. Its
// result is dropped.
func (r *longestChainRunner) abandon(pass *longestChainPass) {
	if pass != nil {
		r.join(pass)
	}
}

// drain joins whatever pass is still unjoined: the replay loop runs it on the
// way out, for a bank that finalized but never reached its timing row.
func (r *longestChainRunner) drain() {
	r.mu.Lock()
	pass := r.last
	r.mu.Unlock()
	r.abandon(pass)
}

// record joins the bank's pass and writes its result into the block's metrics
// record: the chain, how long the pass itself took (LongestChainMeasurement),
// and how long this call had to wait for it (LongestChainWait, normally
// nothing, because the pass finished during the block's tail). A bank replay
// did not execute (one adopted from local production) has no pass and records
// nothing; a bank whose chain could not be measured records the two timings
// and a zero chain.
func (r *longestChainRunner) record(bank *sealevel.SlotCtx) {
	if bank == nil {
		return
	}
	r.mu.Lock()
	pass := r.last
	r.mu.Unlock()
	if pass == nil || pass.bank != bank {
		return
	}
	waitStart := time.Now()
	r.join(pass)
	metrics.GlobalBlockReplay.LongestChainWait.AddTimingSince(waitStart)
	metrics.GlobalBlockReplay.LongestChainMeasurement.AddTiming(pass.duration)
	if !pass.ok {
		return
	}
	metrics.GlobalBlockReplay.LongestChainCU = pass.cu
	metrics.GlobalBlockReplay.LongestChainTransactions = pass.txs
}
