package replay

import (
	"math/rand"
	"sort"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/gagliardetto/solana-go"
)

// Bench only: how much of a fold a query for one transaction really needs.
//
// For a target transaction, the transactions that must run before it are the
// ones that wrote an account it reads, the ones that wrote what those read,
// and so on back to the fold boundary (its cone). Everything else in the fold
// could be left out. The stored hints say what each transaction really wrote,
// so the cone can be computed without executing anything. benchConeReport
// samples targets across one fold and logs how large their cones are, in
// transactions and in consumed compute, against everything before the target.
//
// Not counted: what a block does outside its transactions (sysvars, fee and
// reward distribution, rent). A cone replay would have to apply those too.
type coneTx struct {
	reads  []solana.PublicKey
	writes []solana.PublicKey
	kcu    int64 // zero for a recorded failure, which is not executed
	vote   bool
}

type coneFold struct {
	txs     []coneTx
	writers map[solana.PublicKey][]int32
	// blockEnd[i] is the index after the last transaction of block i.
	blockEnd []int
}

func (fold *coneFold) addBlock(txs []*solana.Transaction, hints *blockHints) {
	if fold.writers == nil {
		fold.writers = make(map[solana.PublicKey][]int32)
	}
	for _, tx := range txs {
		if len(tx.Signatures) == 0 || len(tx.Message.AccountKeys) == 0 {
			continue
		}
		accounts := plannerAccountsFromMessage(&tx.Message)
		entry := coneTx{vote: tx.IsVote()}
		payer := tx.Message.AccountKeys[0]
		hint, known := txHint{}, false
		if hints != nil {
			hint, known = hints.bySig[tx.Signatures[0]]
		}
		for _, access := range accounts.accounts {
			entry.reads = append(entry.reads, access.publicKey)
		}
		switch {
		case !known:
			for _, access := range accounts.writable() {
				entry.writes = append(entry.writes, access.publicKey)
			}
			entry.kcu = int64(requestedKCU(tx))
		case hint.failed:
			entry.writes = append(entry.writes, payer)
			if nonce, isNonce, resolved := advancedNonce(tx); isNonce && resolved {
				entry.writes = append(entry.writes, nonce)
			}
			// A recorded failure is not executed: it only moves the fee, so
			// all it needs are the accounts it writes.
			entry.reads = entry.writes
		default:
			entry.writes = append(append(entry.writes, payer), hint.touched...)
			entry.kcu = int64(hint.kcu)
		}
		index := int32(len(fold.txs))
		for _, key := range entry.writes {
			fold.writers[key] = append(fold.writers[key], index)
		}
		fold.txs = append(fold.txs, entry)
	}
	fold.blockEnd = append(fold.blockEnd, len(fold.txs))
}

// cone returns how many transactions before target must run for it, and their
// consumed compute, target included.
func (fold *coneFold) cone(target int) (txs int, kcu int64) {
	seen := map[int32]bool{int32(target): true}
	stack := []int32{int32(target)}
	for len(stack) > 0 {
		at := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		txs++
		kcu += fold.txs[at].kcu
		for _, key := range fold.txs[at].reads {
			writers := fold.writers[key]
			// the latest writer before this transaction
			i := sort.Search(len(writers), func(i int) bool { return writers[i] >= at })
			if i == 0 {
				continue
			}
			if writer := writers[i-1]; !seen[writer] {
				seen[writer] = true
				stack = append(stack, writer)
			}
		}
	}
	return txs, kcu
}

func benchConeReport(boundary uint64, fold *coneFold) {
	if len(fold.txs) == 0 {
		return
	}
	prefixKCU := make([]int64, len(fold.txs)+1)
	for i, tx := range fold.txs {
		prefixKCU[i+1] = prefixKCU[i] + tx.kcu
	}
	rng := rand.New(rand.NewSource(int64(boundary)))
	report := func(name string, from, to int) {
		var txShare, cuShare, coneTxs, coneKCU []float64
		for attempts := 0; len(txShare) < 200 && attempts < 20000; attempts++ {
			target := from + rng.Intn(to-from)
			if fold.txs[target].vote || fold.txs[target].kcu == 0 {
				continue
			}
			txs, kcu := fold.cone(target)
			txShare = append(txShare, float64(txs)/float64(target+1))
			cuShare = append(cuShare, float64(kcu)/float64(max(1, prefixKCU[target+1])))
			coneTxs = append(coneTxs, float64(txs))
			coneKCU = append(coneKCU, float64(kcu))
		}
		if len(txShare) == 0 {
			return
		}
		at := func(values []float64, p float64) float64 {
			sorted := append([]float64(nil), values...)
			sort.Float64s(sorted)
			return sorted[min(len(sorted)-1, int(float64(len(sorted))*p))]
		}
		mlog.Log.Infof("bench cone: fold after %d, %s (%d targets): transactions in cone median %.0f p90 %.0f max %.0f; share of earlier transactions median %.2f%% p90 %.2f%%; compute in cone median %.1fM p90 %.1fM max %.1fM CU; share of earlier compute median %.2f%% p90 %.2f%% max %.2f%%",
			boundary, name, len(txShare), at(coneTxs, 0.5), at(coneTxs, 0.9), at(coneTxs, 1), 100*at(txShare, 0.5), 100*at(txShare, 0.9),
			at(coneKCU, 0.5)/1000, at(coneKCU, 0.9)/1000, at(coneKCU, 1)/1000, 100*at(cuShare, 0.5), 100*at(cuShare, 0.9), 100*at(cuShare, 1))
	}
	blocks := len(fold.blockEnd)
	report("targets anywhere in the fold", 0, len(fold.txs))
	if blocks >= 8 {
		report("targets in the last eighth of the fold", fold.blockEnd[blocks-blocks/8-1], len(fold.txs))
	}
	mlog.Log.Infof("bench cone: fold after %d has %d blocks, %d transactions, %.1fM CU executed", boundary, blocks, len(fold.txs), float64(prefixKCU[len(fold.txs)])/1000)
}
