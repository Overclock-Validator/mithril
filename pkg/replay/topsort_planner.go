package replay

import (
	"fmt"
	//"time"

	//"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/util"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

// Type wrappers around indices.
type acct int
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

func messageWritableAccounts(msg *solana.Message) []solana.PublicKey {
	numStaticAccounts := len(msg.AccountKeys)
	numWritableLookupAccounts := 0
	if msg.IsResolved() {
		numStaticAccounts -= msg.NumLookups()
		numWritableLookupAccounts = msg.GetAddressTableLookups().NumWritableLookups()
	}
	accounts := make([]solana.PublicKey, 0, len(msg.AccountKeys))

	for idx, account := range msg.AccountKeys {
		isWritable := false
		switch {
		case idx >= numStaticAccounts:
			isWritable = idx-numStaticAccounts < numWritableLookupAccounts
		case idx >= int(msg.Header.NumRequiredSignatures):
			numUnsignedWritable := (numStaticAccounts - int(msg.Header.NumRequiredSignatures)) - int(msg.Header.NumReadonlyUnsignedAccounts)
			isWritable = idx-int(msg.Header.NumRequiredSignatures) < numUnsignedWritable
		default:
			isWritable = idx < int(msg.Header.NumRequiredSignatures-msg.Header.NumReadonlySignedAccounts)
		}

		if isWritable {
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

func getAllAccounts(t *solana.Transaction, tm *rpc.TransactionMeta) []solana.PublicKey {
	if canDeriveAccountsFromMessage(t) {
		return util.DedupePubkeys(append([]solana.PublicKey(nil), t.Message.AccountKeys...))
	}
	if tm == nil {
		panic("unresolved transaction requires txmeta to derive all accounts")
	}

	accounts := make([]solana.PublicKey, 0, len(t.Message.AccountKeys)+len(tm.LoadedAddresses.ReadOnly)+len(tm.LoadedAddresses.Writable))
	accounts = append(accounts, t.Message.AccountKeys...)
	accounts = append(accounts, tm.LoadedAddresses.ReadOnly...)
	accounts = append(accounts, tm.LoadedAddresses.Writable...)
	return util.DedupePubkeys(accounts)
}

func getReadonlyAccounts(t *solana.Transaction, tm *rpc.TransactionMeta) []solana.PublicKey {
	if canDeriveAccountsFromMessage(t) {
		return messageReadonlyAccounts(&t.Message)
	}
	if tm == nil {
		panic("unresolved transaction requires txmeta to derive readonly accounts")
	}
	msg := t.Message
	hdr := msg.Header
	numReadonly := len(tm.LoadedAddresses.ReadOnly)
	signedReadonly := int(hdr.NumReadonlySignedAccounts)
	numReadonly += signedReadonly
	unsignedReadonly := int(hdr.NumReadonlyUnsignedAccounts)
	numReadonly += unsignedReadonly
	accounts := make([]solana.PublicKey, 0, numReadonly)

	accounts = append(accounts, msg.AccountKeys[int(hdr.NumRequiredSignatures)-signedReadonly:hdr.NumRequiredSignatures]...)
	accounts = append(accounts, msg.AccountKeys[len(msg.AccountKeys)-unsignedReadonly:len(msg.AccountKeys)]...)
	accounts = append(accounts, tm.LoadedAddresses.ReadOnly...)
	return util.DedupePubkeys(accounts)
}

func getWritableAccounts(t *solana.Transaction, tm *rpc.TransactionMeta) []solana.PublicKey {
	if canDeriveAccountsFromMessage(t) {
		return messageWritableAccounts(&t.Message)
	}
	if tm == nil {
		panic("unresolved transaction requires txmeta to derive writable accounts")
	}
	msg := t.Message
	hdr := msg.Header
	numWritable := len(tm.LoadedAddresses.Writable)
	signedWritable := int(hdr.NumRequiredSignatures) - int(hdr.NumReadonlySignedAccounts)
	numWritable += signedWritable
	unsignedWritable := len(msg.AccountKeys) - int(hdr.NumRequiredSignatures) - int(hdr.NumReadonlyUnsignedAccounts)
	numWritable += unsignedWritable
	accounts := make([]solana.PublicKey, 0, numWritable)

	accounts = append(accounts, msg.AccountKeys[0:signedWritable]...)
	accounts = append(accounts, msg.AccountKeys[int(hdr.NumRequiredSignatures):int(hdr.NumRequiredSignatures)+unsignedWritable]...)
	accounts = append(accounts, tm.LoadedAddresses.Writable...)
	return util.DedupePubkeys(accounts)
}

func canUseDependencyPlanner(b *block.Block) bool {
	for i, tx := range b.Transactions {
		if i < len(b.TxMetas) && b.TxMetas[i] != nil {
			continue
		}
		if canDeriveAccountsFromMessage(tx) {
			continue
		}
		return false
	}
	return true
}

func blockToDependencyGraph(b *block.Block) (adjacencyList [][]tx, inDegree []int) {
	//start := time.Now()
	// Map between pubkeys and account indices
	var acctToPk []solana.PublicKey
	pkToAcct := make(map[solana.PublicKey]acct, len(b.Transactions)*4)

	for i, tx := range b.Transactions {
		var txMeta *rpc.TransactionMeta
		if i < len(b.TxMetas) {
			txMeta = b.TxMetas[i]
		}

		accounts := getAllAccounts(tx, txMeta)
		for _, acctPk := range accounts {
			if _, exists := pkToAcct[acctPk]; !exists {
				pkToAcct[acctPk] = acct(len(acctToPk))
				acctToPk = append(acctToPk, acctPk)
			}
		}
	}

	acctToReaderTxs := make(map[acct][]tx, len(acctToPk))
	acctToWriterTxs := make(map[acct][]tx, len(acctToPk))
	adjacencyList = make([][]tx, len(b.Transactions))
	inDegree = make([]int, len(b.Transactions))
	for txIdx := range b.Transactions {
		/* Given S < T (S occurs before T) in a sequential execution, we
		   use these restrictions on the parallel execution to reproduce
		   the sequential execution

		   S      | T      | parallel ordering required
		   reads  | reads  | any order
		   writes | reads  | S < T
		   reads  | writes | S < T
		   writes | writes | S < T
		*/
		t := tx(txIdx)
		var txMeta *rpc.TransactionMeta
		if txIdx < len(b.TxMetas) {
			txMeta = b.TxMetas[txIdx]
		}

		//txSig := b.Transactions[txIdx].Signatures[0]
		////mlog.Log.Debugf("printing input accounts for txIdx=%d txSig=%s", txIdx, txSig)
		readonlyAccounts := getReadonlyAccounts(b.Transactions[txIdx], txMeta)
		for _, roAcct := range readonlyAccounts {
			////mlog.Log.Debugf("- roAcct=%s", roAcct.String())
			acct, exists := pkToAcct[roAcct]
			if !exists {
				panic(fmt.Sprintf("invariant error: did not record account index for pk=%s in previous loop?", roAcct.String()))
			}

			// Add an edge for S writes < T reads
			for _, s := range acctToWriterTxs[acct] {
				if s >= t {
					break
				}
				if len(adjacencyList[int(s)]) > 0 && adjacencyList[int(s)][len(adjacencyList[int(s)])-1] == t {
					continue
				}
				adjacencyList[int(s)] = append(adjacencyList[int(s)], t)
				inDegree[int(t)]++
			}
			// Add T as a reader of this account.
			acctToReaderTxs[acct] = append(acctToReaderTxs[acct], t)
		}

		writableAccts := getWritableAccounts(b.Transactions[txIdx], txMeta)
		for _, writeAcct := range writableAccts {
			////mlog.Log.Debugf("- writeAcct=%s", writeAcct.String())
			acct, exists := pkToAcct[writeAcct]
			if !exists {
				panic(fmt.Sprintf("invariant error: expected pkToAcct to contain all public keys of accounts used in block; missing public key=%s", writeAcct.String()))
			}
			// Add an edge for S reads < T writes
			for _, s := range acctToReaderTxs[acct] {
				if s >= t {
					break
				}
				if len(adjacencyList[int(s)]) > 0 && adjacencyList[int(s)][len(adjacencyList[int(s)])-1] == t {
					continue
				}
				adjacencyList[int(s)] = append(adjacencyList[int(s)], t)
				inDegree[int(t)]++
			}
			// Add an edge for S writes < T writes
			for _, s := range acctToWriterTxs[acct] {
				if s >= t {
					break
				}
				if len(adjacencyList[int(s)]) > 0 && adjacencyList[int(s)][len(adjacencyList[int(s)])-1] == t {
					continue
				}
				adjacencyList[int(s)] = append(adjacencyList[int(s)], t)
				inDegree[int(t)]++
			}
			// Add T as a writer of this account.
			acctToWriterTxs[acct] = append(acctToWriterTxs[acct], t)
		}
	}
	return adjacencyList, inDegree
}

// TopsortPlanner returns a list of list of ints.
// The ints are indices into the b.Transactions slices.
// Each list of indices do not have write-after-write or read-after-write conflicts.
func TopsortPlanner(b *block.Block) [][]int {
	adjList, inDegree := blockToDependencyGraph(b)

	// Output a topological sorting of the transactions
	topSorted := 0
	var topSortLevels [][]int
	var roots []int
	for t, deg := range inDegree {
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
			for _, dependentTx := range adjList[int(root)] {
				inDegree[int(dependentTx)]--
				if inDegree[int(dependentTx)] == 0 {
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
	adjList, inDegree := blockToDependencyGraph(b)

	sent := 0
	// Output a topological sorting of the transactions
	for t, deg := range inDegree {
		if deg == 0 {
			out <- t
			sent++
		}
	}

	for sent < len(b.Transactions) {
		in := <-done
		for _, dependentTx := range adjList[int(in)] {
			inDegree[int(dependentTx)]--
			if inDegree[int(dependentTx)] == 0 {
				out <- int(dependentTx)
				sent++
			}
		}
	}
	close(out)
}
