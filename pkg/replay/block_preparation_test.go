package replay

import (
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

func unresolvedPlannerTestTransaction(payer, table solana.PublicKey) *solana.Transaction {
	msg := solana.Message{
		Header: solana.MessageHeader{
			NumRequiredSignatures: 1,
		},
		AccountKeys: []solana.PublicKey{payer},
		AddressTableLookups: solana.MessageAddressTableLookupSlice{
			{
				AccountKey:      table,
				WritableIndexes: []uint8{0},
			},
		},
	}
	msg.SetVersion(solana.MessageVersionV0)
	return &solana.Transaction{Message: msg}
}

func TestPreparedDependencyPlannerSnapshotsUnresolvedMetadata(t *testing.T) {
	sharedWritable := solana.PublicKey{0x40}
	block := &b.Block{
		Slot: 99,
		Transactions: []*solana.Transaction{
			unresolvedPlannerTestTransaction(solana.PublicKey{0x10}, solana.PublicKey{0x20}),
			unresolvedPlannerTestTransaction(solana.PublicKey{0x11}, solana.PublicKey{0x21}),
		},
		TxMetas: []*rpc.TransactionMeta{
			{LoadedAddresses: rpc.LoadedAddresses{Writable: []solana.PublicKey{sharedWritable}}},
			{LoadedAddresses: rpc.LoadedAddresses{Writable: []solana.PublicKey{sharedWritable}}},
		},
	}

	planner := newPreparedDependencyPlanner()
	if !planner.tryStart(block) {
		t.Fatal("planner did not start from unresolved transactions with metadata")
	}

	// Lookup resolution mutates messages, and callers may release RPC metadata
	// once preparation returns. Neither may alter the already snapshotted plan.
	block.Transactions[1].Message.AccountKeys[0] = solana.PublicKey{0x50}
	block.TxMetas[1].LoadedAddresses.Writable[0] = solana.PublicKey{0x51}

	plan, _, available := planner.wait()
	if !available {
		t.Fatal("prepared plan was unavailable")
	}
	if got := len(plan.dependents); got != 1 {
		t.Fatalf("dependency edges = %d, want 1", got)
	}
	if plan.dependents[0] != 1 || plan.offsets[0] != 0 || plan.offsets[1] != 1 {
		t.Fatalf("unexpected prepared dependency plan: offsets=%v dependents=%v", plan.offsets, plan.dependents)
	}
}

func TestPreparedDependencyPlannerStartsAfterResolution(t *testing.T) {
	sharedWritable := solana.PublicKey{0x70}
	transaction := func(payer solana.PublicKey) *solana.Transaction {
		return &solana.Transaction{
			Message: solana.Message{
				Header: solana.MessageHeader{
					NumRequiredSignatures: 1,
				},
				AccountKeys: []solana.PublicKey{payer, sharedWritable},
			},
		}
	}
	block := &b.Block{
		Transactions: []*solana.Transaction{
			transaction(solana.PublicKey{0x71}),
			transaction(solana.PublicKey{0x72}),
		},
	}

	planner := newPreparedDependencyPlanner()
	planner.tryStartResolved(block)
	plan, _, available := planner.wait()
	if !available {
		t.Fatal("planner unavailable for derivable resolved block")
	}
	if got := len(plan.dependents); got != 1 {
		t.Fatalf("dependency edges = %d, want 1", got)
	}
}

func TestPreparedDependencyPlannerPreservesUnresolvedLiveFallback(t *testing.T) {
	block := &b.Block{
		FromLiveStream: true,
		Transactions: []*solana.Transaction{
			unresolvedPlannerTestTransaction(solana.PublicKey{0x80}, solana.PublicKey{0x81}),
		},
	}

	planner := newPreparedDependencyPlanner()
	planner.tryStartResolved(block)
	if _, _, available := planner.wait(); available {
		t.Fatal("planner unexpectedly available for unresolved live transaction without metadata")
	}
}

func TestPreparedDependencyPlannerRejectsIncompleteLookupMetadata(t *testing.T) {
	for _, test := range []struct {
		name   string
		txMeta *rpc.TransactionMeta
	}{
		{name: "missing loaded address", txMeta: &rpc.TransactionMeta{}},
		{
			name: "wrong writable split",
			txMeta: &rpc.TransactionMeta{
				LoadedAddresses: rpc.LoadedAddresses{
					ReadOnly: []solana.PublicKey{{0x92}},
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			block := &b.Block{
				Transactions: []*solana.Transaction{
					unresolvedPlannerTestTransaction(solana.PublicKey{0x90}, solana.PublicKey{0x91}),
				},
				TxMetas: []*rpc.TransactionMeta{test.txMeta},
			}

			planner := newPreparedDependencyPlanner()
			if planner.tryStart(block) {
				t.Fatal("planner started from incomplete or misclassified lookup metadata")
			}
		})
	}
}

func fallbackBatchTestTransaction(
	writableAccounts []solana.PublicKey,
	readonlyAccounts []solana.PublicKey,
) *solana.Transaction {
	accountKeys := make([]solana.PublicKey, 0, len(writableAccounts)+len(readonlyAccounts))
	accountKeys = append(accountKeys, writableAccounts...)
	accountKeys = append(accountKeys, readonlyAccounts...)
	return &solana.Transaction{
		Message: solana.Message{
			Header: solana.MessageHeader{
				NumReadonlyUnsignedAccounts: uint8(len(readonlyAccounts)),
			},
			AccountKeys: accountKeys,
		},
	}
}

func TestLightbringerFallbackRetainsHighestReadBatch(t *testing.T) {
	accountX := solana.PublicKey{0xa0}
	accountA := solana.PublicKey{0xa1}
	transactions := []*solana.Transaction{
		fallbackBatchTestTransaction([]solana.PublicKey{accountX}, nil),
		fallbackBatchTestTransaction(nil, []solana.PublicKey{accountX, accountA}),
		fallbackBatchTestTransaction(nil, []solana.PublicKey{accountA}),
		fallbackBatchTestTransaction([]solana.PublicKey{accountA}, nil),
	}
	entry := &b.TxEntry{Indices: []uint64{0, 1, 2, 3}}

	batches := lightbringerEntryExecutionBatches(transactions, entry, true)
	want := [][]uint64{{0, 2}, {1}, {3}}
	if len(batches) != len(want) {
		t.Fatalf("batches = %v, want %v", batches, want)
	}
	for idx := range want {
		if len(batches[idx]) != len(want[idx]) {
			t.Fatalf("batches = %v, want %v", batches, want)
		}
		for txIdx := range want[idx] {
			if batches[idx][txIdx] != want[idx][txIdx] {
				t.Fatalf("batches = %v, want %v", batches, want)
			}
		}
	}
}

func FuzzLightbringerFallbackPreservesConflictOrder(f *testing.F) {
	// The first seed encodes the regression covered above: a high-batch reader
	// of account A is followed by a lower-batch reader, then a writer of A.
	f.Add([]byte{
		3,
		0, 2, 1,
		1, 2, 0, 1, 0,
		0, 1, 0,
		0, 1, 1,
	})
	f.Add([]byte{7, 3, 1, 1, 2, 0, 3, 1, 2, 1, 4, 0})
	f.Add([]byte{15, 7, 5, 0, 5, 1, 6, 0, 7, 1, 8, 0})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 2 {
			t.Skip()
		}

		cursor := 1
		nextByte := func() byte {
			value := data[cursor%len(data)]
			cursor++
			return value
		}

		transactionCount := 1 + int(data[0]%64)
		transactions := make([]*solana.Transaction, transactionCount)
		indices := make([]uint64, transactionCount)
		// 0 means absent, 1 means read-only, and 2 means writable. Repeated
		// accesses are canonicalized by promoting the account to writable.
		accountModes := make([][32]uint8, transactionCount)
		for txIdx := range transactions {
			indices[txIdx] = uint64(txIdx)
			accessCount := 1 + int(nextByte()%8)
			for range accessCount {
				accountIdx := nextByte() % 32
				mode := uint8(1)
				if nextByte()&1 != 0 {
					mode = 2
				}
				if mode > accountModes[txIdx][accountIdx] {
					accountModes[txIdx][accountIdx] = mode
				}
			}

			payer := solana.PublicKey{0xff, byte(txIdx)}
			accountKeys := []solana.PublicKey{payer}
			numReadonly := 0
			for accountIdx, mode := range accountModes[txIdx] {
				if mode == 2 {
					accountKeys = append(accountKeys, solana.PublicKey{byte(accountIdx)})
				}
			}
			for accountIdx, mode := range accountModes[txIdx] {
				if mode == 1 {
					accountKeys = append(accountKeys, solana.PublicKey{byte(accountIdx)})
					numReadonly++
				}
			}
			transactions[txIdx] = &solana.Transaction{
				Message: solana.Message{
					Header: solana.MessageHeader{
						NumRequiredSignatures:       1,
						NumReadonlyUnsignedAccounts: uint8(numReadonly),
					},
					AccountKeys: accountKeys,
				},
			}
		}

		batches := lightbringerEntryExecutionBatches(
			transactions,
			&b.TxEntry{Indices: indices},
			true,
		)
		batchOf := make([]int, transactionCount)
		seen := make([]bool, transactionCount)
		for batchIdx, batch := range batches {
			for _, rawTxIdx := range batch {
				if rawTxIdx >= uint64(transactionCount) {
					t.Fatalf("batch contains out-of-range transaction %d", rawTxIdx)
				}
				txIdx := int(rawTxIdx)
				if seen[txIdx] {
					t.Fatalf("transaction %d appears in more than one batch", txIdx)
				}
				seen[txIdx] = true
				batchOf[txIdx] = batchIdx
			}
		}
		for txIdx, wasSeen := range seen {
			if !wasSeen {
				t.Fatalf("transaction %d is absent from fallback batches", txIdx)
			}
		}

		conflicts := func(earlier, later int) bool {
			for accountIdx, earlierMode := range accountModes[earlier] {
				laterMode := accountModes[later][accountIdx]
				if earlierMode != 0 && laterMode != 0 &&
					(earlierMode == 2 || laterMode == 2) {
					return true
				}
			}
			return false
		}
		for earlier := 0; earlier < transactionCount; earlier++ {
			for later := earlier + 1; later < transactionCount; later++ {
				if conflicts(earlier, later) && batchOf[earlier] >= batchOf[later] {
					t.Fatalf(
						"conflicting transactions %d and %d have batches %d and %d",
						earlier,
						later,
						batchOf[earlier],
						batchOf[later],
					)
				}
			}
		}
	})
}
