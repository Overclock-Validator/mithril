package replay

import (
	"math/rand"
	"testing"
)

func TestTransactionStatusPartitionedIndexReferenceCounts(t *testing.T) {
	for _, concentrated := range []bool{false, true} {
		rng := rand.New(rand.NewSource(71))
		var index transactionStatusIndex
		index.init(33760)
		reference := make(map[transactionStatusKey]uint16)
		keys := make([]transactionStatusKey, 1024)
		for i := range keys {
			rng.Read(keys[i][:])
			if concentrated {
				keys[i][0] = 255
			}
		}
		var banks []*transactionStatusGroup
		for step := 0; step < 600; step++ {
			if len(banks) > 0 && (step%3 == 0 || len(banks) == 300) {
				group := banks[0]
				banks = banks[1:]
				for k := range group.keys {
					index.remove(k)
					reference[k]--
					if reference[k] == 0 {
						delete(reference, k)
					}
				}
			} else {
				group := &transactionStatusGroup{keys: make(map[transactionStatusKey]struct{})}
				for i := 0; i < 100; i++ {
					group.keys[keys[rng.Intn(len(keys))]] = struct{}{}
				}
				index.addBatch(prepareStatusIndexBatch(group))
				banks = append(banks, group)
				for k := range group.keys {
					reference[k]++
				}
			}
			for _, k := range keys {
				if got := index.count(k); got != reference[k] {
					t.Fatalf("concentrated=%t step=%d count=%d want=%d", concentrated, step, got, reference[k])
				}
			}
		}
		for _, group := range banks {
			for k := range group.keys {
				index.remove(k)
			}
		}
		if !index.empty() {
			t.Fatal("index retained keys after all bank references removed")
		}
	}
}

func TestTransactionStatusIndexBatchPartitionEdges(t *testing.T) {
	for _, first := range []byte{0, 63, 64, 127, 255} {
		group := &transactionStatusGroup{keys: map[transactionStatusKey]struct{}{{first, 1}: {}, {first, 2}: {}}}
		batch := prepareStatusIndexBatch(group)
		var index transactionStatusIndex
		index.addBatch(batch)
		index.addBatch(batch)
		for k := range group.keys {
			if index.count(k) != 2 {
				t.Fatal("lost overlapping bank reference")
			}
			index.remove(k)
			if index.count(k) != 1 {
				t.Fatal("removed key still required by another bank")
			}
			index.remove(k)
		}
		if !index.empty() {
			t.Fatal("partition did not empty")
		}
		index.addBatch(prepareStatusIndexBatch(&transactionStatusGroup{}))
		if !index.empty() {
			t.Fatal("empty batch introduced keys")
		}
	}
}

func TestTransactionStatusIndexSmallGroupDoesNotRepartition(t *testing.T) {
	var index transactionStatusIndex
	index.add(transactionStatusKey{0, 1})
	group := &transactionStatusGroup{keys: make(map[transactionStatusKey]struct{})}
	for i := 0; i < 4096; i++ {
		group.keys[transactionStatusKey{byte(i), byte(i >> 8)}] = struct{}{}
	}
	index.addBatch(prepareStatusIndexBatch(group))
	if len(index) != 1 {
		t.Fatal("existing small group copied into a new layout during publication")
	}
	for k := range group.keys {
		if index.count(k) == 0 {
			t.Fatal("batch lost a key when using compact layout")
		}
	}
}
