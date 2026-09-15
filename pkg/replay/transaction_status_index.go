package replay

// Partition only the mutable lookup index, never the immutable bank deltas or
// checkpoint format. Message-hash bytes select a partition; even adversarially
// concentrated keys retain exactly the same membership/reference-count rules.
// Grouping prepared updates by partition keeps a smaller working set hot while
// publishing. All access is still protected by TransactionStatusCache.mu; this
// introduces neither background publication nor additional mutation workers.
// Recovery guarantee: this is a derived in-memory index. Snapshots still store
// the same immutable per-bank keys; restore rebuilds the counts from those keys.
// No checkpoint coverage, duplicate-check, vote persistence, or unwind rule is
// relaxed, and no prepared batch becomes authoritative before commit succeeds.
const transactionStatusIndexPartitions = 64

type transactionStatusIndex []map[transactionStatusKey]uint16

// Keep small groups in one map. The chosen layout remains fixed for the group
// lifetime: growing an existing group never forces a full-index copy on replay.
func (index *transactionStatusIndex) init(expected int) {
	if len(*index) != 0 {
		return
	}
	partitions := 1
	if expected >= 1024 {
		partitions = transactionStatusIndexPartitions
	}
	*index = make(transactionStatusIndex, partitions)
}

func statusIndexPartition(key transactionStatusKey) int {
	return int(key[0]) & (transactionStatusIndexPartitions - 1)
}

func (index *transactionStatusIndex) count(key transactionStatusKey) uint16 {
	if len(*index) == 0 {
		return 0
	}
	return (*index)[int(key[0])&(len(*index)-1)][key]
}

func (index *transactionStatusIndex) add(key transactionStatusKey) {
	index.init(1)
	partition := int(key[0]) & (len(*index) - 1)
	if (*index)[partition] == nil {
		(*index)[partition] = make(map[transactionStatusKey]uint16)
	}
	(*index)[partition][key]++
}

func (index *transactionStatusIndex) remove(key transactionStatusKey) {
	if len(*index) == 0 {
		return
	}
	number := int(key[0]) & (len(*index) - 1)
	partition := (*index)[number]
	if partition[key] <= 1 {
		delete(partition, key)
	} else {
		partition[key]--
	}
	if len(partition) == 0 {
		(*index)[number] = nil
	}
}

func (index *transactionStatusIndex) empty() bool {
	for _, partition := range *index {
		if len(partition) != 0 {
			return false
		}
	}
	return true
}

// A batch is private preparation scratch, not retained in a bank node or
// serialized. Build it from the deduplicated immutable delta so collisions in
// the stored 20-byte key still contribute only once per bank, as before.
type transactionStatusIndexBatch struct {
	keys []transactionStatusKey
	ends [transactionStatusIndexPartitions]int
}

func prepareStatusIndexBatch(group *transactionStatusGroup) *transactionStatusIndexBatch {
	batch := &transactionStatusIndexBatch{keys: make([]transactionStatusKey, 0, len(group.keys))}
	for key := range group.keys {
		batch.append(key)
	}
	batch.partition()
	return batch
}

func (batch *transactionStatusIndexBatch) append(key transactionStatusKey) {
	batch.keys = append(batch.keys, key)
	batch.ends[statusIndexPartition(key)]++
}

// Counting partition in place: each swap fills one destination position. This
// avoids a second key array and repeated iteration over the immutable key map.
func (batch *transactionStatusIndexBatch) partition() {
	var positions [transactionStatusIndexPartitions]int
	for i := 1; i < len(batch.ends); i++ {
		batch.ends[i] += batch.ends[i-1]
		positions[i] = batch.ends[i-1]
	}
	for bucket, end := range batch.ends {
		for positions[bucket] < end {
			at := positions[bucket]
			key := batch.keys[at]
			destination := statusIndexPartition(key)
			if destination == bucket {
				positions[bucket]++
				continue
			}
			to := positions[destination]
			batch.keys[at], batch.keys[to] = batch.keys[to], key
			positions[destination]++
		}
	}
}

func (index *transactionStatusIndex) addBatch(batch *transactionStatusIndexBatch) {
	index.init(len(batch.keys))
	if len(*index) == 1 {
		if (*index)[0] == nil && len(batch.keys) > 0 {
			(*index)[0] = make(map[transactionStatusKey]uint16, len(batch.keys))
		}
		for _, key := range batch.keys {
			(*index)[0][key]++
		}
		return
	}
	start := 0
	for i, end := range batch.ends {
		if start == end {
			continue
		}
		partition := (*index)[i]
		if partition == nil {
			partition = make(map[transactionStatusKey]uint16, end-start)
			(*index)[i] = partition
		}
		for _, key := range batch.keys[start:end] {
			partition[key]++
		}
		start = end
	}
}
