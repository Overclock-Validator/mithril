package accountsdb

import (
	"bytes"
	"sort"

	"github.com/cockroachdb/pebble"
	"github.com/gagliardetto/solana-go"
)

// compactionLiveRecords requires foldMu: the index cannot change between
// liveness classification and publication/removal. Return source-order indices,
// even though large batches visit index keys in increasing order.
func (db *AccountsDb) compactionLiveRecords(c compactCandidate, keys []solana.PublicKey, entries []AccountIndexEntry) (live []int, retErr error) {
	live = make([]int, 0, len(entries))
	matches := func(i int, value []byte) (bool, error) {
		current, err := UnmarshalAcctIdxEntryValue(value)
		return err == nil && current.FileId == c.fileId && current.Slot == c.slot && current.Offset == entries[i].Offset, err
	}
	if len(keys) < batchIndexIteratorThreshold {
		for i := range keys {
			value, closer, err := db.Index.Get(keys[i][:])
			if err == pebble.ErrNotFound {
				continue
			}
			if err != nil {
				return nil, err
			}
			match, err := matches(i, value)
			closeErr := closer.Close()
			if err != nil {
				return nil, err
			}
			if closeErr != nil {
				return nil, closeErr
			}
			if match {
				live = append(live, i)
			}
		}
		return live, nil
	}
	order := make([]int, len(keys))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool { return bytes.Compare(keys[order[i]][:], keys[order[j]][:]) < 0 })
	opts := pebble.IterOptions{KeyTypes: pebble.IterKeyTypePointsOnly, LowerBound: keys[order[0]][:]}
	var upper [32]byte
	if nextPubkey(keys[order[len(order)-1]], &upper) {
		opts.UpperBound = upper[:]
	}
	iter, err := db.Index.NewIter(&opts)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := iter.Close(); retErr == nil && err != nil {
			live = nil
			retErr = err
		}
	}()
	for _, i := range order {
		if !iter.SeekGE(keys[i][:]) {
			if err := iter.Error(); err != nil {
				return nil, err
			}
			continue
		}
		if !bytes.Equal(iter.Key(), keys[i][:]) {
			continue
		}
		value, err := iter.ValueAndErr()
		if err != nil {
			return nil, err
		}
		match, err := matches(i, value)
		if err != nil {
			return nil, err
		}
		if match {
			live = append(live, i)
		}
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	sort.Ints(live)
	return live, nil
}
