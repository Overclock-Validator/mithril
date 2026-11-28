package transactionhistory

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/cockroachdb/pebble"
)

// ReadBudget is a request-owned sequential work allowance, not an engine-memory,
// physical-I/O or wall-time bound. A failure never refunds admitted work.
// It must not be shared concurrently. Cleanup is never refused by this budget.
type ReadBudget struct {
	ops, bytes uint64
	exhausted  bool
}

// NewReadBudget creates the fixed historical-batch application work policy.
func NewReadBudget() *ReadBudget {
	return &ReadBudget{ops: 4096, bytes: 16 << 20}
}

// Charge checks cancellation and reserves work before its associated operation.
// Bytes are charged after Pebble returns them but before application validation,
// hashing or copying. Pebble's own reads, decompression and cache are not bounded.
// Nil preserves the existing unbudgeted store behavior.
func (b *ReadBudget) Charge(ctx context.Context, operations, bytes uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b == nil {
		return nil
	}
	if b.exhausted || operations > b.ops || bytes > b.bytes {
		b.exhausted = true
		return ErrLimit
	}
	b.ops -= operations
	b.bytes -= bytes
	return nil
}

// Candidate contains validated receipt identity and its existing completion.
// It does not select a canonical branch or claim unrelated body coverage.
type Candidate struct {
	Ref    Ref
	Header BlockHeader
}

// SignatureCandidates validates bounded candidates and returns only their small
// identity/header facts. All facts come from this snapshot and the same completion
// used to validate each receipt; no second completion lookup is needed.
func (r *ReadSnapshot) SignatureCandidates(ctx context.Context, signature [64]byte, budget *ReadBudget) ([]Candidate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.db == nil {
		return nil, ErrClosed
	}
	var candidates []Candidate
	err := r.signatureCandidates(ctx, signature, budget, func(ref Ref, header BlockHeader) {
		candidates = append(candidates, Candidate{Ref: ref, Header: header})
	})
	if err != nil {
		return nil, err
	}
	return candidates, nil
}

// TransactionWithBudget validates and copies one selected receipt, charging the
// same request budget used for candidate discovery. It does not select a fork.
func (r *ReadSnapshot) TransactionWithBudget(ctx context.Context, ref Ref, budget *ReadBudget) (Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.db == nil {
		return Record{}, ErrClosed
	}
	record, _, err := readTransactionWithBudget(ctx, r.db, ref, r.store.limits, true, budget)
	return record, err
}

func (r *ReadSnapshot) signatureCandidates(ctx context.Context, signature [64]byte, budget *ReadBudget, visit func(Ref, BlockHeader)) error {
	if err := budget.Charge(ctx, 1, 0); err != nil {
		return err
	}
	prefix := append([]byte{'s'}, signature[:]...)
	iter, err := r.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
	if err != nil {
		return err
	}
	closeError := func(err error) error { return errors.Join(err, iter.Close()) }
	if err := budget.Charge(ctx, 1, 0); err != nil {
		return closeError(err)
	}
	valid := iter.First()
	var count uint32
	for valid {
		if count == r.store.limits.MaxSignatureRefs {
			return closeError(ErrLimit)
		}
		key := iter.Key()
		if len(key) != 109 {
			return closeError(ErrUnavailable)
		}
		// Existing callers still validate this value through checkReference. The
		// budgeted path additionally accounts the iterator value before any work.
		if budget != nil {
			value := iter.Value()
			if err := budget.Charge(ctx, 0, uint64(len(value))); err != nil {
				return closeError(err)
			}
			if len(value) != 64 {
				return closeError(ErrUnavailable)
			}
		}
		ref := Ref{FirstSignature: signature, Position: binary.BigEndian.Uint32(key[105:109])}
		ref.Block.Slot = binary.BigEndian.Uint64(key[65:73])
		copy(ref.Block.Blockhash[:], key[73:105])
		_, header, err := readTransactionWithBudget(ctx, r.db, ref, r.store.limits, false, budget)
		if err != nil {
			return closeError(err)
		}
		visit(ref, header)
		count++
		if err := budget.Charge(ctx, 1, 0); err != nil {
			return closeError(err)
		}
		valid = iter.Next()
	}
	return closeError(iter.Error())
}

func completionHeader(c completion) BlockHeader {
	return BlockHeader{ID: c.block.ID, ParentSlot: c.block.ParentSlot,
		PreviousBlockhash: c.block.PreviousBlockhash, Bankhash: c.block.Bankhash,
		ParentBankhash: c.block.ParentBankhash, CaptureVersion: c.block.CaptureVersion,
		TransactionCount: c.count}
}

func readCompletionWithBudget(ctx context.Context, snapshot *pebble.Snapshot, id BlockID, limits Limits, budget *ReadBudget) (completion, error) {
	if err := budget.Charge(ctx, 1, 0); err != nil {
		return completion{}, err
	}
	value, closer, err := snapshot.Get(blockKey('c', id))
	if err != nil {
		return completion{}, errors.Join(ErrUnavailable, err)
	}
	if err := budget.Charge(ctx, 0, uint64(len(value))); err != nil {
		return completion{}, errors.Join(err, closer.Close())
	}
	c, decodeErr := decodeCompletion(value, id, limits)
	return c, errors.Join(decodeErr, closer.Close())
}

func checkReferenceWithBudget(ctx context.Context, snapshot *pebble.Snapshot, ref Ref, value []byte, headerDigest [32]byte, budget *ReadBudget) error {
	if err := budget.Charge(ctx, 1, 0); err != nil {
		return err
	}
	want, closer, err := snapshot.Get(signatureKey(ref))
	if err != nil {
		return errors.Join(ErrUnavailable, err)
	}
	if err := budget.Charge(ctx, 0, uint64(len(want))); err != nil {
		return errors.Join(err, closer.Close())
	}
	return errors.Join(checkReferenceValue(want, value, headerDigest), closer.Close())
}

func readTransactionWithBudget(ctx context.Context, snapshot *pebble.Snapshot, ref Ref, limits Limits, owned bool, budget *ReadBudget) (Record, BlockHeader, error) {
	c, err := readCompletionWithBudget(ctx, snapshot, ref.Block, limits, budget)
	if err != nil {
		return Record{}, BlockHeader{}, err
	}
	if ref.Position >= c.count {
		return Record{}, BlockHeader{}, ErrUnavailable
	}
	if err := budget.Charge(ctx, 1, 0); err != nil {
		return Record{}, BlockHeader{}, err
	}
	value, closer, err := snapshot.Get(transactionKey(ref.Block, ref.Position))
	if err != nil {
		return Record{}, BlockHeader{}, errors.Join(ErrUnavailable, err)
	}
	if err := budget.Charge(ctx, 0, uint64(len(value))); err != nil {
		return Record{}, BlockHeader{}, errors.Join(err, closer.Close())
	}
	record, readErr := viewRecord(value, limits)
	if readErr == nil && (record.FirstSignature != ref.FirstSignature || uint64(len(value)) > c.bytes) {
		readErr = ErrUnavailable
	}
	if readErr == nil {
		readErr = checkReferenceWithBudget(ctx, snapshot, ref, value, sha256.Sum256(encodeCompletion(c)), budget)
	}
	if readErr == nil && owned {
		record = ownedRecord(record)
	} else {
		record = Record{}
	}
	err = errors.Join(readErr, closer.Close())
	if err != nil {
		return Record{}, BlockHeader{}, err
	}
	return record, completionHeader(c), nil
}
