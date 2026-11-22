package accounts

import (
	"errors"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/gagliardetto/solana-go"
)

// ErrSealedAccounts is returned by every write through a RecordingReader.
var ErrSealedAccounts = errors.New("accounts: write through a sealed read-only view")

// DeltaEntry returns the account stored in the delta for pubkey and whether the
// delta has an entry for it at all. Every store installs a new object and
// entries are never removed, so an equal (object, present) pair later means no
// store to the key happened in between.
func (o *OverlayAccounts) DeltaEntry(pubkey [32]byte) (*Account, bool) {
	shard := o.shardForKey(pubkey)
	shard.mu.RLock()
	acct, ok := shard.delta[pubkey]
	shard.mu.RUnlock()
	return acct, ok
}

// RecordedRead is one key read through a RecordingReader: the delta object it
// was served from, or InDelta false when the value came from the parent, which
// no store to the overlay changes.
type RecordedRead struct {
	Key     [32]byte
	Version *Account
	InDelta bool
}

// RecordingReader is a read-only Accounts over one OverlayAccounts. It is the
// sealed view an execution is given when its result may be used later, on the
// condition that nothing it read has been stored since.
//
//   - Every read is recorded with the delta version it was served from, and the
//     value returned is that very object, so the record is what was read.
//   - A key read again returns what the first read returned: the execution sees
//     one value per key however the overlay moves underneath it.
//   - Every write is refused and remembered (Wrote).
//
// Unchanged then answers, for any later moment, whether every key read still
// has the version it was read at. If it does, the same reads at that moment
// return the same accounts.
type RecordingReader struct {
	overlay *OverlayAccounts
	mu      sync.Mutex
	reads   []RecordedRead
	index   map[[32]byte]int
	wrote   atomic.Bool
}

// recordingReaderIndexAt is the number of recorded keys above which lookups use
// a map instead of scanning.
const recordingReaderIndexAt = 24

func NewRecordingReader(overlay *OverlayAccounts) *RecordingReader {
	return &RecordingReader{overlay: overlay, reads: make([]RecordedRead, 0, 16)}
}

func (r *RecordingReader) find(key *[32]byte) int {
	if r.index != nil {
		if i, ok := r.index[*key]; ok {
			return i
		}
		return -1
	}
	for i := range r.reads {
		if r.reads[i].Key == *key {
			return i
		}
	}
	return -1
}

func (r *RecordingReader) GetAccount(pubkey *[32]byte) (*Account, error) {
	r.mu.Lock()
	i := r.find(pubkey)
	if i < 0 {
		version, inDelta := r.overlay.DeltaEntry(*pubkey)
		r.reads = append(r.reads, RecordedRead{Key: *pubkey, Version: version, InDelta: inDelta})
		i = len(r.reads) - 1
		if r.index != nil {
			r.index[*pubkey] = i
		} else if len(r.reads) > recordingReaderIndexAt {
			r.index = make(map[[32]byte]int, 2*len(r.reads))
			for j := range r.reads {
				r.index[r.reads[j].Key] = j
			}
		}
	}
	read := r.reads[i]
	r.mu.Unlock()
	if read.InDelta {
		return read.Version, nil
	}
	// Overlay writes never reach the parent, so its value for the key is the
	// same on every read for as long as the overlay lives.
	return r.overlay.parent.GetAccount(pubkey)
}

func (r *RecordingReader) GetAccountWithoutLock(pubkey solana.PublicKey) (*Account, error) {
	key := [32]byte(pubkey)
	return r.GetAccount(&key)
}

func (r *RecordingReader) SetAccount(*[32]byte, *Account) error {
	r.wrote.Store(true)
	return ErrSealedAccounts
}

func (r *RecordingReader) SetAccountWithoutLock(solana.PublicKey, *Account) error {
	r.wrote.Store(true)
	return ErrSealedAccounts
}

// AllAccounts is not a read of named keys, so it cannot be recorded: it is
// refused like a write.
func (r *RecordingReader) AllAccounts() []*Account {
	r.wrote.Store(true)
	return nil
}

// Wrote reports whether anything tried to write through the view, or to read it
// in a way that cannot be recorded. Such an execution's result must not be used.
func (r *RecordingReader) Wrote() bool { return r.wrote.Load() }

// Reads returns a copy of the recorded reads, in first-read order.
func (r *RecordingReader) Reads() []RecordedRead {
	r.mu.Lock()
	out := append([]RecordedRead(nil), r.reads...)
	r.mu.Unlock()
	return out
}

// ReadsOutside counts the keys read so far that are not in keys.
func (r *RecordingReader) ReadsOutside(keys [][32]byte) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	outside := 0
	for i := range r.reads {
		if !slices.Contains(keys, r.reads[i].Key) {
			outside++
		}
	}
	return outside
}

// Unchanged reports whether every key read so far still has the delta version
// it was read at, leaving out keys for which skip returns true (the caller
// checks those another way). skip may be nil. It may be called while the
// execution is still reading; it then covers the reads made so far.
func (r *RecordingReader) Unchanged(skip func(key [32]byte) bool) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.reads {
		read := &r.reads[i]
		if skip != nil && skip(read.Key) {
			continue
		}
		if cur, ok := r.overlay.DeltaEntry(read.Key); ok != read.InDelta || cur != read.Version {
			return false
		}
	}
	return true
}
