package lthash

import (
	"bytes"
	"encoding/binary"
	"runtime"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
)

// accountInput presents an account to a hash tree exactly as AccountHasher
// feeds it to BLAKE3: lamports (8 bytes, little endian), the data, the
// executable flag (1 byte), the owner (32 bytes) and the key (32 bytes).
type accountInput struct {
	acct *accounts.Account
}

const (
	accountInputPrefix = 8
	accountInputSuffix = 1 + 32 + 32
)

func accountInputLen(dataLen int) int { return accountInputPrefix + dataLen + accountInputSuffix }

func (in accountInput) chunkCount() int {
	return (accountInputLen(len(in.acct.Data)) + treeChunkLen - 1) / treeChunkLen
}

func (in accountInput) chunk(index int, scratch *[treeChunkLen]byte) []byte {
	data := in.acct.Data
	start := index * treeChunkLen
	end := min(start+treeChunkLen, accountInputLen(len(data)))
	if start >= accountInputPrefix && end <= accountInputPrefix+len(data) {
		return data[start-accountInputPrefix : end-accountInputPrefix]
	}
	// The first chunk and the chunks holding the trailing fields are assembled.
	var suffix [accountInputSuffix]byte
	if in.acct.Executable {
		suffix[0] = 1
	}
	copy(suffix[1:], in.acct.Owner[:])
	copy(suffix[33:], in.acct.Key[:])
	var prefix [accountInputPrefix]byte
	binary.LittleEndian.PutUint64(prefix[:], in.acct.Lamports)

	out := scratch[:0]
	for pos := start; pos < end; {
		switch {
		case pos < accountInputPrefix:
			n := min(end, accountInputPrefix) - pos
			out = append(out, prefix[pos:pos+n]...)
			pos += n
		case pos < accountInputPrefix+len(data):
			n := min(end, accountInputPrefix+len(data)) - pos
			out = append(out, data[pos-accountInputPrefix:pos-accountInputPrefix+n]...)
			pos += n
		default:
			off := pos - accountInputPrefix - len(data)
			n := end - pos
			out = append(out, suffix[off:off+n]...)
			pos += n
		}
	}
	return out
}

// fullChunkRun reports the chunks that lie wholly inside the data, which are
// consecutive in memory.
func (in accountInput) fullChunkRun(index int) (*byte, int) {
	data := in.acct.Data
	start := index*treeChunkLen - accountInputPrefix
	if index < 1 || start+treeChunkLen > len(data) {
		return nil, 0
	}
	return &data[start], (len(data) - start) / treeChunkLen
}

// accountDirtyChunks lists, in ascending order, the chunks of an account's
// hash input that can differ between prev and next. It is exact about the
// data: a chunk lying wholly inside the data is listed only if its bytes
// differ. The first chunk and every chunk touching the trailing fields are
// always listed, so no comparison of lamports, executable or owner is needed.
// The two accounts must have the same key and the same data length.
func accountDirtyChunks(prev, next *accounts.Account) []int {
	if len(prev.Data) != len(next.Data) || prev.Key != next.Key {
		panic("lthash: dirty chunks need the same key and data length")
	}
	total := accountInputLen(len(next.Data))
	count := (total + treeChunkLen - 1) / treeChunkLen
	scan := func(lo, hi int, dirty []int) []int {
		for i := lo; i < hi; i++ {
			start := i * treeChunkLen
			end := min(start+treeChunkLen, total)
			if end > accountInputPrefix+len(next.Data) {
				dirty = append(dirty, i)
				continue
			}
			from, to := start-accountInputPrefix, end-accountInputPrefix
			if !bytes.Equal(prev.Data[from:to], next.Data[from:to]) {
				dirty = append(dirty, i)
			}
		}
		return dirty
	}
	// Comparing a multi-megabyte account is the largest part of an incremental
	// update, so the chunk range is split across goroutines. Each returns its
	// own ascending list and the lists are joined in order.
	workers := min(dirtyScanWorkers, runtime.GOMAXPROCS(0), (count-1)/dirtyScanChunksPerWorker)
	if workers <= 1 {
		return scan(1, count, []int{0})
	}
	parts := make([][]int, workers)
	var wg sync.WaitGroup
	wg.Add(workers - 1)
	bound := func(w int) int { return 1 + (count-1)*w/workers }
	for w := 1; w < workers; w++ {
		go func() {
			defer wg.Done()
			parts[w] = scan(bound(w), bound(w+1), nil)
		}()
	}
	parts[0] = scan(1, bound(1), []int{0})
	wg.Wait()
	dirty := parts[0]
	for _, part := range parts[1:] {
		dirty = append(dirty, part...)
	}
	return dirty
}

const (
	dirtyScanWorkers         = 4
	dirtyScanChunksPerWorker = 512
)

// TreeAccelerated reports whether hash trees are computed by the 16-lane
// kernel on this machine. Without it the portable code is several times
// slower per byte than hashing an account in full.
func TreeAccelerated() bool { return useTreeAVX512 }

// AccountDirtyChunkCount reports how many chunks of next's hash input would be
// rehashed after prev, and the total number of chunks. It is a measurement aid.
func AccountDirtyChunkCount(prev, next *accounts.Account) (dirty, total int) {
	if len(prev.Data) != len(next.Data) || prev.Key != next.Key {
		return -1, accountInput{next}.chunkCount()
	}
	return len(accountDirtyChunks(prev, next)), accountInput{next}.chunkCount()
}

// AccountHashTree is an incremental hash of one account's LtHash input. Its
// output is always the LtHash AccountHasher computes for the same account.
type AccountHashTree struct {
	tree *hashTree
}

// AccountHashTreeSupported reports whether an account with dataLen bytes of
// data is large enough to have a tree (at least two hash chunks).
func AccountHashTreeSupported(dataLen int) bool {
	return accountInputLen(dataLen) > treeChunkLen
}

// BuildAccountHashTree hashes acct in full, on up to workers goroutines.
func BuildAccountHashTree(acct *accounts.Account, workers int) *AccountHashTree {
	return &AccountHashTree{tree: buildHashTree(accountInput{acct}, workers)}
}

// DirtyChunks lists the hash chunks that can differ between prev, the account
// this tree currently describes, and next. See accountDirtyChunks.
func (t *AccountHashTree) DirtyChunks(prev, next *accounts.Account) []int {
	return accountDirtyChunks(prev, next)
}

// Chunks is the number of hash chunks in the tree.
func (t *AccountHashTree) Chunks() int { return len(t.tree.leaves) }

// Update moves the tree from the account it describes to next, rehashing only
// the chunks in dirty (from DirtyChunks), and writes next's LtHash to dst.
func (t *AccountHashTree) Update(next *accounts.Account, dirty []int, dst *LtHash) {
	t.tree.update(accountInput{next}, dirty)
	t.tree.output(dst.bytes())
}

// HashInto writes the LtHash of the account the tree describes to dst.
func (t *AccountHashTree) HashInto(dst *LtHash) {
	t.tree.output(dst.bytes())
}
