package lthash

import (
	"encoding/binary"
	"math/bits"
	"runtime"
	"sync"
)

// An incremental BLAKE3 hash tree.
//
// BLAKE3 hashes its input in 1024-byte chunks and combines the chunk values in
// a binary tree. hashTree keeps every chunk value and every tree node of one
// input, so after a small change only the changed chunks and the nodes above
// them are recomputed. Its output is byte-for-byte the output of BLAKE3 over
// the whole input; the tests compare it with the BLAKE3 library for every
// input shape.
//
// The compression function below is the portable BLAKE3 reference in Go. It is
// slower per chunk than the library's vectorised code, which is acceptable
// because it only ever hashes the chunks that changed (and, off the block's
// path, the chunks of a tree being built for the first time).

const (
	treeChunkLen = 1024
	treeBlockLen = 64

	flagChunkStart = 1 << 0
	flagChunkEnd   = 1 << 1
	flagParent     = 1 << 2
	flagRoot       = 1 << 3
)

type chainValue [8]uint32

var blake3IV = chainValue{
	0x6A09E667, 0xBB67AE85, 0x3C6EF372, 0xA54FF53A,
	0x510E527F, 0x9B05688C, 0x1F83D9AB, 0x5BE0CD19,
}

func g(s *[16]uint32, a, b, c, d int, mx, my uint32) {
	s[a] = s[a] + s[b] + mx
	s[d] = bits.RotateLeft32(s[d]^s[a], -16)
	s[c] = s[c] + s[d]
	s[b] = bits.RotateLeft32(s[b]^s[c], -12)
	s[a] = s[a] + s[b] + my
	s[d] = bits.RotateLeft32(s[d]^s[a], -8)
	s[c] = s[c] + s[d]
	s[b] = bits.RotateLeft32(s[b]^s[c], -7)
}

// The message word order of each of the seven rounds.
var blake3Schedule = [7][16]uint8{
	{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
	{2, 6, 3, 10, 7, 0, 4, 13, 1, 11, 12, 5, 9, 14, 15, 8},
	{3, 4, 10, 12, 13, 2, 7, 14, 6, 5, 9, 0, 11, 15, 8, 1},
	{10, 7, 12, 9, 14, 3, 13, 15, 4, 0, 11, 2, 5, 8, 1, 6},
	{12, 13, 9, 11, 15, 10, 14, 8, 7, 2, 5, 3, 0, 1, 6, 4},
	{9, 14, 11, 5, 8, 12, 15, 1, 13, 3, 0, 10, 2, 6, 4, 7},
	{11, 15, 5, 0, 1, 9, 8, 6, 14, 10, 2, 12, 3, 4, 7, 13},
}

// compress is the BLAKE3 compression function. It returns the full 16-word
// state; the first eight words are the chaining value.
func compress(cv *chainValue, m *[16]uint32, counter uint64, blockLen, flags uint32) [16]uint32 {
	s := [16]uint32{
		cv[0], cv[1], cv[2], cv[3], cv[4], cv[5], cv[6], cv[7],
		blake3IV[0], blake3IV[1], blake3IV[2], blake3IV[3],
		uint32(counter), uint32(counter >> 32), blockLen, flags,
	}
	for r := range blake3Schedule {
		p := &blake3Schedule[r]
		g(&s, 0, 4, 8, 12, m[p[0]], m[p[1]])
		g(&s, 1, 5, 9, 13, m[p[2]], m[p[3]])
		g(&s, 2, 6, 10, 14, m[p[4]], m[p[5]])
		g(&s, 3, 7, 11, 15, m[p[6]], m[p[7]])
		g(&s, 0, 5, 10, 15, m[p[8]], m[p[9]])
		g(&s, 1, 6, 11, 12, m[p[10]], m[p[11]])
		g(&s, 2, 7, 8, 13, m[p[12]], m[p[13]])
		g(&s, 3, 4, 9, 14, m[p[14]], m[p[15]])
	}
	for i := 0; i < 8; i++ {
		s[i] ^= s[i+8]
		s[i+8] ^= cv[i]
	}
	return s
}

func loadBlock(m *[16]uint32, block []byte) {
	if len(block) == treeBlockLen {
		for i := range m {
			m[i] = binary.LittleEndian.Uint32(block[4*i:])
		}
		return
	}
	var padded [treeBlockLen]byte
	copy(padded[:], block)
	for i := range m {
		m[i] = binary.LittleEndian.Uint32(padded[4*i:])
	}
}

// chunkValue returns the chaining value of chunk number index, which is not
// the only chunk of its input.
func chunkValue(chunk []byte, index uint64) chainValue {
	cv := blake3IV
	var m [16]uint32
	flags := uint32(flagChunkStart)
	for len(chunk) > treeBlockLen {
		loadBlock(&m, chunk[:treeBlockLen])
		s := compress(&cv, &m, index, treeBlockLen, flags)
		copy(cv[:], s[:8])
		flags = 0
		chunk = chunk[treeBlockLen:]
	}
	loadBlock(&m, chunk)
	s := compress(&cv, &m, index, uint32(len(chunk)), flags|flagChunkEnd)
	copy(cv[:], s[:8])
	return cv
}

func parentBlock(m *[16]uint32, left, right *chainValue) {
	copy(m[:8], left[:])
	copy(m[8:], right[:])
}

func parentValue(left, right *chainValue) chainValue {
	var m [16]uint32
	parentBlock(&m, left, right)
	s := compress(&blake3IV, &m, 0, treeBlockLen, flagParent)
	var cv chainValue
	copy(cv[:], s[:8])
	return cv
}

// leftChunks is the number of chunks in the left subtree of a node covering
// count chunks: the largest power of two strictly below count.
func leftChunks(count int) int {
	return 1 << (bits.Len(uint(count-1)) - 1)
}

// hashTree holds the chunk values and tree nodes of one input of at least two
// chunks. A node covering count chunks is followed in nodes by the count-1
// nodes of its subtrees, left subtree first, so the root is nodes[0].
type hashTree struct {
	leaves []chainValue
	nodes  []chainValue
}

// treeInput gives a hash tree access to its input one chunk at a time. chunk
// returns chunk number index; it may use scratch for chunks that are not
// contiguous in memory.
type treeInput interface {
	chunkCount() int
	chunk(index int, scratch *[treeChunkLen]byte) []byte
}

// buildHashTree hashes every chunk of in. The chunks are independent, so they
// are split across workers goroutines.
func buildHashTree(in treeInput, workers int) *hashTree {
	count := in.chunkCount()
	if count < 2 {
		panic("lthash: a hash tree needs at least two chunks")
	}
	t := &hashTree{leaves: make([]chainValue, count), nodes: make([]chainValue, count-1)}
	if useTreeAVX512 {
		t.buildBatched(in, workers)
		return t
	}
	workers = max(1, min(workers, runtime.GOMAXPROCS(0), count/64))
	hashRange := func(lo, hi int) {
		var scratch [treeChunkLen]byte
		for i := lo; i < hi; i++ {
			t.leaves[i] = chunkValue(in.chunk(i, &scratch), uint64(i))
		}
	}
	if workers == 1 {
		hashRange(0, count)
	} else {
		var wg sync.WaitGroup
		wg.Add(workers - 1)
		for w := 1; w < workers; w++ {
			go func() {
				defer wg.Done()
				hashRange(count*w/workers, count*(w+1)/workers)
			}()
		}
		hashRange(0, count/workers)
		wg.Wait()
	}
	t.rebuildNodes(0, 0, count)
	return t
}

// rebuildNodes recomputes every node of the subtree at nodes[base] covering
// count chunks from first, and returns its value.
func (t *hashTree) rebuildNodes(base, first, count int) chainValue {
	if count == 1 {
		return t.leaves[first]
	}
	left := leftChunks(count)
	l := t.rebuildNodes(base+1, first, left)
	r := t.rebuildNodes(base+left, first+left, count-left)
	if base != 0 {
		t.nodes[base] = parentValue(&l, &r)
	}
	return t.nodes[base]
}

// update rehashes the chunks listed in dirty, which must be ascending and
// free of duplicates, and the nodes above them.
func (t *hashTree) update(in treeInput, dirty []int) {
	if len(dirty) == 0 {
		return
	}
	if useTreeAVX512 {
		t.updateBatched(in, dirty)
		return
	}
	var scratch [treeChunkLen]byte
	for _, i := range dirty {
		t.leaves[i] = chunkValue(in.chunk(i, &scratch), uint64(i))
	}
	t.updateNodes(0, 0, len(t.leaves), dirty)
}

func (t *hashTree) updateNodes(base, first, count int, dirty []int) chainValue {
	if count == 1 {
		return t.leaves[first]
	}
	if len(dirty) == 0 {
		return t.nodeValue(base, first, count)
	}
	left := leftChunks(count)
	split := 0
	for split < len(dirty) && dirty[split] < first+left {
		split++
	}
	l := t.updateNodes(base+1, first, left, dirty[:split])
	r := t.updateNodes(base+left, first+left, count-left, dirty[split:])
	if base != 0 {
		t.nodes[base] = parentValue(&l, &r)
	}
	return t.nodes[base]
}

func (t *hashTree) nodeValue(base, first, count int) chainValue {
	if count == 1 {
		return t.leaves[first]
	}
	return t.nodes[base]
}

// output writes the first len(dst) bytes of the hash's extendable output,
// which is what BLAKE3's XOF produces for the whole input.
func (t *hashTree) output(dst []byte) {
	count := len(t.leaves)
	left := leftChunks(count)
	l := t.nodeValue(1, 0, left)
	r := t.nodeValue(left, left, count-left)
	var m [16]uint32
	parentBlock(&m, &l, &r)
	if treeRootOutput(&m, dst) {
		return
	}
	for block := uint64(0); len(dst) > 0; block++ {
		s := compress(&blake3IV, &m, block, treeBlockLen, flagParent|flagRoot)
		var out [treeBlockLen]byte
		for i, word := range s {
			binary.LittleEndian.PutUint32(out[4*i:], word)
		}
		dst = dst[copy(dst, out[:]):]
	}
}

// clone returns an independent copy.
func (t *hashTree) clone() *hashTree {
	return &hashTree{leaves: append([]chainValue(nil), t.leaves...), nodes: append([]chainValue(nil), t.nodes...)}
}
