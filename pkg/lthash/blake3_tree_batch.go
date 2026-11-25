package lthash

import (
	"encoding/binary"
	"runtime"
	"sync"
	"unsafe"
)

// Batched tree hashing: the same chunk values and nodes as the portable code
// in blake3_tree.go, computed 16 at a time by compress16. It is used only when
// useTreeAVX512 is set, which requires that compress16 reproduced the portable
// compression function on this machine at start-up.

const (
	treeLanes     = 16
	treeLaneBytes = treeChunkLen
)

// laneScratch holds one block or one chunk per lane, 1024 bytes apart, which
// is the layout compress16 reads.
type laneScratch [treeLanes * treeLaneBytes]byte

// contiguousInput is implemented by inputs whose full chunks lie consecutively
// in memory. fullChunkRun returns a pointer to chunk index and how many full
// chunks follow it at 1024-byte spacing (0 if chunk index is not such a chunk).
type contiguousInput interface {
	fullChunkRun(index int) (*byte, int)
}

// hashFullChunks16 hashes 16 full chunks laid out 1024 bytes apart at base.
// Lane i is chunk number index[i].
func hashFullChunks16(base *byte, index *[treeLanes]uint64, out *[treeLanes]chainValue) {
	var cv [8][treeLanes]uint32
	for w := range cv {
		for l := range cv[w] {
			cv[w][l] = blake3IV[w]
		}
	}
	var ctr [2 * treeLanes]uint32
	for l, i := range index {
		ctr[l] = uint32(i)
		ctr[treeLanes+l] = uint32(i >> 32)
	}
	for block := 0; block < treeChunkLen/treeBlockLen; block++ {
		flags := uint32(0)
		if block == 0 {
			flags |= flagChunkStart
		}
		if block == treeChunkLen/treeBlockLen-1 {
			flags |= flagChunkEnd
		}
		compress16(&cv, (*byte)(unsafe.Add(unsafe.Pointer(base), block*treeBlockLen)), &ctr, treeBlockLen, flags)
	}
	for l := range out {
		for w := range cv {
			out[l][w] = cv[w][l]
		}
	}
}

// chunkBatch collects full chunks that are not contiguous in memory and hashes
// them 16 at a time.
type chunkBatch struct {
	scratch laneScratch
	index   [treeLanes]uint64
	n       int
	leaves  []chainValue
}

func (b *chunkBatch) add(chunk []byte, index int) {
	copy(b.scratch[b.n*treeLaneBytes:], chunk)
	b.index[b.n] = uint64(index)
	b.n++
	if b.n == treeLanes {
		b.flush()
	}
}

func (b *chunkBatch) flush() {
	if b.n == 0 {
		return
	}
	var out [treeLanes]chainValue
	hashFullChunks16(&b.scratch[0], &b.index, &out)
	for l := 0; l < b.n; l++ {
		b.leaves[b.index[l]] = out[l]
	}
	b.n = 0
}

// hashLeavesBatched fills t.leaves[lo:hi] from in.
func (t *hashTree) hashLeavesBatched(in treeInput, lo, hi int) {
	batch := chunkBatch{leaves: t.leaves}
	contiguous, _ := in.(contiguousInput)
	var scratch [treeChunkLen]byte
	for i := lo; i < hi; {
		if contiguous != nil {
			if base, run := contiguous.fullChunkRun(i); run >= treeLanes && i+treeLanes <= hi {
				var index [treeLanes]uint64
				for l := range index {
					index[l] = uint64(i + l)
				}
				var out [treeLanes]chainValue
				hashFullChunks16(base, &index, &out)
				copy(t.leaves[i:i+treeLanes], out[:])
				i += treeLanes
				continue
			}
		}
		chunk := in.chunk(i, &scratch)
		if len(chunk) == treeChunkLen {
			batch.add(chunk, i)
		} else {
			t.leaves[i] = chunkValue(chunk, uint64(i))
		}
		i++
	}
	batch.flush()
}

// A nodeJob recomputes one node from its two children. A child reference is a
// node index, or -(leaf index + 1) for a chunk.
type nodeJob struct {
	node, left, right int32
}

func childRef(base, first, count int) int32 {
	if count == 1 {
		return int32(-(first + 1))
	}
	return int32(base)
}

func (t *hashTree) refValue(ref int32) *chainValue {
	if ref < 0 {
		return &t.leaves[-ref-1]
	}
	return &t.nodes[ref]
}

// collectJobs lists, by depth, the nodes above the chunks in dirty (every node
// if all is set). The root (depth 0) is never stored and is not listed.
func (t *hashTree) collectJobs(base, first, count, depth int, dirty []int, all bool, levels *[][]nodeJob) {
	if count == 1 || (!all && len(dirty) == 0) {
		return
	}
	left := leftChunks(count)
	split := 0
	if !all {
		for split < len(dirty) && dirty[split] < first+left {
			split++
		}
		t.collectJobs(base+1, first, left, depth+1, dirty[:split], false, levels)
		t.collectJobs(base+left, first+left, count-left, depth+1, dirty[split:], false, levels)
	} else {
		t.collectJobs(base+1, first, left, depth+1, nil, true, levels)
		t.collectJobs(base+left, first+left, count-left, depth+1, nil, true, levels)
	}
	if depth == 0 {
		return
	}
	for len(*levels) <= depth {
		*levels = append(*levels, nil)
	}
	(*levels)[depth] = append((*levels)[depth], nodeJob{int32(base), childRef(base+1, first, left), childRef(base+left, first+left, count-left)})
}

// runJobs recomputes the listed nodes, deepest level first so that a node's
// children are final before it is hashed.
func (t *hashTree) runJobs(levels [][]nodeJob) {
	var scratch laneScratch
	var ctr [2 * treeLanes]uint32
	for depth := len(levels) - 1; depth >= 1; depth-- {
		jobs := levels[depth]
		for len(jobs) > 0 {
			n := min(len(jobs), treeLanes)
			for l := 0; l < n; l++ {
				block := scratch[l*treeLaneBytes:]
				left, right := t.refValue(jobs[l].left), t.refValue(jobs[l].right)
				for w := 0; w < 8; w++ {
					binary.LittleEndian.PutUint32(block[4*w:], left[w])
					binary.LittleEndian.PutUint32(block[32+4*w:], right[w])
				}
			}
			var cv [8][treeLanes]uint32
			for w := range cv {
				for l := range cv[w] {
					cv[w][l] = blake3IV[w]
				}
			}
			compress16(&cv, &scratch[0], &ctr, treeBlockLen, flagParent)
			for l := 0; l < n; l++ {
				node := &t.nodes[jobs[l].node]
				for w := range cv {
					node[w] = cv[w][l]
				}
			}
			jobs = jobs[n:]
		}
	}
}

func (t *hashTree) buildBatched(in treeInput, workers int) {
	count := len(t.leaves)
	workers = max(1, min(workers, runtime.GOMAXPROCS(0), count/(4*treeLanes)))
	if workers == 1 {
		t.hashLeavesBatched(in, 0, count)
	} else {
		var wg sync.WaitGroup
		wg.Add(workers - 1)
		for w := 1; w < workers; w++ {
			go func() {
				defer wg.Done()
				t.hashLeavesBatched(in, count*w/workers, count*(w+1)/workers)
			}()
		}
		t.hashLeavesBatched(in, 0, count/workers)
		wg.Wait()
	}
	var levels [][]nodeJob
	t.collectJobs(0, 0, count, 0, nil, true, &levels)
	t.runJobs(levels)
}

func (t *hashTree) updateBatched(in treeInput, dirty []int) {
	batch := chunkBatch{leaves: t.leaves}
	var scratch [treeChunkLen]byte
	for _, i := range dirty {
		chunk := in.chunk(i, &scratch)
		if len(chunk) == treeChunkLen {
			batch.add(chunk, i)
		} else {
			t.leaves[i] = chunkValue(chunk, uint64(i))
		}
	}
	batch.flush()
	var levels [][]nodeJob
	t.collectJobs(0, 0, len(t.leaves), 0, dirty, false, &levels)
	t.runJobs(levels)
}

// compress16MatchesPortable checks the 16-lane kernel against compress for
// every lane, with distinct chaining values, blocks and counters per lane and
// the block lengths and flags the tree uses.
func compress16MatchesPortable() bool {
	var scratch laneScratch
	seed := uint32(0x9E3779B9)
	next := func() uint32 {
		seed ^= seed << 13
		seed ^= seed >> 17
		seed ^= seed << 5
		return seed
	}
	for i := range scratch {
		scratch[i] = byte(next())
	}
	for _, c := range []struct{ blen, flags uint32 }{
		{treeBlockLen, flagChunkStart}, {treeBlockLen, 0}, {treeBlockLen, flagChunkEnd}, {treeBlockLen, flagParent},
	} {
		var cv [8][treeLanes]uint32
		var want [treeLanes]chainValue
		var ctr [2 * treeLanes]uint32
		for l := 0; l < treeLanes; l++ {
			var in chainValue
			for w := range in {
				in[w] = next()
				cv[w][l] = in[w]
			}
			ctr[l], ctr[treeLanes+l] = next(), next()&3
			var m [16]uint32
			loadBlock(&m, scratch[l*treeLaneBytes:l*treeLaneBytes+treeBlockLen])
			s := compress(&in, &m, uint64(ctr[l])|uint64(ctr[treeLanes+l])<<32, c.blen, c.flags)
			copy(want[l][:], s[:8])
		}
		compress16(&cv, &scratch[0], &ctr, c.blen, c.flags)
		for l := 0; l < treeLanes; l++ {
			for w := 0; w < 8; w++ {
				if cv[w][l] != want[l][w] {
					return false
				}
			}
		}
	}
	return true
}
