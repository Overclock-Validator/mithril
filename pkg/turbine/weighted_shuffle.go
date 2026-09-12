package turbine

type rngSource interface {
	Uint64() uint64
}

const (
	weightedShuffleBitShift = 4
	weightedShuffleFanout   = 1 << weightedShuffleBitShift
	weightedShuffleBitMask  = weightedShuffleFanout - 1
)

type WeightedShuffle struct {
	numNodes int
	tree     [][weightedShuffleFanout]uint64
	weight   uint64
	zeros    []int
}

func NewWeightedShuffle(weights []uint64) *WeightedShuffle {
	numNodes, size := weightedShuffleTreeSize(len(weights))
	tree := make([][weightedShuffleFanout]uint64, size)
	var sum uint64
	zeros := make([]int, 0)
	for k, weight := range weights {
		if weight == 0 {
			zeros = append(zeros, k)
			continue
		}
		next, ok := safeAdd(sum, weight)
		if !ok {
			zeros = append(zeros, k)
			continue
		}
		sum = next
		index := numNodes + k
		for index != 0 {
			offset := (index - 1) & weightedShuffleBitMask
			index = (index - 1) >> weightedShuffleBitShift
			tree[index][offset] += weight
		}
	}
	return &WeightedShuffle{
		numNodes: numNodes,
		tree:     tree,
		weight:   sum,
		zeros:    zeros,
	}
}

func (w *WeightedShuffle) RemoveIndex(k int) {
	index := w.numNodes + k
	offset := (index - 1) & weightedShuffleBitMask
	parent := (index - 1) >> weightedShuffleBitShift
	if parent >= len(w.tree) {
		w.removeZero(k)
		return
	}
	weight := w.tree[parent][offset]
	if weight == 0 {
		w.removeZero(k)
		return
	}
	w.weight -= weight
	index = w.numNodes + k
	for index != 0 {
		offset := (index - 1) & weightedShuffleBitMask
		index = (index - 1) >> weightedShuffleBitShift
		w.tree[index][offset] -= weight
	}
}

func (w *WeightedShuffle) First(rng rngSource) (int, bool) {
	if w.weight > 0 {
		sampler := newUniformU64TraitSampler(w.weight)
		sample := sampler.sample(rng)
		index, _ := w.search(sample)
		return index, true
	}
	if len(w.zeros) == 0 {
		return 0, false
	}
	sampler := newUniformU64TraitSampler(uint64(len(w.zeros)))
	idx := int(sampler.sample(rng))
	return w.zeros[idx], true
}

// Next removes and returns the next index from the stake-weighted shuffle.
// Positive-weight entries are sampled first; zero-weight entries follow in a
// uniformly shuffled tail. This matches Agave's WeightedShuffle iterator and
// is used when deriving the complete Turbine retransmit tree.
func (w *WeightedShuffle) Next(rng rngSource) (int, bool) {
	if w.weight > 0 {
		sampler := newUniformU64TraitSampler(w.weight)
		sample := sampler.sample(rng)
		index, weight := w.search(sample)
		w.remove(index, weight)
		return index, true
	}
	if len(w.zeros) == 0 {
		return 0, false
	}
	sampler := newUniformU64TraitSampler(uint64(len(w.zeros)))
	index := int(sampler.sample(rng))
	value := w.zeros[index]
	w.zeros[index] = w.zeros[len(w.zeros)-1]
	w.zeros = w.zeros[:len(w.zeros)-1]
	return value, true
}

func (w *WeightedShuffle) remove(k int, weight uint64) {
	w.weight -= weight
	index := w.numNodes + k
	for index != 0 {
		offset := (index - 1) & weightedShuffleBitMask
		index = (index - 1) >> weightedShuffleBitShift
		w.tree[index][offset] -= weight
	}
}

func (w *WeightedShuffle) search(val uint64) (int, uint64) {
	index := 0
	for {
		var node uint64
		var offset int
		found := false
		for i, weight := range w.tree[index] {
			if val < weight {
				node = weight
				offset = i
				found = true
				break
			}
			val -= weight
		}
		if !found {
			panic("weighted shuffle search failed")
		}
		index = (index << weightedShuffleBitShift) + offset + 1
		if index >= len(w.tree) {
			return index - w.numNodes, node
		}
	}
}

func (w *WeightedShuffle) removeZero(k int) {
	for i, ix := range w.zeros {
		if ix == k {
			w.zeros[i] = w.zeros[len(w.zeros)-1]
			w.zeros = w.zeros[:len(w.zeros)-1]
			return
		}
	}
}

func weightedShuffleTreeSize(count int) (numNodes, treeSize int) {
	size := 0
	nodes := 1
	for nodes*weightedShuffleFanout < count {
		size += nodes
		nodes *= weightedShuffleFanout
	}
	return size + nodes, size + (count+weightedShuffleFanout-1)/weightedShuffleFanout
}

func safeAdd(a, b uint64) (uint64, bool) {
	if a > ^uint64(0)-b {
		return 0, false
	}
	return a + b, true
}

func rngUint64n(rng rngSource, n uint64) uint64 {
	if n <= 1 {
		return 0
	}
	return rng.Uint64() % n
}
