package rewards

import (
	"sync"

	"github.com/gagliardetto/solana-go"
)

type Partition struct {
	pubkeys      []solana.PublicKey
	mu           sync.Mutex
	partitionIdx uint64
}

type Partitions []*Partition

func NewPartitions(numPartitions uint64) Partitions {
	p := make(Partitions, numPartitions)
	for i := uint64(0); i < numPartitions; i++ {
		p[i] = &Partition{pubkeys: make([]solana.PublicKey, 0, 2000), partitionIdx: i}
	}
	return p
}

func (partitions Partitions) AddPubkey(partitionIdx uint64, pk solana.PublicKey) {
	prt := partitions[partitionIdx]
	prt.mu.Lock()
	prt.pubkeys = append(prt.pubkeys, pk)
	prt.mu.Unlock()
}

func (partitions Partitions) Partition(partitionIdx uint64) *Partition {
	return partitions[partitionIdx]
}

func (partitions Partitions) NumPartitions() uint64 {
	return uint64(len(partitions))
}

func (partition *Partition) NumPubkeys() uint64 {
	return uint64(len(partition.pubkeys))
}

func (partition *Partition) Pubkeys() []solana.PublicKey {
	return partition.pubkeys
}
