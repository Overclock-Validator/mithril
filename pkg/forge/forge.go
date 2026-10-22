package forge

import (
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/blockprod"
	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/packet"
)

// BankSource supplies the active leader working bank.
type BankSource interface {
	WorkingBank() *blockprod.WorkingBank
}

// Stats tracks IOD forge sink outcomes.
type Stats struct {
	InPackets               uint64
	InBytes                 uint64
	Accepted                uint64
	DroppedNoBank           uint64
	DroppedVote             uint64
	DroppedParse            uint64
	DroppedCost             uint64
	DroppedExecution        uint64
	DroppedAlreadyProcessed uint64
	DroppedAccountLocks     uint64
	DroppedLookup           uint64
	DroppedBlockCost        uint64
	DroppedAccountCost      uint64
	DroppedAllocCost        uint64
	DroppedBatchBytes       uint64
}

// Sink is an immediate-or-drop forge stage wired after sigverify.
type Sink struct {
	banks BankSource
	mu    sync.RWMutex
	stats Stats
}

func NewSink(banks BankSource) *Sink {
	return &Sink{banks: banks}
}

func (s *Sink) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.stats
}

func (s *Sink) Receive(pkt packet.Packet) {
	s.mu.Lock()
	data := pkt.Data()
	s.stats.InPackets++
	s.stats.InBytes += uint64(len(data))
	s.mu.Unlock()

	bank := s.banks.WorkingBank()
	if bank == nil {
		s.mu.Lock()
		s.stats.DroppedNoBank++
		s.mu.Unlock()
		pkt.Release()
		return
	}

	result, reason := bank.Forge(data)
	s.mu.Lock()
	switch result {
	case blockprod.ForgeAccepted:
		s.stats.Accepted++
	case blockprod.ForgeDroppedVote:
		s.stats.DroppedVote++
	case blockprod.ForgeDroppedParse:
		s.stats.DroppedParse++
	case blockprod.ForgeDroppedCost:
		s.stats.DroppedCost++
		s.recordCostDrop(reason)
	case blockprod.ForgeDroppedExecution:
		s.stats.DroppedExecution++
	case blockprod.ForgeDroppedAlreadyProcessed:
		s.stats.DroppedAlreadyProcessed++
	case blockprod.ForgeDroppedAccountLocks:
		s.stats.DroppedAccountLocks++
	case blockprod.ForgeDroppedLookup:
		s.stats.DroppedLookup++
	default:
		s.stats.DroppedNoBank++
	}
	s.mu.Unlock()
	pkt.Release()
}

func (s *Sink) recordCostDrop(reason costmodel.ExceedReason) {
	switch reason {
	case costmodel.ExceedBlockCost:
		s.stats.DroppedBlockCost++
	case costmodel.ExceedWritableAccountCost:
		s.stats.DroppedAccountCost++
	case costmodel.ExceedAllocatedDataSize:
		s.stats.DroppedAllocCost++
	case costmodel.ExceedBatchBytes:
		s.stats.DroppedBatchBytes++
	}
}
