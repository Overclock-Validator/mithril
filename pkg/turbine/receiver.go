package turbine

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/gagliardetto/solana-go"
)

type LeaderForSlotFunc func(slot uint64) (solana.PublicKey, bool)

type UDPReceiver struct {
	Addr string

	assembler     *SlotAssembler
	leaderForSlot LeaderForSlotFunc
	repairClient  *repairClient
	blocks        chan *block.Block
	errs          chan error
	ready         chan error
	once          sync.Once
	readyOnce     sync.Once

	packets         atomic.Uint64
	dataShreds      atomic.Uint64
	codingShreds    atomic.Uint64
	parseErrors     atomic.Uint64
	signatureErrors atomic.Uint64
	missingLeaders  atomic.Uint64
	assemblyErrors  atomic.Uint64
	blocksEmitted   atomic.Uint64
	lastPacketUnix  atomic.Int64
	lastDataSlot    atomic.Uint64
	lastBlockSlot   atomic.Uint64
}

type ReceiverStats struct {
	Packets              uint64
	DataShreds           uint64
	CodingShreds         uint64
	ParseErrors          uint64
	SignatureErrors      uint64
	MissingLeaders       uint64
	AssemblyErrors       uint64
	BlocksEmitted        uint64
	RecoveredData        uint64
	EvictedSlots         uint64
	IgnoredOldShreds     uint64
	PriorityRepairSlots  int
	NonCanonicalBlockIDs uint64
	LastNonCanonicalSlot uint64
	LastNonCanonicalGot  solana.Hash
	LastNonCanonicalWant solana.Hash
	Repair               RepairStats
	LastPacketUnix       int64
	LastDataSlot         uint64
	LastBlockSlot        uint64
	ActiveSlots          int
}

func NewUDPReceiver(addr string) *UDPReceiver {
	return &UDPReceiver{
		Addr:      addr,
		assembler: NewSlotAssembler(),
		blocks:    make(chan *block.Block, 1024),
		errs:      make(chan error, 16),
		ready:     make(chan error, 1),
	}
}

func (r *UDPReceiver) SetLeaderForSlot(fn LeaderForSlotFunc) {
	r.leaderForSlot = fn
}

func (r *UDPReceiver) SetRepairPeerSource(identity ed25519.PrivateKey, source func() []gossip.RepairPeer) error {
	client, err := newRepairClient(identity, source)
	if err != nil {
		return err
	}
	r.repairClient = client
	return nil
}

func (r *UDPReceiver) SetKnownAlpenglowBlockID(slot uint64, blockID solana.Hash) {
	if r == nil || r.assembler == nil {
		return
	}
	r.assembler.SetKnownAlpenglowBlockID(slot, blockID)
}

func (r *UDPReceiver) ResetSlot(slot uint64) {
	if r == nil || r.assembler == nil {
		return
	}
	r.assembler.ResetSlot(slot)
}

// ShredEdges reports the monotonic shred frontier from the assembler: highest
// slot with any accepted shred, and highest slot that became full.
func (r *UDPReceiver) ShredEdges() (latestShredSlot, highestFullSlot uint64) {
	return r.assembler.ShredEdges()
}

// ShredObservation reports partial shred arrivals for a slot that never
// became full (skip observability).
func (r *UDPReceiver) ShredObservation(slot uint64) (PartialShredObservation, bool) {
	return r.assembler.ShredObservation(slot)
}

func (r *UDPReceiver) PrioritizeRepairSlot(slot uint64) {
	if r == nil || r.assembler == nil {
		return
	}
	r.assembler.PrioritizeRepairSlot(slot)
}

func (r *UDPReceiver) PrioritizeRepairRange(start, end uint64) {
	if r == nil || r.assembler == nil {
		return
	}
	r.assembler.PrioritizeRepairRange(start, end)
}

func (r *UDPReceiver) Blocks() <-chan *block.Block {
	return r.blocks
}

func (r *UDPReceiver) Errors() <-chan error {
	return r.errs
}

func (r *UDPReceiver) Ready() <-chan error {
	return r.ready
}

func (r *UDPReceiver) Stats() ReceiverStats {
	nonCanonicalCount, nonCanonicalSlot, nonCanonicalGot, nonCanonicalWant := r.assembler.NonCanonicalBlockIDStats()
	return ReceiverStats{
		Packets:              r.packets.Load(),
		DataShreds:           r.dataShreds.Load(),
		CodingShreds:         r.codingShreds.Load(),
		ParseErrors:          r.parseErrors.Load(),
		SignatureErrors:      r.signatureErrors.Load(),
		MissingLeaders:       r.missingLeaders.Load(),
		AssemblyErrors:       r.assemblyErrors.Load(),
		BlocksEmitted:        r.blocksEmitted.Load(),
		RecoveredData:        r.assembler.RecoveredDataShreds(),
		EvictedSlots:         r.assembler.EvictedSlots(),
		IgnoredOldShreds:     r.assembler.IgnoredOldShreds(),
		PriorityRepairSlots:  r.assembler.PriorityRepairSlots(),
		NonCanonicalBlockIDs: nonCanonicalCount,
		LastNonCanonicalSlot: nonCanonicalSlot,
		LastNonCanonicalGot:  nonCanonicalGot,
		LastNonCanonicalWant: nonCanonicalWant,
		Repair:               r.repairStats(),
		LastPacketUnix:       r.lastPacketUnix.Load(),
		LastDataSlot:         r.lastDataSlot.Load(),
		LastBlockSlot:        r.lastBlockSlot.Load(),
		ActiveSlots:          r.assembler.ActiveSlots(),
	}
}

func (r *UDPReceiver) repairStats() RepairStats {
	if r.repairClient == nil {
		return RepairStats{}
	}
	return r.repairClient.stats()
}

func (r *UDPReceiver) signalReady(err error) {
	r.readyOnce.Do(func() {
		r.ready <- err
		close(r.ready)
	})
}

func (r *UDPReceiver) Run(ctx context.Context) error {
	defer r.once.Do(func() {
		close(r.blocks)
		close(r.errs)
	})

	udpAddr, err := net.ResolveUDPAddr("udp", r.Addr)
	if err != nil {
		r.signalReady(err)
		return err
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		r.signalReady(err)
		return err
	}
	defer conn.Close()
	r.signalReady(nil)

	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()
	if r.repairClient != nil {
		go r.repairClient.run(ctx, conn, r.assembler)
	}

	buf := make([]byte, packetDataSize)
	for {
		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		r.packets.Add(1)
		r.lastPacketUnix.Store(time.Now().Unix())
		packet := buf[:n]
		if r.repairClient != nil && r.repairClient.handleRepairPing(conn, packet, addr) {
			continue
		}
		shred, err := ParseShred(packet)
		if err != nil {
			if errors.Is(err, ErrCodingShredIgnored) {
				r.codingShreds.Add(1)
				continue
			}
			r.parseErrors.Add(1)
			select {
			case r.errs <- err:
			default:
			}
			continue
		}
		switch shred.Type {
		case ShredTypeData:
			r.dataShreds.Add(1)
			// Monotonic max: out-of-order packets must not move the reported
			// latest-shred edge backward.
			for {
				cur := r.lastDataSlot.Load()
				if shred.Slot <= cur || r.lastDataSlot.CompareAndSwap(cur, shred.Slot) {
					break
				}
			}
		case ShredTypeCode:
			r.codingShreds.Add(1)
		}
		if r.leaderForSlot != nil {
			leader, ok := r.leaderForSlot(shred.Slot)
			if !ok {
				r.missingLeaders.Add(1)
				select {
				case r.errs <- fmt.Errorf("missing leader for turbine shred slot %d", shred.Slot):
				default:
				}
				continue
			}
			if err := shred.VerifySignature(leader); err != nil {
				r.signatureErrors.Add(1)
				select {
				case r.errs <- err:
				default:
				}
				continue
			}
		}
		fromRepair := false
		if r.repairClient != nil {
			fromRepair = r.repairClient.observeShredResponse(conn, packet, addr, shred)
		}
		blk, err := r.assembler.AddShredFrom(shred, fromRepair)
		if err != nil {
			if errors.Is(err, ErrDuplicateShred) {
				continue
			}
			r.assemblyErrors.Add(1)
			select {
			case r.errs <- err:
			default:
			}
			continue
		}
		if blk == nil {
			continue
		}
		r.blocksEmitted.Add(1)
		r.lastBlockSlot.Store(blk.Slot)
		select {
		case r.blocks <- blk:
		case <-ctx.Done():
			return nil
		}
	}
}
