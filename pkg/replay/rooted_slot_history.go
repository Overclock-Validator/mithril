package replay

import (
	"encoding/base64"
	"encoding/binary"
	"math"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
)

// RootedSlotHistoryCapacity is the protocol bitmap's retained slot count.
const RootedSlotHistoryCapacity = 1024 * 1024

const rootedHistoryBitmapBytes = RootedSlotHistoryCapacity / 8

// RootedSlotHistory is the SlotHistory of one successfully folded bank. Its
// immutable bitmap preserves pre-attempt history without borrowing bank memory.
// Copies are detached values; the retained range is not a cluster-tip estimate.
type RootedSlotHistory struct {
	ObservedAt time.Time
	BankSlot   uint64
	Bankhash   solana.Hash
	OldestSlot uint64
	NextSlot   uint64
	Epoch      RootedHistoryEpoch
	bitmap     string
}

// RootedHistoryEpoch is optional geometry from the same folded bank. Known is
// false when its epoch sysvar is absent, invalid or has an overflowing end.
type RootedHistoryEpoch struct {
	Known                    bool
	Number, FirstSlot, Slots uint64
}

type rootedSlotHistoryObservation struct {
	RootedSlotHistory
	owner      *replayDiagnostics
	generation uint64
}

// RootedSlotHistorySnapshot binds one immutable bank history to its replay
// lifetime without copying the separate completed-row diagnostic history.
type RootedSlotHistorySnapshot struct {
	RunID              string
	Generation         uint64
	Attempt, Rewinds   uint64
	StartedAt, ResetAt time.Time
	History            RootedSlotHistory
}

// RootedHistorySnapshot copies only retained in-memory values. It does not
// inspect the accounts database or wait for a newer bank.
func (p *Progress) RootedHistorySnapshot() (RootedSlotHistorySnapshot, bool) {
	if p == nil {
		return RootedSlotHistorySnapshot{}, false
	}
	d := p.diagnostics.Load()
	if d == nil {
		return RootedSlotHistorySnapshot{}, false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.generation == 0 || d.exhausted || !d.rootedHistory.valid() {
		return RootedSlotHistorySnapshot{}, false
	}
	return RootedSlotHistorySnapshot{RunID: d.runID, Generation: d.generation, Attempt: d.attempt,
		Rewinds: d.rewinds, StartedAt: d.startedAt, ResetAt: d.resetAt, History: d.rootedHistory}, true
}

func captureRootedSlotHistory(slot uint64, ctx *state.ResumeContext, bank *sealevel.BankSysvars, at time.Time) RootedSlotHistory {
	if ctx == nil || bank == nil || slot == math.MaxUint64 || ctx.Slot != slot || bank.Slot() != slot || at.IsZero() {
		return RootedSlotHistory{}
	}
	hash, err := solana.HashFromBase58(ctx.Bankhash)
	if err != nil || hash.IsZero() {
		return RootedSlotHistory{}
	}
	history, ok := bank.SlotHistory()
	if !ok || history.NextSlot != slot+1 || history.Bits.Len != RootedSlotHistoryCapacity ||
		history.Bits.Bits.BlocksLen != RootedSlotHistoryCapacity/64 || len(history.Bits.Bits.Blocks) != RootedSlotHistoryCapacity/64 {
		return RootedSlotHistory{}
	}
	// The bank owns both views. Copy only the canonical bitmap, not the rest of
	// its sysvar accounts or the decoded slice backing array.
	raw, ok := bank.RawView(solana.PublicKey(sealevel.SysvarSlotHistoryAddr))
	if !ok || len(raw) != 1+8+rootedHistoryBitmapBytes+8+8 || raw[0] != 1 ||
		binary.LittleEndian.Uint64(raw[1:9]) != RootedSlotHistoryCapacity/64 ||
		binary.LittleEndian.Uint64(raw[len(raw)-16:len(raw)-8]) != RootedSlotHistoryCapacity ||
		binary.LittleEndian.Uint64(raw[len(raw)-8:]) != history.NextSlot {
		return RootedSlotHistory{}
	}
	bitmap := raw[9 : 9+rootedHistoryBitmapBytes]
	if bitmap[(slot%RootedSlotHistoryCapacity)/8]&(1<<(slot%8)) == 0 {
		return RootedSlotHistory{}
	}
	oldest := uint64(0)
	if history.NextSlot > RootedSlotHistoryCapacity {
		oldest = history.NextSlot - RootedSlotHistoryCapacity
	}
	result := RootedSlotHistory{ObservedAt: at, BankSlot: slot, Bankhash: hash,
		OldestSlot: oldest, NextSlot: history.NextSlot, bitmap: string(bitmap)}
	if schedule, ok := bank.EpochSchedule(); ok && admissionScheduleValid(&schedule) {
		if epoch, end, ok := admissionEpochEnd(&schedule, slot); ok {
			length := schedule.SlotsInEpoch(epoch)
			result.Epoch = RootedHistoryEpoch{Known: true, Number: epoch, FirstSlot: end - length, Slots: length}
		}
	}
	return result
}

// Check distinguishes known produced/skipped slots from future or expired data.
// It does not validate consensus or infer a skip outside this bank's history.
func (h RootedSlotHistory) Check(slot uint64) (produced, known bool) {
	if !h.valid() || slot < h.OldestSlot || slot >= h.NextSlot {
		return false, false
	}
	return h.bitmap[(slot%RootedSlotHistoryCapacity)/8]&(1<<(slot%8)) != 0, true
}

// BitmapBase64 encodes the immutable little-endian protocol bitmap. Callers
// should cache the result, not encode it for each dashboard viewer.
func (h RootedSlotHistory) BitmapBase64() (string, bool) {
	if !h.valid() {
		return "", false
	}
	return base64.StdEncoding.EncodeToString([]byte(h.bitmap)), true
}

func (h RootedSlotHistory) valid() bool {
	if h.ObservedAt.IsZero() || h.Bankhash.IsZero() || h.NextSlot == 0 || h.BankSlot != h.NextSlot-1 || len(h.bitmap) != rootedHistoryBitmapBytes {
		return false
	}
	oldest := uint64(0)
	if h.NextSlot > RootedSlotHistoryCapacity {
		oldest = h.NextSlot - RootedSlotHistoryCapacity
	}
	return h.OldestSlot == oldest && h.bitmap[(h.BankSlot%RootedSlotHistoryCapacity)/8]&(1<<(h.BankSlot%8)) != 0
}

// observeRootedSlotHistory runs on the replay owner, after a successful fold
// and before its exact bank snapshot is pruned. Default-off adds no clock/copy.
func (t *unrootedTail) observeRootedSlotHistory(slot uint64, ctx *state.ResumeContext) {
	if t == nil || t.rootedHistoryOwner == nil {
		return
	}
	d := t.rootedHistoryOwner
	d.mu.Lock()
	generation, exhausted := d.generation, d.exhausted
	d.mu.Unlock()
	observation := rootedSlotHistoryObservation{owner: d, generation: generation}
	if generation != 0 && !exhausted {
		observation.RootedSlotHistory = captureRootedSlotHistory(slot, ctx, t.bankSysvars[slot], time.Now())
	}
	t.rootedHistory = observation
}

// recordRootedSlotHistory is called only by successful loop bookkeeping. It
// publishes no progress, readiness or protocol state and needs no completed row.
func (p *Progress) recordRootedSlotHistory(slot uint64, ctx *state.ResumeContext, committedHash solana.Hash, observation rootedSlotHistoryObservation) {
	if p == nil {
		return
	}
	d := p.diagnostics.Load()
	if d == nil || observation.owner != d {
		return
	}
	s, ok := p.Snapshot()
	if !ok {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.exhausted || d.generation == 0 || observation.generation != d.generation || d.attempt != s.Attempt || d.rewinds != s.Rewinds {
		return
	}
	history := observation.RootedSlotHistory
	if ctx == nil || ctx.Slot != slot || !history.valid() || history.BankSlot != slot ||
		history.Bankhash != committedHash || history.Bankhash.String() != ctx.Bankhash {
		d.rootedHistory = RootedSlotHistory{}
		return
	}
	d.rootedHistory = history
}
