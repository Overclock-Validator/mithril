package leaderschedule

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/gagliardetto/solana-go"
)

const (
	// MaxHeldScheduleSlots bounds a diagnostic copy, not the protocol epoch size.
	MaxHeldScheduleSlots = 1 << 20
	// MaxHeldScheduleLeaders bounds its public identity dictionary.
	MaxHeldScheduleLeaders = 4096
)

// HeldScheduleSnapshot is a detached copy of all locally held assignments in
// one inclusive-first, counted range. Zero indices are unknown assignments,
// not skipped slots. Leaders contains only identities appearing in this range,
// not the validator stake set. The caller owns its dictionary slice.
type HeldScheduleSnapshot struct {
	FirstSlot, SlotCount, KnownSlots uint64
	Stride                           uint8
	Leaders                          []solana.PublicKey
	Indices                          string
}

// CaptureHeldSchedule copies one immutable schedule without touching the
// next-identity cache, global state or account data. Callers cache the result
// per schedule pointer and range; it is not a per-viewer or protocol operation.
func (ls *LeaderSchedule) CaptureHeldSchedule(first, count uint64) (HeldScheduleSnapshot, error) {
	if ls == nil {
		return HeldScheduleSnapshot{}, errors.New("schedule unavailable")
	}
	if count == 0 || count > MaxHeldScheduleSlots || first > math.MaxUint64-(count-1) {
		return HeldScheduleSnapshot{}, errors.New("unsupported schedule range")
	}
	snapshot := HeldScheduleSnapshot{FirstSlot: first, SlotCount: count, Stride: 1, Leaders: make([]solana.PublicKey, 0)}
	dictionary := make(map[solana.PublicKey]uint16)
	indices := make([]byte, int(count)*2)
	for offset := uint64(0); offset < count; offset++ {
		leader, ok := ls.lsMap[first+offset]
		if !ok || leader.nodePubkey.IsZero() {
			continue
		}
		index, exists := dictionary[leader.nodePubkey]
		if !exists {
			if len(snapshot.Leaders) == MaxHeldScheduleLeaders {
				return HeldScheduleSnapshot{}, errors.New("schedule identity limit")
			}
			snapshot.Leaders = append(snapshot.Leaders, leader.nodePubkey)
			index = uint16(len(snapshot.Leaders))
			dictionary[leader.nodePubkey] = index
		}
		binary.LittleEndian.PutUint16(indices[int(offset)*2:], index)
		snapshot.KnownSlots++
	}
	// Group only after checking every member, including an incomplete tail.
	// Node-keyed or partial schedules need not have four identical assignments.
	grouped := true
	for offset := 0; offset < len(indices); offset += 8 {
		index := binary.LittleEndian.Uint16(indices[offset:])
		for member := offset + 2; member < min(offset+8, len(indices)); member += 2 {
			if binary.LittleEndian.Uint16(indices[member:]) != index {
				grouped = false
				break
			}
		}
		if !grouped {
			break
		}
	}
	if grouped {
		for offset := 0; offset < len(indices); offset += 8 {
			copy(indices[offset/4:offset/4+2], indices[offset:offset+2])
		}
		indices = indices[:((int(count)+3)/4)*2]
		snapshot.Stride = 4
	}
	snapshot.Indices = string(indices)
	return snapshot, nil
}

// LeaderForSlot looks up this copied range. False means outside its range or
// an unknown assignment, never evidence that a block was skipped.
func (s HeldScheduleSnapshot) LeaderForSlot(slot uint64) (solana.PublicKey, bool) {
	if slot < s.FirstSlot || slot-s.FirstSlot >= s.SlotCount || (s.Stride != 1 && s.Stride != 4) {
		return solana.PublicKey{}, false
	}
	offset := (slot - s.FirstSlot) / uint64(s.Stride) * 2
	if offset >= uint64(len(s.Indices)) || uint64(len(s.Indices))-offset < 2 {
		return solana.PublicKey{}, false
	}
	index := uint16(s.Indices[offset]) | uint16(s.Indices[offset+1])<<8
	if index == 0 || int(index) > len(s.Leaders) {
		return solana.PublicKey{}, false
	}
	leader := s.Leaders[index-1]
	return leader, !leader.IsZero()
}
