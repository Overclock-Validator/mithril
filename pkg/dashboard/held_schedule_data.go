package dashboard

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"math/bits"
	"strconv"
	"strings"
	"time"
)

const (
	// MaxHeldScheduleDataBytes bounds current and next epoch datasets together.
	// It does not enlarge ordinary status, manifests or replay pages.
	MaxHeldScheduleDataBytes = 6 << 20
	maxHeldEpochSlots        = 1 << 20
	maxHeldEpochLeaders      = 4096
	minimumWarmupEpochSlots  = 32
)

// HeldEpochAssignments contains every slot in one locally held epoch range.
// Index 0 is unknown; other indices address Leaders[index-1]. Stride 4 denotes
// verified identical groups relative to FirstSlot, not assumed leader turns.
type HeldEpochAssignments struct {
	Epoch              uint64                   `json:"epoch,string"`
	FirstSlot          uint64                   `json:"first_slot,string"`
	SlotCount          uint64                   `json:"slot_count,string"`
	KnownSlots         uint64                   `json:"known_slots,string"`
	Encoding           string                   `json:"encoding"`
	Stride             uint8                    `json:"stride"`
	Leaders            []string                 `json:"leaders"`
	Indices            string                   `json:"indices"`
	StakeSet           *HeldEpochStakeSet       `json:"stake_set,omitempty"`
	StakeSetReason     string                   `json:"stake_set_reason,omitempty"`
	LocalReplay        *HeldEpochReplayTimes    `json:"local_replay,omitempty"`
	TargetSlotDuration *HeldEpochTargetDuration `json:"target_slot_duration,omitempty"`
}

// HeldEpochTargetDuration is the target implied by the captured bank's active
// features and restored timing seed, not observed timing or a producer setting.
type HeldEpochTargetDuration struct {
	Nanoseconds uint64 `json:"nanoseconds,string"`
	Provenance  string `json:"provenance"`
}

// HeldEpochReplayTimes records locally observed successful replay boundaries.
// A missing date was not observed in this replay lifetime. These are not bank
// Clock dates, finality times or estimates; wall-clock order is not guaranteed.
type HeldEpochReplayTimes struct {
	StartedAt *time.Time `json:"started_at"`
	EndedAt   *time.Time `json:"ended_at"`
}

// HeldEpochStakeSet is a complete VAT-selected cache generation, independently
// observed from held assignments. It does not prove those assignments used this
// generation. The parallel arrays are sorted by canonical identity text;
// no vote accounts, BLS keys or account balances are exposed.
type HeldEpochStakeSet struct {
	Generation uint64   `json:"generation,string"`
	TotalStake uint64   `json:"total_stake,string"`
	Identities []string `json:"identities"`
	Stakes     []string `json:"stakes"`
}

// HeldEpochSchedule is the checked epoch geometry used by this dataset.
type HeldEpochSchedule struct {
	SlotsPerEpoch    uint64 `json:"slots_per_epoch,string"`
	FirstNormalEpoch uint64 `json:"first_normal_epoch,string"`
	FirstNormalSlot  uint64 `json:"first_normal_slot,string"`
	Warmup           bool   `json:"warmup"`
}

// HeldScheduleData is an allowlisted immutable public dataset. ObservedAt and
// the original geometry bank are facts about its creation, not freshness or
// proof that this schedule produced, certified or finalized any block. Lease
// and collector dates belong to its separate small manifest.
type HeldScheduleData struct {
	SchemaVersion           int                   `json:"schema_version"`
	Family                  string                `json:"family"`
	Scope                   string                `json:"scope"`
	Binding                 ReplayDetailBinding   `json:"binding"`
	ObservedAt              time.Time             `json:"observed_at"`
	GeometryBankSlot        uint64                `json:"geometry_bank_slot,string"`
	GeometryBankhash        string                `json:"geometry_bankhash"`
	GeometryBankCompletedAt time.Time             `json:"geometry_bank_completed_at"`
	EpochSchedule           *HeldEpochSchedule    `json:"epoch_schedule,omitempty"`
	Current                 HeldEpochAssignments  `json:"current"`
	Next                    *HeldEpochAssignments `json:"next"`
	NextReason              string                `json:"next_reason"`
}

// HeldScheduleSummary describes dataset coverage without retaining indices or
// identities. KnownSlots counts held assignments, never blocks or skips.
type HeldScheduleSummary struct {
	Epoch      uint64 `json:"epoch,string"`
	FirstSlot  uint64 `json:"first_slot,string"`
	SlotCount  uint64 `json:"slot_count,string"`
	KnownSlots uint64 `json:"known_slots,string"`
	Leaders    int    `json:"leaders"`
}

// PreparedHeldScheduleData owns one canonical body and small copied metadata.
// It retains no raw construction buffer, decoded indices or dictionary slice.
// Node and private feed can share this immutable object without another body.
type PreparedHeldScheduleData struct {
	body       string
	digest     string
	binding    ReplayDetailBinding
	observedAt time.Time
	current    HeldScheduleSummary
	next       *HeldScheduleSummary
	nextReason string
}

// PrepareHeldScheduleData validates and seals the complete current/next data.
// Call it once per held-pointer/range/replay lifetime, never for each viewer.
func PrepareHeldScheduleData(d HeldScheduleData) (*PreparedHeldScheduleData, error) {
	if err := validateHeldScheduleData(d); err != nil {
		return nil, err
	}
	body, err := json.Marshal(d)
	if err != nil || len(body) > MaxHeldScheduleDataBytes {
		return nil, errors.New("response_limit")
	}
	hash := sha256.Sum256(body)
	p := &PreparedHeldScheduleData{body: string(body), digest: hex.EncodeToString(hash[:]), binding: d.Binding,
		observedAt: d.ObservedAt, current: heldScheduleSummary(d.Current), nextReason: d.NextReason}
	if d.Binding.GenesisHash != nil {
		value := *d.Binding.GenesisHash
		p.binding.GenesisHash = &value
	}
	if d.Next != nil {
		next := heldScheduleSummary(*d.Next)
		p.next = &next
	}
	return p, nil
}

func decodeHeldScheduleData(body []byte) (*PreparedHeldScheduleData, error) {
	if len(body) > MaxHeldScheduleDataBytes {
		return nil, errors.New("response_limit")
	}
	var d HeldScheduleData
	if json.Unmarshal(body, &d) != nil {
		return nil, errors.New("source_invalid")
	}
	p, err := PrepareHeldScheduleData(d)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal([]byte(p.body), body) {
		return nil, errors.New("source_invalid")
	}
	return p, nil
}

func validateHeldScheduleData(d HeldScheduleData) error {
	if d.SchemaVersion != 1 || d.Family != "held_schedule" || d.Scope != "locally_held_epoch_assignments" ||
		!validReplayDetailBinding(d.Binding) || !validReplayDetailTime(d.ObservedAt) ||
		!validReplayDetailHash(d.GeometryBankhash, true) || !validReplayDetailTime(d.GeometryBankCompletedAt) ||
		validateHeldEpoch(d.Current) != nil || d.GeometryBankSlot < d.Current.FirstSlot ||
		d.GeometryBankSlot-d.Current.FirstSlot >= d.Current.SlotCount {
		return errors.New("source_invalid")
	}
	if d.EpochSchedule != nil {
		if !validHeldEpochSchedule(*d.EpochSchedule) || !heldEpochMatchesSchedule(d.Current, *d.EpochSchedule) {
			return errors.New("source_invalid")
		}
	}
	if d.EpochSchedule == nil && (d.Current.TargetSlotDuration != nil || d.Next != nil && d.Next.TargetSlotDuration != nil) {
		return errors.New("source_invalid")
	}
	if d.Next == nil {
		if d.NextReason != "waiting_source" && d.NextReason != "response_limit" && d.NextReason != "range_unavailable" {
			return errors.New("source_invalid")
		}
		return nil
	}
	if d.NextReason != "" || validateHeldEpoch(*d.Next) != nil || d.Current.Epoch == math.MaxUint64 ||
		d.Current.SlotCount > math.MaxUint64-d.Current.FirstSlot ||
		d.Next.Epoch != d.Current.Epoch+1 || d.Next.FirstSlot != d.Current.FirstSlot+d.Current.SlotCount ||
		d.EpochSchedule != nil && !heldEpochMatchesSchedule(*d.Next, *d.EpochSchedule) ||
		d.Next.TargetSlotDuration != nil && (d.Current.TargetSlotDuration == nil || d.Next.TargetSlotDuration.Nanoseconds > d.Current.TargetSlotDuration.Nanoseconds) {
		return errors.New("source_invalid")
	}
	return nil
}

func validHeldEpochSchedule(s HeldEpochSchedule) bool {
	if s.SlotsPerEpoch == 0 {
		return false
	}
	if !s.Warmup {
		return s.FirstNormalEpoch == 0 && s.FirstNormalSlot == 0
	}
	if s.SlotsPerEpoch < minimumWarmupEpochSlots || s.SlotsPerEpoch > 1<<63 {
		return false
	}
	epoch := uint64(bits.Len64(s.SlotsPerEpoch-1) - bits.TrailingZeros64(minimumWarmupEpochSlots))
	return s.FirstNormalEpoch == epoch && s.FirstNormalSlot == ((uint64(1)<<epoch)-1)*minimumWarmupEpochSlots
}

// heldEpochMatchesSchedule uses validated scalar geometry so the public reader
// does not depend on the execution runtime. Its caller checks the schedule first.
func heldEpochMatchesSchedule(e HeldEpochAssignments, s HeldEpochSchedule) bool {
	if e.FirstSlot >= s.FirstNormalSlot {
		offset := e.FirstSlot - s.FirstNormalSlot
		delta := offset / s.SlotsPerEpoch
		return delta <= math.MaxUint64-s.FirstNormalEpoch && e.Epoch == s.FirstNormalEpoch+delta &&
			offset%s.SlotsPerEpoch == 0 && e.SlotCount == s.SlotsPerEpoch
	}
	epoch := uint64(bits.Len64(e.FirstSlot+minimumWarmupEpochSlots) - 1 - bits.TrailingZeros64(minimumWarmupEpochSlots))
	count := uint64(minimumWarmupEpochSlots) << epoch
	return e.Epoch == epoch && e.FirstSlot == count-minimumWarmupEpochSlots && e.SlotCount == count
}

func validateHeldEpoch(e HeldEpochAssignments) error {
	if !validHeldEpochGeometry(e.Epoch, e.FirstSlot, e.SlotCount) || e.KnownSlots > e.SlotCount ||
		e.Encoding != "base64_u16_le" || (e.Stride != 1 && e.Stride != 4) ||
		e.Leaders == nil || len(e.Leaders) > maxHeldEpochLeaders {
		return errors.New("source_invalid")
	}
	if e.TargetSlotDuration != nil && (e.TargetSlotDuration.Nanoseconds == 0 || e.TargetSlotDuration.Provenance != "snapshot_seed_and_bank_features") {
		return errors.New("source_invalid")
	}
	if e.LocalReplay != nil {
		if e.LocalReplay.StartedAt == nil && e.LocalReplay.EndedAt == nil ||
			e.LocalReplay.StartedAt != nil && !validReplayDetailTime(*e.LocalReplay.StartedAt) ||
			e.LocalReplay.EndedAt != nil && !validReplayDetailTime(*e.LocalReplay.EndedAt) {
			return errors.New("source_invalid")
		}
	}
	if e.StakeSet == nil {
		if e.StakeSetReason != "" && e.StakeSetReason != "waiting_source" && e.StakeSetReason != "selection_unverified" &&
			e.StakeSetReason != "not_applicable" && e.StakeSetReason != "source_invalid" && e.StakeSetReason != "response_limit" {
			return errors.New("source_invalid")
		}
	} else if e.StakeSetReason != "" || validateHeldEpochStakeSet(*e.StakeSet) != nil {
		return errors.New("source_invalid")
	}
	groups := (e.SlotCount + uint64(e.Stride) - 1) / uint64(e.Stride)
	if len(e.Indices) != base64.StdEncoding.EncodedLen(int(groups)*2) || strings.ContainsAny(e.Indices, "\r\n") {
		return errors.New("source_invalid")
	}
	indices, err := base64.StdEncoding.Strict().DecodeString(e.Indices)
	if err != nil || len(indices) != int(groups)*2 {
		return errors.New("source_invalid")
	}
	identities := make(map[string]struct{}, len(e.Leaders))
	for _, identity := range e.Leaders {
		if !validReplayDetailHash(identity, true) {
			return errors.New("source_invalid")
		}
		if _, exists := identities[identity]; exists {
			return errors.New("source_invalid")
		}
		identities[identity] = struct{}{}
	}
	seen := make([]bool, len(e.Leaders)+1)
	nextIndex, known := uint16(1), uint64(0)
	groupable := true
	for i := uint64(0); i < groups; i++ {
		index := binary.LittleEndian.Uint16(indices[i*2:])
		if int(index) > len(e.Leaders) {
			return errors.New("source_invalid")
		}
		if index != 0 {
			known += min(uint64(e.Stride), e.SlotCount-i*uint64(e.Stride))
			if !seen[index] {
				if index != nextIndex {
					return errors.New("source_invalid")
				}
				seen[index] = true
				nextIndex++
			}
		}
		if e.Stride == 1 && i%4 != 0 && index != binary.LittleEndian.Uint16(indices[(i-i%4)*2:]) {
			groupable = false
		}
	}
	if known != e.KnownSlots || int(nextIndex)-1 != len(e.Leaders) || e.Stride == 1 && groupable {
		return errors.New("source_invalid")
	}
	return nil
}

func validateHeldEpochStakeSet(s HeldEpochStakeSet) error {
	if s.Generation == 0 || s.Identities == nil || s.Stakes == nil || len(s.Identities) > 2000 || len(s.Identities) != len(s.Stakes) {
		return errors.New("source_invalid")
	}
	var total uint64
	for i, identity := range s.Identities {
		if !validReplayDetailHash(identity, true) || i > 0 && s.Identities[i-1] >= identity {
			return errors.New("source_invalid")
		}
		stake, err := strconv.ParseUint(s.Stakes[i], 10, 64)
		if err != nil || stake == 0 || strconv.FormatUint(stake, 10) != s.Stakes[i] || stake > math.MaxUint64-total {
			return errors.New("source_invalid")
		}
		total += stake
	}
	if total != s.TotalStake {
		return errors.New("source_invalid")
	}
	return nil
}

func validHeldEpochGeometry(epoch, first, count uint64) bool {
	return count != 0 && count <= maxHeldEpochSlots && first <= math.MaxUint64-(count-1) &&
		epoch <= first && (epoch == 0) == (first == 0)
}

func heldScheduleSummary(e HeldEpochAssignments) HeldScheduleSummary {
	return HeldScheduleSummary{Epoch: e.Epoch, FirstSlot: e.FirstSlot, SlotCount: e.SlotCount,
		KnownSlots: e.KnownSlots, Leaders: len(e.Leaders)}
}
