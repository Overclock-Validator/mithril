package dashboard

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/leaderschedule"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func heldEpochDataFixture(t *testing.T, epoch, first uint64) HeldEpochAssignments {
	return heldEpochDataRangeFixture(t, epoch, first, 8)
}

func heldEpochDataRangeFixture(t *testing.T, epoch, first, count uint64) HeldEpochAssignments {
	t.Helper()
	a, b := solana.PublicKey{1}, solana.PublicKey{2}
	rows := map[solana.PublicKey][]uint64{}
	for i := uint64(0); i < count; i++ {
		key := a
		if i >= count/2 {
			key = b
		}
		rows[key] = append(rows[key], i)
	}
	ls := leaderschedule.NewLeaderScheduleFromKeyedSlots(rows, first)
	snapshot, err := ls.CaptureHeldSchedule(first, count)
	require.NoError(t, err)
	leaders := make([]string, len(snapshot.Leaders))
	for i, key := range snapshot.Leaders {
		leaders[i] = key.String()
	}
	return HeldEpochAssignments{Epoch: epoch, FirstSlot: first, SlotCount: snapshot.SlotCount, KnownSlots: snapshot.KnownSlots,
		Encoding: "base64_u16_le", Stride: snapshot.Stride, Leaders: leaders, Indices: base64.StdEncoding.EncodeToString([]byte(snapshot.Indices))}
}

func TestHeldScheduleDataWarmupAndFinalRepresentableSlot(t *testing.T) {
	d := heldScheduleDataFixture(t)
	d.EpochSchedule = &HeldEpochSchedule{SlotsPerEpoch: 128, FirstNormalEpoch: 2, FirstNormalSlot: 96, Warmup: true}
	d.Current = heldEpochDataRangeFixture(t, 0, 0, 32)
	next := heldEpochDataRangeFixture(t, 1, 32, 64)
	d.Next = &next
	p, err := PrepareHeldScheduleData(d)
	require.NoError(t, err)
	decoded, err := decodeHeldScheduleData([]byte(p.body))
	require.NoError(t, err)
	require.Equal(t, uint64(32), decoded.current.SlotCount)
	require.Equal(t, uint64(64), decoded.next.SlotCount, "warmup does not repeat the current epoch's length")

	d.Current = heldEpochDataRangeFixture(t, (^uint64(0)-1)/2, ^uint64(0)-1, 2)
	d.EpochSchedule = &HeldEpochSchedule{SlotsPerEpoch: 2}
	d.GeometryBankSlot = ^uint64(0)
	d.Next, d.NextReason = nil, "range_unavailable"
	p, err = PrepareHeldScheduleData(d)
	require.NoError(t, err, "an inclusive range may end at the final representable slot")
	_, err = decodeHeldScheduleData([]byte(p.body))
	require.NoError(t, err)
	next = heldEpochDataRangeFixture(t, 0, 0, 1)
	d.Next, d.NextReason = &next, ""
	_, err = PrepareHeldScheduleData(d)
	require.Error(t, err, "an overflowing successor cannot wrap to slot0")
}

func heldScheduleDataFixture(t *testing.T) HeldScheduleData {
	t.Helper()
	base := rootedHistoryDetailFixture()
	next := heldEpochDataFixture(t, 1, 8)
	return HeldScheduleData{SchemaVersion: 1, Family: "held_schedule", Scope: "locally_held_epoch_assignments", Binding: base.Binding,
		ObservedAt: base.ObservedAt, GeometryBankSlot: 3, GeometryBankhash: base.Bankhash, GeometryBankCompletedAt: base.CapturedAt,
		EpochSchedule: &HeldEpochSchedule{SlotsPerEpoch: 8}, Current: heldEpochDataFixture(t, 0, 0), Next: &next}
}

func TestHeldScheduleDataCanonicalDetachedAndCoverage(t *testing.T) {
	d := heldScheduleDataFixture(t)
	genesis := d.GeometryBankhash
	d.Binding.GenesisHash = &genesis
	p, err := PrepareHeldScheduleData(d)
	require.NoError(t, err)
	body := p.body
	hash := sha256.Sum256([]byte(body))
	require.Equal(t, hex.EncodeToString(hash[:]), p.digest)
	require.Equal(t, uint64(8), p.current.KnownSlots)
	require.Equal(t, uint64(1), p.next.Epoch)
	decoded, err := decodeHeldScheduleData([]byte(body))
	require.NoError(t, err)
	require.Equal(t, p.body, decoded.body)
	d.Current.Leaders[0] = "changed"
	d.Next.Indices = "changed"
	d.EpochSchedule.SlotsPerEpoch = 16
	genesis = "changed"
	require.Equal(t, body, p.body)
	require.NotEqual(t, genesis, *p.binding.GenesisHash)

	d = heldScheduleDataFixture(t)
	d.Next, d.NextReason = nil, "waiting_source"
	d.Current.Leaders = []string{}
	d.Current.Indices = base64.StdEncoding.EncodeToString(make([]byte, 4))
	d.Current.KnownSlots = 0
	p, err = PrepareHeldScheduleData(d)
	require.NoError(t, err, "an empty held range is unknown, not a missing dataset")
	require.Zero(t, p.current.KnownSlots)
	require.Nil(t, p.next)
	require.Equal(t, "waiting_source", p.nextReason)

	d = heldScheduleDataFixture(t)
	d.Next, d.NextReason = nil, "range_unavailable"
	d.Current.SlotCount, d.Current.KnownSlots = 7, 7
	d.EpochSchedule.SlotsPerEpoch = 7
	_, err = PrepareHeldScheduleData(d)
	require.NoError(t, err, "the final stride group contributes only its three slots")
	d.Current.KnownSlots = 6
	_, err = PrepareHeldScheduleData(d)
	require.Error(t, err)
}

func TestHeldScheduleDataRejectsMalformedAssignmentsAndEnvelope(t *testing.T) {
	for _, change := range []func(*HeldScheduleData){
		func(d *HeldScheduleData) { d.Scope = "certified_schedule" },
		func(d *HeldScheduleData) { d.GeometryBankSlot = 8 },
		func(d *HeldScheduleData) { d.ObservedAt = time.Time{} },
		func(d *HeldScheduleData) { d.Binding.SourceGeneration = 0 },
		func(d *HeldScheduleData) { d.EpochSchedule.SlotsPerEpoch = 0 },
		func(d *HeldScheduleData) { d.EpochSchedule.SlotsPerEpoch = 16 },
		func(d *HeldScheduleData) { d.EpochSchedule.FirstNormalEpoch = 1 },
		func(d *HeldScheduleData) {
			d.EpochSchedule = &HeldEpochSchedule{SlotsPerEpoch: 64, FirstNormalEpoch: 2, FirstNormalSlot: 32, Warmup: true}
		},
		func(d *HeldScheduleData) { d.Current.SlotCount = 0 },
		func(d *HeldScheduleData) { d.Current.SlotCount = maxHeldEpochSlots + 1 },
		func(d *HeldScheduleData) { d.Current.KnownSlots = 7 },
		func(d *HeldScheduleData) { d.Current.Stride = 2 },
		func(d *HeldScheduleData) { d.Current.Encoding = "u16" },
		func(d *HeldScheduleData) { d.Current.Leaders = nil },
		func(d *HeldScheduleData) { d.Current.Leaders[0] = (solana.PublicKey{}).String() },
		func(d *HeldScheduleData) { d.Current.Leaders[1] = d.Current.Leaders[0] },
		func(d *HeldScheduleData) {
			d.Current.Leaders = append(d.Current.Leaders, (solana.PublicKey{3}).String())
		},
		func(d *HeldScheduleData) { d.Current.Indices = base64.StdEncoding.EncodeToString([]byte{3, 0, 2, 0}) },
		func(d *HeldScheduleData) { d.Current.Indices = base64.StdEncoding.EncodeToString([]byte{2, 0, 1, 0}) },
		func(d *HeldScheduleData) { d.Current.Indices = "\n" + d.Current.Indices[1:] },
		func(d *HeldScheduleData) {
			d.Current.Stride = 1
			d.Current.Indices = base64.StdEncoding.EncodeToString([]byte{1, 0, 1, 0, 1, 0, 1, 0, 2, 0, 2, 0, 2, 0, 2, 0})
		},
		func(d *HeldScheduleData) { d.Current.Epoch = 1 },
		func(d *HeldScheduleData) { d.Next.FirstSlot++ },
		func(d *HeldScheduleData) { d.Next.Epoch++ },
		func(d *HeldScheduleData) { d.NextReason = "waiting_source" },
		func(d *HeldScheduleData) { d.Next = nil; d.NextReason = "" },
	} {
		d := heldScheduleDataFixture(t)
		change(&d)
		_, err := PrepareHeldScheduleData(d)
		require.Error(t, err)
	}
	p, err := PrepareHeldScheduleData(heldScheduleDataFixture(t))
	require.NoError(t, err)
	for _, body := range []string{p.body + " ", strings.Replace(p.body, `"schema_version":1`, `"schema_version":1,"schema_version":1`, 1),
		strings.Replace(p.body, `"slot_count":"8"`, `"slot_count":"08"`, 1),
		strings.Replace(p.body, `"family":"held_schedule"`, `"family":"held_schedule","private_path":"secret"`, 1)} {
		_, err := decodeHeldScheduleData([]byte(body))
		require.Error(t, err)
	}
	legacy := heldScheduleDataFixture(t)
	legacy.EpochSchedule = nil
	p, err = PrepareHeldScheduleData(legacy)
	require.NoError(t, err, "legacy omission remains valid")
	_, err = decodeHeldScheduleData([]byte(strings.Replace(p.body, `"current":`, `"epoch_schedule":null,"current":`, 1)))
	require.Error(t, err, "explicit null is not the canonical legacy omission")
}

func heldScheduleMaximumDataFixture(t *testing.T) HeldScheduleData {
	t.Helper()
	d := heldScheduleDataFixture(t)
	leaders := make([]string, maxHeldEpochLeaders)
	for i := range leaders {
		key := solana.PublicKey{255}
		binary.LittleEndian.PutUint64(key[8:16], uint64(i)+1)
		leaders[i] = key.String()
	}
	indices := make([]byte, 2*maxHeldEpochSlots)
	for i := 0; i < maxHeldEpochSlots; i++ {
		binary.LittleEndian.PutUint16(indices[i*2:], uint16(i%maxHeldEpochLeaders+1))
	}
	d.Current = HeldEpochAssignments{SlotCount: maxHeldEpochSlots, KnownSlots: maxHeldEpochSlots, Encoding: "base64_u16_le", Stride: 1,
		Leaders: leaders, Indices: base64.StdEncoding.EncodeToString(indices)}
	d.EpochSchedule = &HeldEpochSchedule{SlotsPerEpoch: maxHeldEpochSlots}
	next := d.Current
	next.Epoch, next.FirstSlot = 1, maxHeldEpochSlots
	d.Next = &next
	d.Binding.RunID = strings.Repeat("<", 128)
	return d
}

func TestHeldScheduleDataMaximumFitsWithoutRetainingDecodedBuffers(t *testing.T) {
	d := heldScheduleMaximumDataFixture(t)
	p, err := PrepareHeldScheduleData(d)
	require.NoError(t, err)
	require.LessOrEqual(t, len(p.body), MaxHeldScheduleDataBytes)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(p.body), &fields))
	require.Len(t, fields, 12)
	decoded, err := decodeHeldScheduleData([]byte(p.body))
	require.NoError(t, err)
	require.Equal(t, p.digest, decoded.digest)
	require.Equal(t, p.body, decoded.body)
	require.Less(t, len(p.digest)+len(p.binding.RunID)+len(p.nextReason), 4096)
	t.Logf("two full epoch datasets: %d/%d bytes; prepared owner retains one body plus scalar metadata", len(p.body), MaxHeldScheduleDataBytes)
	_, err = decodeHeldScheduleData(bytes.Repeat([]byte{' '}, MaxHeldScheduleDataBytes+1))
	require.Error(t, err)
}

func TestHeldEpochTargetsKeepExactBoundsProvenanceAndUnknowns(t *testing.T) {
	d := heldScheduleDataFixture(t)
	d.Current.TargetSlotDuration = &HeldEpochTargetDuration{Nanoseconds: math.MaxUint64, Provenance: "snapshot_seed_and_bank_features"}
	d.Next.TargetSlotDuration = &HeldEpochTargetDuration{Nanoseconds: 200_000_000, Provenance: "snapshot_seed_and_bank_features"}
	p, err := PrepareHeldScheduleData(d)
	require.NoError(t, err)
	original := p.body
	decoded, err := decodeHeldScheduleData([]byte(original))
	require.NoError(t, err)
	require.Equal(t, original, decoded.body)
	d.Current.TargetSlotDuration.Nanoseconds = 1
	require.Equal(t, original, p.body)
	for _, mutate := range []func(*HeldScheduleData){
		func(d *HeldScheduleData) { d.Current.TargetSlotDuration.Nanoseconds = 0 },
		func(d *HeldScheduleData) { d.Current.TargetSlotDuration.Provenance = "producer_default" },
		func(d *HeldScheduleData) { d.Next.TargetSlotDuration.Nanoseconds = 400_000_000 },
		func(d *HeldScheduleData) { d.Current.TargetSlotDuration = nil },
		func(d *HeldScheduleData) { d.EpochSchedule = nil },
	} {
		bad := heldScheduleDataFixture(t)
		bad.Current.TargetSlotDuration = &HeldEpochTargetDuration{Nanoseconds: 350_000_000, Provenance: "snapshot_seed_and_bank_features"}
		bad.Next.TargetSlotDuration = &HeldEpochTargetDuration{Nanoseconds: 200_000_000, Provenance: "snapshot_seed_and_bank_features"}
		mutate(&bad)
		_, err = PrepareHeldScheduleData(bad)
		require.Error(t, err)
	}
	d = heldScheduleDataFixture(t)
	d.Current.TargetSlotDuration = &HeldEpochTargetDuration{Nanoseconds: 1, Provenance: "snapshot_seed_and_bank_features"}
	p, err = PrepareHeldScheduleData(d)
	require.NoError(t, err, "an unknown next target is not zero")
	require.NotContains(t, p.body, `"nanoseconds":"0"`)
	d.Current.TargetSlotDuration = nil
	p, err = PrepareHeldScheduleData(d)
	require.NoError(t, err)
	require.NotContains(t, p.body, "target_slot_duration")
	_, err = decodeHeldScheduleData([]byte(strings.Replace(p.body, `},"next":`, `,"target_slot_duration":null},"next":`, 1)))
	require.Error(t, err, "explicit null cannot replace canonical omission")
}

func TestHeldEpochGeometryMatchesRuntime(t *testing.T) {
	for _, s := range []HeldEpochSchedule{
		{SlotsPerEpoch: 1}, {SlotsPerEpoch: 7}, {SlotsPerEpoch: math.MaxUint64},
		{SlotsPerEpoch: 128, FirstNormalEpoch: 2, FirstNormalSlot: 96, Warmup: true},
		{SlotsPerEpoch: 65, FirstNormalEpoch: 2, FirstNormalSlot: 96, Warmup: true},
		{SlotsPerEpoch: 1 << 63, FirstNormalEpoch: 58, FirstNormalSlot: (1<<58 - 1) * 32, Warmup: true},
	} {
		require.True(t, validHeldEpochSchedule(s))
		runtime := sealevel.SysvarEpochSchedule{SlotsPerEpoch: s.SlotsPerEpoch, FirstNormalEpoch: s.FirstNormalEpoch,
			FirstNormalSlot: s.FirstNormalSlot, Warmup: s.Warmup}
		for _, slot := range []uint64{0, 1, 30, 31, 32, 94, 95, 96, 127, 128, 1 << 20, math.MaxUint64 - 1, math.MaxUint64} {
			epoch, index := runtime.GetEpochAndSlotIndex(slot)
			count := runtime.SlotsInEpoch(epoch)
			require.LessOrEqual(t, index, slot)
			first := slot - index
			if count-1 > math.MaxUint64-first {
				continue // A whole inclusive range cannot be represented.
			}
			e := HeldEpochAssignments{Epoch: epoch, FirstSlot: first, SlotCount: count}
			require.True(t, heldEpochMatchesSchedule(e, s), "slot %d config %+v", slot, s)
			e.SlotCount--
			require.False(t, heldEpochMatchesSchedule(e, s))
		}
	}
}
