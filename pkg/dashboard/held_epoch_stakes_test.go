package dashboard

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"slices"
	"strconv"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func heldStakeSetFixture(count int) *HeldEpochStakeSet {
	identities := make([]string, count)
	stakes := make([]string, count)
	for i := range identities {
		key := solana.PublicKey{255}
		binary.LittleEndian.PutUint64(key[8:16], uint64(i)+1)
		identities[i] = key.String()
	}
	slices.Sort(identities)
	var total uint64
	for i := range stakes {
		stake := uint64(math.MaxUint64) / uint64(count)
		if i == 0 {
			stake += uint64(math.MaxUint64) % uint64(count)
		}
		total += stake
		stakes[i] = strconv.FormatUint(stake, 10)
	}
	return &HeldEpochStakeSet{Generation: math.MaxUint64, TotalStake: total, Identities: identities, Stakes: stakes}
}

func TestHeldEpochStakeSetCanonicalAndBounds(t *testing.T) {
	set := heldStakeSetFixture(2)
	require.NoError(t, validateHeldEpochStakeSet(*set))
	require.NoError(t, validateHeldEpochStakeSet(HeldEpochStakeSet{Generation: 1, Identities: []string{}, Stakes: []string{}}))
	for _, change := range []func(*HeldEpochStakeSet){
		func(s *HeldEpochStakeSet) { s.Generation = 0 },
		func(s *HeldEpochStakeSet) { s.Identities = nil },
		func(s *HeldEpochStakeSet) { s.Stakes = nil },
		func(s *HeldEpochStakeSet) { s.Stakes = s.Stakes[:1] },
		func(s *HeldEpochStakeSet) { s.Identities[0] = s.Identities[1] },
		func(s *HeldEpochStakeSet) { s.Identities[0], s.Identities[1] = s.Identities[1], s.Identities[0] },
		func(s *HeldEpochStakeSet) { s.Identities[0] = "11111111111111111111111111111111" },
		func(s *HeldEpochStakeSet) { s.Stakes[0] = "0" },
		func(s *HeldEpochStakeSet) { s.Stakes[0] = "01" },
		func(s *HeldEpochStakeSet) { s.Stakes[0] = "18446744073709551616" },
		func(s *HeldEpochStakeSet) { s.Stakes[0], s.Stakes[1] = "18446744073709551615", "1" },
		func(s *HeldEpochStakeSet) { s.TotalStake-- },
	} {
		candidate := *set
		candidate.Identities, candidate.Stakes = slices.Clone(set.Identities), slices.Clone(set.Stakes)
		change(&candidate)
		require.Error(t, validateHeldEpochStakeSet(candidate))
	}
	require.Error(t, validateHeldEpochStakeSet(*heldStakeSetFixture(2001)))
	d := heldScheduleDataFixture(t)
	d.Current.StakeSet = set
	p, err := PrepareHeldScheduleData(d)
	require.NoError(t, err)
	original := p.body
	set.Stakes[0] = "changed"
	require.Equal(t, original, p.body, "prepared data borrowed caller's table")
	_, err = decodeHeldScheduleData([]byte(p.body))
	require.NoError(t, err)
	d.Current.StakeSetReason = "selection_unverified"
	_, err = PrepareHeldScheduleData(d)
	require.Error(t, err, "known data cannot have an unavailable reason")
	d.Current.StakeSet = nil
	for _, reason := range []string{"", "waiting_source", "selection_unverified", "not_applicable", "source_invalid", "response_limit"} {
		d.Current.StakeSetReason = reason
		_, err = PrepareHeldScheduleData(d)
		require.NoError(t, err)
	}
	d.Current.StakeSetReason = "invented"
	_, err = PrepareHeldScheduleData(d)
	require.Error(t, err)
}

func TestHeldEpochStakeSetsMaximumCombinedDocument(t *testing.T) {
	d := heldScheduleMaximumDataFixture(t)
	d.Current.StakeSet, d.Next.StakeSet = heldStakeSetFixture(2000), heldStakeSetFixture(2000)
	p, err := PrepareHeldScheduleData(d)
	require.NoError(t, err)
	require.LessOrEqual(t, len(p.body), MaxHeldScheduleDataBytes)
	_, err = decodeHeldScheduleData([]byte(p.body))
	require.NoError(t, err)
	var data HeldScheduleData
	require.NoError(t, json.Unmarshal([]byte(p.body), &data))
	require.Equal(t, d.Current.StakeSet, data.Current.StakeSet)
	require.Equal(t, d.Next.StakeSet, data.Next.StakeSet)
	t.Logf("two maximum schedules plus two complete VAT identity sets: %d/%d bytes", len(p.body), MaxHeldScheduleDataBytes)
}
