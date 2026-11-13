package dashboard

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func rootedHistoryDetailFixture() RootedHistoryDetail {
	at := time.Date(2026, 11, 1, 20, 0, 0, 999999999, time.UTC)
	bitmap := make([]byte, RootedHistoryCapacity/8)
	bitmap[0] = 5
	bitmap[20/8] |= 1 << (20 % 8)
	return RootedHistoryDetail{SchemaVersion: 1, Family: "rooted_history", Scope: "rooted_bank_slot_history", State: "fresh",
		Binding: ReplayDetailBinding{RunID: "rooted-test", NodeStartedAt: at, SourceGeneration: 1, Attempt: 2, Rewinds: 3,
			AttemptStartedAt: at, ResetAt: at}, CapturedAt: at, ObservedAt: at.Add(-time.Second),
		BankSlot: 20, Bankhash: (solana.Hash{1}).String(), OldestSlot: 0, NextSlot: 21,
		Capacity: RootedHistoryCapacity, Encoding: "base64_le_lsb0", Bitmap: base64.StdEncoding.EncodeToString(bitmap)}
}

func TestRootedHistoryDetailCanonicalAndMaximum(t *testing.T) {
	d := rootedHistoryDetailFixture()
	body, err := json.Marshal(d)
	require.NoError(t, err)
	decoded, err := decodeRootedHistoryDetail(body)
	require.NoError(t, err)
	require.Equal(t, d, decoded)
	require.Less(t, len(body), MaxRootedHistoryBytes)
	for _, slot := range []uint64{0, RootedHistoryCapacity - 1, RootedHistoryCapacity, math.MaxUint64 - 1} {
		d = rootedHistoryDetailFixture()
		d.BankSlot, d.NextSlot = slot, slot+1
		if d.NextSlot > RootedHistoryCapacity {
			d.OldestSlot = d.NextSlot - RootedHistoryCapacity
		}
		d.Bitmap = base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{255}, RootedHistoryCapacity/8))
		d.Binding.RunID = strings.Repeat("r", 128)
		d.Binding.SourceGeneration, d.Binding.Attempt, d.Binding.Rewinds = math.MaxUint64, math.MaxUint64, math.MaxUint64
		d.Epoch = &RootedHistoryEpoch{Number: slot, FirstSlot: slot, Slots: 1}
		genesis := (solana.Hash{255, 255}).String()
		d.Binding.GenesisHash = &genesis
		body, err = json.Marshal(d)
		require.NoError(t, err)
		require.LessOrEqual(t, len(body), MaxRootedHistoryBytes)
		_, err = decodeRootedHistoryDetail(body)
		require.NoError(t, err)
	}
}

func TestRootedHistoryEpochValidationAndBankIdentity(t *testing.T) {
	d := rootedHistoryDetailFixture()
	d.Epoch = &RootedHistoryEpoch{Number: 0, FirstSlot: 0, Slots: 32}
	body, err := json.Marshal(d)
	require.NoError(t, err)
	got, err := decodeRootedHistoryDetail(body)
	require.NoError(t, err)
	require.Equal(t, d, got)
	require.True(t, sameRootedHistoryBank(d, got))
	for _, e := range []RootedHistoryEpoch{{0, 0, 0}, {0, 21, 32}, {0, 0, 20}, {0, 1, math.MaxUint64}, {1, 0, 32}, {0, 1, 32}, {2, 1, 32}} {
		bad := d
		bad.Epoch = &e
		require.Error(t, validateRootedHistoryDetail(bad))
	}
	got.Epoch.Slots = 64
	require.False(t, sameRootedHistoryBank(d, got), "same-bank epoch geometry is immutable")
	got.Epoch = nil
	require.False(t, sameRootedHistoryBank(d, got), "geometry cannot disappear in place")
	d.Epoch = nil
	require.True(t, sameRootedHistoryBank(d, got), "legacy omission remains valid")
	for _, invalid := range []string{`null`, `{"number":"0","first_slot":"0","slots":"32","extra":1}`,
		`{"number":"0","first_slot":"0","slots":"32","slots":"32"}`} {
		raw, err := json.Marshal(d)
		require.NoError(t, err)
		raw = append(append(raw[:len(raw)-1], []byte(`,"epoch":`)...), []byte(invalid+`}`)...)
		_, err = decodeRootedHistoryDetail(raw)
		require.Error(t, err)
	}
}

func TestRootedHistoryDetailRejectsInvalidMetadataAndBitmap(t *testing.T) {
	for _, mutate := range []func(*RootedHistoryDetail){
		func(d *RootedHistoryDetail) { d.SchemaVersion++ },
		func(d *RootedHistoryDetail) { d.Family = "replay" },
		func(d *RootedHistoryDetail) { d.Scope = "tip" },
		func(d *RootedHistoryDetail) { d.State = "stale" },
		func(d *RootedHistoryDetail) { d.Reason = "waiting_source" },
		func(d *RootedHistoryDetail) { d.Binding.RunID = "" },
		func(d *RootedHistoryDetail) { d.Binding.RunID = strings.Repeat("r", 129) },
		func(d *RootedHistoryDetail) { d.Binding.RunID = string([]byte{255}) },
		func(d *RootedHistoryDetail) { d.Binding.SourceGeneration = 0 },
		func(d *RootedHistoryDetail) { d.Binding.Attempt = 0 },
		func(d *RootedHistoryDetail) { d.Binding.NodeStartedAt = time.Time{} },
		func(d *RootedHistoryDetail) { d.Binding.AttemptStartedAt = time.Time{} },
		func(d *RootedHistoryDetail) { d.Binding.ResetAt = time.Time{} },
		func(d *RootedHistoryDetail) { v := "bad"; d.Binding.GenesisHash = &v },
		func(d *RootedHistoryDetail) { d.CapturedAt = time.Time{} },
		func(d *RootedHistoryDetail) { d.ObservedAt = time.Time{} },
		func(d *RootedHistoryDetail) { d.ObservedAt = d.ObservedAt.In(time.FixedZone("offset", 3600)) },
		func(d *RootedHistoryDetail) { d.Bankhash = (solana.Hash{}).String() },
		func(d *RootedHistoryDetail) { d.BankSlot++ },
		func(d *RootedHistoryDetail) { d.NextSlot = 0 },
		func(d *RootedHistoryDetail) { d.OldestSlot++ },
		func(d *RootedHistoryDetail) { d.Capacity-- },
		func(d *RootedHistoryDetail) { d.Encoding = "base64" },
		func(d *RootedHistoryDetail) { d.Bitmap = "" },
		func(d *RootedHistoryDetail) { d.Bitmap += "=" },
		func(d *RootedHistoryDetail) { d.Bitmap = "!" + d.Bitmap[1:] },
		func(d *RootedHistoryDetail) { d.Bitmap = "\n" + d.Bitmap[1:] },
		func(d *RootedHistoryDetail) { d.Bitmap = d.Bitmap[:len(d.Bitmap)-2] + "B=" },
		func(d *RootedHistoryDetail) {
			d.Bitmap = base64.StdEncoding.EncodeToString(make([]byte, RootedHistoryCapacity/8))
		},
	} {
		d := rootedHistoryDetailFixture()
		mutate(&d)
		require.Error(t, validateRootedHistoryDetail(d))
	}
	body, err := json.Marshal(rootedHistoryDetailFixture())
	require.NoError(t, err)
	for _, raw := range [][]byte{
		append([]byte(" "), body...),
		append(append([]byte(nil), body...), ' '),
		bytes.Replace(body, []byte(`"schema_version":1`), []byte(`"schema_version":1,"schema_version":1`), 1),
		bytes.Replace(body, []byte(`"bank_slot":"20"`), []byte(`"bank_slot":"020"`), 1),
		bytes.Replace(body, []byte(`"bank_slot":"20"`), []byte(`"bank_slot":20`), 1),
		bytes.Repeat([]byte{' '}, MaxRootedHistoryBytes+1),
	} {
		_, err := decodeRootedHistoryDetail(raw)
		require.Error(t, err)
	}
}
