package dashboard

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHeldScheduleManifestRefreshDoesNotRebuildOrRedateDataset(t *testing.T) {
	d := heldScheduleDataFixture(t)
	p, err := PrepareHeldScheduleData(d)
	require.NoError(t, err)
	now := time.Now()
	c := HeldScheduleCapture{Data: p, CapturedAt: now.UTC(), FreshUntil: now.Add(30 * time.Second),
		ReferenceSlot: 3, ReferenceBankhash: d.GeometryBankhash, ReferenceCompletedAt: d.GeometryBankCompletedAt}
	m, body, err := prepareHeldScheduleManifest(c, 1, now)
	require.NoError(t, err)
	require.Less(t, len(body), 4096)
	var decoded HeldScheduleManifest
	require.NoError(t, json.Unmarshal([]byte(body), &decoded))
	require.Equal(t, m, decoded)
	// A new owner sequence disambiguates a refresh even if wall time repeats.
	m2, _, err := prepareHeldScheduleManifest(c, 2, now)
	require.NoError(t, err)
	require.NotEqual(t, m.CaptureSequence, m2.CaptureSequence)
	require.Equal(t, m.Digest, m2.Digest)
	require.Equal(t, m.ObservedAt, m2.ObservedAt)
	require.Equal(t, p.body, c.Data.body)
	m.Next.KnownSlots = 0
	require.Equal(t, uint64(8), p.next.KnownSlots, "manifest must not lend the prepared owner's metadata")
}

func TestHeldScheduleManifestRejectsBadReferenceAndLease(t *testing.T) {
	d := heldScheduleDataFixture(t)
	p, err := PrepareHeldScheduleData(d)
	require.NoError(t, err)
	now := time.Now()
	good := HeldScheduleCapture{Data: p, CapturedAt: now.UTC(), FreshUntil: now.Add(30 * time.Second),
		ReferenceSlot: 3, ReferenceBankhash: d.GeometryBankhash, ReferenceCompletedAt: d.GeometryBankCompletedAt}
	for _, change := range []func(*HeldScheduleCapture){
		func(c *HeldScheduleCapture) { c.Data = nil }, func(c *HeldScheduleCapture) { c.ReferenceSlot = 8 },
		func(c *HeldScheduleCapture) { c.ReferenceBankhash = "bad" }, func(c *HeldScheduleCapture) { c.ReferenceCompletedAt = time.Time{} },
		func(c *HeldScheduleCapture) { c.CapturedAt = c.CapturedAt.Add(time.Second) },
		func(c *HeldScheduleCapture) { c.FreshUntil = now.Add(-time.Second) },
		func(c *HeldScheduleCapture) { c.FreshUntil = c.FreshUntil.Round(0) },
		func(c *HeldScheduleCapture) { c.FreshUntil = c.FreshUntil.Add(time.Second) },
	} {
		bad := good
		change(&bad)
		_, _, err := prepareHeldScheduleManifest(bad, 1, now)
		require.Error(t, err)
	}
	_, _, err = prepareHeldScheduleManifest(good, 0, now)
	require.Error(t, err)
}

func TestHeldScheduleManifestDecoderRejectsNoncanonicalAndInconsistentFields(t *testing.T) {
	f, _ := newHeldReaderFixture(t)
	body, err := json.Marshal(f.manifest)
	require.NoError(t, err)
	m, err := decodeHeldScheduleManifest(body)
	require.NoError(t, err)
	require.Equal(t, f.manifest, m)
	for _, raw := range []string{
		string(body) + " ",
		strings.Replace(string(body), `"schema_version":1`, `"schema_version":1,"schema_version":1`, 1),
		strings.Replace(string(body), `"capture_sequence":"1"`, `"capture_sequence":"01"`, 1),
		strings.Replace(string(body), `"reason":""`, `"reason":"","private_path":"secret"`, 1),
		strings.Repeat(" ", MaxSnapshotBytes+1),
	} {
		_, err := decodeHeldScheduleManifest([]byte(raw))
		require.Error(t, err)
	}
	for _, change := range []func(*HeldScheduleManifest){
		func(m *HeldScheduleManifest) { m.Scope = "produced_slots" },
		func(m *HeldScheduleManifest) { m.Current.Leaders = -1 },
		func(m *HeldScheduleManifest) { m.Current.Leaders = 0 },
		func(m *HeldScheduleManifest) { m.Current.SlotCount = 0 },
		func(m *HeldScheduleManifest) { m.Current.Epoch = 1 },
		func(m *HeldScheduleManifest) { m.Next.FirstSlot++ },
		func(m *HeldScheduleManifest) { m.NextReason = "waiting_source" },
		func(m *HeldScheduleManifest) { m.Next = nil },
	} {
		copy := f.manifest
		next := *copy.Next
		copy.Next = &next
		change(&copy)
		raw, err := json.Marshal(copy)
		require.NoError(t, err)
		_, err = decodeHeldScheduleManifest(raw)
		require.Error(t, err)
	}
}
