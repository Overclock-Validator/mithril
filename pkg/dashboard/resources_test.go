package dashboard

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestPublicResourceCPUIntervals(t *testing.T) {
	at := time.Now().UTC()
	s := resourceFixture(at)
	var rate resourceCPURate
	observe := func() *PublicCPURate { return rate.observe(&s, publicResources(&s, "fresh", s.SampledAt)) }
	advance := func(d time.Duration, cpu float64) {
		s.SampledAt = s.SampledAt.Add(d)
		s.Resources.SampledAt = s.SampledAt
		s.Resources.CPUSeconds = &cpu
	}
	if observe() != nil {
		t.Fatal("first sample fabricated a CPU rate")
	}
	advance(10*time.Second, 67.5)
	got := observe()
	if got == nil || got.CPUSeconds != 25 || got.DurationSeconds != 10 || got.PercentOneCore != 250 || !got.WindowStart.Equal(at) || !got.WindowEnd.Equal(s.SampledAt) {
		t.Fatalf("incorrect two-sample CPU rate: %+v", got)
	}
	if repeated := observe(); !reflect.DeepEqual(repeated, got) {
		t.Fatalf("unchanged producer changed its rate: %+v", repeated)
	}
	s.SampledAt = s.SampledAt.Add(time.Second)
	if repeated := observe(); !reflect.DeepEqual(repeated, got) {
		t.Fatalf("feed refresh changed the producer's CPU interval: %+v", repeated)
	}
	s.SampledAt = got.WindowEnd
	advance(0, 68)
	if observe() != nil || !rate.at.IsZero() {
		t.Fatal("conflicting same-time CPU counter retained coverage")
	}
	if observe() != nil {
		t.Fatal("recovery after conflicting sample fabricated a rate")
	}
	advance(10*time.Second, 68)
	if got := observe(); got == nil || got.PercentOneCore != 0 {
		t.Fatalf("zero CPU interval confused with unknown: %+v", got)
	}
	for _, mutate := range []func(){
		func() { s.RunID = "next-run" },
		func() { s.StartedAt = s.StartedAt.Add(time.Second) },
		func() { advance(10*time.Second, 1) },
		func() { advance(31*time.Second, 2) },
		func() { advance(-time.Second, 2) },
	} {
		mutate()
		if got := observe(); got != nil {
			t.Fatalf("reset or gap retained CPU rate: %+v", got)
		}
		advance(10*time.Second, *s.Resources.CPUSeconds+1)
		if got := observe(); got == nil || got.PercentOneCore != 10 {
			t.Fatalf("new CPU window did not recover: %+v", got)
		}
	}
	p := publicResources(&s, "stale", s.SampledAt)
	if rate.observe(&s, p) != nil || !rate.at.IsZero() {
		t.Fatal("stale source retained CPU coverage")
	}
	s.Resources.CPUSeconds = nil
	if observe() != nil || !rate.at.IsZero() {
		t.Fatal("missing CPU counter fabricated a rate")
	}
}

func resourceFixture(at time.Time) Snapshot {
	s := testSnapshot(at)
	rss, cpu := uint64(1<<53+7), 42.5
	s.Resources = &Resources{SampledAt: at, RSSBytes: &rss, CPUSeconds: &cpu,
		Root: &Filesystem{TotalBytes: math.MaxUint64, AvailableBytes: 0, Inodes: 100, FreeInodes: 0}}
	return s
}

func TestPublicResourcesProjection(t *testing.T) {
	at := time.Now().UTC()
	s := resourceFixture(at)
	p := publicResources(&s, "fresh", at)
	if p.State != "fresh" || p.SampledAt == nil || !p.SampledAt.Equal(at) || *p.RSSBytes != 1<<53+7 || *p.CPUSeconds != 42.5 || p.Root.AvailableBytes != 0 || p.Accounts != nil || p.Ledger != nil {
		t.Fatalf("incorrect resource projection: %+v", p)
	}
	b, err := json.Marshal(p)
	if err != nil || !strings.Contains(string(b), `"rss_bytes":"9007199254740999"`) || !strings.Contains(string(b), `"total_bytes":"18446744073709551615"`) {
		t.Fatalf("resource precision lost: %s, %v", b, err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(b, &fields); err != nil {
		t.Fatal(err)
	}
	want := []string{"state", "sampled_at", "rss_bytes", "cpu_seconds", "cpu", "root", "accounts", "ledger"}
	if len(fields) != len(want) {
		t.Fatalf("unexpected public resource fields: %v", fields)
	}
	for _, name := range want {
		if _, ok := fields[name]; !ok {
			t.Fatalf("missing field %q", name)
		}
	}
	*s.Resources.RSSBytes, *s.Resources.CPUSeconds, s.Resources.Root.Inodes = 1, 0, 1
	if *p.RSSBytes != 1<<53+7 || *p.CPUSeconds != 42.5 || p.Root.Inodes != 100 {
		t.Fatal("public projection aliases private resource values")
	}
	_, b = publicValue(t, &s, "fresh", at)
	if strings.Contains(string(b), `"resources"`) || strings.Contains(string(b), `"rss_bytes"`) {
		t.Fatal("default public profile discloses resources")
	}
}

func TestPublicResourcesUnavailableAndStale(t *testing.T) {
	at := time.Now().UTC()
	for _, state := range []string{"fresh", "stale", "unavailable"} {
		if got := publicResources(nil, state, at); !reflect.DeepEqual(got, &PublicResources{State: "unavailable"}) {
			t.Fatalf("missing source fabricated resources: %+v", got)
		}
	}
	for _, when := range []time.Time{{}, at.Add(2 * time.Second), at.Add(-2 * time.Hour)} {
		s := resourceFixture(at)
		s.Resources.SampledAt = when
		if got := publicResources(&s, "fresh", at); got.State != "unavailable" || got.SampledAt != nil || got.RSSBytes != nil {
			t.Fatalf("invalid producer time published: %+v", got)
		}
	}
	for _, state := range []string{"unavailable", "invalid"} {
		s := resourceFixture(at)
		if got := publicResources(&s, state, at); got.State != "unavailable" || got.RSSBytes != nil {
			t.Fatalf("unavailable source published: %+v", got)
		}
	}
	s := resourceFixture(at)
	if got := publicResources(&s, "stale", at); got.State != "stale" || got.RSSBytes == nil {
		t.Fatalf("stale source lost historical values: %+v", got)
	}
	s.Resources.SampledAt = at.Add(-31 * time.Second)
	if got := publicResources(&s, "fresh", at); got.State != "stale" {
		t.Fatalf("fresh feed concealed stale producer: %+v", got)
	}
	// A producer dated after its feed sample cannot borrow the collector clock.
	s.Resources.SampledAt = at.Add(2 * time.Second)
	if got := publicResources(&s, "fresh", at.Add(5*time.Second)); got.State != "unavailable" {
		t.Fatalf("producer exceeds its feed sample: %+v", got)
	}
}

func TestPublicFilesystemUsedBytes(t *testing.T) {
	used := uint64(40)
	f := &Filesystem{TotalBytes: 100, AvailableBytes: 50, UsedBytes: &used}
	p := publicFilesystem(f)
	if p == nil || p.UsedBytes == nil || *p.UsedBytes != 40 {
		t.Fatalf("reserved space changed reported usage: %+v", p)
	}
	used = 0
	if *p.UsedBytes != 40 {
		t.Fatal("public used bytes alias the source")
	}
	zero := publicFilesystem(f)
	if zero == nil || zero.UsedBytes == nil || *zero.UsedBytes != 0 {
		t.Fatalf("known zero usage was lost: %+v", zero)
	}
	body, err := json.Marshal(zero)
	if err != nil || !strings.Contains(string(body), `"used_bytes":"0"`) {
		t.Fatalf("known zero usage omitted from JSON: %s, %v", body, err)
	}
	for _, value := range []uint64{51, 101} {
		used = value
		if publicFilesystem(f) != nil {
			t.Fatalf("invalid used/available counts accepted: used=%d", value)
		}
	}
	f.UsedBytes = nil
	legacy := publicFilesystem(f)
	if legacy == nil || legacy.UsedBytes != nil {
		t.Fatalf("legacy absence became reported usage: %+v", legacy)
	}
	body, err = json.Marshal(legacy)
	if err != nil || strings.Contains(string(body), "used_bytes") {
		t.Fatalf("legacy usage was not omitted: %s, %v", body, err)
	}
}
