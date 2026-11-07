package node

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/gagliardetto/solana-go"
	"github.com/spf13/viper"
)

func TestDashboardCacheOwnerLifetime(t *testing.T) {
	var disabled *nodeDashboard
	for _, d := range []*nodeDashboard{disabled, {}} {
		if !d.attachCacheOwner(nil) {
			t.Fatal("disabled dashboard attempted to attach an owner")
		}
		if _, err := d.sampleCacheDetail(time.Now()); err == nil || err.Error() != "source_unavailable" {
			t.Fatal("disabled cache details available")
		}
	}
	oldProgram, oldCommon, oldLayout := accountsdb.ProgramCacheMaxMB, accountsdb.CommonAccountCacheMaxMB, accountsdb.CommonAccountCacheLayout
	accountsdb.ProgramCacheMaxMB, accountsdb.CommonAccountCacheMaxMB, accountsdb.CommonAccountCacheLayout = 1, 1, "flat"
	t.Cleanup(func() {
		accountsdb.ProgramCacheMaxMB, accountsdb.CommonAccountCacheMaxMB, accountsdb.CommonAccountCacheLayout = oldProgram, oldCommon, oldLayout
	})
	db := &accountsdb.AccountsDb{}
	d := &nodeDashboard{cache: &cacheDashboardMonitor{}, state: dashboard.Snapshot{RunID: "cache-owner-run", StartedAt: time.Now().Add(-time.Minute).UTC()}}
	wantError := func(reason string) {
		t.Helper()
		if _, err := d.sampleCacheDetail(time.Now()); err == nil || err.Error() != reason {
			t.Fatalf("want %s, got %v", reason, err)
		}
	}
	wantError("waiting_source")
	if d.attachCacheOwner(db) {
		t.Fatal("attached an uninitialized database")
	}
	db.InitCachesWithActivity(true)
	if d.attachCacheOwner(db) {
		t.Fatal("attached before chain binding")
	}
	d.bootstrap("11111111111111111111111111111111")
	if !d.attachCacheOwner(db) || !d.attachCacheOwner(db) {
		t.Fatal("initialized owner attachment was not idempotent")
	}
	wantError("waiting_source")
	d.observeRuntime(time.Now(), runtime.MemStats{}, nil)
	first, err := d.sampleCacheDetail(time.Now())
	if err != nil || first.Detail.Binding.CacheGeneration != 1 || first.Detail.Vote.EstimatedEntries != 0 ||
		first.Detail.Vote.CapacityEntries != 2500 || first.Detail.Program.CapacityCostUnits != 1 || first.Detail.Program.CostUnitBytes != 1<<20 ||
		first.Detail.Common.Layout != "flat" || first.Detail.Common.ConfiguredBudgetBytes != 1<<20 || first.Detail.Common.LargeCapacityBytes != 1<<20 ||
		first.Detail.Common.FixedEmptyCapacityEntries != 0 || first.Detail.Common.FixedSmallCapacityEntries != 0 ||
		first.Detail.Common.FixedEmptyOccupiedEntries == nil || *first.Detail.Common.FixedEmptyOccupiedEntries != 0 ||
		first.Detail.Common.FixedSmallOccupiedEntries == nil || *first.Detail.Common.FixedSmallOccupiedEntries != 0 {
		t.Fatalf("real initialized owner mapping: %+v, %v", first, err)
	}
	if first.FreshUntil == first.FreshUntil.UTC() {
		t.Fatal("owner deadline lost monotonic clock")
	}
	if first.Detail.Activity == nil || first.Detail.Activity.InsertionWeights == nil ||
		first.Detail.Activity.InsertionWeights.ProgramCostUnits == nil || *first.Detail.Activity.InsertionWeights.ProgramCostUnits != 0 ||
		first.Detail.Activity.InsertionWeights.CommonLargeAdmissionBytes == nil || *first.Detail.Activity.InsertionWeights.CommonLargeAdmissionBytes != 0 {
		t.Fatal("initialized empty caches did not publish known-zero insertion weights")
	}
	if p := first.Detail.Activity.ProgramSets; p == nil || p.Count == nil || *p.Count != 0 || p.Reason != "" {
		t.Fatalf("initialized cache did not publish zero accepted Set calls: %+v", p)
	}
	key := solana.PublicKey{1}
	if !db.VoteAcctCache.Set(key, &accounts.Account{Key: key}) {
		t.Fatal("real vote fixture was not cached")
	}
	db.AddProgramToCache(key, &accountsdb.ProgramCacheEntry{})
	db.CommonAcctsCache.Set(key, &accounts.Account{Key: key, Data: make([]byte, 400)})
	for i := 0; i < 20; i++ {
		copied, err := d.sampleCacheDetail(time.Now().Add(time.Hour))
		if err != nil || !reflect.DeepEqual(copied, first) {
			t.Fatal("Feed copy sampled occupancy, renewed time, or trusted its supplied clock")
		}
	}
	// A caller cannot change the monitor's pinned chain binding through a DTO.
	*first.Detail.Binding.GenesisHash = "changed-by-caller"
	*first.Detail.Common.FixedEmptyOccupiedEntries = 99
	*first.Detail.Activity.InsertionWeights.ProgramCostUnits = 99
	*first.Detail.Activity.ProgramSets.Count = 99
	d.cache.observe()
	populated, err := d.sampleCacheDetail(time.Now())
	if err != nil || populated.Detail.Vote.EstimatedEntries != 1 || *populated.Detail.Binding.GenesisHash != "11111111111111111111111111111111" ||
		populated.Detail.Common.FixedEmptyOccupiedEntries == nil || *populated.Detail.Common.FixedEmptyOccupiedEntries != 0 {
		t.Fatal("diagnostics did not publish a detached positive owner observation")
	}
	if populated.Detail.Activity.InsertionWeights == nil || *populated.Detail.Activity.InsertionWeights.ProgramCostUnits != 1 ||
		*populated.Detail.Activity.InsertionWeights.CommonLargeAdmissionBytes != 656 {
		t.Fatal("actual initialized owner insertion weights changed or retained a caller pointer")
	}
	if p := populated.Detail.Activity.ProgramSets; p == nil || p.Count == nil || *p.Count != 1 || p.Reason != "" {
		t.Fatalf("actual accepted Set count changed or retained caller state: %+v", p)
	}
	if first.Detail.Activity == nil || first.Detail.Activity.Window != nil || populated.Detail.Activity == nil || populated.Detail.Activity.Window == nil ||
		populated.Detail.Activity.Window.DurationNanoseconds == 0 || !populated.Detail.Activity.Window.StartedAt.Equal(first.Detail.CapturedAt) ||
		!populated.Detail.Activity.Window.EndedAt.Equal(populated.Detail.CapturedAt) {
		t.Fatal("actual cache owner did not retain both original interval endpoints")
	}
	populated.Detail.Activity.Window.Vote.Hits = 99
	detached, err := d.sampleCacheDetail(time.Now())
	if err != nil || detached.Detail.Activity.Window.Vote.Hits == 99 {
		t.Fatal("Feed caller changed the actual owner's cached interval")
	}
	checkCachePublicCapture(t, d, detached)
	var observers sync.WaitGroup
	observers.Go(func() {
		for i := 0; i < 100; i++ {
			d.cache.observe()
		}
	})
	for i := 0; i < 100; i++ {
		if _, err := d.sampleCacheDetail(time.Now()); err != nil {
			t.Error(err)
		}
	}
	observers.Wait()
	for _, field := range []string{"run", "start", "genesis"} {
		d.mu.Lock()
		previous := d.state
		switch field {
		case "run":
			d.state.RunID = "new-run"
		case "start":
			d.state.StartedAt = d.state.StartedAt.Add(time.Second)
		case "genesis":
			genesis := solana.PublicKey{2}.String()
			d.state.GenesisHash = &genesis
		}
		d.mu.Unlock()
		d.cache.observe()
		wantError("binding_changed")
		d.mu.Lock()
		d.state = previous
		d.mu.Unlock()
	}
	// Reinitialize the same owner through its real API. The previous captured
	// generation must disappear before diagnostics obtains replacement gauges.
	db.InitCaches()
	wantError("binding_changed")
	d.cache.observe()
	reinitialized, err := d.sampleCacheDetail(time.Now())
	if err != nil || reinitialized.Detail.Binding.CacheGeneration != 2 || reinitialized.Detail.Vote.EstimatedEntries != 0 ||
		reinitialized.Detail.Activity == nil || reinitialized.Detail.Activity.Window != nil || reinitialized.Detail.Activity.ProgramSets == nil ||
		reinitialized.Detail.Activity.ProgramSets.Count == nil || *reinitialized.Detail.Activity.ProgramSets.Count != 0 {
		t.Fatalf("same-owner reinitialization: %+v, %v", reinitialized, err)
	}
	if d.attachCacheOwner(&accountsdb.AccountsDb{}) {
		t.Fatal("replacement owner inherited the previous run")
	}
	wantError("source_unavailable")
	if !d.cache.stopped || d.cache.owner != nil || d.cache.latest.Load() != nil || d.attachCacheOwner(db) {
		t.Fatal("replacement refusal did not clear and permanently stop the monitor")
	}
	for i := 0; i < 100; i++ {
		d.cache.observe()
	}
	if d.cache.latest.Load() != nil {
		t.Fatal("stopped diagnostics published again")
	}
}

// checkCachePublicCapture exercises real initialized caches and the normal
// observer's cached capture through Unix Feed and public reader. The owner was
// invoked directly by the test, not by a natural ten-second diagnostics tick.
func checkCachePublicCapture(t *testing.T, d *nodeDashboard, capture dashboard.CacheDetailCapture) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dir, err := os.MkdirTemp("/tmp", "cache-wire-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	socket := filepath.Join(dir, "node.sock")
	feed, err := dashboard.ListenFeedWithDetails(ctx, socket, "", func() dashboard.Snapshot {
		s := d.snapshot()
		s.Mode, s.Phase = "verifying", "starting"
		return s
	}, dashboard.DetailSamplers{Cache: d.sampleCacheDetail})
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close()
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := reserved.Addr().String()
	if err := reserved.Close(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- dashboard.ServePublicWithOptions(ctx, socket, address, "https://cache.invalid", dashboard.PublicOptions{Resources: true, CacheDetails: true})
	}()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("stop cache public reader: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("cache public reader did not join")
		}
	}()
	client := &http.Client{Timeout: time.Second}
	defer client.CloseIdleConnections()
	deadline := time.Now().Add(3 * time.Second)
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+"/api/v1/details/cache", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Host = "cache.invalid"
		response, requestErr := client.Do(req)
		var body []byte
		if requestErr == nil {
			body, err = io.ReadAll(io.LimitReader(response.Body, dashboard.MaxCacheDetailBytes+1))
			response.Body.Close()
		}
		var public dashboard.PublicCacheDetail
		if requestErr == nil && err == nil && response.StatusCode == http.StatusOK && len(body) <= dashboard.MaxCacheDetailBytes &&
			json.Unmarshal(body, &public) == nil && public.State == "fresh" {
			if !reflect.DeepEqual(public.Activity, capture.Detail.Activity) || !reflect.DeepEqual(public.Binding, capture.Detail.Binding) ||
				!public.CapturedAt.Equal(capture.Detail.CapturedAt) {
				t.Fatal("actual cache owner values changed through Feed/public reader")
			}
			t.Logf("actual initialized-cache Feed public fixture: %s", body)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("cache public fixture unavailable: request=%v read=%v body=%s", requestErr, err, body)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestDashboardCacheMeasurementClocksAndMapping(t *testing.T) {
	now := time.Now()
	binding := dashboard.CacheDetailBinding{RunID: "mapping", NodeStartedAt: now.Add(-time.Minute).UTC()}
	value := accountsdb.CacheObservation{Generation: ^uint64(0) - 1, VoteCapacityEntries: ^uint64(0), VoteEstimatedEntries: ^uint64(0) - 2,
		ProgramCapacityCostUnits: 3, ProgramCostUnitBytes: 1 << 20, ProgramEstimatedEntries: 4, CommonLayout: "fixed",
		CommonConfiguredBudgetBytes: 100, CommonFixedEmptyCapacityEntries: 5, CommonFixedSmallCapacityEntries: 6, CommonLargeCapacityBytes: 70, CommonLargeEstimatedEntries: 7}
	capture := cacheDashboardObservation{value: value, startedAt: now, completedAt: now}
	got, err := cacheDetailMeasurement(binding, capture, now.Add(time.Second))
	if err != nil || got.Detail.Vote.CapacityEntries != value.VoteCapacityEntries || got.Detail.Vote.EstimatedEntries != value.VoteEstimatedEntries ||
		got.Detail.Binding.CacheGeneration != value.Generation || got.Detail.Common.FixedEmptyCapacityEntries != 5 || got.Detail.Common.FixedSmallCapacityEntries != 6 ||
		got.Detail.Common.LargeEstimatedEntries != 7 || got.Detail.CapturedAt != now.UTC() || got.FreshUntil != now.Add(30*time.Second) {
		t.Fatalf("lossy or redated scalar mapping: %+v, %v", got, err)
	}
	for _, tc := range []struct {
		at, read time.Time
		reason   string
	}{
		{time.Time{}, now, "source_time_invalid"},
		{now.Add(time.Second), now, "source_time_invalid"},
		{binding.NodeStartedAt.Add(-time.Second), now, "source_time_invalid"},
		{now, now.Add(30 * time.Second), "status_stale"},
	} {
		capture.completedAt = tc.at
		if _, err := cacheDetailMeasurement(binding, capture, tc.read); err == nil || err.Error() != tc.reason {
			t.Fatalf("clock refusal %s: %v", tc.reason, err)
		}
	}
	capture.completedAt = now
	for _, generation := range []uint64{0, ^uint64(0)} {
		capture.value.Generation = generation
		if _, err := cacheDetailMeasurement(binding, capture, now); err == nil || err.Error() != "source_invalid" {
			t.Fatal("reserved generation was published")
		}
	}
}

func TestDashboardCacheAcquisitionAgeAndOccupancy(t *testing.T) {
	start := time.Now()
	binding := dashboard.CacheDetailBinding{RunID: "acquisition", NodeStartedAt: start.Add(-time.Minute).UTC()}
	value := accountsdb.CacheObservation{Generation: 1, CommonFixedOccupancyKnown: true,
		CommonFixedEmptyOccupiedEntries: 3, CommonFixedSmallOccupiedEntries: 4}
	for _, duration := range []time.Duration{0, 900 * time.Millisecond, time.Second} {
		capture := cacheDashboardObservation{value: value, startedAt: start, completedAt: start.Add(duration)}
		got, err := cacheDetailMeasurement(binding, capture, capture.completedAt)
		if err != nil || got.Detail.CapturedAt != capture.completedAt.UTC() || got.FreshUntil != start.Add(30*time.Second) ||
			got.Detail.Common.FixedEmptyOccupiedEntries == nil || *got.Detail.Common.FixedEmptyOccupiedEntries != 3 ||
			got.Detail.Common.FixedSmallOccupiedEntries == nil || *got.Detail.Common.FixedSmallOccupiedEntries != 4 {
			t.Fatalf("acquisition mapping: %+v, %v", got, err)
		}
		if _, err := cacheDetailMeasurement(binding, capture, start.Add(30*time.Second)); err == nil || err.Error() != "status_stale" {
			t.Fatal("completion renewed an older acquisition")
		}
	}
	for _, duration := range []time.Duration{-time.Nanosecond, time.Second + time.Nanosecond} {
		if _, err := cacheDetailMeasurement(binding, cacheDashboardObservation{value: value, startedAt: start, completedAt: start.Add(duration)}, start.Add(2*time.Second)); err == nil || err.Error() != "source_time_invalid" {
			t.Fatal("invalid acquisition interval was published")
		}
	}
	value.CommonFixedOccupancyKnown = false
	got, err := cacheDetailMeasurement(binding, cacheDashboardObservation{value: value, startedAt: start, completedAt: start}, start)
	if err != nil || got.Detail.Common.FixedEmptyOccupiedEntries != nil || got.Detail.Common.FixedSmallOccupiedEntries != nil {
		t.Fatal("unknown occupancy became known zero or partial counts")
	}
}

func TestDashboardCacheStopJoinsLifetimeReaders(t *testing.T) {
	m := &cacheDashboardMonitor{owner: &accountsdb.AccountsDb{}}
	m.latest.Store(&cacheDashboardObservation{})
	m.mu.RLock()
	done := make(chan struct{})
	go func() { m.stop(); close(done) }()
	// A queued writer excludes new readers. This checks the same lifetime lock
	// held across CacheObservation; it does not simulate a blocked cache method.
	deadline := time.Now().Add(time.Second)
	for m.mu.TryRLock() {
		m.mu.RUnlock()
		if time.Now().After(deadline) {
			m.mu.RUnlock()
			t.Fatal("stop did not acquire the lifetime writer")
		}
		runtime.Gosched()
	}
	select {
	case <-done:
		m.mu.RUnlock()
		t.Fatal("stop returned before lifetime reader finished")
	default:
	}
	m.mu.RUnlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stop did not join after lifetime reader finished")
	}
	if !m.stopped || m.owner != nil || m.latest.Load() != nil {
		t.Fatal("joined stop retained owner or gauges")
	}
	d := &nodeDashboard{cache: m}
	d.stopping()
	d.close()
	m.observe()
	if m.latest.Load() != nil {
		t.Fatal("repeated shutdown revived diagnostics")
	}
}

func TestDashboardCacheDetailsOptIn(t *testing.T) {
	if validateDashboardCacheDetails(false, "") != nil || validateDashboardCacheDetails(true, "/private/node.sock") != nil {
		t.Fatal("valid config rejected")
	}
	for _, socket := range []string{"", " \t"} {
		if validateDashboardCacheDetails(true, socket) == nil {
			t.Fatal("cache details enabled without a feed")
		}
	}
	flag := Run.Flags().Lookup("dashboard-cache-details")
	if flag == nil || flag.DefValue != "false" {
		t.Fatal("cache details not default off")
	}
	oldValue, oldChanged := flag.Value.String(), flag.Changed
	t.Cleanup(func() {
		viper.Reset()
		if err := flag.Value.Set(oldValue); err != nil {
			t.Error(err)
		}
		flag.Changed = oldChanged
	})
	viper.Reset()
	if err := flag.Value.Set("false"); err != nil {
		t.Fatal(err)
	}
	flag.Changed = false
	resolve := func() bool {
		return resolveBoolOption(flag, viper.IsSet("dashboard.cache_details"), viper.GetBool("dashboard.cache_details"))
	}
	viper.Set("dashboard.host_resources", true)
	if resolve() {
		t.Fatal("unrelated disclosure option enabled cache reads")
	}
	viper.Set("dashboard.cache_details", true)
	if !resolve() {
		t.Fatal("TOML opt-in ignored")
	}
	if err := Run.Flags().Set("dashboard-cache-details", "false"); err != nil {
		t.Fatal(err)
	}
	if resolve() {
		t.Fatal("explicit CLI false ignored")
	}
	for _, enabled := range []bool{false, true} {
		dir, err := os.MkdirTemp("/tmp", "dashboard-cache-")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := os.RemoveAll(dir); err != nil {
				t.Error(err)
			}
		})
		d := startNodeDashboard(context.Background(), time.Now(), filepath.Join(dir, "node.sock"), "", "cache-run", "isolated", "verifying", false, false, false, false, false, false, false, enabled)
		if d == nil || (d.cache != nil) != enabled {
			t.Fatal("cache monitor configuration did not follow opt-in")
		}
		d.close()
	}
}
