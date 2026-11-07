package node

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/spf13/viper"
)

func TestDashboardMachineDetailsOptIn(t *testing.T) {
	f := Run.Flags().Lookup("dashboard-machine-details")
	if f == nil || f.DefValue != "false" {
		t.Fatal("machine collection is not default off")
	}
	oldValue, oldChanged := f.Value.String(), f.Changed
	t.Cleanup(func() {
		viper.Reset()
		if err := f.Value.Set(oldValue); err != nil {
			t.Error(err)
		}
		f.Changed = oldChanged
	})
	viper.Reset()
	if err := f.Value.Set("false"); err != nil {
		t.Fatal(err)
	}
	f.Changed = false
	resolve := func() bool {
		return resolveBoolOption(f, viper.IsSet("dashboard.machine_details"), viper.GetBool("dashboard.machine_details"))
	}
	if resolve() {
		t.Fatal("default resolution enabled machine collection")
	}
	viper.Set("dashboard.host_resources", true)
	if resolve() {
		t.Fatal("host resources implicitly enabled machine collection")
	}
	viper.Set("dashboard.machine_details", true)
	if !resolve() {
		t.Fatal("TOML machine opt-in ignored")
	}
	if err := Run.Flags().Set("dashboard-machine-details", "false"); err != nil {
		t.Fatal(err)
	}
	if resolve() {
		t.Fatal("explicit CLI false did not override TOML")
	}
	for _, socket := range []string{"", " \t\n", "/private/feed.sock"} {
		for _, host := range []bool{false, true} {
			if validateDashboardMachineDetails(false, socket, host) != nil {
				t.Fatal("disabled machine option requires resources")
			}
			valid := socket == "/private/feed.sock" && host
			if (validateDashboardMachineDetails(true, socket, host) == nil) != valid {
				t.Fatalf("machine prerequisites: socket=%q host=%t", socket, host)
			}
		}
	}
}

func TestMachineNodeAttachmentDisabled(t *testing.T) {
	for _, host := range []bool{false, true} {
		t.Run("host_"+strconv.FormatBool(host), func(t *testing.T) {
			dir, err := os.MkdirTemp("/tmp", "machine-off-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := os.RemoveAll(dir); err != nil {
					t.Error(err)
				}
			})
			socket := filepath.Join(dir, "feed.sock")
			d := startNodeDashboard(context.Background(), time.Now(), socket, "", "machine-off", "isolated", "verifying", true, false, host, false, false, false, false, false)
			if d == nil {
				t.Fatal("node feed did not start")
			}
			t.Cleanup(d.close)
			if d.machine != nil || (d.hostSampler != nil) != host {
				t.Fatal("machine opt-out changed host configuration or allocated a machine owner")
			}
			d.observeRuntime(time.Now(), runtime.MemStats{}, nil)
			if d.machineGeneration != 0 {
				t.Fatal("machine opt-out advanced publication state")
			}
			transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", socket)
			}}
			t.Cleanup(transport.CloseIdleConnections)
			client := &http.Client{Transport: transport, Timeout: time.Second}
			response, err := client.Get("http://node/details/machine")
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusNotFound {
				t.Fatalf("machine opt-out status = %d, want 404", response.StatusCode)
			}
		})
	}
}

func TestMachineNodeAttachment(t *testing.T) {
	for _, role := range []string{"verifying", "validator"} {
		t.Run(role, func(t *testing.T) {
			dir, err := os.MkdirTemp("/tmp", "machine-node-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := os.RemoveAll(dir); err != nil {
					t.Error(err)
				}
			})
			socket := filepath.Join(dir, "feed.sock")
			d := startNodeDashboard(context.Background(), time.Now().Add(-time.Minute), socket, "", "machine-"+role, "isolated", role, true, false, true, false, true, false, false, false)
			if d == nil || d.machine == nil {
				t.Fatal("machine opt-in did not register an owner before Feed startup")
			}
			t.Cleanup(d.close)
			d.bootstrap("JEKNVnkbo3jma5nREBBJCDoXFVeKkD56V3xKrvRmWxFG")
			captures := 0
			install := func() {
				d.machine.captureMetadata = func() machineDashboardMetadata {
					captures++
					at := time.Now()
					return machineDashboardMetadata{
						topology: metrics.TopologyObservation{Generation: 1, StartedAt: at, SampledAt: at, CPUState: metrics.TopologyUnsupported, NUMAState: metrics.TopologyUnsupported},
						affinity: metrics.ThreadAffinityObservation{Generation: 1, State: metrics.TopologyUnsupported},
						links:    metrics.LinkTopologyObservation{Generation: 1, State: metrics.TopologyUnsupported},
						routes:   metrics.RouteSummaryObservation{Generation: 1, State: metrics.TopologyUnsupported},
					}
				}
			}
			install()
			before := time.Now()
			// A delayed ticker date must not become the RSS read's original clock.
			d.observeRuntime(before.Add(-time.Hour), runtime.MemStats{}, nil)
			capture, err := d.sampleMachineDetail(time.Now())
			if err != nil {
				t.Fatal(err)
			}
			page, ok := capture.Prepared.Page(0)
			var header dashboard.MachineDetailPage
			if !ok || json.Unmarshal([]byte(page), &header) != nil || header.Header == nil || header.Header.RSSHour == nil || header.Header.RSSHour.Current.At == nil || header.Header.RSSHour.Current.At.Before(before) || header.Header.GOMAXPROCS == nil {
				t.Fatal("node lost the actual RSS clock or resolved runtime setting")
			}
			transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", socket)
			}}
			t.Cleanup(transport.CloseIdleConnections)
			client := &http.Client{Transport: transport, Timeout: time.Second}
			read := func() (int, dashboard.MachineDetailManifest) {
				response, err := client.Get("http://node/details/machine")
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				body, err := io.ReadAll(io.LimitReader(response.Body, dashboard.MaxMachineManifestBytes+1))
				if err != nil || len(body) > dashboard.MaxMachineManifestBytes {
					t.Fatal("machine manifest read failed or exceeded its bound", err)
				}
				var manifest dashboard.MachineDetailManifest
				if response.StatusCode == http.StatusOK && json.Unmarshal(body, &manifest) != nil {
					t.Fatal("invalid machine manifest")
				}
				return response.StatusCode, manifest
			}
			await := func(generation uint64) {
				deadline := time.Now().Add(4 * time.Second)
				for {
					status, manifest := read()
					if status == http.StatusOK && manifest.Binding.SourceGeneration == generation {
						return
					}
					if time.Now().After(deadline) {
						t.Fatalf("machine generation %d not published: status=%d manifest=%+v", generation, status, manifest)
					}
					time.Sleep(50 * time.Millisecond)
				}
			}
			await(1)
			for range 3 {
				read()
			}
			if captures != 1 {
				t.Fatal("viewer caused metadata sampling")
			}
			old := d.machine
			d.configureMachineDetails(true)
			install()
			d.observeRuntime(time.Now(), runtime.MemStats{}, nil)
			await(2)
			if _, err := old.sample(); err == nil || captures != 2 || d.machineGeneration != 2 {
				t.Fatal("replacement reused a publication generation or left the old owner active")
			}
			d.stopping()
			if _, err := d.sampleMachineDetail(time.Now()); err == nil {
				t.Fatal("stopping node retained a live machine publication")
			}
			d.configureMachineDetails(true)
			if d.machine != nil {
				t.Fatal("stopped node recreated the owner")
			}
		})
	}
}
