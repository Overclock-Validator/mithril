package mcp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestBuildPprofURL(t *testing.T) {
	cases := []struct {
		endpoint, path, query, want string
	}{
		{"http://127.0.0.1:6060", "/debug/pprof/heap", "", "http://127.0.0.1:6060/debug/pprof/heap"},
		{"http://127.0.0.1:6060/", "/debug/pprof/heap", "", "http://127.0.0.1:6060/debug/pprof/heap"},
		{"http://127.0.0.1:6060/old/path", "/debug/pprof/heap", "", "http://127.0.0.1:6060/debug/pprof/heap"},
		{"http://127.0.0.1:6060", "/debug/pprof/profile", "seconds=30", "http://127.0.0.1:6060/debug/pprof/profile?seconds=30"},
	}
	for _, c := range cases {
		got, err := buildPprofURL(c.endpoint, c.path, c.query)
		if err != nil || got != c.want {
			t.Errorf("buildPprofURL(%q,%q,%q) = %q,%v; want %q", c.endpoint, c.path, c.query, got, err, c.want)
		}
	}
}

func TestFetchPprofSizeCap(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(mithrilEndpointHeader, mithrilPprofEndpoint)
		_, _ = w.Write([]byte(strings.Repeat("x", maxProfileBytes+1)))
	}))
	defer server.Close()
	if _, err := fetchPprof(context.Background(), server.URL, "/debug/pprof/heap", "", 5*time.Second); err == nil {
		t.Fatal("profile above cap must fail")
	}
}

func TestMaxPprofResultFitsDefaultWireBudget(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(mithrilEndpointHeader, mithrilPprofEndpoint)
		_, _ = w.Write([]byte(strings.Repeat("x", maxProfileBytes)))
	}))
	defer server.Close()
	session := startInMemorySession(t, Config{Profile: ProfileDiagnostic, PprofURL: server.URL})
	text, isError := callToolText(t, session, "mithril_pprof_heap", nil)
	if isError {
		t.Fatalf("maximum accepted profile was rejected by the default wire budget: %s", text)
	}
	if !strings.Contains(text, `"size_bytes":327680`) {
		t.Fatalf("profile result has wrong size metadata")
	}
}

func TestFetchPprofRejectsUnmarkedEndpoint(t *testing.T) {
	for _, identity := range []string{"", mithrilMetricsEndpoint} {
		t.Run(identity, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if identity != "" {
					w.Header().Set(mithrilEndpointHeader, identity)
				}
				_, _ = w.Write([]byte("profile"))
			}))
			defer server.Close()

			if _, err := fetchPprof(context.Background(), server.URL, "/debug/pprof/heap", "", 5*time.Second); err == nil || !strings.Contains(err.Error(), "expected Mithril service") {
				t.Fatalf("fetchPprof identity %q error = %v", identity, err)
			}
		})
	}
}

func TestPprofGateHonorsCancellation(t *testing.T) {
	if err := acquirePprof(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer releasePprof()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := acquirePprof(ctx); err == nil {
		t.Fatal("second profile must not enter after its context is canceled")
	}
}

func TestBusyPprofFailsFastWithoutStarvingOtherTools(t *testing.T) {
	if err := acquirePprof(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer releasePprof()
	session := startInMemorySession(t, Config{Profile: ProfileDiagnostic, PprofURL: "http://127.0.0.1:1"})
	started := time.Now()
	text, isError := callToolText(t, session, "mithril_pprof_profile", map[string]any{"seconds": 30})
	if !isError || !strings.Contains(text, "already in progress") {
		t.Fatalf("busy profile result = error:%v %s", isError, text)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("busy profile waited %v instead of failing fast", elapsed)
	}
	if _, isError := callToolText(t, session, "mithril_mcp_info", nil); isError {
		t.Fatal("busy pprof starved an ordinary monitoring tool")
	}
}

func TestBuildPprofURLRejectsUnsafe(t *testing.T) {
	if _, err := buildPprofURL("file:///etc/passwd", "/debug/pprof/heap", ""); err == nil {
		t.Error("file:// scheme should be rejected")
	}
	if _, err := buildPprofURL("http://10.0.0.1:6060", "/debug/pprof/heap", ""); err == nil {
		t.Error("private IP should be rejected")
	}
}
