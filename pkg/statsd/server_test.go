package statsd

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMetricsServerUsesDedicatedMux(t *testing.T) {
	oldMux := http.DefaultServeMux
	http.DefaultServeMux = http.NewServeMux()
	t.Cleanup(func() { http.DefaultServeMux = oldMux })
	for _, path := range []string{"/debug/pprof/", "/setblockprofilerate"} {
		http.DefaultServeMux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	}
	server := newMetricsServer("127.0.0.1:9090")
	if server.Addr != "127.0.0.1:9090" {
		t.Fatalf("unexpected address: %q", server.Addr)
	}
	for _, tc := range []struct {
		path   string
		status int
	}{
		{"/metrics", http.StatusOK},
		{"/debug/pprof/", http.StatusNotFound},
		{"/setblockprofilerate", http.StatusNotFound},
		{"/", http.StatusNotFound},
	} {
		t.Run(tc.path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			server.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))
			if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d", rec.Code, tc.status)
			}
		})
	}
	// Constructing the metrics listener must not remove or change profiling handlers.
	for _, path := range []string{"/debug/pprof/", "/setblockprofilerate"} {
		rec := httptest.NewRecorder()
		http.DefaultServeMux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("default mux handler %s changed: %d", path, rec.Code)
		}
	}
}
