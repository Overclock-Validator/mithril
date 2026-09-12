package node

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPprofHandlerIsIsolatedFromMetrics(t *testing.T) {
	previousDefault := http.DefaultServeMux
	http.DefaultServeMux = http.NewServeMux()
	http.DefaultServeMux.HandleFunc("/metrics", func(http.ResponseWriter, *http.Request) {})
	t.Cleanup(func() { http.DefaultServeMux = previousDefault })

	handler := newPprofHandler()

	index := httptest.NewRecorder()
	handler.ServeHTTP(index, httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil))
	if index.Code != http.StatusOK {
		t.Fatalf("GET /debug/pprof/ status = %d, want 200", index.Code)
	}
	if got := index.Header().Get(mithrilEndpointHeader); got != mithrilPprofEndpoint {
		t.Fatalf("GET /debug/pprof/ identity = %q, want %q", got, mithrilPprofEndpoint)
	}

	metrics := httptest.NewRecorder()
	handler.ServeHTTP(metrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if metrics.Code != http.StatusNotFound {
		t.Fatalf("GET /metrics status = %d, want 404", metrics.Code)
	}
}

func TestPprofServerRejectsOccupiedAddress(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve address: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	if err := startPprofHTTPServer(listener.Addr().String()); err == nil {
		t.Fatal("startPprofHTTPServer() error = nil, want occupied-address error")
	}
}
