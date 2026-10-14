package rpcserver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestServeHTTPQuietlyHandlesUnsupportedMethod(t *testing.T) {
	rpcServer := &RpcServer{}
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"jsonrpc":"2.0","method":"unsupportedMethod","id":7}`))
	rec := httptest.NewRecorder()

	rpcServer.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status %d, got %d", http.StatusInternalServerError, rec.Code)
	}

	var resp struct {
		JSONRPC string `json:"jsonrpc"`
		ID      int    `json:"id"`
		Error   struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.JSONRPC != "2.0" || resp.ID != 7 {
		t.Fatalf("unexpected response identity: %+v", resp)
	}
	if resp.Error.Code != -32601 || resp.Error.Message != "method 'unsupportedMethod' not found" {
		t.Fatalf("unexpected error response: %+v", resp.Error)
	}
}

func TestServeHTTPQuietlyHandlesInvalidRequest(t *testing.T) {
	rpcServer := &RpcServer{}
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(""))
	rec := httptest.NewRecorder()

	rpcServer.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}

	var resp struct {
		JSONRPC string `json:"jsonrpc"`
		ID      any    `json:"id"`
		Error   struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.JSONRPC != "2.0" || resp.ID != nil {
		t.Fatalf("unexpected response identity: %+v", resp)
	}
	if resp.Error.Code != -32600 || resp.Error.Message != "Invalid request" {
		t.Fatalf("unexpected error response: %+v", resp.Error)
	}
}

func TestServeHTTPQuietlyHandlesNonRPCProbe(t *testing.T) {
	rpcServer := &RpcServer{}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	rpcServer.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d", http.StatusNotFound, rec.Code)
	}
}
