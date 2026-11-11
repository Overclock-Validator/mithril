package node

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

// genesisStub is the only RPC endpoint a backtest node sees: a loopback
// listener inside the backtest process that answers getGenesisHash from the
// cluster configuration and rejects every other method. Startup's genesis
// check passes without any outside endpoint, and the node's background tip
// polling gets an error instead of reaching the network.
type genesisStub struct {
	URL      string
	Rejected atomic.Int64 // calls to any other method
	server   *http.Server
}

func startGenesisStub(genesisHash string) (*genesisStub, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	stub := &genesisStub{URL: "http://" + ln.Addr().String()}
	stub.server = &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		reply := func(req map[string]any) map[string]any {
			if req["method"] == "getGenesisHash" {
				return map[string]any{"jsonrpc": "2.0", "id": req["id"], "result": genesisHash}
			}
			stub.Rejected.Add(1)
			return map[string]any{"jsonrpc": "2.0", "id": req["id"],
				"error": map[string]any{"code": -32601, "message": "backtest: no RPC beyond getGenesisHash"}}
		}
		var out any
		if trimmed := bytes.TrimSpace(body); len(trimmed) > 0 && trimmed[0] == '[' {
			var batch []map[string]any
			if err := json.Unmarshal(trimmed, &batch); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			replies := make([]map[string]any, len(batch))
			for i, req := range batch {
				replies[i] = reply(req)
			}
			out = replies
		} else {
			var req map[string]any
			if err := json.Unmarshal(trimmed, &req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			out = reply(req)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})}
	go func() { _ = stub.server.Serve(ln) }()
	return stub, nil
}

func (s *genesisStub) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = s.server.Shutdown(ctx)
}
