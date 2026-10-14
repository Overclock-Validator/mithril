package rpcserver

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

func TestRPCHandlerMethodSet(t *testing.T) {
	typ := reflect.TypeOf(rpcHandler{})
	if typ.NumMethod() != len(supportedRPCMethods) {
		t.Fatalf("registered %d methods, public method list has %d", typ.NumMethod(), len(supportedRPCMethods))
	}
	for i := 0; i < typ.NumMethod(); i++ {
		name := typ.Method(i).Name
		name = strings.ToLower(name[:1]) + name[1:]
		if _, ok := supportedRPCMethods[name]; !ok {
			t.Fatalf("non-public method registered: %s", name)
		}
	}
}

func TestInternalMethodsAreNotRPC(t *testing.T) {
	for _, mode := range []string{"single", "mixed-batch", "oversized-single", "direct-dispatch"} {
		t.Run(mode, func(t *testing.T) {
			rpc := NewRpcServer(nil, 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}, solana.Hash{1})
			t.Cleanup(func() { _ = rpc.listener.Close() })
			rpc.SetRootedBankState(100, 90, 80)
			call := map[string]any{"jsonrpc": "2.0", "id": 1, "method": "setRootedBankState", "params": []any{777, 666, 555}}
			if mode == "oversized-single" {
				call["padding"] = strings.Repeat("x", maxQuietMethodProbeBody+1)
			}
			var request any = call
			if mode == "mixed-batch" {
				request = []any{map[string]any{"jsonrpc": "2.0", "id": 2, "method": "getGenesisHash", "params": []any{}}, call}
			}
			body, err := json.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			if mode == "direct-dispatch" {
				rpc.rpcService.HandleRequest(context.Background(), bytes.NewReader(body), response)
			} else {
				rpc.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body)))
			}
			var replies []struct {
				ID     int    `json:"id"`
				Result string `json:"result"`
				Error  *struct {
					Code int `json:"code"`
				} `json:"error"`
			}
			result := response.Body.Bytes()
			if mode != "mixed-batch" {
				result = append(append([]byte{'['}, result...), ']')
			}
			if err := json.Unmarshal(result, &replies); err != nil {
				t.Fatalf("decode response %q: %v", result, err)
			}
			found := false
			for _, reply := range replies {
				if reply.ID == 1 {
					found = true
					if reply.Error == nil || reply.Error.Code != -32601 {
						t.Fatalf("internal method was not rejected: %s", result)
					}
				}
				if reply.ID == 2 && (reply.Error != nil || reply.Result != (solana.Hash{1}).String()) {
					t.Fatalf("public batch method failed: %s", result)
				}
			}
			if !found || (mode == "mixed-batch" && len(replies) != 2) {
				t.Fatalf("missing response: %s", result)
			}
			root, _ := rpc.getRootedBankState()
			if root.Slot != 100 {
				t.Fatalf("internal request changed rooted bank: %d", root.Slot)
			}
		})
	}
}
