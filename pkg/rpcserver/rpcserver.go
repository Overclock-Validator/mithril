package rpcserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/filecoin-project/go-jsonrpc"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

type RpcServer struct {
	isReady       bool
	rpcService    *jsonrpc.RPCServer
	serv          *httptest.Server
	listener      net.Listener
	acctsDb       *accountsdb.AccountsDb
	epochSchedule *sealevel.SysvarEpochSchedule
	slotCtx       *sealevel.SlotCtx
	slotCtxMu     sync.RWMutex
	genesisHash   string

	leaderTPUCacheMu         sync.RWMutex
	leaderTPUByIdentity      map[solana.PublicKey]tpuEndpoint
	leaderTPUCacheUpdatedAt  time.Time
	clusterNodesRefreshEvery time.Duration
	clusterNodesRefreshOnce  sync.Once

	clusterRPCEndpoints []string
	clusterNodesFetcher clusterNodesFetcher
	// transactionSender is injectable for tests; production supports QUIC with UDP fallback.
	transactionSender                 transactionSender
	sendTransactionLeaderForwardCount uint64
}

const maxQuietMethodProbeBody = 64 << 10

var supportedRPCMethods = map[string]struct{}{
	"getAccountInfo":      {},
	"getBankHash":         {},
	"getBlockHeight":      {},
	"getEpochInfo":        {},
	"getGenesisHash":      {},
	"getLatestBlockhash":  {},
	"sendTransaction":     {},
	"simulateTransaction": {},
}

func NewRpcServer(acctsDb *accountsdb.AccountsDb, port uint16, epochSchedule *sealevel.SysvarEpochSchedule, genesisHash solana.Hash) *RpcServer {
	var err error
	rpcServer := &RpcServer{genesisHash: genesisHash.String()}

	addrStr := fmt.Sprintf("0.0.0.0:%d", port)
	rpcServer.listener, err = net.Listen("tcp", addrStr)
	if err != nil {
		panic(err)
	}

	rpcErrors := rpcErrorRegistry()
	rpcServer.rpcService = jsonrpc.NewServer(
		jsonrpc.WithServerMethodNameFormatter(
			func(namespace, method string) string {
				return strings.ToLower(string(method[0])) + method[1:]
			}),
		jsonrpc.WithServerErrors(rpcErrors),
	)

	rpcServer.rpcService.Register("MithrilRpc", rpcServer)
	rpcServer.acctsDb = acctsDb
	if epochSchedule != nil {
		rpcServer.epochSchedule = epochSchedule
	} else {
		rpcServer.epochSchedule = fetchAndUnmarshalEpochScheduleSysvar(acctsDb)
	}
	rpcServer.leaderTPUByIdentity = make(map[solana.PublicKey]tpuEndpoint)
	rpcServer.clusterNodesRefreshEvery = sendTransactionClusterNodesRefreshEvery
	rpcServer.clusterRPCEndpoints = configuredSendTransactionRPCEndpoints()
	rpcServer.transactionSender = defaultTransactionSender
	rpcServer.sendTransactionLeaderForwardCount = sendTransactionLeaderForwardCount

	return rpcServer
}

func fetchAndUnmarshalEpochScheduleSysvar(acctsDb *accountsdb.AccountsDb) *sealevel.SysvarEpochSchedule {
	epochScheduleAcct, err := acctsDb.GetAccount(0, sealevel.SysvarEpochScheduleAddr)
	if err != nil {
		panic("unable to get epochschedule when creating RPC server")
	}

	decoder := bin.NewBinDecoder(epochScheduleAcct.Data)
	var epochSchedule sealevel.SysvarEpochSchedule
	epochSchedule.MustUnmarshalWithDecoder(decoder)

	return &epochSchedule
}

func (rpcServer *RpcServer) SetSlotCtx(slotCtx *sealevel.SlotCtx) {
	rpcServer.slotCtxMu.Lock()
	rpcServer.slotCtx = slotCtx
	rpcServer.slotCtxMu.Unlock()
}

func (rpcServer *RpcServer) getSlotCtx() *sealevel.SlotCtx {
	rpcServer.slotCtxMu.RLock()
	defer rpcServer.slotCtxMu.RUnlock()
	return rpcServer.slotCtx
}

func (rpcServer *RpcServer) Start() {
	rpcServer.startClusterNodesRefreshLoop()
	go http.Serve(rpcServer.listener, rpcServer)
}

func (rpcServer *RpcServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if quietNonRPCProbe(w, r) || writeQuietRPCProbeError(w, r) {
		return
	}
	rpcServer.rpcService.ServeHTTP(w, r)
}

type rpcMethodProbe struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
}

type rpcProbeErrorResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Error   rpcProbeError   `json:"error"`
}

type rpcProbeError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func quietNonRPCProbe(w http.ResponseWriter, r *http.Request) bool {
	if strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade") {
		return false
	}
	if r.Method == http.MethodPost {
		return false
	}
	http.NotFound(w, r)
	return true
}

func writeQuietRPCProbeError(w http.ResponseWriter, r *http.Request) bool {
	body, ok := readQuietMethodProbeBody(r)
	if !ok && r.ContentLength != 0 {
		return false
	}
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		writeRPCProbeError(w, nil, http.StatusBadRequest, -32600, "Invalid request")
		return true
	}
	if body[0] == '[' {
		return writeQuietBatchProbeError(w, body)
	}

	var req rpcMethodProbe
	if err := json.Unmarshal(body, &req); err != nil {
		writeRPCProbeError(w, nil, http.StatusInternalServerError, -32700, "Parse error")
		return true
	}
	if req.Method == "" {
		writeRPCProbeError(w, req.ID, http.StatusBadRequest, -32600, "Invalid request")
		return true
	}
	if _, ok := supportedRPCMethods[req.Method]; ok {
		return false
	}

	writeRPCProbeError(w, req.ID, http.StatusInternalServerError, -32601, fmt.Sprintf("method '%s' not found", req.Method))
	return true
}

func writeQuietBatchProbeError(w http.ResponseWriter, body []byte) bool {
	var reqs []rpcMethodProbe
	if err := json.Unmarshal(body, &reqs); err != nil {
		writeRPCProbeError(w, nil, http.StatusInternalServerError, -32700, "Parse error")
		return true
	}
	if len(reqs) == 0 {
		writeRPCProbeError(w, nil, http.StatusBadRequest, -32600, "Invalid request")
		return true
	}
	allQuiet := true
	for _, req := range reqs {
		if _, ok := supportedRPCMethods[req.Method]; ok {
			allQuiet = false
			break
		}
	}
	if !allQuiet {
		return false
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusInternalServerError)
	resps := make([]rpcProbeErrorResponse, 0, len(reqs))
	for _, req := range reqs {
		code := -32601
		message := fmt.Sprintf("method '%s' not found", req.Method)
		if req.Method == "" {
			code = -32600
			message = "Invalid request"
		}
		resps = append(resps, rpcProbeErrorResponse{
			JSONRPC: "2.0",
			ID:      normalizedRPCProbeID(req.ID),
			Error: rpcProbeError{
				Code:    code,
				Message: message,
			},
		})
	}
	_ = json.NewEncoder(w).Encode(resps)
	return true
}

func writeRPCProbeError(w http.ResponseWriter, id json.RawMessage, status int, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(rpcProbeErrorResponse{
		JSONRPC: "2.0",
		ID:      normalizedRPCProbeID(id),
		Error: rpcProbeError{
			Code:    code,
			Message: message,
		},
	})
}

func normalizedRPCProbeID(id json.RawMessage) json.RawMessage {
	if len(id) == 0 {
		return json.RawMessage("null")
	}
	return id
}

func readQuietMethodProbeBody(r *http.Request) ([]byte, bool) {
	if r.Body == nil || r.Body == http.NoBody || r.ContentLength == 0 || r.ContentLength > maxQuietMethodProbeBody {
		return nil, false
	}
	if r.ContentLength > 0 {
		body, err := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		return body, err == nil
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxQuietMethodProbeBody+1))
	if err != nil {
		r.Body = io.NopCloser(io.MultiReader(bytes.NewReader(body), r.Body))
		return nil, false
	}
	if len(body) > maxQuietMethodProbeBody {
		r.Body = io.NopCloser(io.MultiReader(bytes.NewReader(body), r.Body))
		return nil, false
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	return body, true
}

func (rpcServer *RpcServer) startClusterNodesRefreshLoop() {
	rpcServer.clusterNodesRefreshOnce.Do(func() {
		if rpcServer.clusterNodesFetcher == nil && len(rpcServer.clusterRPCEndpoints) == 0 {
			return
		}

		go func() {
			if err := rpcServer.refreshLeaderTPUCache(context.Background()); err != nil {
				mlog.Log.Warnf("sendTransaction: initial cluster node refresh failed: %v", err)
			}

			ticker := time.NewTicker(rpcServer.clusterNodesRefreshInterval())
			defer ticker.Stop()

			for range ticker.C {
				if err := rpcServer.refreshLeaderTPUCache(context.Background()); err != nil {
					mlog.Log.Warnf("sendTransaction: periodic cluster node refresh failed: %v", err)
				}
			}
		}()
	})
}

func (rpcServer *RpcServer) clusterNodesRefreshInterval() time.Duration {
	if rpcServer.clusterNodesRefreshEvery > 0 {
		return rpcServer.clusterNodesRefreshEvery
	}
	return sendTransactionClusterNodesRefreshEvery
}
