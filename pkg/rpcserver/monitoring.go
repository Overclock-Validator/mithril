package rpcserver

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/filecoin-project/go-jsonrpc"
)

// MethodMetrics counts completed public method invocations, including batch
// members and notifications. Rejected transport/envelope requests are excluded.
type MethodMetrics struct {
	Name      string
	Completed uint64
	Errors    uint64
	Active    uint64
	// Non-cumulative bins: <=1ms, <=10ms, <=100ms, <=1s, <=10s, >10s.
	DurationBuckets [6]uint64
}

// MonitoringSnapshot contains copied publication state and bounded method totals.
type MonitoringSnapshot struct {
	MonitoringStartedAt    time.Time
	Serving                bool
	HasPublishedSlot       bool
	PublishedSlot          uint64
	PublishedAt            time.Time
	HistoryRecoveryPending bool
	Methods                []MethodMetrics
}

type requestMetrics struct {
	// ponytail: one short lock for nine methods; split only if contention is measured.
	mu        sync.Mutex
	methods   map[string]MethodMetrics
	startedAt time.Time
}

// Counters are initialized only for methods wrapped by observeRPC.
var instrumentedRPCMethods = [...]string{
	"getAccountInfo", "getBankHash", "getBlockHeight", "getEpochInfo",
	"getGenesisHash", "getInflationReward", "getLatestBlockhash",
	"sendTransaction", "simulateTransaction",
}

// EnableMonitoring starts counters at zero without changing RPC responses or
// registration. It is safe to repeat and does not expose a JSON-RPC method.
func (s *RpcServer) EnableMonitoring() {
	m := &requestMetrics{methods: make(map[string]MethodMetrics, len(instrumentedRPCMethods)), startedAt: time.Now().UTC()}
	for _, name := range instrumentedRPCMethods {
		if _, ok := supportedRPCMethods[name]; ok {
			m.methods[name] = MethodMetrics{Name: name}
		}
	}
	s.monitoring.CompareAndSwap(nil, m)
}

// MonitoringSnapshot never calls an RPC method or reads mutable bank fields.
func (s *RpcServer) MonitoringSnapshot() MonitoringSnapshot {
	result := MonitoringSnapshot{Serving: s.serving.Load()}
	s.slotCtxMu.RLock()
	result.HasPublishedSlot, result.PublishedSlot, result.PublishedAt = s.hasPublishedSlot, s.publishedSlot, s.publishedAt
	s.slotCtxMu.RUnlock()
	s.historyRecoveryMu.RLock()
	result.HistoryRecoveryPending = s.historyRecoveryPending
	s.historyRecoveryMu.RUnlock()
	if m := s.monitoring.Load(); m != nil {
		result.MonitoringStartedAt = m.startedAt
		m.mu.Lock()
		result.Methods = make([]MethodMetrics, 0, len(m.methods))
		for _, value := range m.methods {
			result.Methods = append(result.Methods, value)
		}
		m.mu.Unlock()
		sort.Slice(result.Methods, func(i, j int) bool { return result.Methods[i].Name < result.Methods[j].Name })
	}
	return result
}

func observeRPC[T any](h rpcHandler, method string, call func() (T, error)) (T, error) {
	m := h.monitoring.Load()
	if m == nil {
		return call()
	}
	started := time.Now()
	m.mu.Lock()
	value, ok := m.methods[method]
	if !ok {
		m.mu.Unlock()
		return call()
	}
	value.Active++
	m.methods[method] = value
	m.mu.Unlock()
	succeeded := false
	defer func() {
		elapsed := time.Since(started)
		bucket := 5
		for i, limit := range [...]time.Duration{time.Millisecond, 10 * time.Millisecond, 100 * time.Millisecond, time.Second, 10 * time.Second} {
			if elapsed <= limit {
				bucket = i
				break
			}
		}
		m.mu.Lock()
		value := m.methods[method]
		value.Active--
		value.Completed++
		if !succeeded {
			value.Errors++
		}
		value.DurationBuckets[bucket]++
		m.methods[method] = value
		m.mu.Unlock()
	}()
	result, err := call()
	succeeded = err == nil
	return result, err
}

func (h rpcHandler) GetAccountInfo(ctx context.Context, params jsonrpc.RawParams) (GetAccountInfoResp, error) {
	return observeRPC(h, "getAccountInfo", func() (GetAccountInfoResp, error) { return h.rpcMethods.GetAccountInfo(ctx, params) })
}

func (h rpcHandler) GetBankHash(ctx context.Context, params jsonrpc.RawParams) (string, error) {
	return observeRPC(h, "getBankHash", func() (string, error) { return h.rpcMethods.GetBankHash(ctx, params) })
}

func (h rpcHandler) GetBlockHeight(ctx context.Context, params jsonrpc.RawParams) (uint64, error) {
	return observeRPC(h, "getBlockHeight", func() (uint64, error) { return h.rpcMethods.GetBlockHeight(ctx, params) })
}

func (h rpcHandler) GetEpochInfo(ctx context.Context, params jsonrpc.RawParams) (GetEpochInfoResp, error) {
	return observeRPC(h, "getEpochInfo", func() (GetEpochInfoResp, error) { return h.rpcMethods.GetEpochInfo(ctx, params) })
}

func (h rpcHandler) GetGenesisHash(ctx context.Context, params jsonrpc.RawParams) (string, error) {
	return observeRPC(h, "getGenesisHash", func() (string, error) { return h.rpcMethods.GetGenesisHash(ctx, params) })
}

func (h rpcHandler) GetInflationReward(ctx context.Context, params jsonrpc.RawParams) ([]*InflationRewardResp, error) {
	return observeRPC(h, "getInflationReward", func() ([]*InflationRewardResp, error) { return h.rpcMethods.GetInflationReward(ctx, params) })
}

func (h rpcHandler) GetLatestBlockhash(ctx context.Context, params jsonrpc.RawParams) (GetLatestBlockhashResp, error) {
	return observeRPC(h, "getLatestBlockhash", func() (GetLatestBlockhashResp, error) { return h.rpcMethods.GetLatestBlockhash(ctx, params) })
}

func (h rpcHandler) SendTransaction(ctx context.Context, params jsonrpc.RawParams) (string, error) {
	return observeRPC(h, "sendTransaction", func() (string, error) { return h.rpcMethods.SendTransaction(ctx, params) })
}

func (h rpcHandler) SimulateTransaction(ctx context.Context, params jsonrpc.RawParams) (SimulateTransactionResp, error) {
	return observeRPC(h, "simulateTransaction", func() (SimulateTransactionResp, error) { return h.rpcMethods.SimulateTransaction(ctx, params) })
}
