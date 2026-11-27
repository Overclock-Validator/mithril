package rpcserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/bankview"
	"github.com/Overclock-Validator/mithril/pkg/blockhistory"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/filecoin-project/go-jsonrpc"
)

// Methods answered from data the node already holds: the epoch schedule,
// published banks, the leader schedule, retained block summaries, the
// inflation settings replay uses and the shreds turbine has seen.

// GetEpochScheduleResp matches Agave's EpochSchedule.
type GetEpochScheduleResp struct {
	SlotsPerEpoch            uint64 `json:"slotsPerEpoch"`
	LeaderScheduleSlotOffset uint64 `json:"leaderScheduleSlotOffset"`
	Warmup                   bool   `json:"warmup"`
	FirstNormalEpoch         uint64 `json:"firstNormalEpoch"`
	FirstNormalSlot          uint64 `json:"firstNormalSlot"`
}

func (rpcServer *RpcServer) GetEpochSchedule(ctx context.Context, p jsonrpc.RawParams) (GetEpochScheduleResp, error) {
	if err := noParams("getEpochSchedule", p); err != nil {
		return GetEpochScheduleResp{}, err
	}
	s := rpcServer.epochSchedule
	if s == nil {
		return GetEpochScheduleResp{}, errors.New("node has no epoch schedule available")
	}
	return GetEpochScheduleResp{SlotsPerEpoch: s.SlotsPerEpoch, LeaderScheduleSlotOffset: s.LeaderScheduleSlotOffset,
		Warmup: s.Warmup, FirstNormalEpoch: s.FirstNormalEpoch, FirstNormalSlot: s.FirstNormalSlot}, nil
}

func (rpcServer *RpcServer) GetTransactionCount(ctx context.Context, p jsonrpc.RawParams) (uint64, error) {
	conf, err := scalarConfig(ctx, "getTransactionCount", p)
	if err != nil {
		return 0, err
	}
	view, err := rpcServer.currentView(ctx, conf)
	if err != nil {
		return 0, err
	}
	return view.TransactionCount, nil
}

func (rpcServer *RpcServer) GetSlotLeader(ctx context.Context, p jsonrpc.RawParams) (string, error) {
	conf, err := scalarConfig(ctx, "getSlotLeader", p)
	if err != nil {
		return "", err
	}
	view, err := rpcServer.currentView(ctx, conf)
	if err != nil {
		return "", err
	}
	leader, ok := global.LeaderForSlot(view.Slot)
	if !ok {
		return "", fmt.Errorf("no leader schedule for slot %d", view.Slot)
	}
	return leader.String(), nil
}

const maxSlotLeaders = 5000 // Agave's MAX_GET_SLOT_LEADERS

func (rpcServer *RpcServer) GetSlotLeaders(ctx context.Context, p jsonrpc.RawParams) ([]string, error) {
	params, err := jsonrpc.DecodeParams[[]json.RawMessage](p)
	if err != nil || len(params) != 2 {
		return nil, &InvalidParamsError{Message: "getSlotLeaders requires a start slot and a limit"}
	}
	var start, limit uint64
	if json.Unmarshal(params[0], &start) != nil || json.Unmarshal(params[1], &limit) != nil {
		return nil, &InvalidParamsError{Message: "invalid start slot or limit"}
	}
	if limit > maxSlotLeaders {
		return nil, &InvalidParamsError{Message: fmt.Sprintf("Invalid limit; max %d", maxSlotLeaders)}
	}
	out := make([]string, 0, limit)
	for i := uint64(0); i < limit; i++ {
		leader, ok := global.LeaderForSlot(start + i)
		if !ok {
			return nil, &InvalidParamsError{Message: fmt.Sprintf("Invalid slot range: leader schedule for slot %d is unavailable", start+i)}
		}
		out = append(out, leader.String())
	}
	return out, nil
}

// GetBlockTime returns the retained block's time, as getBlock reports it.
func (rpcServer *RpcServer) GetBlockTime(ctx context.Context, p jsonrpc.RawParams) (*int64, error) {
	params, err := jsonrpc.DecodeParams[[]json.RawMessage](p)
	if err != nil || len(params) != 1 {
		return nil, &InvalidParamsError{Message: "getBlockTime requires a slot"}
	}
	var slot uint64
	if json.Unmarshal(params[0], &slot) != nil {
		return nil, &InvalidParamsError{Message: "invalid slot"}
	}
	rpcServer.historyRecoveryMu.RLock()
	defer rpcServer.historyRecoveryMu.RUnlock()
	if rpcServer.historyRecoveryPending {
		return nil, errors.New("RPC history is unavailable during account recovery")
	}
	if rpcServer.blockHistory == nil {
		return nil, &BlockNotAvailableError{Slot: slot}
	}
	record, err := rpcServer.blockHistory.Get(slot)
	if err != nil {
		return nil, blockHistoryRPCError(slot, err)
	}
	return record.BlockTime, nil // null when the block recorded no time
}

// GetBlocksWithLimit returns up to limit retained rooted blocks from start.
func (server *RpcServer) GetBlocksWithLimit(ctx context.Context, p jsonrpc.RawParams) ([]uint64, error) {
	params, err := jsonrpc.DecodeParams[[]json.RawMessage](p)
	if err != nil || len(params) < 2 || len(params) > 3 {
		return nil, &InvalidParamsError{Message: "getBlocksWithLimit requires a start slot, a limit and optional config"}
	}
	var start, limit uint64
	if json.Unmarshal(params[0], &start) != nil || json.Unmarshal(params[1], &limit) != nil {
		return nil, &InvalidParamsError{Message: "invalid start slot or limit"}
	}
	if limit > blockhistory.MaxBlocksRange {
		return nil, &InvalidParamsError{Message: fmt.Sprintf("limit too large; max %d", blockhistory.MaxBlocksRange)}
	}
	var config blocksConfig
	if len(params) == 3 {
		var c *blocksConfig
		if json.Unmarshal(params[2], &c) != nil {
			return nil, &InvalidParamsError{Message: "invalid getBlocksWithLimit config"}
		}
		if c != nil {
			config = *c
		}
	}
	if config.Commitment != nil && *config.Commitment != "confirmed" && *config.Commitment != "finalized" {
		return nil, &InvalidParamsError{Message: "getBlocksWithLimit requires confirmed or finalized commitment"}
	}
	if limit == 0 {
		return []uint64{}, nil
	}
	if err := acquireQueryResponse(ctx); err != nil {
		return nil, err
	}
	// Skipped slots mean limit blocks span more than limit slots: widen the
	// window until it holds enough or reaches the rooted bank.
	span := limit + limit/8 + 16
	for {
		end := uint64(math.MaxUint64)
		if w := min(span, blockhistory.MaxBlocksRange); start <= math.MaxUint64-w {
			end = start + w - 1
		}
		var previous historyReadStamp
		var slots []uint64
		for attempt := 0; ; attempt++ {
			slots, err = server.getBlocksOnce(ctx, start, &end, config, &previous)
			if err != errHistoryViewChanged {
				break
			}
			if attempt == 1 {
				return nil, &BlockNotAvailableError{Slot: start}
			}
			if err := waitForRootedPublication(ctx); err != nil {
				return nil, err
			}
		}
		if err != nil {
			return nil, err
		}
		rooted := previous.bank
		if uint64(len(slots)) >= limit {
			return slots[:limit], nil
		}
		if span >= blockhistory.MaxBlocksRange || rooted == nil || end >= rooted.Slot {
			return slots, nil
		}
		span *= 2
	}
}

func (rpcServer *RpcServer) GetStakeMinimumDelegation(ctx context.Context, p jsonrpc.RawParams) (GetBalanceResp, error) {
	conf, err := scalarConfig(ctx, "getStakeMinimumDelegation", p)
	if err != nil {
		return GetBalanceResp{}, err
	}
	view, err := rpcServer.currentView(ctx, conf)
	if err != nil {
		return GetBalanceResp{}, err
	}
	if view.Features == nil {
		return GetBalanceResp{}, errors.New("node has no feature set for the bank")
	}
	return GetBalanceResp{Context: accountContext(view.Slot), Value: sealevel.MinimumDelegation(*view.Features)}, nil
}

// inflationState is the inflation configuration replay uses.
type inflationState struct {
	inflation    rewards.Inflation
	slotsPerYear float64
}

// SetInflation records the inflation settings and slots per year replay uses.
func (rpcServer *RpcServer) SetInflation(inflation rewards.Inflation, slotsPerYear float64) {
	if cur := rpcServer.inflation.Load(); cur != nil && cur.inflation == inflation && cur.slotsPerYear == slotsPerYear {
		return
	}
	rpcServer.inflation.Store(&inflationState{inflation: inflation, slotsPerYear: slotsPerYear})
}

type GetInflationGovernorResp struct {
	Initial        float64 `json:"initial"`
	Terminal       float64 `json:"terminal"`
	Taper          float64 `json:"taper"`
	Foundation     float64 `json:"foundation"`
	FoundationTerm float64 `json:"foundationTerm"`
}

func (rpcServer *RpcServer) GetInflationGovernor(ctx context.Context, p jsonrpc.RawParams) (GetInflationGovernorResp, error) {
	if _, err := scalarConfig(ctx, "getInflationGovernor", p); err != nil {
		return GetInflationGovernorResp{}, err
	}
	st := rpcServer.inflation.Load()
	if st == nil {
		return GetInflationGovernorResp{}, errors.New("node has no inflation settings yet")
	}
	i := st.inflation
	return GetInflationGovernorResp{Initial: i.Initial, Terminal: i.Terminal, Taper: i.Taper, Foundation: i.FoundationVal, FoundationTerm: i.FoundationTerm}, nil
}

type GetInflationRateResp struct {
	Total      float64 `json:"total"`
	Validator  float64 `json:"validator"`
	Foundation float64 `json:"foundation"`
	Epoch      uint64  `json:"epoch"`
}

// GetInflationRate is Agave's: rates for the current epoch, at the year the
// rewards calculation uses.
func (rpcServer *RpcServer) GetInflationRate(ctx context.Context, p jsonrpc.RawParams) (GetInflationRateResp, error) {
	if err := noParams("getInflationRate", p); err != nil {
		return GetInflationRateResp{}, err
	}
	st := rpcServer.inflation.Load()
	if st == nil || st.slotsPerYear == 0 {
		return GetInflationRateResp{}, errors.New("node has no inflation settings yet")
	}
	if rpcServer.epochSchedule == nil {
		return GetInflationRateResp{}, errors.New("node has no epoch schedule available")
	}
	view, err := rpcServer.currentView(ctx, commitmentConfig{level: bankview.Finalized})
	if err != nil {
		return GetInflationRateResp{}, err
	}
	if view.Features == nil {
		return GetInflationRateResp{}, errors.New("node has no feature set for the bank")
	}
	epoch := rpcServer.epochSchedule.GetEpoch(view.Slot)
	year := rewards.SlotInYearForInflation(rpcServer.epochSchedule, st.slotsPerYear, epoch, view.Features)
	i := st.inflation
	return GetInflationRateResp{Total: i.Total(year), Validator: i.Validator(year), Foundation: i.Foundation(year), Epoch: epoch}, nil
}

type GetBlockCommitmentResp struct {
	Commitment *[32]uint64 `json:"commitment"`
	TotalStake uint64      `json:"totalStake"`
}

// GetBlockCommitment reports the epoch's total stake. Alpenglow has no vote
// lockouts, so there is no per-lockout stake array; Agave returns null there
// too once a slot's commitment is no longer cached.
func (rpcServer *RpcServer) GetBlockCommitment(ctx context.Context, p jsonrpc.RawParams) (GetBlockCommitmentResp, error) {
	params, err := jsonrpc.DecodeParams[[]json.RawMessage](p)
	if err != nil || len(params) != 1 {
		return GetBlockCommitmentResp{}, &InvalidParamsError{Message: "getBlockCommitment requires a slot"}
	}
	var slot uint64
	if json.Unmarshal(params[0], &slot) != nil {
		return GetBlockCommitmentResp{}, &InvalidParamsError{Message: "invalid slot"}
	}
	view, err := rpcServer.currentView(ctx, commitmentConfig{level: bankview.Finalized})
	if err != nil {
		return GetBlockCommitmentResp{}, err
	}
	return GetBlockCommitmentResp{TotalStake: view.TotalEpochStake}, nil
}

// The highest slot turbine has received a data shred for. This node does
// not insert into a blockstore or keep retransmit state of its own, so both
// methods report it.
func (rpcServer *RpcServer) GetMaxShredInsertSlot(ctx context.Context, p jsonrpc.RawParams) (uint64, error) {
	if err := noParams("getMaxShredInsertSlot", p); err != nil {
		return 0, err
	}
	return turbine.NewestDataShredSlot(), nil
}

func (rpcServer *RpcServer) GetMaxRetransmitSlot(ctx context.Context, p jsonrpc.RawParams) (uint64, error) {
	if err := noParams("getMaxRetransmitSlot", p); err != nil {
		return 0, err
	}
	return turbine.NewestDataShredSlot(), nil
}

// NoSnapshotError is Agave's -32008: this node serves no snapshots.
type NoSnapshotError struct{}

func (e *NoSnapshotError) Error() string { return "No snapshot" }

func (e *NoSnapshotError) ToJSONRPCError() (jsonrpc.JSONRPCError, error) {
	return jsonrpc.JSONRPCError{Code: -32008, Message: e.Error()}, nil
}

func (e *NoSnapshotError) FromJSONRPCError(jsonrpc.JSONRPCError) error { return e }

func (rpcServer *RpcServer) GetHighestSnapshotSlot(ctx context.Context, p jsonrpc.RawParams) (struct{}, error) {
	if err := noParams("getHighestSnapshotSlot", p); err != nil {
		return struct{}{}, err
	}
	return struct{}{}, &NoSnapshotError{}
}

// Performance samples: one per minute of rooted banks, as Agave keeps them.
const (
	perfSamplePeriod = 60 * time.Second
	maxPerfSamples   = 720
)

type PerfSample struct {
	Slot                   uint64  `json:"slot"`
	NumTransactions        uint64  `json:"numTransactions"`
	NumNonVoteTransactions *uint64 `json:"numNonVoteTransactions"`
	NumSlots               uint64  `json:"numSlots"`
	SamplePeriodSecs       uint16  `json:"samplePeriodSecs"`
}

type perfSampler struct {
	mu        sync.Mutex
	started   bool
	at        time.Time
	slot, txs uint64
	samples   []PerfSample // newest last
}

func (s *perfSampler) observe(now time.Time, slot, txs uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started || slot < s.slot || txs < s.txs {
		s.started, s.at, s.slot, s.txs = true, now, slot, txs
		return
	}
	if now.Sub(s.at) < perfSamplePeriod {
		return
	}
	period := now.Sub(s.at).Round(time.Second)
	s.samples = append(s.samples, PerfSample{Slot: slot, NumTransactions: txs - s.txs, NumSlots: slot - s.slot, SamplePeriodSecs: uint16(min(period/time.Second, 65535))})
	if len(s.samples) > maxPerfSamples {
		s.samples = s.samples[len(s.samples)-maxPerfSamples:]
	}
	s.at, s.slot, s.txs = now, slot, txs
}

func (s *perfSampler) newest(limit int) []PerfSample {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]PerfSample, 0, min(limit, len(s.samples)))
	for i := len(s.samples) - 1; i >= 0 && len(out) < limit; i-- {
		out = append(out, s.samples[i])
	}
	return out
}

func (rpcServer *RpcServer) GetRecentPerformanceSamples(ctx context.Context, p jsonrpc.RawParams) ([]PerfSample, error) {
	params, err := decodeOptionalParams(p)
	if err != nil || len(params) > 1 {
		return nil, &InvalidParamsError{Message: "getRecentPerformanceSamples accepts an optional limit"}
	}
	limit := uint64(maxPerfSamples)
	if len(params) == 1 && params[0] != nil {
		if limit, err = rpcUint64(params[0], "limit"); err != nil {
			return nil, err
		}
		if limit > maxPerfSamples {
			return nil, &InvalidParamsError{Message: fmt.Sprintf("Invalid limit; max %d", maxPerfSamples)}
		}
	}
	return rpcServer.perfSamples.newest(int(limit)), nil
}

// noParams accepts an absent or empty parameter list.
func noParams(method string, p jsonrpc.RawParams) error {
	params, err := decodeOptionalParams(p)
	if err != nil || len(params) != 0 {
		return &InvalidParamsError{Message: method + " takes no parameters"}
	}
	return nil
}
