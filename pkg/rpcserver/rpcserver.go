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
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/epochrewards"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/filecoin-project/go-jsonrpc"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

// rpcHandler exposes only public RPC methods to go-jsonrpc's reflection-based
// registration. Internal setters and lifecycle hooks remain Go-only.
type rpcHandler struct {
	rpcMethods
}

type rpcMethods interface {
	GetAccountInfo(context.Context, jsonrpc.RawParams) (GetAccountInfoResp, error)
	GetBalance(context.Context, jsonrpc.RawParams) (GetBalanceResp, error)
	GetBankHash(context.Context, jsonrpc.RawParams) (string, error)
	GetBlockHeight(context.Context, jsonrpc.RawParams) (uint64, error)
	GetBlockProduction(context.Context, jsonrpc.RawParams) (GetBlockProductionResp, error)
	GetEpochInfo(context.Context, jsonrpc.RawParams) (GetEpochInfoResp, error)
	GetGenesisHash(context.Context, jsonrpc.RawParams) (string, error)
	GetInflationReward(context.Context, jsonrpc.RawParams) ([]*InflationRewardResp, error)
	GetHealth(context.Context, jsonrpc.RawParams) (string, error)
	GetIdentity(context.Context, jsonrpc.RawParams) (GetIdentityResp, error)
	GetLatestBlockhash(context.Context, jsonrpc.RawParams) (GetLatestBlockhashResp, error)
	GetLeaderSchedule(context.Context, jsonrpc.RawParams) (map[string][]uint64, error)
	GetSlot(context.Context, jsonrpc.RawParams) (uint64, error)
	GetVoteAccounts(context.Context, jsonrpc.RawParams) (GetVoteAccountsResp, error)
	GetVersion(context.Context, jsonrpc.RawParams) (GetVersionResp, error)
	SendTransaction(context.Context, jsonrpc.RawParams) (string, error)
	SimulateTransaction(context.Context, jsonrpc.RawParams) (SimulateTransactionResp, error)
}

type RpcServer struct {
	historyRecoveryMu      sync.RWMutex
	historyRecoveryPending bool

	isReady       bool
	rpcService    *jsonrpc.RPCServer
	serv          *httptest.Server
	listener      net.Listener
	acctsDb       *accountsdb.AccountsDb
	epochSchedule *sealevel.SysvarEpochSchedule
	slotCtx       *sealevel.SlotCtx
	slotCtxMu     sync.RWMutex
	healthSlot    func() (uint64, bool)
	healthReady   bool
	genesisHash   string
	epochRewards  *epochrewards.Store
	rootedBank    atomic.Pointer[rootedBankState]
	identity      string

	identitySource func() string

	stakeCacheOnce  sync.Once
	stakeCache      rootedStakeCache
	voteRecordsOnce sync.Once
	voteRecords     rootedVoteRecordCache

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

// rootedBankState identifies the durable AccountsDB view served by state RPCs.
// Publishing it only after a successful fold keeps response context and account
// values on the same finalized bank.
type rootedBankState struct {
	Slot             uint64
	BlockHeight      uint64
	TransactionCount uint64
}

const maxQuietMethodProbeBody = 64 << 10

var supportedRPCMethods = map[string]struct{}{
	"getAccountInfo":      {},
	"getBalance":          {},
	"getBankHash":         {},
	"getBlockProduction":  {},
	"getBlockHeight":      {},
	"getEpochInfo":        {},
	"getGenesisHash":      {},
	"getInflationReward":  {},
	"getHealth":           {},
	"getIdentity":         {},
	"getLatestBlockhash":  {},
	"getLeaderSchedule":   {},
	"getSlot":             {},
	"getVoteAccounts":     {},
	"getVersion":          {},
	"sendTransaction":     {},
	"simulateTransaction": {},
}

// EnableEpochRewards attaches durable Alpenglow reward history to the RPC server.
func (rpcServer *RpcServer) EnableEpochRewards(dir string, retentionSlots, rootedSlot uint64) error {
	store, err := epochrewards.Open(dir, retentionSlots)
	if err != nil {
		return err
	}
	if err := store.SetRooted(rootedSlot); err != nil {
		return err
	}
	rpcServer.epochRewards = store
	return nil
}

// RecordEpochRewards records rewards from a successfully replayed block.
func (rpcServer *RpcServer) RecordEpochRewards(block *b.Block) error {
	if rpcServer == nil || rpcServer.epochRewards == nil {
		return nil
	}
	return rpcServer.epochRewards.RecordBlock(block)
}

// RecordEpochRewardStatus records coverage from the successfully replayed bank.
func (rpcServer *RpcServer) RecordEpochRewardStatus(block *b.Block, bank *sealevel.BankSysvars, partitioned bool) {
	if rpcServer == nil || rpcServer.epochRewards == nil || rpcServer.epochSchedule == nil || block == nil || block.Epoch == 0 {
		return
	}
	if block.ParentSlot >= block.Slot || rpcServer.epochSchedule.GetEpoch(block.Slot) != block.Epoch {
		return
	}
	boundary := rpcServer.epochSchedule.GetEpoch(block.ParentSlot) < block.Epoch
	complete := false
	if bank != nil && bank.Slot() == block.Slot {
		if partitioned {
			rewards, ok := bank.EpochRewards()
			complete = ok && !rewards.Active
		} else {
			complete = boundary
		}
	}
	rpcServer.epochRewards.RecordCoverage(block.Epoch-1, block.Slot, boundary, complete)
	if partitioned && bank != nil && bank.Slot() == block.Slot && block.BlockHeight != 0 {
		if rewards, ok := bank.EpochRewards(); ok && rewards.Active {
			rpcServer.epochRewards.RecordDistribution(block.Epoch-1, block.Slot, epochrewards.Distribution{
				BlockHeight: block.BlockHeight, StartingBlockHeight: rewards.DistributionStartingBlockHeight,
				NumPartitions: rewards.NumPartitions, ParentBlockhash: rewards.ParentBlockhash,
			})
		}
	}
}

// PrepareEpochRewards stages rewards with the matching durable fold.
func (rpcServer *RpcServer) PrepareEpochRewards(through uint64) error {
	if rpcServer == nil || rpcServer.epochRewards == nil {
		return nil
	}
	rpcServer.historyRecoveryMu.Lock()
	defer rpcServer.historyRecoveryMu.Unlock()
	return rpcServer.epochRewards.Prepare(through)
}

// PublishEpochRewards exposes a prefix that replay has verified as canonical.
// It is independent of the durable account checkpoint and is not an RPC method.
func (rpcServer *RpcServer) PublishEpochRewards(slot uint64) {
	if rpcServer == nil || rpcServer.epochRewards == nil {
		return
	}
	rpcServer.historyRecoveryMu.Lock()
	defer rpcServer.historyRecoveryMu.Unlock()
	if !rpcServer.historyRecoveryPending {
		rpcServer.epochRewards.Publish(slot)
	}
}

// ResetEpochRewardView withdraws process-local publication during replay recovery.
func (rpcServer *RpcServer) ResetEpochRewardView() {
	if rpcServer == nil || rpcServer.epochRewards == nil {
		return
	}
	rpcServer.historyRecoveryMu.Lock()
	defer rpcServer.historyRecoveryMu.Unlock()
	rpcServer.epochRewards.ResetPublished()
}

// SetRootedEpochRewardsSlot sets the durable reward watermark.
func (rpcServer *RpcServer) SetRootedEpochRewardsSlot(slot uint64) error {
	if rpcServer == nil || rpcServer.epochRewards == nil {
		return nil
	}
	rpcServer.historyRecoveryMu.Lock()
	defer rpcServer.historyRecoveryMu.Unlock()
	return rpcServer.epochRewards.SetRooted(slot)
}

// RewindEpochRewards drops speculative rewards from a discarded fork suffix.
func (rpcServer *RpcServer) RewindEpochRewards(fromSlot uint64) error {
	if rpcServer == nil || rpcServer.epochRewards == nil {
		return nil
	}
	rpcServer.historyRecoveryMu.Lock()
	defer rpcServer.historyRecoveryMu.Unlock()
	return rpcServer.epochRewards.Rewind(fromSlot)
}

// SetHistoryRecoveryPending blocks history reads during a durable account rewind.
// Failed recovery leaves the gate closed without deleting retained records.
func (rpcServer *RpcServer) SetHistoryRecoveryPending(pending bool) {
	if rpcServer == nil {
		return
	}
	rpcServer.historyRecoveryMu.Lock()
	defer rpcServer.historyRecoveryMu.Unlock()
	rpcServer.historyRecoveryPending = pending
	if pending && rpcServer.epochRewards != nil {
		rpcServer.epochRewards.ResetPublished()
	}
}

func (rpcServer *RpcServer) SetRootedBankState(slot, blockHeight, transactionCount uint64) {
	rpcServer.rootedBank.Store(&rootedBankState{
		Slot:             slot,
		BlockHeight:      blockHeight,
		TransactionCount: transactionCount,
	})
}

func (rpcServer *RpcServer) getRootedBankState() (rootedBankState, bool) {
	rooted := rpcServer.rootedBank.Load()
	if rooted == nil {
		return rootedBankState{}, false
	}
	return *rooted, true
}

func (rpcServer *RpcServer) readRootedAccount(ctx context.Context, pubkey solana.PublicKey) (rootedBankState, *accounts.Account, error) {
	rooted, accountSet, err := rpcServer.readRootedAccounts(ctx, []solana.PublicKey{pubkey})
	if err != nil {
		return rooted, nil, err
	}
	// Batch reads use zero-lamport placeholders for absent/deleted accounts.
	// Translate that loader representation into absence at the RPC boundary.
	if len(accountSet) != 1 || accountSet[0] == nil || accountSet[0].Lamports == 0 {
		return rooted, nil, accountsdb.ErrNoAccount
	}
	return rooted, accountSet[0], nil
}

func (rpcServer *RpcServer) readRootedAccounts(ctx context.Context, pubkeys []solana.PublicKey) (rootedBankState, []*accounts.Account, error) {
	for {
		bank := rpcServer.rootedBank.Load()
		if bank == nil {
			return rootedBankState{}, nil, fmt.Errorf("node has no rooted bank available")
		}
		rooted := *bank
		if rpcServer.acctsDb == nil {
			return rootedBankState{}, nil, fmt.Errorf("node has no accounts database available")
		}
		version, stable := rpcServer.acctsDb.CommittedAccountVersion()
		if !stable || rpcServer.rootedPublicationPending(rooted.Slot) {
			if err := waitForRootedPublication(ctx); err != nil {
				return rooted, nil, err
			}
			continue
		}
		accounts, stats, err := rpcServer.acctsDb.GetAccountsBatchSharedWithStats(ctx, rooted.Slot, pubkeys)
		latest, stable := rpcServer.acctsDb.CommittedAccountVersion()
		if !stable || latest != version || rpcServer.rootedBank.Load() != bank || stats.PendingFoldHits > 0 ||
			rpcServer.rootedPublicationPending(rooted.Slot) {
			if err := waitForRootedPublication(ctx); err != nil {
				return rooted, nil, err
			}
			continue
		}
		return rooted, accounts, err
	}
}

func (rpcServer *RpcServer) rootedPublicationPending(rootedSlot uint64) bool {
	if !rpcServer.acctsDb.RootedDurable {
		committed := rpcServer.acctsDb.CommittedAccountSlot()
		return committed != 0 && committed != rootedSlot
	}
	durableThrough := rpcServer.acctsDb.DurableThrough()
	// Zero means the snapshot baseline is active and no fold has committed yet.
	return durableThrough != 0 && durableThrough != rootedSlot
}

func waitForRootedPublication(ctx context.Context) error {
	timer := time.NewTimer(time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
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

	rpcServer.rpcService.Register("MithrilRpc", rpcHandler{rpcServer})
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
	rpcServer.healthReady = slotCtx != nil
	rpcServer.slotCtxMu.Unlock()
}

func (rpcServer *RpcServer) SetIdentity(identity string) {
	rpcServer.slotCtxMu.Lock()
	rpcServer.identity = identity
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
