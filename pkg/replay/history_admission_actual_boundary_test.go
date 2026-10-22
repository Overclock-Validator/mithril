package replay

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	blockpkg "github.com/Overclock-Validator/mithril/pkg/block"
	consensusengine "github.com/Overclock-Validator/mithril/pkg/consensus"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/Overclock-Validator/mithril/pkg/transactionhistory"
	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// This context observes the real loop's cancellation check. With no trailing
// verifier, ReplayBlocks does not use ctx before the loop. A candidate wait
// evaluates Done before that check; poll reaches Err first. Only the former
// releases the held write. The real publication callback releases either arm
// after execution, so the negative control cannot deadlock or change execution.
type actualAdmissionContext struct {
	context.Context
	checked atomic.Bool
	release func()
}

func (c *actualAdmissionContext) Done() <-chan struct{} {
	if !c.checked.Load() {
		c.release()
	}
	return c.Context.Done()
}
func (c *actualAdmissionContext) Err() error {
	c.checked.Store(true)
	return c.Context.Err()
}

type actualAdmissionPublisher struct {
	slot *sealevel.SlotCtx
	done func()
}

func (p *actualAdmissionPublisher) SetSlotCtx(slot *sealevel.SlotCtx) {
	p.slot = slot
	p.done()
}
func (*actualAdmissionPublisher) SetRootedBankState(uint64, uint64, uint64) {}

type actualAdmissionEngine struct{ historySessionEngine }

func (actualAdmissionEngine) ObserveBlock(context.Context, consensusengine.BlockObservation) error {
	return nil
}
func (actualAdmissionEngine) OnReplayResult(context.Context, consensusengine.SlotReplayResult) error {
	return nil
}

type actualAdmissionOutcome struct {
	bank                       []byte
	accounts                   map[solana.PublicKey]*accounts.Account
	signatures, compute, burnt uint64
}

func actualAdmissionRun(t *testing.T, held bool, afterReplay ...func(context.Context)) (actualAdmissionOutcome, transactionhistory.Block, error) {
	t.Helper()
	require.LessOrEqual(t, len(afterReplay), 1)
	env := newLifecycleEnv(t)
	recent, ok := env.parent.RecentBlockhashes()
	require.True(t, ok)
	recent = append(sealevel.SysvarRecentBlockhashes{{Blockhash: solana.Hash{3}, FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: 5000}}}, recent...)
	var err error
	env.parent, err = env.parent.WithAccounts(&accounts.Account{Key: sealevel.SysvarRecentBlockHashesAddr, Lamports: 1, Data: recent.MustMarshal()})
	require.NoError(t, err)
	vote := solana.PublicKey{0xc1}
	db := epochBoundaryStakeDB(t, 7, []sealevel.Delegation{{
		VoterPubkey: vote, StakeLamports: 1000, ActivationEpoch: math.MaxUint64, DeactivationEpoch: math.MaxUint64,
	}})
	dir := filepath.Dir(db.AcctsDir)
	var accts []*accounts.Account
	require.NoError(t, env.parent.RangeAccountViews(func(_ solana.PublicKey, a *accounts.Account) error {
		accts = append(accts, a.Clone())
		return nil
	}))
	for _, a := range env.durable.AllAccounts() {
		accts = append(accts, a.Clone())
	}
	activation := uint64(0)
	featureData, err := features.MarshalFeatureAcct(&features.FeatureAcct{ActivatedAt: &activation})
	require.NoError(t, err)
	for _, gate := range []features.FeatureGate{features.Alpenglow, features.SkipRentRewrites, features.FormalizeLoadedTransactionDataSize} {
		accts = append(accts, &accounts.Account{Key: gate.Address, Owner: addresses.FeatureAddr, Lamports: 1, Data: featureData})
	}
	for key, a := range validAlpenglowStartupAccounts(t, 0) {
		if key != solana.PublicKey(features.Alpenglow.Address) {
			accts = append(accts, a)
		}
	}
	var rewardsData bytes.Buffer
	require.NoError(t, (&sealevel.SysvarEpochRewards{}).MarshalWithEncoder(bin.NewBinEncoder(&rewardsData)))
	accts = append(accts, &accounts.Account{Key: sealevel.SysvarEpochRewardsAddr, Lamports: 1, Data: rewardsData.Bytes()})
	stored := make(chan struct{})
	require.NoError(t, db.StoreAccounts(accts, 7, func() { close(stored) }))
	<-stored
	// Seed the disposable snapshot before enabling replay's rooted-only writes.
	db.RootedDurable = true

	// Complete genesis-through-parent rooted Agave status-cache coverage.
	seed := make([]byte, 8+8*17)
	binary.LittleEndian.PutUint64(seed, 8)
	for slot := uint64(0); slot <= 7; slot++ {
		offset := 8 + int(slot)*17
		binary.LittleEndian.PutUint64(seed[offset:], slot)
		seed[offset+8] = 1
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, txstatus.SnapshotSeedFileName), seed, 0600))
	seeds := make(map[uint64]string)
	for _, epoch := range []uint64{0, 1} {
		global.ClearEpochStakes(epoch)
		t.Cleanup(func() { global.ClearEpochStakes(epoch) })
		raw, e := json.Marshal(epochstakes.PersistedEpochStakes{Epoch: epoch, TotalStake: 1000,
			Stakes:    map[string]uint64{vote.String(): 1000},
			VoteAccts: map[string]*epochstakes.VoteAccountJSON{vote.String(): {NodePubkey: vote.String(), Owner: solana.PublicKey(addresses.VoteProgramAddr).String()}}})
		require.NoError(t, e)
		seeds[epoch] = string(raw)
	}
	ms := &state.MithrilState{ManifestParentSlot: 7, ManifestParentBankhash: solana.Hash{0x88}.String(),
		ManifestCapitalization: 1_000_000_000, ManifestSlotsPerYear: 78_892_314.984, ManifestBlockHeight: 7,
		ManifestEvictedBlockhash: solana.Hash{0x61}.String(), ManifestEpochStakes: seeds,
		ManifestLamportsPerSignature: 5000, ManifestFeeRateGovernor: &state.ManifestFeeRateGovernorSeed{
			TargetLamportsPerSignature: 5000, TargetSignaturesPerSlot: 20000, MinLamportsPerSignature: 5000, MaxLamportsPerSignature: 5000, BurnPercent: 50},
		ManifestRecentBlockhashes: []state.BlockhashEntry{{}},
	}
	ms.ManifestRecentBlockhashes[0].Blockhash = env.parentRecentHash(t).String()
	block := env.block(transferTransactions(t, 1, 1))
	block.Features = nil
	block.Leader = vote
	block.FromLiveStream = false
	raw, err := json.Marshal(block)
	require.NoError(t, err)
	var decoded blockpkg.Block
	require.NoError(t, json.Unmarshal(raw, &decoded), "BlockDir fixture must decode")
	require.NoError(t, decoded.FixupTxVersions(), "BlockDir fixture must restore transaction versions")
	blockDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(blockDir, "8.json"), raw, 0600))
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if request.Method != "getSlot" {
			t.Errorf("unexpected RPC %s", request.Method)
			http.Error(w, "unexpected method", 400)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": uint64(100)})
	}))
	defer endpoint.Close()
	store, err := transactionhistory.Open(t.TempDir(), [32]byte{1}, transactionhistory.DefaultLimits())
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	session, err := NewTransactionHistorySession(store, "verifying")
	require.NoError(t, err)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer func() { unblock(); requireHistorySessionStopped(t, session, nil) }()
	if held {
		original := session.write
		entered := make(chan struct{})
		session.write = func(block transactionhistory.Block) error {
			if block.ID.Slot == 6 {
				close(entered)
				<-release
			}
			return original(block)
		}
		capture := session.begin()
		capture.Record = &blockMetadata{Slot: 6, Blockhash: [32]byte{6}}
		session.finish(capture, true)
		<-entered
	}
	base, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx := &actualAdmissionContext{Context: base, release: unblock}
	published := &actualAdmissionPublisher{done: func() { unblock(); cancel() }}
	if len(afterReplay) != 0 {
		published.done = unblock
	}
	result := ReplayBlocks(ctx, db, dir, ms, nil, 8, 9, []string{endpoint.URL},
		"", "", "", "", "", 0, "", nil, blockDir, 1, true, false, false, nil, nil, published,
		&BlockFetchOpts{MaxInflight: 1, MaxRPS: 1},
		&ConsensusOpts{Alpenglow: true, Engine: actualAdmissionEngine{}, TransactionHistory: session}, nil)
	require.NoError(t, result.Error)
	if len(afterReplay) == 0 {
		require.True(t, result.WasCancelled)
	} else {
		afterReplay[0](base)
	}
	require.NotNil(t, published.slot)
	outcome := actualAdmissionOutcome{bank: append([]byte(nil), published.slot.FinalBankhash...),
		accounts: make(map[solana.PublicKey]*accounts.Account), signatures: published.slot.NumSignatures,
		compute: published.slot.TotalComputeUnitsConsumed, burnt: published.slot.LamportsBurnt}
	for key := range published.slot.ModifiedAccts {
		a, e := published.slot.Accounts.GetAccount((*[32]byte)(&key))
		require.NoError(t, e)
		outcome.accounts[key] = a.Clone()
	}
	requireHistorySessionStopped(t, session, nil)
	view, err := store.Snapshot()
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	receipt, err := view.Block(transactionhistory.BlockID{Slot: 8, Blockhash: solana.Hash(block.Blockhash)})
	return outcome, receipt, err
}

func (env *lifecycleEnv) parentRecentHash(t *testing.T) solana.Hash {
	t.Helper()
	recent, ok := env.parent.RecentBlockhashes()
	require.True(t, ok)
	return recent[0].Blockhash
}

func TestHistoryAdmissionActualReplayBoundary(t *testing.T) {
	// Replay startup owns process-wide caches; isolate it like the existing
	// epoch-startup tests, including the same race-instrumented binary.
	const childFlag = "MITHRIL_TEST_HISTORY_ADMISSION_CHILD"
	if os.Getenv(childFlag) != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestHistoryAdmissionActualReplayBoundary$", "-test.timeout=40s", "-test.v")
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
		cmd.WaitDelay = 5 * time.Second
		for _, key := range []string{"PATH", "HOME", "TMPDIR", "GOTMPDIR", "GOCACHE", "GOMODCACHE", "GOPATH", "GOPROXY", "GOSUMDB", "GOENV", "GOWORK", "GOTOOLCHAIN", "CGO_ENABLED", "GOMAXPROCS", "GOMEMLIMIT"} {
			if value, ok := os.LookupEnv(key); ok {
				cmd.Env = append(cmd.Env, key+"="+value)
			}
		}
		cmd.Env = append(cmd.Env, childFlag+"=1")
		output, err := cmd.CombinedOutput()
		t.Logf("isolated replay output:\n%s", output)
		require.NoError(t, err)
		return
	}
	// The concrete database and normal block source are real; certificate/fold
	// readiness is outside this local admission test, as in lifecycle fixtures.
	cfg := mlog.DefaultConfig()
	// Admission testing needs no file logger or background log flusher.
	cfg.Dir = ""
	require.NoError(t, mlog.Initialize(cfg, "history-admission-boundary"))
	defer mlog.Shutdown()
	previousVerifier := TrailingVerifierCfg
	TrailingVerifierCfg.Enabled = false
	defer func() { TrailingVerifierCfg = previousVerifier }()
	var want actualAdmissionOutcome
	var wantReceipt transactionhistory.Block
	require.True(t, t.Run("reference", func(t *testing.T) {
		var err error
		want, wantReceipt, err = actualAdmissionRun(t, false)
		require.NoError(t, err)
		require.Len(t, wantReceipt.Records, 1)
		require.Greater(t, want.compute, uint64(0), "fixture transaction must execute")
	}))
	t.Run("held_write", func(t *testing.T) {
		got, gotReceipt, err := actualAdmissionRun(t, true)
		require.Equal(t, want, got, "both arms must execute the same bank and account delta")
		require.NoError(t, err, "actual ReplayBlocks boundary must retain the next receipt")
		require.Equal(t, wantReceipt, gotReceipt)
	})
}
