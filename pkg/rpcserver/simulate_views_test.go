package rpcserver

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/stretchr/testify/require"
)

// simBank is a server whose finalized bank is slot 10 and processed bank is
// slot 20. The payer holds 10 SOL at slot 10 and 5 SOL at slot 20.
type simBank struct {
	server *RpcServer
	accts  *viewAccounts
	payer  solana.PrivateKey
	dest   solana.PublicKey
	older  solana.Hash // in both banks' recent-blockhashes sysvar
}

func newSimBank(t *testing.T) *simBank {
	t.Helper()
	server, _, accts := newViewServer(t, sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000, LeaderScheduleSlotOffset: 432000})
	payer := solana.NewWallet().PrivateKey
	dest := solana.NewWallet().PublicKey()
	older := solana.Hash{0xaa}

	server.acctsDb = newRPCAccountsDB(t) // program cache; accounts come from the views
	for _, acct := range simBankSysvars(t, 10, older) {
		accts.durable[acct.Key] = acct
	}
	accts.ws.Add(20, simBankSysvars(t, 20, older))
	accts.durable[solana.SystemProgramID] = &accounts.Account{
		Key: solana.SystemProgramID, Lamports: 1, Owner: addresses.NativeLoaderAddr, Executable: true, RentEpoch: math.MaxUint64,
	}
	accts.durable[payer.PublicKey()] = &accounts.Account{
		Key: payer.PublicKey(), Lamports: 10_000_000_000, Owner: [32]byte(solana.SystemProgramID), RentEpoch: math.MaxUint64,
	}
	accts.durable[dest] = &accounts.Account{
		Key: dest, Lamports: 1_000_000_000, Owner: [32]byte(solana.SystemProgramID), RentEpoch: math.MaxUint64,
	}
	accts.ws.Add(20, []*accounts.Account{{
		Key: payer.PublicKey(), Lamports: 5_000_000_000, Owner: [32]byte(solana.SystemProgramID), RentEpoch: math.MaxUint64,
	}})

	b := &simBank{server: server, accts: accts, payer: payer, dest: dest, older: older}
	b.publish(t)
	return b
}

// publish makes slot 10 finalized and slot 20 processed.
func (b *simBank) publish(t *testing.T) {
	feats := features.NewFeaturesDefault()
	feats.EnableFeature(features.FormalizeLoadedTransactionDataSize, 0)
	views := b.server.BankViews()
	for _, slot := range []uint64{10, 20} {
		v := testView(t, slot, slot)
		v.Features = feats
		v.LamportsPerSignature = 5000
		v.FeeRateGovernor = sealevel.FeeRateGovernor{LamportsPerSignature: 5000, PrevLamportsPerSignature: 5000}
		v.LatestEvictedBlockhash = solana.Hash{0xee}
		v.EpochStakes, v.TotalEpochStake = map[solana.PublicKey]uint64{{1}: 9}, 9
		views.PublishProcessed(v)
	}
	views.MarkFinalized(10)
}

// simBankSysvars is a bank's complete sysvar set: its own blockhash heads the
// recent-blockhashes queue, followed by older.
func simBankSysvars(t *testing.T, slot uint64, older solana.Hash) []*accounts.Account {
	t.Helper()
	clock := sealevel.SysvarClock{Slot: slot}
	rent := sealevel.NewDefaultRentSysvar()
	recent := sealevel.SysvarRecentBlockhashes{
		{Blockhash: [32]byte(solana.Hash{byte(slot)}), FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: 5000}},
		{Blockhash: older, FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: 5000}},
	}
	history := sealevel.SysvarSlotHistory{NextSlot: slot + 1}
	schedule := make([]byte, 33)
	binary.LittleEndian.PutUint64(schedule[:8], 432000)
	sysvar := func(key [32]byte, data []byte) *accounts.Account {
		return &accounts.Account{Key: solana.PublicKey(key), Lamports: 1, Data: data}
	}
	return []*accounts.Account{
		sysvar(sealevel.SysvarClockAddr, clock.MustMarshal()),
		sysvar(sealevel.SysvarRentAddr, rent.MustMarshal()),
		sysvar(sealevel.SysvarEpochScheduleAddr, schedule),
		sysvar(sealevel.SysvarSlotHashesAddr, make([]byte, 8)),
		sysvar(sealevel.SysvarStakeHistoryAddr, make([]byte, 8)),
		sysvar(sealevel.SysvarLastRestartSlotAddr, make([]byte, 8)),
		sysvar(sealevel.SysvarRecentBlockHashesAddr, recent.MustMarshal()),
		sysvar(sealevel.SysvarSlotHistoryAddr, history.MustMarshal()),
	}
}

func (b *simBank) transfer(t *testing.T, blockhash solana.Hash) string {
	t.Helper()
	tx, err := solana.NewTransaction(
		[]solana.Instruction{system.NewTransferInstruction(1_000, b.payer.PublicKey(), b.dest).Build()},
		blockhash, solana.TransactionPayer(b.payer.PublicKey()))
	require.NoError(t, err)
	_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
		if key.Equals(b.payer.PublicKey()) {
			return &b.payer
		}
		return nil
	})
	require.NoError(t, err)
	wire, err := tx.MarshalBinary()
	require.NoError(t, err)
	return base64.StdEncoding.EncodeToString(wire)
}

func (b *simBank) simulate(t *testing.T, tx string, conf map[string]any) (SimulateTransactionResp, error) {
	t.Helper()
	conf["encoding"] = "base64"
	return b.server.SimulateTransaction(context.Background(), mustRawParams(t, []interface{}{tx, conf}))
}

func TestSimulateTransactionUsesTheRequestedBank(t *testing.T) {
	b := newSimBank(t)
	tx := b.transfer(t, b.older)
	for _, tc := range []struct {
		commitment string
		slot       uint64
		payer      uint64
	}{
		{"processed", 20, 5_000_000_000},
		{"finalized", 10, 10_000_000_000},
		{"confirmed", 10, 10_000_000_000}, // no notarized bank yet: served from finalized
	} {
		resp, err := b.simulate(t, tx, map[string]any{
			"commitment": tc.commitment,
			"sigVerify":  true,
			"accounts":   map[string]any{"addresses": []string{b.payer.PublicKey().String()}, "encoding": "base64"},
		})
		require.NoError(t, err, tc.commitment)
		require.Nil(t, resp.Value.Err, tc.commitment)
		require.Equal(t, tc.slot, resp.Context.Slot, tc.commitment)
		require.Equal(t, tc.payer, (*resp.Value.PreBalances)[0], tc.commitment)
		require.Equal(t, tc.payer-1_000-5_000, (*resp.Value.PostBalances)[0], tc.commitment)
		require.Equal(t, tc.payer-1_000-5_000, (*resp.Value.Accounts)[0].Lamports, tc.commitment)
		require.Equal(t, []string{
			"Program 11111111111111111111111111111111 invoke [1]",
			"Program 11111111111111111111111111111111 success",
		}, *resp.Value.Logs, tc.commitment)
	}

	// Simulation must not change the bank it ran against.
	acct, _ := b.accts.ws.LookupAt([32]byte(b.payer.PublicKey()), 20)
	require.EqualValues(t, 5_000_000_000, acct.Lamports)
	require.EqualValues(t, 10_000_000_000, b.accts.durable[b.payer.PublicKey()].Lamports)
}

func TestSimulateTransactionBlockhashWindowIsTheBanks(t *testing.T) {
	b := newSimBank(t)

	// A frozen bank accepts its own blockhash, as Agave's does.
	resp, err := b.simulate(t, b.transfer(t, solana.Hash{20}), map[string]any{"commitment": "processed"})
	require.NoError(t, err)
	require.Nil(t, resp.Value.Err)

	// Slot 20's blockhash is not valid in the finalized bank at slot 10.
	resp, err = b.simulate(t, b.transfer(t, solana.Hash{20}), map[string]any{"commitment": "finalized"})
	require.NoError(t, err)
	txErr, ok := resp.Value.Err.(*replay.TransactionError)
	require.True(t, ok, "err %#v", resp.Value.Err)
	require.Equal(t, replay.TransactionErrorBlockhashNotFound, txErr.ErrorType)
	require.EqualValues(t, 10, resp.Context.Slot)

	// replaceRecentBlockhash substitutes the selected bank's blockhash.
	resp, err = b.simulate(t, b.transfer(t, solana.Hash{0xbb}), map[string]any{"commitment": "finalized", "replaceRecentBlockhash": true})
	require.NoError(t, err)
	require.Nil(t, resp.Value.Err)
	require.Equal(t, solana.Hash{10}.String(), resp.Value.ReplacementBlockhash.Blockhash)
	require.EqualValues(t, 10+maxProcessingAge, resp.Value.ReplacementBlockhash.LastValidBlockHeight)
}

func TestSimulateTransactionMinContextSlot(t *testing.T) {
	b := newSimBank(t)
	_, err := b.simulate(t, b.transfer(t, b.older), map[string]any{"commitment": "processed", "minContextSlot": 21})
	var notReached *MinContextSlotNotReachedError
	require.ErrorAs(t, err, &notReached)
	require.EqualValues(t, 20, notReached.ContextSlot)

	resp, err := b.simulate(t, b.transfer(t, b.older), map[string]any{"commitment": "processed", "minContextSlot": 20})
	require.NoError(t, err)
	require.EqualValues(t, 20, resp.Context.Slot)
}

func TestSendTransactionPreflightUsesPreflightCommitment(t *testing.T) {
	b := newSimBank(t)
	// The payer is created at slot 20, so the finalized bank cannot pay.
	fresh := solana.NewWallet().PrivateKey
	b.accts.ws.Add(20, []*accounts.Account{{
		Key: fresh.PublicKey(), Lamports: 1_000_000_000, Owner: [32]byte(solana.SystemProgramID), RentEpoch: math.MaxUint64,
	}})
	b.payer = fresh
	tx := b.transfer(t, b.older)

	send := func(conf map[string]any) error {
		conf["encoding"] = "base64"
		_, err := b.server.SendTransaction(context.Background(), mustRawParams(t, []interface{}{tx, conf}))
		return err
	}

	var preflight *SendTransactionPreflightFailureError
	require.ErrorAs(t, send(map[string]any{}), &preflight, "default preflight commitment is finalized")
	require.ErrorAs(t, send(map[string]any{"preflightCommitment": "finalized"}), &preflight)

	// At processed the preflight passes; the send then fails only because
	// this test server has no leaders to forward to.
	err := send(map[string]any{"preflightCommitment": "processed"})
	require.Error(t, err)
	require.False(t, isPreflightFailure(err), "unexpected preflight failure: %v", err)
	require.Contains(t, err.Error(), "leader")

	// minContextSlot is checked against the preflight bank, even when
	// preflight is skipped.
	var notReached *MinContextSlotNotReachedError
	require.ErrorAs(t, send(map[string]any{"skipPreflight": true, "preflightCommitment": "finalized", "minContextSlot": 11}), &notReached)
	require.EqualValues(t, 10, notReached.ContextSlot)
}

// forkSwitchAccounts runs a fork switch the first time the payer is read,
// while a simulation is in progress.
type forkSwitchAccounts struct {
	*viewAccounts
	payer    [32]byte
	onRead   func()
	switched int
	reads    int
}

func (f *forkSwitchAccounts) LookupAt(pk [32]byte, maxSlot uint64) (*accounts.Account, bool) {
	if pk == f.payer {
		f.reads++
		if f.switched == 0 && f.onRead != nil {
			f.switched++
			f.onRead()
		}
	}
	return f.viewAccounts.LookupAt(pk, maxSlot)
}

func TestSimulateTransactionRetriesAcrossAForkSwitch(t *testing.T) {
	b := newSimBank(t)
	views := b.server.BankViews()
	tx := b.transfer(t, b.older)

	// Payer reads in one undisturbed simulation.
	plain := &forkSwitchAccounts{viewAccounts: b.accts, payer: [32]byte(b.payer.PublicKey())}
	views.Reset(plain)
	b.publish(t)
	_, err := b.simulate(t, tx, map[string]any{"commitment": "processed"})
	require.NoError(t, err)
	require.Positive(t, plain.reads)

	hooked := &forkSwitchAccounts{viewAccounts: b.accts, payer: [32]byte(b.payer.PublicKey())}
	hooked.onRead = func() {
		// Slot 30 is replayed and then abandoned: the working set rewrites
		// its suffix and the surviving bank is republished.
		b.accts.ws.Add(30, []*accounts.Account{{Key: b.payer.PublicKey(), Lamports: 1, Owner: [32]byte(solana.SystemProgramID)}})
		b.accts.ws.EvictFrom(30)
		views.Abandon(30)
	}
	views.Reset(hooked)
	b.publish(t)

	resp, err := b.simulate(t, tx, map[string]any{"commitment": "processed"})
	require.NoError(t, err)
	require.Equal(t, 1, hooked.switched)
	require.Equal(t, 2*plain.reads, hooked.reads, "the overlapped attempt is discarded and run again")
	require.EqualValues(t, 20, resp.Context.Slot)
	require.EqualValues(t, 5_000_000_000, (*resp.Value.PreBalances)[0])
}

func isPreflightFailure(err error) bool {
	_, ok := err.(*SendTransactionPreflightFailureError)
	return ok
}

// newSanitizeTestServer serves one finalized bank with feats, for tests that
// fail before any account is read.
func newSanitizeTestServer(t *testing.T, feats *features.Features) *RpcServer {
	t.Helper()
	server, _, _ := newViewServer(t, sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000, LeaderScheduleSlotOffset: 432000})
	v := testView(t, 123, 123)
	v.Features = feats
	server.BankViews().PublishProcessed(v)
	server.BankViews().MarkFinalized(123)
	return server
}
