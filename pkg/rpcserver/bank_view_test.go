package rpcserver

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/bankview"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	solanarpc "github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/require"
)

// viewAccounts is a WorkingSet over a fixed durable map.
type viewAccounts struct {
	ws      *accounts.WorkingSet
	durable map[solana.PublicKey]*accounts.Account
}

func (v *viewAccounts) Generation() uint64 { return v.ws.Generation() }
func (v *viewAccounts) LookupAt(pk [32]byte, maxSlot uint64) (*accounts.Account, bool) {
	return v.ws.LookupAt(pk, maxSlot)
}
func (v *viewAccounts) ReadPublication() (uint64, uint64, bool) { return 0, 0, true }
func (v *viewAccounts) GetDurable(pk solana.PublicKey) (*accounts.Account, error) {
	return v.durable[pk], nil
}
func (v *viewAccounts) GetDurableBatch(_ context.Context, pks []solana.PublicKey, _ uint64) ([]*accounts.Account, error) {
	out := make([]*accounts.Account, len(pks))
	for i, pk := range pks {
		out[i] = v.durable[pk]
	}
	return out, nil
}

func newViewServer(t *testing.T, schedule sealevel.SysvarEpochSchedule) (*RpcServer, *solanarpc.Client, *viewAccounts) {
	t.Helper()
	server := NewRpcServer(nil, 0, &schedule, solana.Hash{1})
	require.NoError(t, server.listener.Close())
	endpoint := httptest.NewServer(server)
	t.Cleanup(endpoint.Close)
	accts := &viewAccounts{ws: accounts.NewWorkingSet(), durable: map[solana.PublicKey]*accounts.Account{}}
	server.BankViews().Reset(accts)
	return server, solanarpc.New(endpoint.URL), accts
}

func testView(t *testing.T, slot, height uint64) *bankview.View {
	t.Helper()
	return &bankview.View{
		Slot: slot, ParentSlot: slot - 1, BlockHeight: height, TransactionCount: slot * 10,
		Blockhash: solana.Hash{byte(slot)}, BlockID: solana.Hash{byte(slot), 1}, HasBlockID: true,
	}
}

// putSysvar stores a sysvar account in the durable layer.
func putSysvar(accts *viewAccounts, addr [32]byte, data []byte) {
	pk := solana.PublicKey(addr)
	accts.durable[pk] = &accounts.Account{Key: pk, Lamports: 1, Data: data}
}

func lamportsAccount(pk solana.PublicKey, lamports uint64) *accounts.Account {
	return &accounts.Account{Key: pk, Lamports: lamports, Owner: [32]byte(solana.SystemProgramID)}
}

func TestReadMethodsServeTheSelectedBank(t *testing.T) {
	server, client, accts := newViewServer(t, sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32, LeaderScheduleSlotOffset: 32})
	views := server.BankViews()
	wallet := solana.PublicKey{7}
	accts.durable[wallet] = lamportsAccount(wallet, 100)

	accts.ws.Add(40, []*accounts.Account{lamportsAccount(wallet, 140)})
	views.PublishProcessed(testView(t, 40, 30))
	views.MarkFinalized(40)
	accts.ws.Add(41, []*accounts.Account{lamportsAccount(wallet, 141)})
	views.PublishProcessed(testView(t, 41, 31))
	// Slot 42 has executed, but its view is not published yet.
	accts.ws.Add(42, []*accounts.Account{lamportsAccount(wallet, 142)})

	// Globals describe the executing block and must not leak into responses.
	savedSlot, savedHeight := global.Slot(), global.BlockHeight()
	t.Cleanup(func() { global.SetSlot(savedSlot); global.SetBlockHeight(savedHeight) })
	global.SetSlot(42)
	global.SetBlockHeight(32)

	ctx := t.Context()
	for _, tc := range []struct {
		commitment     solanarpc.CommitmentType
		slot, height   uint64
		lamports       uint64
		servedFromView string
	}{
		{solanarpc.CommitmentProcessed, 41, 31, 141, "processed"},
		{solanarpc.CommitmentConfirmed, 40, 30, 140, "confirmed falls back to finalized"},
		{solanarpc.CommitmentFinalized, 40, 30, 140, "finalized"},
	} {
		info, err := client.GetAccountInfoWithOpts(ctx, wallet, &solanarpc.GetAccountInfoOpts{Commitment: tc.commitment})
		require.NoError(t, err, tc.servedFromView)
		require.Equal(t, tc.slot, info.Context.Slot, tc.servedFromView)
		require.Equal(t, tc.lamports, info.Value.Lamports, tc.servedFromView)

		balance, err := client.GetBalance(ctx, wallet, tc.commitment)
		require.NoError(t, err)
		require.Equal(t, tc.slot, balance.Context.Slot)
		require.Equal(t, tc.lamports, balance.Value)

		slot, err := client.GetSlot(ctx, tc.commitment)
		require.NoError(t, err)
		require.Equal(t, tc.slot, slot)

		height, err := client.GetBlockHeight(ctx, tc.commitment)
		require.NoError(t, err)
		require.Equal(t, tc.height, height)

		latest, err := client.GetLatestBlockhash(ctx, tc.commitment)
		require.NoError(t, err)
		require.Equal(t, tc.slot, latest.Context.Slot)
		require.Equal(t, solana.Hash{byte(tc.slot)}, latest.Value.Blockhash)
		require.Equal(t, tc.height+maxProcessingAge, latest.Value.LastValidBlockHeight)

		epoch, err := client.GetEpochInfo(ctx, tc.commitment)
		require.NoError(t, err)
		require.Equal(t, tc.slot, epoch.AbsoluteSlot)
		require.Equal(t, tc.height, epoch.BlockHeight)
		require.Equal(t, tc.slot*10, *epoch.TransactionCount)
	}
}

func TestAccountReadsReportMissingAndDeletedAsNull(t *testing.T) {
	server, client, accts := newViewServer(t, sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32, LeaderScheduleSlotOffset: 32})
	live, deleted, missing := solana.PublicKey{1}, solana.PublicKey{2}, solana.PublicKey{3}
	accts.durable[live] = lamportsAccount(live, 5)
	accts.durable[deleted] = lamportsAccount(deleted, 9)
	accts.ws.Add(10, []*accounts.Account{{Key: deleted}})
	server.BankViews().PublishProcessed(testView(t, 10, 10))

	ctx := t.Context()
	opts := &solanarpc.GetAccountInfoOpts{Commitment: solanarpc.CommitmentProcessed}
	for _, pk := range []solana.PublicKey{deleted, missing} {
		_, err := client.GetAccountInfoWithOpts(ctx, pk, opts)
		require.ErrorIs(t, err, solanarpc.ErrNotFound, "account %s", pk)
	}
	multi, err := client.GetMultipleAccountsWithOpts(ctx, []solana.PublicKey{live, deleted, missing}, &solanarpc.GetMultipleAccountsOpts{Commitment: solanarpc.CommitmentProcessed})
	require.NoError(t, err)
	require.EqualValues(t, 10, multi.Context.Slot)
	require.Len(t, multi.Value, 3)
	require.EqualValues(t, 5, multi.Value[0].Lamports)
	require.Nil(t, multi.Value[1])
	require.Nil(t, multi.Value[2])

	balance, err := client.GetBalance(ctx, deleted, solanarpc.CommitmentProcessed)
	require.NoError(t, err)
	require.Zero(t, balance.Value)
}

func TestReadsBeforeFirstBankAndBelowMinContextSlot(t *testing.T) {
	server, client, _ := newViewServer(t, sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32, LeaderScheduleSlotOffset: 32})
	ctx := t.Context()
	_, err := client.GetSlot(ctx, solanarpc.CommitmentFinalized)
	require.Error(t, err)
	require.Contains(t, err.Error(), "-32005")

	server.BankViews().PublishProcessed(testView(t, 50, 50))
	minSlot := uint64(51)
	_, err = client.GetAccountInfoWithOpts(ctx, solana.PublicKey{9}, &solanarpc.GetAccountInfoOpts{Commitment: solanarpc.CommitmentProcessed, MinContextSlot: &minSlot})
	require.Error(t, err)
	require.Contains(t, err.Error(), "-32016")
}

func TestRentAndBlockhashValidityComeFromTheBank(t *testing.T) {
	server, client, accts := newViewServer(t, sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32, LeaderScheduleSlotOffset: 32})
	rent := sealevel.SysvarRent{LamportsPerUint8Year: 3480, ExemptionThreshold: 2, BurnPercent: 50}
	older := solana.Hash{0xaa}
	recent := sealevel.SysvarRecentBlockhashes{{Blockhash: older}}
	putSysvar(accts, sealevel.SysvarRentAddr, rent.MustMarshal())
	putSysvar(accts, sealevel.SysvarRecentBlockHashesAddr, recent.MustMarshal())
	view := testView(t, 60, 60)
	server.BankViews().PublishProcessed(view)
	ctx := t.Context()

	minimum, err := client.GetMinimumBalanceForRentExemption(ctx, 165, solanarpc.CommitmentProcessed)
	require.NoError(t, err)
	require.Equal(t, rent.MinimumBalance(165), minimum)

	for hash, want := range map[solana.Hash]bool{view.Blockhash: true, older: true, {0xbb}: false} {
		got, err := client.IsBlockhashValid(ctx, hash, solanarpc.CommitmentProcessed)
		require.NoError(t, err)
		require.Equal(t, want, got.Value, "blockhash %s", hash)
		require.EqualValues(t, 60, got.Context.Slot)
	}
}

func TestGetFeeForMessage(t *testing.T) {
	server, client, accts := newViewServer(t, sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32, LeaderScheduleSlotOffset: 32})
	stale := solana.Hash{0xcc}
	recent := sealevel.SysvarRecentBlockhashes{{Blockhash: stale}}
	putSysvar(accts, sealevel.SysvarRecentBlockHashesAddr, recent.MustMarshal())
	view := testView(t, 70, 70)
	server.BankViews().PublishProcessed(view)

	payer, recipient := solana.PublicKey{1}, solana.PublicKey{2}
	transfer := solana.NewInstruction(solana.SystemProgramID, solana.AccountMetaSlice{
		solana.Meta(payer).WRITE().SIGNER(), solana.Meta(recipient).WRITE(),
	}, []byte{2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0})
	computeBudget := solana.MustPublicKeyFromBase58("ComputeBudget111111111111111111111111111111")
	limit := solana.NewInstruction(computeBudget, nil, []byte{2, 0x40, 0x0d, 0x03, 0x00})       // 200,000 CU
	price := solana.NewInstruction(computeBudget, nil, []byte{3, 0x10, 0x27, 0, 0, 0, 0, 0, 0}) // 10,000 micro-lamports per CU

	encode := func(blockhash solana.Hash, ixs ...solana.Instruction) string {
		tx, err := solana.NewTransaction(ixs, blockhash, solana.TransactionPayer(payer))
		require.NoError(t, err)
		raw, err := tx.Message.MarshalBinary()
		require.NoError(t, err)
		return base64.StdEncoding.EncodeToString(raw)
	}
	ctx := t.Context()
	for _, tc := range []struct {
		name string
		msg  string
		want *uint64
	}{
		{"signature fee only", encode(view.Blockhash, transfer), ptr(uint64(5000))},
		{"with priority fee", encode(view.Blockhash, limit, price, transfer), ptr(uint64(5000 + 2000))},
		{"older valid blockhash", encode(stale, transfer), ptr(uint64(5000))},
		{"expired blockhash", encode(solana.Hash{0xdd}, transfer), nil},
	} {
		got, err := client.GetFeeForMessage(ctx, tc.msg, solanarpc.CommitmentProcessed)
		require.NoError(t, err, tc.name)
		require.EqualValues(t, 70, got.Context.Slot, tc.name)
		require.Equal(t, tc.want, got.Value, tc.name)
	}
}

func ptr[T any](v T) *T { return &v }

func TestGetSlotStatus(t *testing.T) {
	server, endpoint := newViewServerURL(t)
	views := server.BankViews()
	views.PublishProcessed(testView(t, 10, 10))
	views.MarkFinalized(10)
	views.PublishProcessed(testView(t, 12, 11))

	var resp struct {
		Result GetSlotStatusResp `json:"result"`
	}
	callJSONRPC(t, endpoint, `{"jsonrpc":"2.0","id":1,"method":"getSlotStatus","params":[[10,11,12,13]]}`, &resp)
	require.EqualValues(t, 12, resp.Result.Context.Slot)
	require.Equal(t, "finality", resp.Result.Value.NotarizationSource)
	var statuses []string
	for _, s := range resp.Result.Value.Slots {
		statuses = append(statuses, s.Status)
	}
	require.Equal(t, []string{"finalized", "skipped", "processed", "unknown"}, statuses)
	require.Equal(t, solana.Hash{12, 1}.String(), resp.Result.Value.Slots[2].BlockID)
}

func newViewServerURL(t *testing.T) (*RpcServer, string) {
	t.Helper()
	schedule := sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32, LeaderScheduleSlotOffset: 32}
	server := NewRpcServer(nil, 0, &schedule, solana.Hash{1})
	require.NoError(t, server.listener.Close())
	endpoint := httptest.NewServer(server)
	t.Cleanup(endpoint.Close)
	server.BankViews().Reset(&viewAccounts{ws: accounts.NewWorkingSet(), durable: map[solana.PublicKey]*accounts.Account{}})
	return server, endpoint.URL
}

func callJSONRPC(t *testing.T, url, body string, out any) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.NoError(t, json.NewDecoder(resp.Body).Decode(out))
}

func TestGetSlotStatusResolvesOldSlotsFromSlotHistory(t *testing.T) {
	server, _, accts := newViewServer(t, sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32, LeaderScheduleSlotOffset: 32})
	history := sealevel.SysvarSlotHistory{Bits: sealevel.SlotHistoryBitvec{Bits: sealevel.SlotHistoryInner{BlocksLen: 16384, Blocks: make([]uint64, 16384)}}}
	for _, slot := range []uint64{3, 10} {
		history.Add(slot)
		history.SetNextSlot(slot + 1)
	}
	putSysvar(accts, sealevel.SysvarSlotHistoryAddr, history.MustMarshal())
	views := server.BankViews()
	views.PublishProcessed(testView(t, 10, 10))
	views.MarkDurable(10)

	_, _, statuses, err := views.SlotStatuses([]uint64{3, 4, 10})
	require.NoError(t, err)
	require.NoError(t, server.resolveSlotHistory(t.Context(), statuses))
	require.Equal(t, "durable", statuses[0].Status)
	require.Equal(t, "skipped", statuses[1].Status)
	require.Equal(t, "durable", statuses[2].Status)
}
