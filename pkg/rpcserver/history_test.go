package rpcserver

import (
	"encoding/binary"
	"net/http/httptest"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/rpcindex"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	solanarpc "github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/require"
)

func historyTx(t *testing.T, payer, to solana.PublicKey, sigByte byte) *solana.Transaction {
	t.Helper()
	ix := solana.NewInstruction(solana.SystemProgramID, solana.AccountMetaSlice{
		solana.Meta(payer).WRITE().SIGNER(), solana.Meta(to).WRITE(),
	}, []byte{2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0})
	tx, err := solana.NewTransaction([]solana.Instruction{ix}, solana.Hash{9}, solana.TransactionPayer(payer))
	require.NoError(t, err)
	tx.Signatures = []solana.Signature{{sigByte, 1}}
	return tx
}

func TestHistoryMethodsServeWatchedWallet(t *testing.T) {
	server, client, _ := newViewServer(t, sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32, LeaderScheduleSlotOffset: 32})
	wallet, other := solana.PublicKey{1}, solana.PublicKey{2}
	require.NoError(t, server.EnableHistory(t.TempDir(), solana.Hash{1}, []solana.PublicKey{wallet}, 0))
	ix := server.RPCIndexer()
	views := server.BankViews()

	sent := historyTx(t, wallet, other, 1)
	failed := historyTx(t, other, wallet, 2)
	recent := historyTx(t, wallet, other, 3)
	capture := func(tx *solana.Transaction) *rpcindex.Captured {
		return &rpcindex.Captured{Signature: tx.Signatures[0], Keys: tx.Message.AccountKeys, Fee: 5000,
			PreBalances: []uint64{10, 0, 1}, PostBalances: []uint64{4, 1, 1}}
	}
	failedCapture := capture(failed)
	failedCapture.Err = errSentinel{}
	failedCapture.ErrJSON = []byte(`{"InstructionError":[0,{"Custom":1}]}`)

	views.PublishProcessed(testView(t, 20, 20))
	ix.CompletedBlock(20, solana.Hash{20}, &b.Block{Slot: 20, UnixTimestamp: 1_700_000_000, Transactions: []*solana.Transaction{sent, failed}}, []*rpcindex.Captured{capture(sent), failedCapture}, nil)
	views.PublishProcessed(testView(t, 21, 21))
	ix.CompletedBlock(21, solana.Hash{21}, &b.Block{Slot: 21, Transactions: []*solana.Transaction{recent}}, []*rpcindex.Captured{capture(recent)}, nil)
	views.MarkFinalized(20)
	ix.Finalized(20)
	views.MarkNotarized(21, solana.Hash{21, 1})
	ix.Sync()

	ctx := t.Context()
	confirmed, err := client.GetSignaturesForAddressWithOpts(ctx, wallet, &solanarpc.GetSignaturesForAddressOpts{Commitment: solanarpc.CommitmentConfirmed})
	require.NoError(t, err)
	require.Len(t, confirmed, 3)
	require.Equal(t, recent.Signatures[0], confirmed[0].Signature)
	require.Equal(t, solanarpc.ConfirmationStatusConfirmed, confirmed[0].ConfirmationStatus)
	require.Equal(t, failed.Signatures[0], confirmed[1].Signature)
	require.NotNil(t, confirmed[1].Err)
	require.Equal(t, solanarpc.ConfirmationStatusFinalized, confirmed[2].ConfirmationStatus)
	require.EqualValues(t, 1_700_000_000, *confirmed[2].BlockTime)

	final, err := client.GetSignaturesForAddress(ctx, wallet)
	require.NoError(t, err)
	require.Len(t, final, 2, "finalized commitment excludes slot 21")

	before := failed.Signatures[0]
	page, err := client.GetSignaturesForAddressWithOpts(ctx, wallet, &solanarpc.GetSignaturesForAddressOpts{Commitment: solanarpc.CommitmentConfirmed, Before: before})
	require.NoError(t, err)
	require.Len(t, page, 1)
	require.Equal(t, sent.Signatures[0], page[0].Signature)

	_, err = client.GetSignaturesForAddress(ctx, solana.PublicKey{3})
	require.Error(t, err, "unwatched addresses are refused")
}

type errSentinel struct{}

func (errSentinel) Error() string { return "custom" }

func TestTokenBalanceAndSupply(t *testing.T) {
	server, client, accts := newViewServer(t, sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32, LeaderScheduleSlotOffset: 32})
	mint, tokenAcct, owner := solana.PublicKey{5}, solana.PublicKey{6}, solana.PublicKey{7}
	mintData := make([]byte, 82)
	binary.LittleEndian.PutUint64(mintData[36:44], 2_500_000)
	mintData[44], mintData[45] = 6, 1
	accts.durable[mint] = &accounts.Account{Key: mint, Lamports: 1, Owner: [32]byte(rpcindex.SPLTokenProgram), Data: mintData}
	acctData := make([]byte, 165)
	copy(acctData[0:32], mint[:])
	copy(acctData[32:64], owner[:])
	binary.LittleEndian.PutUint64(acctData[64:72], 1_500_000)
	acctData[108] = 1
	accts.durable[tokenAcct] = &accounts.Account{Key: tokenAcct, Lamports: 1, Owner: [32]byte(rpcindex.SPLTokenProgram), Data: acctData}
	server.BankViews().PublishProcessed(testView(t, 30, 30))

	balance, err := client.GetTokenAccountBalance(t.Context(), tokenAcct, solanarpc.CommitmentProcessed)
	require.NoError(t, err)
	require.Equal(t, "1500000", balance.Value.Amount)
	require.Equal(t, "1.5", balance.Value.UiAmountString)
	require.EqualValues(t, 6, balance.Value.Decimals)

	supply, err := client.GetTokenSupply(t.Context(), mint, solanarpc.CommitmentProcessed)
	require.NoError(t, err)
	require.Equal(t, "2500000", supply.Value.Amount)
	require.EqualValues(t, 30, supply.Context.Slot)

	_, err = client.GetTokenAccountBalance(t.Context(), mint, solanarpc.CommitmentProcessed)
	require.Error(t, err, "a mint is not a token account")
}

func TestGetTokenAccountsByOwnerFromDiscoveries(t *testing.T) {
	server, client, accts := newViewServer(t, sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32, LeaderScheduleSlotOffset: 32})
	wallet, mint := solana.PublicKey{1}, solana.PublicKey{5}
	require.NoError(t, server.EnableHistory(t.TempDir(), solana.Hash{1}, []solana.PublicKey{wallet}, 0))
	ix := server.RPCIndexer()
	views := server.BankViews()

	mintData := make([]byte, 82)
	mintData[44], mintData[45] = 6, 1
	accts.durable[mint] = &accounts.Account{Key: mint, Lamports: 1, Owner: [32]byte(rpcindex.SPLTokenProgram), Data: mintData}
	held, gone := solana.PublicKey{10}, solana.PublicKey{11}
	for _, k := range []solana.PublicKey{held, gone} {
		data := make([]byte, 165)
		copy(data[0:32], mint[:])
		copy(data[32:64], wallet[:])
		binary.LittleEndian.PutUint64(data[64:72], 2_000_000)
		data[108] = 1
		accts.durable[k] = &accounts.Account{Key: k, Lamports: 2_039_280, Owner: [32]byte(rpcindex.SPLTokenProgram), Data: data}
	}
	views.PublishProcessed(testView(t, 40, 40))
	ix.CompletedBlock(40, solana.Hash{40}, &b.Block{Slot: 40}, nil, []rpcindex.TokenAccountChange{
		{Owner: wallet, Mint: mint, Account: held, Program: rpcindex.SPLTokenProgram},
		{Owner: wallet, Mint: mint, Account: gone, Program: rpcindex.SPLTokenProgram},
	})
	// gone is closed in slot 41.
	views.PublishProcessed(testView(t, 41, 41))
	accts.ws.Add(41, []*accounts.Account{{Key: gone}})
	ix.CompletedBlock(41, solana.Hash{41}, &b.Block{Slot: 41}, nil, []rpcindex.TokenAccountChange{{Account: gone, Closed: true}})
	views.MarkFinalized(41)
	ix.Finalized(41)
	ix.Sync()

	ctx := t.Context()
	_, err := client.GetTokenAccountsByOwner(ctx, wallet, &solanarpc.GetTokenAccountsConfig{Mint: &mint}, &solanarpc.GetTokenAccountsOpts{Commitment: solanarpc.CommitmentFinalized})
	require.Error(t, err, "without allowPartial the node refuses an incomplete enumeration")

	var resp struct {
		Result GetTokenAccountsByOwnerResp `json:"result"`
		Error  any                         `json:"error"`
	}
	url := newViewServerEndpoint(t, server)
	callJSONRPC(t, url, `{"jsonrpc":"2.0","id":1,"method":"getTokenAccountsByOwner","params":["`+wallet.String()+`",{"mint":"`+mint.String()+`"},{"encoding":"jsonParsed","allowPartial":true}]}`, &resp)
	require.Nil(t, resp.Error)
	require.EqualValues(t, 41, resp.Result.Context.Slot)
	require.Len(t, resp.Result.Value, 1, "the closed account is gone")
	require.Equal(t, held.String(), resp.Result.Value[0].Pubkey)
	parsed := resp.Result.Value[0].Account.(map[string]any)["data"].(map[string]any)["parsed"].(map[string]any)["info"].(map[string]any)
	require.Equal(t, "2", parsed["tokenAmount"].(map[string]any)["uiAmountString"])

	rows, err := ix.Store().TokenAccounts(wallet)
	require.NoError(t, err)
	require.Len(t, rows, 1, "finalization persisted the creation and the close")
}

func newViewServerEndpoint(t *testing.T, server *RpcServer) string {
	t.Helper()
	endpoint := httptest.NewServer(server)
	t.Cleanup(endpoint.Close)
	return endpoint.URL
}
