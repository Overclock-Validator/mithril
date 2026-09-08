package rpcserver

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/leaderschedule"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
	solanarpc "github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSendTransactionConfig_Defaults(t *testing.T) {
	conf := parseSendTransactionConfig([]interface{}{"tx"})
	assert.Equal(t, "base58", conf.encoding)
	assert.False(t, conf.skipPreflight)
	assert.Nil(t, conf.maxRetries)
	assert.Nil(t, conf.minContextSlot)
}

func TestDecodeSendTransactionV1Base64AndSizeLimits(t *testing.T) {
	tx, wire := testV1Transaction(t, 0)
	require.Equal(t, byte(0x81), wire[0])

	decoded, decodedWire, err := decodeSendTransaction(base64.StdEncoding.EncodeToString(wire), "base64")
	require.NoError(t, err)
	assert.Equal(t, wire, decodedWire)
	assert.Equal(t, solana.MessageVersionV1, decoded.Message.GetVersion())
	assert.Equal(t, tx.Message.TransactionConfig, decoded.Message.TransactionConfig)

	_, maxWire := testV1Transaction(t, v1TransactionSize-len(wire))
	require.Len(t, maxWire, v1TransactionSize)
	_, _, err = decodeSendTransaction(base64.StdEncoding.EncodeToString(maxWire), "base64")
	require.NoError(t, err)

	tooLarge := append(append([]byte(nil), maxWire...), 0)
	_, _, err = decodeSendTransaction(base64.StdEncoding.EncodeToString(tooLarge), "base64")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decoded solana_transaction too large")
}

func TestDecodeSendTransactionV1RejectsTrailingBytes(t *testing.T) {
	_, wire := testV1Transaction(t, 0)
	wire = append(wire, 0)

	_, _, err := decodeSendTransaction(base64.StdEncoding.EncodeToString(wire), "base64")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "trailing bytes")
}

func TestDecodeSendTransactionLegacyRejectsTrailingBytes(t *testing.T) {
	_, wire := testLegacyTransaction(t)
	wire = append(wire, 0)

	_, _, err := decodeSendTransaction(base64.StdEncoding.EncodeToString(wire), "base64")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to deserialize solana_transaction")
}

func TestDecodeSendTransactionRetainsLegacyPacketLimit(t *testing.T) {
	_, wire := testLegacyTransaction(t)
	wire = append(wire, make([]byte, legacyTransactionSize-len(wire)+1)...)
	require.Len(t, wire, legacyTransactionSize+1)

	_, _, err := decodeSendTransaction(base64.StdEncoding.EncodeToString(wire), "base64")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decoded solana_transaction too large")
	assert.Contains(t, err.Error(), "max: 1232 bytes")
}

func TestDefaultTransactionSenderRejectsLargeUDPTransaction(t *testing.T) {
	err := defaultTransactionSender(
		context.Background(),
		make([]byte, legacyTransactionSize+1),
		tpuEndpoint{Addr: netip.MustParseAddrPort("127.0.0.1:1"), Transport: tpuTransportUDP},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "UDP TPU supports at most 1232 bytes")
}

func TestSendTransaction_RejectsSanitizeFailure(t *testing.T) {
	rpcServer := &RpcServer{}
	tx := &solana.Transaction{
		Message: solana.Message{
			Header: solana.MessageHeader{
				NumRequiredSignatures:     1,
				NumReadonlySignedAccounts: 1,
			},
			AccountKeys: []solana.PublicKey{{1}},
		},
	}
	wire, err := tx.MarshalBinary()
	require.NoError(t, err)

	_, err = rpcServer.SendTransaction(
		context.Background(),
		mustRawParams(t, []interface{}{
			base64.StdEncoding.EncodeToString(wire),
			map[string]interface{}{"encoding": "base64", "skipPreflight": true},
		}),
	)
	require.Error(t, err)

	invalidParams, ok := err.(*InvalidParamsError)
	require.True(t, ok)
	assert.Equal(t, "invalid transaction: Transaction failed to sanitize accounts offsets correctly", invalidParams.Message)
}

func TestSendTransaction_PreflightSignatureFailure(t *testing.T) {
	rpcServer := &RpcServer{
		slotCtx: &sealevel.SlotCtx{
			Slot:     123,
			Features: features.NewFeaturesDefault(),
		},
	}

	tx, wire := testLegacyTransaction(t)
	_, err := rpcServer.SendTransaction(
		context.Background(),
		mustRawParams(t, []interface{}{
			base64.StdEncoding.EncodeToString(wire),
			map[string]interface{}{"encoding": "base64"},
		}),
	)
	require.Error(t, err)

	preflightErr, ok := err.(*SendTransactionPreflightFailureError)
	require.True(t, ok)
	assert.Equal(t, "Transaction simulation failed: Transaction did not pass signature verification", preflightErr.Message)
	assert.Equal(t, "SignatureFailure", preflightErr.Result.Err)
	require.NotNil(t, preflightErr.Result.Logs)
	assert.Equal(t, []string{}, *preflightErr.Result.Logs)
	require.NotNil(t, preflightErr.Result.UnitsConsumed)
	assert.Equal(t, uint64(0), *preflightErr.Result.UnitsConsumed)
	require.NotNil(t, preflightErr.Result.LoadedAccountsDataSize)
	assert.Equal(t, uint32(0), *preflightErr.Result.LoadedAccountsDataSize)
	assert.Nil(t, preflightErr.Result.LoadedAddresses)

	sig, sigErr := firstSignature(tx)
	require.NoError(t, sigErr)
	assert.Equal(t, tx.Signatures[0], sig)
}

func TestFailureLoadedAccountsDataSizeUsesLoaderSelectedSemantics(t *testing.T) {
	output := replay.LoadAndExecuteTransactionOutput{
		ProcessingResult: replay.TransactionProcessingResult{
			TransactionError: &replay.TransactionError{
				ErrorType: replay.TransactionErrorMaxLoadedAccountsDataSizeExceeded,
			},
		},
		LoadedAccountsDataSize: 1234,
	}
	assert.Equal(t, uint32(1234), failureLoadedAccountsDataSize(output))

	output.ProcessingResult.TransactionError.ErrorType = replay.TransactionErrorBlockhashNotFound
	assert.Zero(t, failureLoadedAccountsDataSize(output), "only fee-only load failures publish the selected size")
}

func TestSendTransaction_SkipPreflight_FansOutToUpcomingLeaders(t *testing.T) {
	listenerA := mustListenUDP(t)
	defer listenerA.Close()
	listenerB := mustListenUDP(t)
	defer listenerB.Close()
	listenerC := mustListenUDP(t)
	defer listenerC.Close()
	listenerD := mustListenUDP(t)
	defer listenerD.Close()
	listenerE := mustListenUDP(t)
	defer listenerE.Close()
	listenerF := mustListenUDP(t)
	defer listenerF.Close()

	leaderA := solana.PublicKey{0xA1}
	leaderB := solana.PublicKey{0xB2}
	leaderC := solana.PublicKey{0xC3}
	leaderD := solana.PublicKey{0xD4}
	leaderE := solana.PublicKey{0xE5}
	leaderF := solana.PublicKey{0xF6}

	global.SetLeaderSchedule(leaderschedule.NewLeaderScheduleFromKeyedSlots(
		map[solana.PublicKey][]uint64{
			leaderA: {0, 1, 2, 3},
			leaderB: {4, 5, 6, 7},
			leaderC: {8, 9, 10, 11},
			leaderD: {12, 13, 14, 15},
			leaderE: {16, 17, 18, 19},
			leaderF: {20, 21, 22, 23},
		},
		100,
	))
	global.SetSlot(100)
	defer global.SetLeaderSchedule(nil)
	defer global.SetSlot(0)

	tx, wire := testLegacyTransaction(t)
	fetchCount := 0
	rpcServer := &RpcServer{
		transactionSender:                 defaultTransactionSender,
		clusterNodesRefreshEvery:          sendTransactionClusterNodesRefreshEvery,
		sendTransactionLeaderForwardCount: sendTransactionLeaderForwardCount,
		clusterNodesFetcher: func(context.Context) ([]*solanarpc.GetClusterNodesResult, error) {
			fetchCount++
			return []*solanarpc.GetClusterNodesResult{
				{Pubkey: leaderA, TPU: stringPtr(listenerA.LocalAddr().String())},
				{Pubkey: leaderB, TPU: stringPtr(listenerB.LocalAddr().String())},
				{Pubkey: leaderC, TPU: stringPtr(listenerC.LocalAddr().String())},
				{Pubkey: leaderD, TPU: stringPtr(listenerD.LocalAddr().String())},
				{Pubkey: leaderE, TPU: stringPtr(listenerE.LocalAddr().String())},
				{Pubkey: leaderF, TPU: stringPtr(listenerF.LocalAddr().String())},
			}, nil
		},
	}

	gotSig, err := rpcServer.SendTransaction(
		context.Background(),
		mustRawParams(t, []interface{}{
			base64.StdEncoding.EncodeToString(wire),
			map[string]interface{}{"encoding": "base64", "skipPreflight": true},
		}),
	)
	require.NoError(t, err)
	assert.Equal(t, tx.Signatures[0].String(), gotSig)
	assert.Equal(t, 1, fetchCount, "first send should populate the TPU cache once")

	assert.Equal(t, wire, mustReadUDP(t, listenerA))
	assert.Equal(t, wire, mustReadUDP(t, listenerB))
	assert.Equal(t, wire, mustReadUDP(t, listenerC))
	assert.Equal(t, wire, mustReadUDP(t, listenerD))
	assert.Equal(t, wire, mustReadUDP(t, listenerE))
	assert.Equal(t, wire, mustReadUDP(t, listenerF))
}

func TestResolveUpcomingLeaderTPUEndpoints_UsesFreshCacheWithoutRefetch(t *testing.T) {
	leaderA := solana.PublicKey{0x01}
	leaderB := solana.PublicKey{0x02}
	leaderC := solana.PublicKey{0x03}
	leaderD := solana.PublicKey{0x04}
	leaderE := solana.PublicKey{0x05}
	leaderF := solana.PublicKey{0x06}

	global.SetLeaderSchedule(leaderschedule.NewLeaderScheduleFromKeyedSlots(
		map[solana.PublicKey][]uint64{
			leaderA: {0},
			leaderB: {1},
			leaderC: {2},
			leaderD: {3},
			leaderE: {4},
			leaderF: {5},
		},
		500,
	))
	global.SetSlot(500)
	defer global.SetLeaderSchedule(nil)
	defer global.SetSlot(0)

	fetchCount := 0
	rpcServer := &RpcServer{
		clusterNodesRefreshEvery:          sendTransactionClusterNodesRefreshEvery,
		sendTransactionLeaderForwardCount: sendTransactionLeaderForwardCount,
		clusterNodesFetcher: func(context.Context) ([]*solanarpc.GetClusterNodesResult, error) {
			fetchCount++
			return []*solanarpc.GetClusterNodesResult{
				{Pubkey: leaderA, TPU: stringPtr("127.0.0.1:9001")},
				{Pubkey: leaderB, TPU: stringPtr("127.0.0.1:9002")},
				{Pubkey: leaderC, TPU: stringPtr("127.0.0.1:9003")},
				{Pubkey: leaderD, TPU: stringPtr("127.0.0.1:9004")},
				{Pubkey: leaderE, TPU: stringPtr("127.0.0.1:9005")},
				{Pubkey: leaderF, TPU: stringPtr("127.0.0.1:9006")},
			}, nil
		},
	}

	require.NoError(t, rpcServer.refreshLeaderTPUCache(context.Background()))
	targets, err := rpcServer.resolveUpcomingLeaderTPUEndpoints(context.Background(), 6)
	require.NoError(t, err)
	require.Len(t, targets, 6)
	assert.Equal(t, 1, fetchCount, "fresh cache should satisfy resolution without another RPC poll")
}

func TestResolveUpcomingLeaderTPUEndpoints_RefreshesStaleCache(t *testing.T) {
	leaderA := solana.PublicKey{0x11}
	leaderB := solana.PublicKey{0x12}
	leaderC := solana.PublicKey{0x13}
	leaderD := solana.PublicKey{0x14}
	leaderE := solana.PublicKey{0x15}
	leaderF := solana.PublicKey{0x16}

	global.SetLeaderSchedule(leaderschedule.NewLeaderScheduleFromKeyedSlots(
		map[solana.PublicKey][]uint64{
			leaderA: {0},
			leaderB: {1},
			leaderC: {2},
			leaderD: {3},
			leaderE: {4},
			leaderF: {5},
		},
		900,
	))
	global.SetSlot(900)
	defer global.SetLeaderSchedule(nil)
	defer global.SetSlot(0)

	fetchCount := 0
	rpcServer := &RpcServer{
		clusterNodesRefreshEvery:          10 * time.Minute,
		sendTransactionLeaderForwardCount: sendTransactionLeaderForwardCount,
		clusterNodesFetcher: func(context.Context) ([]*solanarpc.GetClusterNodesResult, error) {
			fetchCount++
			return []*solanarpc.GetClusterNodesResult{
				{Pubkey: leaderA, TPU: stringPtr("127.0.0.1:9101")},
				{Pubkey: leaderB, TPU: stringPtr("127.0.0.1:9102")},
				{Pubkey: leaderC, TPU: stringPtr("127.0.0.1:9103")},
				{Pubkey: leaderD, TPU: stringPtr("127.0.0.1:9104")},
				{Pubkey: leaderE, TPU: stringPtr("127.0.0.1:9105")},
				{Pubkey: leaderF, TPU: stringPtr("127.0.0.1:9106")},
			}, nil
		},
	}

	require.NoError(t, rpcServer.refreshLeaderTPUCache(context.Background()))
	rpcServer.leaderTPUCacheMu.Lock()
	rpcServer.leaderTPUCacheUpdatedAt = time.Now().Add(-11 * time.Minute)
	rpcServer.leaderTPUCacheMu.Unlock()

	targets, err := rpcServer.resolveUpcomingLeaderTPUEndpoints(context.Background(), 6)
	require.NoError(t, err)
	require.Len(t, targets, 6)
	assert.Equal(t, 2, fetchCount, "stale cache should trigger a refresh before resolving targets")
}

func TestRefreshLeaderTPUCache_PrefersQUICEndpoints(t *testing.T) {
	leaderA := solana.PublicKey{0x21}
	leaderB := solana.PublicKey{0x22}
	leaderC := solana.PublicKey{0x23}

	rpcServer := &RpcServer{
		clusterNodesFetcher: func(context.Context) ([]*solanarpc.GetClusterNodesResult, error) {
			return []*solanarpc.GetClusterNodesResult{
				{
					Pubkey:  leaderA,
					TPU:     stringPtr("127.0.0.1:9001"),
					TPUQUIC: stringPtr("127.0.0.1:10001"),
				},
				{
					Pubkey:  leaderB,
					TPUQUIC: stringPtr("127.0.0.1:10002"),
				},
				{
					Pubkey: leaderC,
					TPU:    stringPtr("127.0.0.1:9003"),
				},
			}, nil
		},
	}

	require.NoError(t, rpcServer.refreshLeaderTPUCache(context.Background()))

	rpcServer.leaderTPUCacheMu.RLock()
	defer rpcServer.leaderTPUCacheMu.RUnlock()

	assert.Equal(t, tpuEndpoint{Addr: netip.MustParseAddrPort("127.0.0.1:10001"), Transport: tpuTransportQUIC}, rpcServer.leaderTPUByIdentity[leaderA])
	assert.Equal(t, tpuEndpoint{Addr: netip.MustParseAddrPort("127.0.0.1:10002"), Transport: tpuTransportQUIC}, rpcServer.leaderTPUByIdentity[leaderB])
	assert.Equal(t, tpuEndpoint{Addr: netip.MustParseAddrPort("127.0.0.1:9003"), Transport: tpuTransportUDP}, rpcServer.leaderTPUByIdentity[leaderC])
}

func mustRawParams(t *testing.T, params []interface{}) jsonrpc.RawParams {
	t.Helper()
	raw, err := json.Marshal(params)
	require.NoError(t, err)
	return jsonrpc.RawParams(raw)
}

func testLegacyTransaction(t *testing.T) (*solana.Transaction, []byte) {
	t.Helper()

	tx := &solana.Transaction{
		Signatures: []solana.Signature{{}},
		Message: solana.Message{
			Header:      solana.MessageHeader{NumRequiredSignatures: 1},
			AccountKeys: []solana.PublicKey{{0x11}},
		},
	}

	wire, err := tx.MarshalBinary()
	require.NoError(t, err)
	return tx, wire
}

func testV1Transaction(t *testing.T, dataLen int) (*solana.Transaction, []byte) {
	t.Helper()

	msg := solana.Message{
		Header: solana.MessageHeader{
			NumRequiredSignatures:       1,
			NumReadonlyUnsignedAccounts: 1,
		},
		AccountKeys:       []solana.PublicKey{{0x11}, {0x22}},
		RecentBlockhash:   solana.Hash{0x33},
		TransactionConfig: solana.TransactionConfig{}.WithPriorityFee(7).WithComputeUnitLimit(100_000),
		Instructions: []solana.CompiledInstruction{{
			ProgramIDIndex: 1,
			Accounts:       []uint16{0},
			Data:           make([]byte, dataLen),
		}},
	}
	_, err := msg.SetVersion(solana.MessageVersionV1)
	require.NoError(t, err)
	tx := &solana.Transaction{Message: msg, Signatures: []solana.Signature{{}}}
	wire, err := tx.MarshalBinary()
	require.NoError(t, err)
	require.NoError(t, tx.Sanitize())
	return tx, wire
}

func mustListenUDP(t *testing.T) *net.UDPConn {
	t.Helper()

	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	require.NoError(t, err)
	return conn
}

func mustReadUDP(t *testing.T, conn *net.UDPConn) []byte {
	t.Helper()

	buf := make([]byte, packetDataSize)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	n, _, err := conn.ReadFromUDP(buf)
	require.NoError(t, err)
	out := make([]byte, n)
	copy(out, buf[:n])
	return out
}

func stringPtr(v string) *string {
	return &v
}
