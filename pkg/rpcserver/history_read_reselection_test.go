package rpcserver

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// The same retained transaction must remain available after one fresh selection
// when a forward fold invalidates the first read. The cold-SST pause reaches
// actual HTTP and storage code.
func TestHistoryReadHTTPForwardFoldReselection(t *testing.T) {
	server, block, endpoint := getBlockHistoryHTTPFixture(t)
	record, _, _ := historyResponseFixture(t, solana.MessageVersionV1)
	block.Records[0] = record
	require.NoError(t, server.transactionHistory.WriteBlock(block))
	endpoint.Client().Timeout = 5 * time.Second
	params := func(encoding string) string {
		return fmt.Sprintf(`[%q,{"encoding":%q,"commitment":"finalized","maxSupportedTransactionVersion":1}]`, solana.Signature(record.FirstSignature).String(), encoding)
	}
	before := getBlockHistoryHTTPCall(t, endpoint, "getTransaction", params("json"))
	require.NotContains(t, before, "error")
	beforeResult := historyResponseObject(t, before["result"])
	require.JSONEq(t, `20`, string(beforeResult["slot"]))
	require.JSONEq(t, `1`, string(beforeResult["version"]))

	fs := pauseHistoryBankhashRead(t, server)
	type response struct {
		status int
		body   []byte
		err    error
	}
	readDone := make(chan struct{})
	var paused response
	go func() {
		defer close(readDone)
		request := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"getTransaction","params":%s}`, params("base64"))
		got, err := endpoint.Client().Post(endpoint.URL, "application/json", strings.NewReader(request))
		if err != nil {
			paused.err = err
			return
		}
		paused.status = got.StatusCode
		paused.body, paused.err = io.ReadAll(io.LimitReader(got.Body, 64<<10))
		paused.err = errors.Join(paused.err, got.Body.Close())
	}()
	t.Cleanup(func() {
		fs.resume()
		select {
		case <-readDone:
		case <-time.After(6 * time.Second):
			t.Error("HTTP request did not join during cleanup")
		}
	})
	select {
	case <-fs.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("HTTP request did not reach the cold bankhash read")
	}
	require.Equal(t, 1, len(server.transactionHistoryResponses), "one response permit covers the paused request and its reselection")

	foldDone := make(chan struct{})
	var foldErr error
	go func() {
		defer close(foldDone)
		_, foldErr = server.acctsDb.CommitBatch([]accounts.SlotDelta{{Slot: 30, Delta: []*accounts.Account{{Key: solana.PublicKey{201}, Lamports: 30}}}}, 30, nil, nil)
		if foldErr == nil {
			foldErr = server.RecordBlockHistory(&b.Block{Slot: 30, ParentSlot: 20, Blockhash: solana.Hash{40}, LastBlockhash: solana.Hash(block.ID.Blockhash)})
		}
		if foldErr == nil {
			foldErr = server.PrepareBlockHistory(30)
		}
		if foldErr == nil {
			foldErr = server.SetRootedBlockHistorySlot(30)
		}
		if foldErr == nil {
			server.SetRootedBankState(30, 8, 1)
		}
	}()
	t.Cleanup(func() {
		fs.resume()
		select {
		case <-foldDone:
		case <-time.After(6 * time.Second):
			t.Error("forward fold did not join during cleanup")
		}
	})
	select {
	case <-foldDone:
		require.NoError(t, foldErr)
	case <-time.After(time.Second):
		t.Fatal("forward publication waited for the optional history read")
	}
	fs.resume()
	select {
	case <-readDone:
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP request did not finish after the read resumed")
	}
	require.NoError(t, paused.err)
	t.Logf("paused HTTP status=%d body=%s", paused.status, paused.body)
	require.Equal(t, http.StatusOK, paused.status)
	var reply map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(paused.body, &reply))
	require.JSONEq(t, `"2.0"`, string(reply["jsonrpc"]))
	require.JSONEq(t, `1`, string(reply["id"]))
	require.NotContains(t, reply, "error", "stable historical transaction should remain available across a forward fold")
	selected := historyResponseObject(t, reply["result"])
	var selectedWire []string
	require.NoError(t, json.Unmarshal(selected["transaction"], &selectedWire))
	require.Equal(t, []string{base64.StdEncoding.EncodeToString(record.Transaction), "base64"}, selectedWire)
	for _, field := range []string{"slot", "version", "blockTime", "meta"} {
		require.JSONEq(t, string(beforeResult[field]), string(selected[field]), field)
	}

	after := getBlockHistoryHTTPCall(t, endpoint, "getTransaction", params("base64"))
	require.NotContains(t, after, "error")
	afterResult := historyResponseObject(t, after["result"])
	for _, field := range []string{"slot", "version", "blockTime", "meta"} {
		require.JSONEq(t, string(beforeResult[field]), string(afterResult[field]), field)
	}
	var wire []string
	require.NoError(t, json.Unmarshal(afterResult["transaction"], &wire))
	require.Equal(t, []string{base64.StdEncoding.EncodeToString(record.Transaction), "base64"}, wire)
	require.Zero(t, len(server.transactionHistoryReads))
	require.Eventually(t, func() bool { return len(server.transactionHistoryResponses) == 0 }, time.Second, time.Millisecond)
}
