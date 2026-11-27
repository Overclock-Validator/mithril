package rpcserver

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/rpcindex"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

type wsTestClient struct {
	t       *testing.T
	conn    *websocket.Conn
	next    int
	pending []wsFrame // notifications read while waiting for something else
}

func startPubSub(t *testing.T, server *RpcServer) *wsTestClient {
	t.Helper()
	ps := NewPubSub(server)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = ps.Serve(ctx, listener) }()
	conn, _, err := websocket.DefaultDialer.Dial("ws://"+listener.Addr().String(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return &wsTestClient{t: t, conn: conn}
}

// call sends a request and returns its result, skipping notifications.
func (c *wsTestClient) call(method string, params ...any) json.RawMessage {
	c.t.Helper()
	c.next++
	require.NoError(c.t, c.conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": c.next, "method": method, "params": params}))
	for {
		msg := c.read()
		if msg.ID != nil && *msg.ID == c.next {
			require.Nil(c.t, msg.Error, "%s failed", method)
			return msg.Result
		}
		c.pending = append(c.pending, msg)
	}
}

type wsFrame struct {
	ID     *int            `json:"id"`
	Method string          `json:"method"`
	Result json.RawMessage `json:"result"`
	Error  any             `json:"error"`
	Params struct {
		Result       json.RawMessage `json:"result"`
		Subscription uint64          `json:"subscription"`
	} `json:"params"`
}

func (c *wsTestClient) read() wsFrame {
	c.t.Helper()
	require.NoError(c.t, c.conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	var f wsFrame
	require.NoError(c.t, c.conn.ReadJSON(&f))
	return f
}

// notification waits for the next notification of method.
func (c *wsTestClient) notification(method string) json.RawMessage {
	c.t.Helper()
	for i, f := range c.pending {
		if f.Method == method {
			c.pending = append(c.pending[:i], c.pending[i+1:]...)
			return f.Params.Result
		}
	}
	for {
		require.NoError(c.t, c.conn.SetReadDeadline(time.Now().Add(5*time.Second)))
		var f wsFrame
		require.NoError(c.t, c.conn.ReadJSON(&f), "waiting for %s", method)
		if f.Method == method {
			return f.Params.Result
		}
		c.pending = append(c.pending, f)
	}
}

func TestPubSubSlotAccountAndSignature(t *testing.T) {
	server, _, accts := newViewServer(t, sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32, LeaderScheduleSlotOffset: 32})
	wallet, other := solana.PublicKey{1}, solana.PublicKey{2}
	require.NoError(t, server.EnableHistory(t.TempDir(), solana.Hash{1}, []solana.PublicKey{wallet}, 0))
	accts.durable[wallet] = &accounts.Account{Key: wallet, Lamports: 100, Owner: [32]byte(solana.SystemProgramID)}
	views := server.BankViews()
	views.PublishProcessed(testView(t, 10, 10))
	c := startPubSub(t, server)
	tx := historyTx(t, wallet, other, 7)

	c.call("slotSubscribe")
	c.call("accountSubscribe", wallet.String(), map[string]any{"commitment": "processed", "encoding": "base64"})
	c.call("signatureSubscribe", tx.Signatures[0].String(), map[string]any{"commitment": "confirmed"})

	// Slot 11 credits the wallet and lands the transaction.
	accts.ws.Add(11, []*accounts.Account{{Key: wallet, Lamports: 250, Owner: [32]byte(solana.SystemProgramID)}})
	server.RPCIndexer().CompletedBlock(11, solana.Hash{11}, &b.Block{Slot: 11, Transactions: []*solana.Transaction{tx}},
		[]*rpcindex.Captured{{Signature: tx.Signatures[0], Keys: tx.Message.AccountKeys, Fee: 5000}}, nil)
	server.RPCIndexer().Sync()
	views.PublishProcessed(testView(t, 11, 11))

	var slot struct{ Parent, Root, Slot uint64 }
	require.NoError(t, json.Unmarshal(c.notification("slotNotification"), &slot))
	require.EqualValues(t, 11, slot.Slot)
	require.EqualValues(t, 10, slot.Parent)

	var acct GetAccountInfoResp
	require.NoError(t, json.Unmarshal(c.notification("accountNotification"), &acct))
	require.EqualValues(t, 11, acct.Context.Slot)
	require.EqualValues(t, 250, acct.Value.Lamports)

	// Confirmed arrives once the slot is notarized; here it is finalized.
	views.MarkFinalized(11)
	var sig struct {
		Context struct{ Slot uint64 }
		Value   struct{ Err any }
	}
	require.NoError(t, json.Unmarshal(c.notification("signatureNotification"), &sig))
	require.EqualValues(t, 11, sig.Context.Slot)
	require.Nil(t, sig.Value.Err)
}

func TestPubSubRejectsUnknownMethods(t *testing.T) {
	server, _, _ := newViewServer(t, sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32, LeaderScheduleSlotOffset: 32})
	ps := NewPubSub(server)
	c := &pubsubClient{out: make(chan []byte, 1), done: make(chan struct{})}
	_, err := ps.handle(c, &pubsubRequest{Method: "programSubscribe"})
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "not found"))
}
