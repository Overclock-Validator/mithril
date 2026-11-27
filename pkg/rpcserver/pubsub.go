package rpcserver

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/bankview"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/gagliardetto/solana-go"
	"github.com/gorilla/websocket"
)

// Solana pubsub over WebSocket: slotSubscribe, rootSubscribe,
// accountSubscribe and signatureSubscribe. Replay never waits on clients:
// the bank-view publisher hands events to a buffered channel, and a slow
// client is disconnected instead of being waited for.

const (
	pubsubEventBuffer   = 4096
	pubsubClientBuffer  = 256
	pubsubWriteDeadline = 10 * time.Second
	maxSubscriptionsPer = 1024
)

type viewEvent struct {
	level bankview.Level
	view  *bankview.View
}

type subscription struct {
	id     uint64
	client *pubsubClient
	kind   string // "slot", "root", "account", "signature"
	level  bankview.Level
	key    solana.PublicKey
	sig    solana.Signature
	conf   *GetAccountInfoConfig
	last   string // last account value sent, to skip unchanged notifications
}

type pubsubClient struct {
	conn *websocket.Conn
	out  chan []byte
	done chan struct{}
	once sync.Once
}

func (c *pubsubClient) close() {
	c.once.Do(func() {
		close(c.done)
		_ = c.conn.Close()
	})
}

func (c *pubsubClient) send(msg []byte) {
	select {
	case c.out <- msg:
	case <-c.done:
	default:
		mlog.Log.Warnf("pubsub: disconnecting a client that is not reading")
		c.close()
	}
}

// PubSub serves Solana subscriptions from published bank views.
type PubSub struct {
	server   *RpcServer
	events   chan viewEvent
	upgrader websocket.Upgrader

	mu     sync.Mutex
	nextID uint64
	subs   map[uint64]*subscription
	perCli map[*pubsubClient]int
}

// NewPubSub attaches subscriptions to rpcServer's bank views.
func NewPubSub(rpcServer *RpcServer) *PubSub {
	ps := &PubSub{
		server:   rpcServer,
		events:   make(chan viewEvent, pubsubEventBuffer),
		upgrader: websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }},
		subs:     map[uint64]*subscription{},
		perCli:   map[*pubsubClient]int{},
	}
	rpcServer.bankViews.SetListener(func(level bankview.Level, v *bankview.View) {
		select {
		case ps.events <- viewEvent{level: level, view: v}:
		default: // a lagging dispatcher skips events rather than slowing replay
		}
	})
	return ps
}

// Serve accepts WebSocket connections on listener until ctx ends.
func (ps *PubSub) Serve(ctx context.Context, listener net.Listener) error {
	go ps.dispatch(ctx)
	srv := &http.Server{Handler: ps, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	return srv.Serve(listener)
}

func (ps *PubSub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := ps.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	c := &pubsubClient{conn: conn, out: make(chan []byte, pubsubClientBuffer), done: make(chan struct{})}
	go ps.writeLoop(c)
	ps.readLoop(c)
}

func (ps *PubSub) writeLoop(c *pubsubClient) {
	for {
		select {
		case <-c.done:
			return
		case msg := <-c.out:
			_ = c.conn.SetWriteDeadline(time.Now().Add(pubsubWriteDeadline))
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				c.close()
				return
			}
		}
	}
}

type pubsubRequest struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params []interface{}   `json:"params"`
}

func (ps *PubSub) readLoop(c *pubsubClient) {
	defer func() {
		c.close()
		ps.mu.Lock()
		for id, sub := range ps.subs {
			if sub.client == c {
				delete(ps.subs, id)
			}
		}
		delete(ps.perCli, c)
		ps.mu.Unlock()
	}()
	c.conn.SetReadLimit(64 << 10)
	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		var req pubsubRequest
		if err := json.Unmarshal(data, &req); err != nil {
			c.send(rpcErrorFrame(nil, -32700, "Parse error"))
			continue
		}
		result, rerr := ps.handle(c, &req)
		if rerr != nil {
			code := -32602
			if e, ok := rerr.(*InvalidParamsError); ok {
				c.send(rpcErrorFrame(req.ID, code, e.Message))
			} else {
				c.send(rpcErrorFrame(req.ID, -32601, rerr.Error()))
			}
			continue
		}
		frame, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "result": result, "id": req.ID})
		c.send(frame)
	}
}

func rpcErrorFrame(id json.RawMessage, code int, msg string) []byte {
	frame, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "error": map[string]any{"code": code, "message": msg}, "id": id})
	return frame
}

func (ps *PubSub) handle(c *pubsubClient, req *pubsubRequest) (any, error) {
	switch req.Method {
	case "slotSubscribe":
		return ps.subscribe(c, &subscription{kind: "slot", level: bankview.Processed})
	case "rootSubscribe":
		return ps.subscribe(c, &subscription{kind: "root", level: bankview.Finalized})
	case "accountSubscribe":
		s, ok := firstString(req.Params)
		if !ok {
			return nil, &InvalidParamsError{Message: "accountSubscribe requires an account"}
		}
		pk, err := solana.PublicKeyFromBase58(s)
		if err != nil {
			return nil, &InvalidParamsError{Message: "Invalid param: Invalid"}
		}
		commitment, _, err := parseCommitmentConfig(req.Params, 1)
		if err != nil {
			return nil, err
		}
		conf, err := parseGetAccountInfoConfMap(req.Params)
		if err != nil {
			return nil, &InvalidParamsError{Message: err.Error()}
		}
		return ps.subscribe(c, &subscription{kind: "account", level: commitment.level, key: pk, conf: conf})
	case "signatureSubscribe":
		s, ok := firstString(req.Params)
		if !ok {
			return nil, &InvalidParamsError{Message: "signatureSubscribe requires a signature"}
		}
		sig, err := solana.SignatureFromBase58(s)
		if err != nil {
			return nil, &InvalidParamsError{Message: "Invalid param: Invalid"}
		}
		commitment, _, err := parseCommitmentConfig(req.Params, 1)
		if err != nil {
			return nil, err
		}
		id, err := ps.subscribe(c, &subscription{kind: "signature", level: commitment.level, sig: sig})
		if err == nil {
			ps.checkSignatureNow(id) // it may already be at the requested level
		}
		return id, err
	case "slotUnsubscribe", "rootUnsubscribe", "accountUnsubscribe", "signatureUnsubscribe":
		if len(req.Params) < 1 {
			return nil, &InvalidParamsError{Message: "missing subscription id"}
		}
		f, ok := req.Params[0].(float64)
		if !ok {
			return nil, &InvalidParamsError{Message: "invalid subscription id"}
		}
		return ps.unsubscribe(c, uint64(f)), nil
	}
	return nil, fmt.Errorf("method '%s' not found", req.Method)
}

func (ps *PubSub) subscribe(c *pubsubClient, sub *subscription) (uint64, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.perCli[c] >= maxSubscriptionsPer {
		return 0, &InvalidParamsError{Message: "too many subscriptions on this connection"}
	}
	ps.nextID++
	sub.id, sub.client = ps.nextID, c
	ps.subs[sub.id] = sub
	ps.perCli[c]++
	return sub.id, nil
}

func (ps *PubSub) unsubscribe(c *pubsubClient, id uint64) bool {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	sub, ok := ps.subs[id]
	if !ok || sub.client != c {
		return false
	}
	delete(ps.subs, id)
	ps.perCli[c]--
	return true
}

func (ps *PubSub) notify(sub *subscription, method string, result any) {
	frame, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  map[string]any{"result": result, "subscription": sub.id},
	})
	sub.client.send(frame)
}

func (ps *PubSub) dispatch(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-ps.events:
			ps.handleEvent(ctx, ev)
		}
	}
}

func (ps *PubSub) snapshotSubs() []*subscription {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	out := make([]*subscription, 0, len(ps.subs))
	for _, s := range ps.subs {
		out = append(out, s)
	}
	// Notify in subscription order, so one event's notifications reach a
	// client in a stable order.
	slices.SortFunc(out, func(a, b *subscription) int { return cmp.Compare(a.id, b.id) })
	return out
}

func (ps *PubSub) handleEvent(ctx context.Context, ev viewEvent) {
	var finalizedSlot uint64
	if fin, _, err := ps.server.bankViews.Current(bankview.Finalized); err == nil {
		finalizedSlot = fin.Slot
	}
	for _, sub := range ps.snapshotSubs() {
		switch sub.kind {
		case "slot":
			if ev.level == bankview.Processed {
				ps.notify(sub, "slotNotification", map[string]uint64{"parent": ev.view.ParentSlot, "root": finalizedSlot, "slot": ev.view.Slot})
			}
		case "root":
			if ev.level == bankview.Finalized {
				ps.notify(sub, "rootNotification", ev.view.Slot)
			}
		case "account":
			if sub.level == ev.level || (sub.level == bankview.Notarized && ev.level == bankview.Finalized) {
				ps.notifyAccount(ctx, sub)
			}
		case "signature":
			if ev.level >= sub.level { // a finalized block is also notarized and processed
				ps.checkSignature(ctx, sub)
			}
		}
	}
}

func (ps *PubSub) notifyAccount(ctx context.Context, sub *subscription) {
	var result GetAccountInfoResp
	err := ps.server.readView(ctx, commitmentConfig{level: sub.level}, func(r *bankview.Read) error {
		acct, err := r.Account(sub.key)
		if err != nil {
			return err
		}
		val, err := accountInfoValue(acct, sub.conf)
		if err != nil {
			return err
		}
		result = GetAccountInfoResp{Context: accountContext(r.View.Slot), Value: val}
		return nil
	})
	if err != nil {
		return
	}
	encoded, _ := json.Marshal(result.Value)
	if string(encoded) == sub.last {
		return
	}
	sub.last = string(encoded)
	ps.notify(sub, "accountNotification", result)
}

func (ps *PubSub) checkSignatureNow(id uint64) {
	ps.mu.Lock()
	sub := ps.subs[id]
	ps.mu.Unlock()
	if sub != nil {
		ps.checkSignature(context.Background(), sub)
	}
}

// checkSignature notifies once the signature's transaction is visible at the
// subscription's level, then ends the subscription, as Solana does.
func (ps *PubSub) checkSignature(ctx context.Context, sub *subscription) {
	if ps.server.history == nil {
		return
	}
	view, err := ps.server.currentView(ctx, commitmentConfig{level: sub.level})
	if err != nil {
		return
	}
	rec, _, err := findRecord(ctx, ps.server, sub.sig, view.Slot)
	if err != nil || rec == nil || rec.Slot > view.Slot {
		return
	}
	ps.mu.Lock()
	_, live := ps.subs[sub.id]
	if live {
		delete(ps.subs, sub.id)
		ps.perCli[sub.client]--
	}
	ps.mu.Unlock()
	if !live {
		return
	}
	ps.notify(sub, "signatureNotification", map[string]any{
		"context": map[string]uint64{"slot": view.Slot},
		"value":   map[string]any{"err": metaErr(rec.Meta)},
	})
}

// PubSubPort is the WebSocket port clients derive from an RPC port: the next
// port, as web3.js does.
func PubSubPort(rpcPort uint16) string { return strconv.Itoa(int(rpcPort) + 1) }
