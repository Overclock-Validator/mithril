package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

type observedListener struct {
	net.Listener
	closed chan struct{}
	bytes  atomic.Int64
	once   sync.Once
}

func (l *observedListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &observedConn{Conn: c, listener: l}, nil
}

type observedConn struct {
	net.Conn
	listener *observedListener
}

func (c *observedConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.listener.bytes.Add(int64(n))
	return n, err
}

func (c *observedConn) Close() error {
	err := c.Conn.Close()
	c.listener.once.Do(func() { close(c.listener.closed) })
	return err
}

type floodTestConn struct {
	net.Conn
	*bytes.Reader
	closed bool
}

func (c *floodTestConn) Read(p []byte) (int, error) { return c.Reader.Read(p) }
func (c *floodTestConn) Close() error               { c.closed = true; return nil }

func TestInputLimiterRejectsOverBudget(t *testing.T) {
	source := &floodTestConn{Reader: bytes.NewReader(make([]byte, 8193))}
	conn := inputLimitedConn{Conn: source, limit: rate.NewLimiter(0, 8192)}
	buf := make([]byte, 4096)
	for i := 0; i < 2; i++ {
		if n, err := conn.Read(buf); n != len(buf) || err != nil {
			t.Fatalf("burst read = %d, %v", n, err)
		}
	}
	if n, err := conn.Read(buf); n != 0 || err == nil || !source.closed {
		t.Fatalf("over-budget read = %d, %v; closed=%t", n, err, source.closed)
	}
}

func TestViewerAcceptsSmallFragmentedPing(t *testing.T) {
	address, stop := startViewerServer(t, socketPath(t))
	defer stop()
	conn := connectViewer(t, address)
	awaitStatus(t, conn, "unavailable", "")
	payload := []byte(`{"topic":"summary","key":"ping","id":42}`)
	for i, part := range [][]byte{payload[:10], payload[10:]} {
		opcode := byte(0x01)
		if i == 1 {
			opcode = 0x80 // final continuation
		}
		frame := append([]byte{opcode, 0x80 | byte(len(part)), 0, 0, 0, 0}, part...)
		if _, err := conn.UnderlyingConn().Write(frame); err != nil {
			t.Fatal(err)
		}
	}
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	for {
		_, b, err := conn.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		var reply message
		if err := json.Unmarshal(b, &reply); err != nil {
			t.Fatal(err)
		}
		if reply.Topic == "summary" && reply.Key == "ping" {
			if reply.ID == nil || *reply.ID != 42 {
				t.Fatalf("invalid ping response: %s", b)
			}
			return
		}
	}
}

func TestViewerRejectsFragmentedInputFlood(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	observed := &observedListener{Listener: listener, closed: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	path := socketPath(t)
	go func() { done <- serve(ctx, path, observed) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("viewer server failed to stop")
		}
	})
	address := "ws://" + listener.Addr().String() + "/websocket"
	conn := connectViewer(t, address)
	awaitStatus(t, conn, "unavailable", "")
	select {
	case <-observed.closed:
		t.Fatal("connection closed before flood")
	default:
	}

	// One empty, non-final text frame followed by 8192 empty continuation
	// frames: 49,158 bytes of masked framing, but zero message payload bytes.
	batch := bytes.Repeat([]byte{0x00, 0x80, 0, 0, 0, 0}, 8193)
	batch[0] = 0x01
	raw := conn.UnderlyingConn()
	if err := raw.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Write(batch); err != nil {
		if e, ok := err.(net.Error); ok && e.Timeout() {
			t.Fatalf("fragment batch write timed out: %v", err)
		}
		// Closing during the bounded write is a valid rejection. The server
		// side is checked below.
	}
	select {
	case <-observed.closed:
		if observed.bytes.Load() <= 8192 {
			t.Fatalf("connection closed after only %d bytes", observed.bytes.Load())
		}
	case <-time.After(time.Second):
		t.Fatal("server did not close the flooded connection within 1s")
	}
}
