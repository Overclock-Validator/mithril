package dashboard

import (
	"bytes"
	"encoding/json"
	"net"
	"testing"
	"time"
)

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
	address, stop := startViewerServer(t, socketPath(t))
	defer stop()
	conn := connectViewer(t, address)
	awaitStatus(t, conn, "unavailable", "")

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
		// Closing during the bounded write is a valid rejection. The read
		// below still checks that the peer actually disconnected.
	}
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			if e, ok := err.(net.Error); ok && e.Timeout() {
				t.Fatal("fragment flood was not disconnected within 1s")
			}
			return
		}
	}
}
