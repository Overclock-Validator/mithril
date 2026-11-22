package dashboard

import (
	"net"
	"net/http"
	"sync"
	"testing"
	"time"
)

type observedWriteConn struct {
	net.Conn
	started chan struct{}
	once    sync.Once
}

func (c *observedWriteConn) Write(b []byte) (int, error) {
	c.once.Do(func() { close(c.started) })
	return c.Conn.Write(b)
}

func TestFastResponseInvalidationDuringBufferedFlush(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	conn := &observedWriteConn{Conn: server, started: make(chan struct{})}
	listener := &pipeListener{connections: make(chan net.Conn, 1), done: make(chan struct{})}
	listener.connections <- conn
	defer listener.Close()
	invalidated, handled := make(chan struct{}), make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handled)
		writeFastResponse(w, r, []byte(`{"x":1}`), time.Time{}, time.Now().Add(3*time.Second), invalidated, nil, func() bool { return true })
	})
	httpServer := &http.Server{Handler: h}
	defer httpServer.Close()
	go httpServer.Serve(listener)
	if _, err := client.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-conn.started:
	case <-time.After(time.Second):
		t.Fatal("response did not reach blocked connection")
	}
	close(invalidated)
	select {
	case <-handled:
	case <-time.After(time.Second):
		t.Fatal("invalidation did not interrupt flush")
	}
	client.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	buf := make([]byte, 4096)
	if n, _ := client.Read(buf); n != 0 {
		t.Fatalf("invalidated response sent %d bytes", n)
	}
}
