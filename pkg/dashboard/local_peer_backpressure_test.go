package dashboard

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The public server expects a TCP listener address. Connections themselves use
// the existing unbuffered pipe fixture so a nonreader really blocks Write.
type localPeerPipeListener struct{ *pipeListener }

func (*localPeerPipeListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}
}

type localPeerClosingConn struct {
	net.Conn
	closed chan struct{}
	once   sync.Once
}

func (c *localPeerClosingConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() { close(c.closed) })
	return err
}

func TestLocalPeerPublicSlowWriters(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	now := time.Now()
	snapshot := testSnapshot(now.UTC())
	detail := localPeerTestDetail(LocalPeerCapacity)
	snapshot.GenesisHash = detail.Binding.GenesisHash
	detail.Binding = NodeNetworkBinding{RunID: snapshot.RunID, NodeStartedAt: snapshot.StartedAt, GenesisHash: snapshot.GenesisHash}
	detail.ClientStartedAt, detail.RunStartedAt = snapshot.StartedAt, snapshot.StartedAt
	detail.CapturedAt, detail.SampledAt = now.UTC(), now.UTC()
	for i := range detail.Rows {
		detail.Rows[i].ObservedAt = now.UTC()
	}
	prepared, err := PrepareLocalPeers(detail)
	require.NoError(t, err)
	var samples atomic.Int64
	path := socketPath(t)
	feed, err := ListenFeedWithDetails(ctx, path, "", func() Snapshot {
		s := snapshot
		s.SampledAt = time.Now().UTC()
		return s
	}, DetailSamplers{LocalPeers: func(time.Time) (LocalPeerCapture, error) {
		samples.Add(1)
		return LocalPeerCapture{Data: prepared, FreshUntil: now.Add(30 * time.Second)}, nil
	}})
	require.NoError(t, err)
	defer feed.Close()
	require.Eventually(t, func() bool { return feed.localPeers.latest.Load() != nil }, 2*time.Second, time.Millisecond)

	listener := &localPeerPipeListener{&pipeListener{connections: make(chan net.Conn, 8), done: make(chan struct{})}}
	done := make(chan error, 1)
	go func() {
		done <- serveProfile(ctx, path, listener, "https://stats.example", PublicOptions{LocalPeers: true})
	}()
	defer func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(3 * time.Second):
			t.Error("public reader and its collection worker did not join")
		}
	}()
	transport := &http.Transport{DisableKeepAlives: true, DialContext: func(context.Context, string, string) (net.Conn, error) {
		server, client := net.Pipe()
		listener.connections <- server
		return client, nil
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second}
	get := func(path string) (int, int64) {
		t.Helper()
		response, err := client.Get("http://stats.example" + path)
		require.NoError(t, err)
		n, err := io.Copy(io.Discard, response.Body)
		require.NoError(t, err)
		require.NoError(t, response.Body.Close())
		return response.StatusCode, n
	}
	require.Eventually(t, func() bool {
		status, n := get(localPeerPublicPath)
		return status == http.StatusOK && n == int64(prepared.Bytes())
	}, 5*time.Second, 600*time.Millisecond, "maximum body did not reach the actual public reader")

	var blocked [2]*localPeerClosingConn
	before := samples.Load()
	started := time.Now()
	for i := range blocked {
		server, client := net.Pipe()
		blocked[i] = &localPeerClosingConn{Conn: server, closed: make(chan struct{})}
		listener.connections <- blocked[i]
		defer client.Close()
		require.NoError(t, client.SetDeadline(time.Now().Add(2*time.Second)))
		_, err := fmt.Fprint(client, "GET "+localPeerPublicPath+" HTTP/1.1\r\nHost: stats.example\r\nConnection: close\r\n\r\n")
		require.NoError(t, err)
		response, err := http.ReadResponse(bufio.NewReader(client), nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, response.StatusCode)
		require.NotEmpty(t, response.Header.Get(replayDetailRemainingHeader))
		// Do not read/close Body: it is larger than the header reader buffer.
		require.NoError(t, client.SetDeadline(time.Time{}))
	}
	// Refill the request limiter while both body writers remain blocked, so
	// the refusal below exercises the two-writer admission bound, not rate alone.
	time.Sleep(time.Second)
	for _, conn := range blocked {
		select {
		case <-conn.closed:
			t.Fatal("slow connection closed before the backpressure assertion")
		default:
		}
	}
	status, _ := get(localPeerPublicPath)
	require.Equal(t, http.StatusTooManyRequests, status)
	status, _ = get("/api/v1/status")
	require.Equal(t, http.StatusOK, status, "slow peer bodies blocked ordinary status")
	require.Equal(t, before, samples.Load(), "viewer requests sampled the node")
	deadline := time.NewTimer(time.Until(started.Add(7 * time.Second)))
	defer deadline.Stop()
	for _, conn := range blocked {
		select {
		case <-conn.closed:
		case <-deadline.C:
			t.Fatal("public HTTP write deadline did not release both slow viewers")
		}
	}
	status, n := get(localPeerPublicPath)
	require.Equal(t, http.StatusOK, status, "writer slots were not released")
	require.Equal(t, int64(prepared.Bytes()), n)
	t.Logf("max body=%d bytes; two nonreaders closed and admission recovered in %s", prepared.Bytes(), time.Since(started))
}
