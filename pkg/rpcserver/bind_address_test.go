package rpcserver

import (
	"bufio"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestRPCBindAddressLoopbackServesHTTPAndWebSocket(t *testing.T) {
	hash := solana.Hash{1}
	rpc, err := NewRpcServerWithBindAddress(nil, "127.0.0.1", 0, &sealevel.SysvarEpochSchedule{}, hash)
	require.NoError(t, err)
	addr := rpc.listener.Addr().(*net.TCPAddr)
	require.True(t, addr.IP.Equal(net.ParseIP("127.0.0.1")), "private RPC must not bind a wildcard interface")
	// Use the production listener without starting validator or cluster discovery.
	server := &http.Server{Handler: rpc}
	done := make(chan error, 1)
	go func() { done <- server.Serve(rpc.listener) }()
	t.Cleanup(func() {
		require.NoError(t, server.Close())
		require.ErrorIs(t, <-done, http.ErrServerClosed)
	})
	url := "http://" + addr.String()
	call := `{"jsonrpc":"2.0","id":7,"method":"getGenesisHash","params":[]}`
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Post(url, "application/json", strings.NewReader(call))
	require.NoError(t, err)
	var body struct{ Result string }
	require.NoError(t, json.NewDecoder(response.Body).Decode(&body))
	require.NoError(t, response.Body.Close())
	require.Equal(t, hash.String(), body.Result)

	conn, err := net.DialTimeout("tcp", addr.String(), 5*time.Second)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
	req, err := http.NewRequest(http.MethodGet, url, strings.NewReader(call))
	require.NoError(t, err)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", "MDEyMzQ1Njc4OWFiY2RlZg==")
	require.NoError(t, req.Write(conn))
	reader := bufio.NewReader(conn)
	upgrade, err := http.ReadResponse(reader, req)
	require.NoError(t, err)
	defer upgrade.Body.Close()
	require.Equal(t, http.StatusSwitchingProtocols, upgrade.StatusCode)
	writeTestWebSocketText(t, conn, call)
	var header [2]byte
	_, err = io.ReadFull(reader, header[:])
	require.NoError(t, err)
	require.Equal(t, byte(0x81), header[0])
	require.Less(t, int(header[1]), 126)
	payload := make([]byte, int(header[1]))
	_, err = io.ReadFull(reader, payload)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(payload, &body))
	require.Equal(t, hash.String(), body.Result)
}

func TestRPCBindAddressRejectsAmbiguousValues(t *testing.T) {
	for _, address := range []string{"", "localhost", "127.0.0.1:8899", "[::1]", " 127.0.0.1", "https://127.0.0.1"} {
		t.Run(address, func(t *testing.T) {
			server, err := NewRpcServerWithBindAddress(nil, address, 0, &sealevel.SysvarEpochSchedule{}, solana.Hash{1})
			require.ErrorContains(t, err, "literal IP address")
			require.Nil(t, server)
		})
	}
}

func TestRPCDefaultConstructorKeepsWildcardBinding(t *testing.T) {
	server := NewRpcServer(nil, 0, &sealevel.SysvarEpochSchedule{}, solana.Hash{1})
	defer server.listener.Close()
	require.True(t, server.listener.Addr().(*net.TCPAddr).IP.IsUnspecified())
}
