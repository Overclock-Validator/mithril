package rpcserver

import (
	"bufio"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestWebSocketControlValidation(t *testing.T) {
	for _, method := range []string{"xrpc.cancel", "xrpc.ch.val", "xrpc.ch.close"} {
		t.Run(method, func(t *testing.T) {
			server := NewRpcServer(nil, 0, &sealevel.SysvarEpochSchedule{}, solana.Hash{1})
			require.NoError(t, server.listener.Close())
			endpoint := httptest.NewServer(server)
			t.Cleanup(endpoint.Close)

			conn, err := net.DialTimeout("tcp", strings.TrimPrefix(endpoint.URL, "http://"), 5*time.Second)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })
			require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))

			// A valid RPC body passes the probe filter before the WebSocket upgrade.
			call := `{"jsonrpc":"2.0","id":7,"method":"getBlockHeight","params":[]}`
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, endpoint.URL, strings.NewReader(call))
			require.NoError(t, err)
			req.Header.Set("Connection", "Upgrade")
			req.Header.Set("Upgrade", "websocket")
			req.Header.Set("Sec-WebSocket-Version", "13")
			req.Header.Set("Sec-WebSocket-Key", "MDEyMzQ1Njc4OWFiY2RlZg==")
			require.NoError(t, req.Write(conn))
			reader := bufio.NewReader(conn)
			response, err := http.ReadResponse(reader, req)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, response.Body.Close()) })
			require.Equal(t, http.StatusSwitchingProtocols, response.StatusCode)

			for _, test := range []struct{ name, params string }{
				{"empty", `,"params":[]`},
				{"omitted", ""},
				{"null", `,"params":null`},
				{"object", `,"params":{}`},
				{"scalar", `,"params":1`},
				{"array_id", `,"params":[[]]`},
				{"object_id", `,"params":[{}]`},
				{"channel_without_value", `,"params":[0]`},
				{"invalid_channel", `,"params":[{},null]`},
			} {
				t.Run(test.name, func(t *testing.T) {
					require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))
					writeTestWebSocketText(t, conn, fmt.Sprintf(`{"jsonrpc":"2.0","method":%q%s}`, method, test.params))
					writeTestWebSocketText(t, conn, call)

					// The next RPC response proves the control frame was processed without a crash.
					var header [2]byte
					_, err := io.ReadFull(reader, header[:])
					require.NoError(t, err)
					require.Equal(t, byte(0x81), header[0], "expected a complete text frame")
					require.Less(t, int(header[1]), 126, "expected a short, unmasked response")
					payload := make([]byte, int(header[1]))
					_, err = io.ReadFull(reader, payload)
					require.NoError(t, err)
					require.JSONEq(t, fmt.Sprintf(`{"jsonrpc":"2.0","id":7,"result":%d}`, global.BlockHeight()), string(payload))
				})
			}
		})
	}
}

// writeTestWebSocketText writes a short, masked client frame over the upgraded connection.
func writeTestWebSocketText(t *testing.T, conn net.Conn, payload string) {
	t.Helper()
	require.Less(t, len(payload), 126)
	var mask [4]byte
	_, err := rand.Read(mask[:])
	require.NoError(t, err)
	frame := append([]byte{0x81, 0x80 | byte(len(payload))}, mask[:]...)
	for i := range len(payload) {
		frame = append(frame, payload[i]^mask[i%len(mask)])
	}
	n, err := conn.Write(frame)
	require.NoError(t, err)
	require.Equal(t, len(frame), n)
}
