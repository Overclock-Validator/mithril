package rpcserver

import (
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/identity"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/stretchr/testify/require"
)

func TestGetIdentityTracksHolderSwap(t *testing.T) {
	seed := make([]byte, ed25519.SeedSize)
	first := ed25519.NewKeyFromSeed(seed)
	seed[0] = 1
	second := ed25519.NewKeyFromSeed(seed)
	holder, err := identity.NewHolder(first)
	require.NoError(t, err)
	firstIdentity := holder.PublicKey().String()
	server := &RpcServer{}
	source := func() string {
		server.SetIdentity("fallback")
		return holder.PublicKey().String()
	}
	server.SetIdentitySource(source)
	dispatcher := jsonrpc.NewServer(jsonrpc.WithServerMethodNameFormatter(func(_, method string) string {
		return strings.ToLower(method[:1]) + method[1:]
	}))
	dispatcher.Register("MithrilRpc", rpcHandler{server})
	readIdentity := func() (string, error) {
		response := httptest.NewRecorder()
		dispatcher.HandleRequest(t.Context(), strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"getIdentity"}`), response)
		var reply struct {
			Result GetIdentityResp       `json:"result"`
			Error  *jsonrpc.JSONRPCError `json:"error"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &reply); err != nil {
			return "", err
		}
		if reply.Error != nil {
			return "", fmt.Errorf("getIdentity: %v", reply.Error)
		}
		return reply.Result.Identity, nil
	}
	got, err := readIdentity()
	require.NoError(t, err)
	require.Equal(t, holder.PublicKey().String(), got)
	_, err = holder.Swap(second)
	require.NoError(t, err)
	got, err = readIdentity()
	require.NoError(t, err)
	require.Equal(t, holder.PublicKey().String(), got)
	secondIdentity := holder.PublicKey().String()
	errors := make(chan error, 4)
	var readers sync.WaitGroup
	for range 4 {
		readers.Go(func() {
			for range 64 {
				got, err := readIdentity()
				if err != nil {
					errors <- err
					return
				}
				if got != firstIdentity && got != secondIdentity {
					errors <- fmt.Errorf("getIdentity returned unknown identity %q", got)
					return
				}
			}
		})
	}
	for range 64 {
		_, err = holder.Swap(first)
		require.NoError(t, err)
		_, err = holder.Swap(second)
		require.NoError(t, err)
		server.SetIdentitySource(source)
	}
	readers.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	server.SetIdentitySource(nil)
	got, err = readIdentity()
	require.NoError(t, err)
	require.Equal(t, "fallback", got)
}
