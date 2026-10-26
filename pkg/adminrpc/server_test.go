package adminrpc

import (
	"bufio"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/identity"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// shortTempDir keeps socket paths under the 104-byte sun_path limit on macOS.
func shortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "arpc")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func newKey(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return key
}

func pubkeyOf(key ed25519.PrivateKey) solana.PublicKey {
	return solana.PublicKeyFromBytes(key.Public().(ed25519.PublicKey))
}

type testNode struct {
	holder  *identity.Holder
	service *IdentityService
	server  *Server
	ledger  string
	stakes  map[solana.PublicKey]uint64
}

func startTestNode(t *testing.T) *testNode {
	t.Helper()
	holder, err := identity.NewHolder(newKey(t))
	require.NoError(t, err)
	n := &testNode{holder: holder, ledger: shortTempDir(t), stakes: map[solana.PublicKey]uint64{}}
	n.service = &IdentityService{
		Holder:      holder,
		StakedNodes: func() (map[solana.PublicKey]uint64, bool) { return n.stakes, true },
	}
	n.server, err = Listen(SocketPath(n.ledger), n.service)
	require.NoError(t, err)
	t.Cleanup(func() { n.server.Close() })
	return n
}

// rawCall writes one request line exactly as given and returns the reply line.
func rawCall(t *testing.T, conn net.Conn, reader *bufio.Reader, line string) string {
	t.Helper()
	_, err := conn.Write([]byte(line + "\n"))
	require.NoError(t, err)
	reply, err := reader.ReadString('\n')
	require.NoError(t, err)
	return strings.TrimSuffix(reply, "\n")
}

func bytesParam(key []byte) string {
	values := make([]string, len(key))
	for i, b := range key {
		values[i] = fmt.Sprint(b)
	}
	return "[" + strings.Join(values, ",") + "]"
}

func TestSetIdentityFromBytesAgaveWireShape(t *testing.T) {
	n := startTestNode(t)
	conn, err := net.Dial("unix", SocketPath(n.ledger))
	require.NoError(t, err)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	first := newKey(t)
	reply := rawCall(t, conn, reader, `{"jsonrpc":"2.0","id":1,"method":"setIdentityFromBytes","params":[`+bytesParam(first)+`, false, false]}`)
	require.Equal(t, `{"jsonrpc":"2.0","result":null,"id":1}`, reply)
	require.Equal(t, pubkeyOf(first), n.holder.PublicKey())

	second := newKey(t)
	reply = rawCall(t, conn, reader, `{"jsonrpc":"2.0","id":2,"method":"setIdentityFromBytes","params":[`+bytesParam(second)+`, false, false]}`)
	require.Equal(t, `{"jsonrpc":"2.0","result":null,"id":2}`, reply)
	require.Equal(t, pubkeyOf(second), n.holder.Current().PublicKey())
}

func TestSetIdentityReadsKeypairFile(t *testing.T) {
	n := startTestNode(t)
	key := newKey(t)
	path := filepath.Join(n.ledger, "id.json")
	require.NoError(t, os.WriteFile(path, []byte(bytesParam(key)), 0o600))
	client, err := Dial(n.ledger)
	require.NoError(t, err)
	defer client.Close()

	require.NoError(t, client.SetIdentity(path, false, false))
	require.Equal(t, pubkeyOf(key), n.holder.PublicKey())
	var rpcErr *Error
	require.ErrorAs(t, client.SetIdentity(filepath.Join(n.ledger, "missing.json"), false, false), &rpcErr)
	require.Equal(t, CodeInvalidParams, rpcErr.Code)
}

func requireRPCError(t *testing.T, err error, code int, contains string) {
	t.Helper()
	var rpcErr *Error
	require.ErrorAs(t, err, &rpcErr)
	require.Equal(t, code, rpcErr.Code)
	require.Contains(t, rpcErr.Message, contains)
}

func TestSetIdentityRejections(t *testing.T) {
	n := startTestNode(t)
	client, err := Dial(n.ledger)
	require.NoError(t, err)
	defer client.Close()
	start := n.holder.PublicKey()

	requireRPCError(t, client.SetIdentityFromBytes(newKey(t)[:32], false, false), CodeInvalidParams, "has 32 bytes, want 64")

	staked := newKey(t)
	n.stakes[start] = 10
	n.stakes[pubkeyOf(staked)] = 20
	requireRPCError(t, client.SetIdentityFromBytes(staked, false, false), CodeInvalidParams,
		"Changing identity from staked node "+start.String()+" to staked node "+pubkeyOf(staked).String()+" is not supported")

	requireRPCError(t, client.SetIdentityFromBytes(newKey(t), true, false), CodeInvalidParams, "Mithril has no tower file for this identity")

	n.service.VoteHistoryRequired = func(solana.PublicKey) bool { return true }
	n.service.LoadVoteHistory = func(solana.PublicKey) error { return os.ErrNotExist }
	requireRPCError(t, client.SetIdentityFromBytes(newKey(t), false, true), CodeInvalidParams, "Unable to load vote history file")

	n.holder.RegisterUpdater("failing", func(*identity.Keypair) error { return errors.New("certificate refused") })
	requireRPCError(t, client.SetIdentityFromBytes(newKey(t), false, false), CodeInternalError, "to failing: certificate refused")

	require.Equal(t, start, n.holder.PublicKey())
}

func TestSetIdentityStakeTransitions(t *testing.T) {
	n := startTestNode(t)
	client, err := Dial(n.ledger)
	require.NoError(t, err)
	defer client.Close()

	// Unstaked to staked, then staked to unstaked.
	staked := newKey(t)
	n.stakes[pubkeyOf(staked)] = 10
	require.NoError(t, client.SetIdentityFromBytes(staked, false, false))
	require.NoError(t, client.SetIdentityFromBytes(newKey(t), false, false))
	// The same staked key again is accepted, even with another staked node.
	n.stakes[n.holder.PublicKey()] = 10
	require.NoError(t, client.SetIdentityFromBytes(n.holder.Current().PrivateKey(), false, false))
	// A vote history is only checked when the vote account needs one.
	n.service.VoteHistoryRequired = func(solana.PublicKey) bool { return false }
	n.service.LoadVoteHistory = func(solana.PublicKey) error { return os.ErrNotExist }
	delete(n.stakes, n.holder.PublicKey())
	require.NoError(t, client.SetIdentityFromBytes(newKey(t), false, true))
}

func TestServerProtocolErrors(t *testing.T) {
	n := startTestNode(t)
	conn, err := net.Dial("unix", SocketPath(n.ledger))
	require.NoError(t, err)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	var resp response
	require.NoError(t, json.Unmarshal([]byte(rawCall(t, conn, reader, `{"jsonrpc":"2.0","id":7,"method":"exit","params":[]}`)), &resp))
	require.Equal(t, CodeMethodNotFound, resp.Error.Code)
	require.NoError(t, json.Unmarshal([]byte(rawCall(t, conn, reader, `{"jsonrpc":"2.0","id":8,"method":"setIdentityFromBytes","params":[[1,2]]}`)), &resp))
	require.Equal(t, CodeInvalidParams, resp.Error.Code)
	require.NoError(t, json.Unmarshal([]byte(rawCall(t, conn, reader, `{"jsonrpc":"2.0","id":9,"method":"setIdentityFromBytes","params":[[256], false, false]}`)), &resp))
	require.Equal(t, CodeInvalidParams, resp.Error.Code)
	require.NoError(t, json.Unmarshal([]byte(rawCall(t, conn, reader, `{not json`)), &resp))
	require.Equal(t, CodeParseError, resp.Error.Code)
}

func TestSocketPermissionsAndCleanup(t *testing.T) {
	n := startTestNode(t)
	path := SocketPath(n.ledger)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.NotZero(t, info.Mode()&os.ModeSocket)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	// A second node cannot take over a live socket.
	_, err = Listen(path, n.service)
	require.ErrorContains(t, err, "in use by another process")

	require.NoError(t, n.server.Close())
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = Dial(n.ledger)
	require.Error(t, err)
}
