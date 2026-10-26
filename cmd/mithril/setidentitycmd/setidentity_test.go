package setidentitycmd

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/adminrpc"
	"github.com/Overclock-Validator/mithril/pkg/identity"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// recordingSetter remembers the flags of the last request it forwards.
type recordingSetter struct {
	*adminrpc.IdentityService
	requireTower, requireVoteHistory bool
}

func (r *recordingSetter) SetIdentity(key []byte, requireTower, requireVoteHistory bool) error {
	r.requireTower, r.requireVoteHistory = requireTower, requireVoteHistory
	return r.IdentityService.SetIdentity(key, requireTower, requireVoteHistory)
}

type testNode struct {
	ledger string
	holder *identity.Holder
	setter *recordingSetter
	stakes map[solana.PublicKey]uint64
}

func startNode(t *testing.T) *testNode {
	t.Helper()
	ledger, err := os.MkdirTemp("/tmp", "setid") // short enough for sun_path on macOS
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(ledger) })
	holder, err := identity.NewEphemeralHolder()
	require.NoError(t, err)
	n := &testNode{ledger: ledger, holder: holder, stakes: map[solana.PublicKey]uint64{}}
	n.setter = &recordingSetter{IdentityService: &adminrpc.IdentityService{
		Holder:      holder,
		StakedNodes: func() (map[solana.PublicKey]uint64, bool) { return n.stakes, true },
	}}
	server, err := adminrpc.Listen(adminrpc.SocketPath(ledger), n.setter)
	require.NoError(t, err)
	t.Cleanup(func() { server.Close() })
	return n
}

func writeKeypair(t *testing.T, dir string, key []byte) string {
	t.Helper()
	values := make([]int, len(key))
	for i, b := range key {
		values[i] = int(b)
	}
	data, err := json.Marshal(values)
	require.NoError(t, err)
	path := filepath.Join(dir, "keypair.json")
	require.NoError(t, os.WriteFile(path, data, 0o600))
	return path
}

func newKey(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return key
}

func run(args ...string) (string, error) {
	cmd := Command()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

func TestSetIdentityFromKeypairFile(t *testing.T) {
	n := startNode(t)
	key := newKey(t)
	pubkey := solana.PublicKeyFromBytes(key.Public().(ed25519.PublicKey))
	// The file only needs to be readable by the CLI; the node gets the bytes.
	path := writeKeypair(t, t.TempDir(), key)

	out, err := run("--ledger", n.ledger, path)
	require.NoError(t, err)
	require.Contains(t, out, "Validator identity set to "+pubkey.String())
	require.Equal(t, pubkey, n.holder.PublicKey())
	require.False(t, n.setter.requireTower)
	require.True(t, n.setter.requireVoteHistory, "require_vote_history defaults to true")

	_, err = run("--ledger", n.ledger, "--require-vote-history=false", writeKeypair(t, t.TempDir(), newKey(t)))
	require.NoError(t, err)
	require.False(t, n.setter.requireVoteHistory)
	_, err = run("--ledger", n.ledger, "--do-not-require-vote-history", writeKeypair(t, t.TempDir(), newKey(t)))
	require.NoError(t, err)
	require.False(t, n.setter.requireVoteHistory)
}

func TestSetIdentityToUnstakedKeypairFile(t *testing.T) {
	n := startNode(t)
	staked := newKey(t)
	stakedPubkey := solana.PublicKeyFromBytes(staked.Public().(ed25519.PublicKey))
	n.stakes[stakedPubkey] = 1
	_, err := run("--ledger", n.ledger, writeKeypair(t, t.TempDir(), staked))
	require.NoError(t, err)
	require.Equal(t, stakedPubkey, n.holder.PublicKey())

	// Staked to unstaked takes the node out of voting.
	random := newKey(t)
	out, err := run("--ledger", n.ledger, writeKeypair(t, t.TempDir(), random))
	require.NoError(t, err)
	require.Equal(t, solana.PublicKeyFromBytes(random.Public().(ed25519.PublicKey)), n.holder.PublicKey())
	require.Contains(t, out, "Validator identity set to "+n.holder.PublicKey().String())
}

func TestSetIdentityRequiresKeypair(t *testing.T) {
	n := startNode(t)
	before := n.holder.PublicKey()
	_, err := run("--ledger", n.ledger)
	require.ErrorContains(t, err, "accepts 1 arg(s), received 0")
	require.Equal(t, before, n.holder.PublicKey())
}

func TestSetIdentityFailures(t *testing.T) {
	n := startNode(t)
	start := n.holder.PublicKey()

	_, err := run("--ledger", filepath.Join(n.ledger, "missing"), writeKeypair(t, t.TempDir(), newKey(t)))
	require.ErrorContains(t, err, "connect to admin RPC at "+filepath.Join(n.ledger, "missing", "admin.rpc"))

	_, err = run("--ledger", n.ledger, writeKeypair(t, t.TempDir(), newKey(t)[:32]))
	require.ErrorContains(t, err, "set-identity: Failed to read identity keypair: identity keypair has 32 bytes, want 64")

	staked := newKey(t)
	n.stakes[start] = 1
	n.stakes[solana.PublicKeyFromBytes(staked.Public().(ed25519.PublicKey))] = 1
	_, err = run("--ledger", n.ledger, writeKeypair(t, t.TempDir(), staked))
	require.ErrorContains(t, err, "to staked node")
	require.ErrorContains(t, err, "is not supported")

	_, err = run("--ledger", n.ledger, "--require-tower", writeKeypair(t, t.TempDir(), newKey(t)))
	require.ErrorContains(t, err, "Mithril has no tower file for this identity")

	n.setter.VoteHistoryRequired = func(solana.PublicKey) bool { return true }
	n.setter.LoadVoteHistory = func(solana.PublicKey) error { return os.ErrNotExist }
	_, err = run("--ledger", n.ledger, writeKeypair(t, t.TempDir(), newKey(t)))
	require.ErrorContains(t, err, "Unable to load vote history file")

	require.Equal(t, start, n.holder.PublicKey())
}
