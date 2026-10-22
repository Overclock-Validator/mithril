package consensus

import (
	"crypto/ed25519"
	"os"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/identity"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// newIdentitySwapTestVoter enables voting for current with the authorized
// voter left empty unless authorized is given.
func newIdentitySwapTestVoter(t *testing.T, current, authorized ed25519.PrivateKey) (*identity.Holder, *alpenglowVoter, string) {
	t.Helper()
	holder, err := identity.NewHolder(current)
	require.NoError(t, err)
	blsKey := authorized
	if blsKey == nil {
		blsKey = current
	}
	voteAccount := solana.PublicKey(voterTestKey(93).Public().(ed25519.PublicKey))
	set := voterTestValidatorSet(t, current, blsKey, voteAccount)
	engine, err := NewEngine(Config{AlpenglowShredVersion: 0x1234, AlpenglowIdentity: holder})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	engine.SetAlpenglowEpochLookup(func(uint64) uint64 { return set.Epoch })
	require.NoError(t, engine.SetAlpenglowValidatorSet(set))
	engine.SetAlpenglowRoot(alpenglow.BlockID{Slot: 39, Hash: solana.Hash{0x39}})
	dir := t.TempDir()
	require.NoError(t, engine.EnableVoting(VotingConfig{
		AuthorizedVoter: authorized,
		VoteAccount:     voteAccount,
		HistoryDir:      dir,
		EpochForSlot:    func(uint64) uint64 { return set.Epoch },
		Peers:           func([]alpenglow.ValidatorStake) []alpenglow.VotorPeer { return nil },
	}))
	engine.voterMu.RLock()
	voter := engine.voter
	engine.voterMu.RUnlock()
	return holder, voter, dir
}

func blsPublicKey(t *testing.T, key ed25519.PrivateKey) [48]byte {
	t.Helper()
	signer, err := alpenglow.DeriveBLSSigner(key)
	require.NoError(t, err)
	return signer.PublicKeyCompressed()
}

func TestVoterMovesToNewIdentityHistoryOnSwap(t *testing.T) {
	oldKey, newKey := voterTestKey(91), voterTestKey(92)
	oldNode := solana.PublicKey(oldKey.Public().(ed25519.PublicKey))
	newNode := solana.PublicKey(newKey.Public().(ed25519.PublicKey))
	holder, voter, dir := newIdentitySwapTestVoter(t, oldKey, nil)
	_, err := os.Stat(alpenglow.VoteHistoryFilename(dir, newNode))
	require.ErrorIs(t, err, os.ErrNotExist)

	_, err = holder.Swap(newKey)
	require.NoError(t, err)

	// Swap returns only after the voter loop has adopted the new identity.
	require.Equal(t, newNode, voter.node)
	require.Equal(t, newNode, voter.history.NodePubkey)
	require.Equal(t, uint64(39), voter.history.Root)
	// The defaulted authorized voter follows the identity.
	require.Equal(t, blsPublicKey(t, newKey), voter.signer.PublicKeyCompressed())
	history, err := alpenglow.LoadVoteHistory(dir, newNode)
	require.NoError(t, err)
	require.Equal(t, newNode, history.NodePubkey)
	// The old identity's history lock was released.
	lock, err := alpenglow.LockVoteHistory(dir, oldNode)
	require.NoError(t, err)
	require.NoError(t, lock.Close())
}

func TestVoterKeepsConfiguredAuthorizedVoterOnSwap(t *testing.T) {
	authorized := voterTestKey(94)
	holder, voter, _ := newIdentitySwapTestVoter(t, voterTestKey(91), authorized)

	_, err := holder.Swap(voterTestKey(92))
	require.NoError(t, err)
	require.Equal(t, blsPublicKey(t, authorized), voter.signer.PublicKeyCompressed())
}

func TestVoterRejectsSwapToUnreadableHistory(t *testing.T) {
	oldKey, newKey := voterTestKey(91), voterTestKey(92)
	oldNode := solana.PublicKey(oldKey.Public().(ed25519.PublicKey))
	newNode := solana.PublicKey(newKey.Public().(ed25519.PublicKey))
	holder, voter, dir := newIdentitySwapTestVoter(t, oldKey, nil)
	require.NoError(t, os.WriteFile(alpenglow.VoteHistoryFilename(dir, newNode), []byte("corrupt"), 0o600))

	_, err := holder.Swap(newKey)
	require.ErrorContains(t, err, "refuse unsafe vote-history reset for "+newNode.String())
	require.Equal(t, oldNode, holder.PublicKey())
	require.Equal(t, oldNode, voter.node)
	require.Equal(t, blsPublicKey(t, oldKey), voter.signer.PublicKeyCompressed())
	// The voter still holds and can persist its own history.
	require.NoError(t, voter.saveHistory())
}
