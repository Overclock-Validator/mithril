package turbine

import (
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestVerifySpooledShred(t *testing.T) {
	leader := testShredLeader(t)
	gen := ShredGenerator{Slot: 100, ParentSlot: 99, Version: 7}
	packets, _, _, _, err := gen.MakeShredsFromData(leader, make([]byte, 3000), true, solana.Hash{}, 0, 0)
	require.NoError(t, err)
	shred, err := ParseShred(packets[0])
	require.NoError(t, err)

	r := NewUDPReceiver("127.0.0.1:0")
	require.ErrorContains(t, r.verifySpooledShred(shred), "no leader schedule")

	other, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)
	leaders := map[uint64]solana.PublicKey{100: leader.PublicKey()}
	r.SetLeaderForSlot(func(slot uint64) (solana.PublicKey, bool) {
		pk, ok := leaders[slot]
		return pk, ok
	})
	require.NoError(t, r.verifySpooledShred(shred))

	leaders[100] = other.PublicKey()
	require.ErrorContains(t, r.verifySpooledShred(shred), "slot 100 shred 0")

	delete(leaders, 100)
	require.ErrorContains(t, r.verifySpooledShred(shred), "no leader known")
}
