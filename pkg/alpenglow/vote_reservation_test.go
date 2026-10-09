package alpenglow

import (
	"crypto/ed25519"
	"encoding/json"
	"os"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestReservedHistoryFormatAndIntegrity(t *testing.T) {
	key := ed25519.NewKeyFromSeed(make([]byte, 32))
	node := solana.PublicKey(key.Public().(ed25519.PublicKey))
	dir := t.TempDir()
	h := NewVoteHistory(node, 39)
	require.Error(t, SaveReservedVoteHistory(dir, h, key))
	h.ReservationRequired = true
	require.NoError(t, h.AddVote(NewSkipVote(44)))
	require.NoError(t, SaveReservedVoteHistory(dir, h, key))
	raw, err := os.ReadFile(VoteHistoryFilename(dir, node))
	require.NoError(t, err)
	var envelope savedVoteHistory
	require.NoError(t, json.Unmarshal(raw, &envelope))
	require.Equal(t, uint32(2), envelope.Version, "legacy reader must reject hybrid history")
	loaded, err := LoadVoteHistory(dir, node)
	require.NoError(t, err)
	require.True(t, loaded.HasSkipped(44))
	require.True(t, loaded.ReservationRequired)
	envelope.Data[10] ^= 1
	raw, err = json.Marshal(envelope)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(VoteHistoryFilename(dir, node), raw, 0600))
	_, err = LoadVoteHistory(dir, node)
	require.Error(t, err)
}

func BenchmarkVoteHistoryPersistence(b *testing.B) {
	key := ed25519.NewKeyFromSeed(make([]byte, 32))
	node := solana.PublicKey(key.Public().(ed25519.PublicKey))
	for _, reserved := range []bool{false, true} {
		name := "synchronous"
		if reserved {
			name = "reserved-write-rename"
		}
		b.Run(name, func(b *testing.B) {
			dir := b.TempDir()
			h := NewVoteHistory(node, 39)
			h.ReservationRequired = reserved
			for slot := uint64(40); slot < 72; slot++ {
				if err := h.AddVote(NewNotarizationVote(slot, solana.Hash{byte(slot)})); err != nil {
					b.Fatal(err)
				}
			}
			save := SaveVoteHistory
			if reserved {
				save = SaveReservedVoteHistory
			}
			if err := SaveVoteHistory(dir, h, key); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := save(dir, h, key); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
