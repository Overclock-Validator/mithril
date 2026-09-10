package turbine

import (
	"compress/gzip"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// This capture stalled replay despite all 128 data shreds being present: the
// cluster producer padded the header, entry batches, and footer to full FEC
// batches. Keep signed packet bytes intact so a generator cannot hide the layout.
func TestAgavePaddedSlot1752420(t *testing.T) {
	packets := agavePaddedSlot1752420Packets(t)
	parentID := solana.MustHashFromBase58("6AbHyLMf4sRSxyjgKSHG8E9xE6iSpFqiHBLdxHkxsj5N")

	t.Run("components", func(t *testing.T) {
		shreds := make([]*Shred, len(packets))
		for i, packet := range packets {
			var err error
			shreds[i], err = ParseShred(packet)
			require.NoError(t, err)
		}
		components, err := DecodeComponentsFromDataShreds(shreds)
		require.NoError(t, err)
		require.Len(t, components, 4)
		require.NotNil(t, components[0].Marker)
		require.Equal(t, &BlockHeader{ParentSlot: 1752419, ParentBlockID: parentID}, components[0].Marker.Header)
		require.Len(t, components[1].EntryBatch, 64)
		require.NotNil(t, components[2].Marker)
		assertAgavePaddedSlot1752420Footer(t, components[2].Marker.Footer)
		require.Len(t, components[3].EntryBatch, 1)
		require.Empty(t, components[3].EntryBatch[0].Txns)
		entries := EntriesFromComponents(components)
		require.Len(t, entries, 65)
		txCount := 0
		for _, entry := range entries {
			txCount += len(entry.Txns)
		}
		require.Equal(t, 64, txCount)
	})

	t.Run("assembler", func(t *testing.T) {
		assembler := NewSlotAssembler()
		var completed *block.Block
		for i, packet := range packets {
			blk, err := assembler.AddPacket(packet)
			require.NoError(t, err, "packet %d", i)
			if blk != nil {
				require.Nil(t, completed, "slot must be delivered once")
				completed = blk
			}
		}
		require.NotNil(t, completed)
		require.Equal(t, uint64(1752420), completed.Slot)
		require.Equal(t, uint64(1752419), completed.SourceParentSlot)
		require.True(t, completed.HasAlpenglowParentBlockID)
		require.Equal(t, parentID, solana.Hash(completed.AlpenglowParentBlockID))
		require.True(t, completed.HasAlpenglowBlockID)
		require.Equal(t, "9tQ22tmp7HEo2QMqZnDjdwz6baRQWXuqWyL3zPfY1Wsh", solana.Hash(completed.AlpenglowBlockID).String())
		require.Len(t, completed.Entries, 65)
		require.Len(t, completed.Transactions, 64)
		require.True(t, completed.TransactionSignaturesVerified())
		require.True(t, completed.HasAlpenglowFooter)
		require.True(t, completed.HasExpectedBankhash)
		require.Equal(t, completed.BlockFinalCert, completed.AlpenglowFinalCert)
		assertAgavePaddedSlot1752420Footer(t, &BlockFooter{
			BankHash:               completed.ExpectedBankhash,
			BlockProducerTimeNanos: completed.FooterProducerTimeNanos,
			BlockFinalCert:         completed.BlockFinalCert,
			SkipRewardCert:         completed.SkipRewardCert,
			NotarRewardCert:        completed.NotarRewardCert,
		})
	})
}

func assertAgavePaddedSlot1752420Footer(t *testing.T, footer *BlockFooter) {
	t.Helper()
	require.NotNil(t, footer)
	require.Equal(t, "1KmAg7sHspJ1YcE1tKdrqssAiitMv3kNAVJvcZRVWjX", footer.BankHash.String())
	require.Equal(t, uint64(1788983416111639913), footer.BlockProducerTimeNanos)
	require.Empty(t, footer.BlockUserAgent)
	require.Empty(t, footer.SkipRewardCert)
	cert, err := UnmarshalFinalCertificate(footer.BlockFinalCert)
	require.NoError(t, err)
	require.Equal(t, uint64(1752418), cert.Slot)
	require.Nil(t, cert.NotarAggregate)
	require.GreaterOrEqual(t, len(footer.NotarRewardCert), 8)
	require.Equal(t, uint64(1752412), binary.LittleEndian.Uint64(footer.NotarRewardCert))
}

func agavePaddedSlot1752420Packets(t *testing.T) [][]byte {
	t.Helper()
	f, err := os.Open("testdata/agave_alpenglow_slot1752420.shreds.gz")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })
	gz, err := gzip.NewReader(f)
	require.NoError(t, err)
	data, err := io.ReadAll(gz)
	require.NoError(t, err)
	require.NoError(t, gz.Close())
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "s1752420.shreds"), data, 0600))
	spool, err := OpenShredSpool(dir, 0)
	require.NoError(t, err)
	t.Cleanup(spool.Close)
	packets, err := spool.ReadSlot(1752420)
	require.NoError(t, err)
	require.Len(t, packets, 128)
	return packets
}
