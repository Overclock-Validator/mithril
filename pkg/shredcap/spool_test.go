package shredcap

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestReadBankHashLog(t *testing.T) {
	a, b := solana.Hash{1}, solana.Hash{2}
	log := "# run:x started:y slots:1-2\n10 " + a.String() + "\n11 " + a.String() + "\n# run:z\n11 " + b.String() + "\n\n"
	got, err := ReadBankHashLog(strings.NewReader(log))
	require.NoError(t, err)
	require.Equal(t, map[uint64][32]byte{10: a, 11: b}, got, "a slot logged again keeps its last hash")

	_, err = ReadBankHashLog(strings.NewReader("10\n"))
	require.ErrorContains(t, err, "line 1")
	_, err = ReadBankHashLog(strings.NewReader("x " + a.String() + "\n"))
	require.Error(t, err)
}

// spoolSlots writes each slot's packets into a spool directory, as a node does.
func spoolSlots(t *testing.T, dir string, packets map[uint64][][]byte, slots ...uint64) {
	t.Helper()
	spool, err := turbine.OpenShredSpool(dir, 0)
	require.NoError(t, err)
	for _, slot := range slots {
		for _, p := range packets[slot] {
			spool.Append(slot, p)
		}
	}
	spool.Close()
}

// A run's archive holds the replayed slots behind the floor and its spool the
// newest ones. The corpus is the parent chain to the highest slot with a
// logged bank hash, with the shreds of both directories.
func TestImportSpoolBuildsTheChainFromArchiveAndSpool(t *testing.T) {
	leader, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)
	parents := map[uint64]uint64{100: 99, 101: 100, 102: 101, 103: 100, 104: 102}
	packets := map[uint64][][]byte{}
	for slot, parent := range parents {
		packets[slot] = slotPackets(t, leader, slot, parent)
	}
	archive, spool := t.TempDir(), t.TempDir()
	spoolSlots(t, archive, packets, 100, 101, 103) // 103 is an abandoned fork
	spoolSlots(t, spool, packets, 102, 104)
	hashes := map[uint64][32]byte{100: {100}, 101: {101}, 102: {102}, 103: {103}, 104: {104}}

	out := filepath.Join(t.TempDir(), "corpus")
	man, stats, spoolStats, err := ImportSpool([]string{archive, spool}, hashes, out, ImportOptions{})
	require.NoError(t, err)
	// The replayed chain runs back from 104; the abandoned fork 103 is left out.
	require.Equal(t, []uint64{100, 101, 102, 104}, man.Slots)
	require.Equal(t, uint64(99), man.Checkpoint)
	require.Equal(t, solana.Hash([32]byte{101}).String(), man.BankHashes["101"])
	require.Contains(t, stats.OffChain, uint64(103))
	require.Empty(t, stats.CountMismatch, "the records carry each slot's real data shred count")
	require.Equal(t, 5, spoolStats.SlotFiles)
	requireCorpus(t, out, packets, man.Slots)

	// The range bounds what is read: from 101 to 102, the chain starts after 100.
	man, _, _, err = ImportSpool([]string{archive, spool}, hashes, filepath.Join(t.TempDir(), "c2"), ImportOptions{FromSlot: 101, ToSlot: 102})
	require.NoError(t, err)
	require.Equal(t, []uint64{101, 102}, man.Slots)
	require.Equal(t, uint64(100), man.Checkpoint)

	_, _, _, err = ImportSpool([]string{archive}, hashes, filepath.Join(t.TempDir(), "c3"), ImportOptions{FromSlot: 500})
	require.ErrorContains(t, err, "no spooled slots")
}

// A slot missing from the run (pruned before the archive was on) breaks the
// chain: it then starts after the gap, which the command's --checkpoint
// check catches.
func TestImportSpoolChainStopsAtAGap(t *testing.T) {
	leader, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)
	packets := map[uint64][][]byte{}
	for slot := uint64(100); slot <= 103; slot++ {
		packets[slot] = slotPackets(t, leader, slot, slot-1)
	}
	dir := t.TempDir()
	spoolSlots(t, dir, packets, 100, 102, 103) // 101 lost
	hashes := map[uint64][32]byte{100: {1}, 102: {2}, 103: {3}}
	man, _, _, err := ImportSpool([]string{dir}, hashes, filepath.Join(t.TempDir(), "c"), ImportOptions{})
	require.NoError(t, err)
	require.Equal(t, []uint64{102, 103}, man.Slots)
	require.Equal(t, uint64(101), man.Checkpoint, "not the run's starting slot 99")
}
