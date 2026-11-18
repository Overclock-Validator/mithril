package corpuscmd

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/shredcap"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func runFromSpoolCmd(t *testing.T, args ...string) (string, error) {
	t.Helper()
	spoolDirs, bankHashLog, spoolCapture = nil, "", ""
	fromSlot, toSlot, spoolCheckpoint = 0, 0, 0
	var out bytes.Buffer
	CorpusCmd.SetOut(&out)
	CorpusCmd.SetErr(&out)
	CorpusCmd.SetArgs(append([]string{"from-spool"}, args...))
	err := CorpusCmd.Execute()
	return out.String(), err
}

func TestFromSpoolCommand(t *testing.T) {
	leader, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)
	spoolDir := t.TempDir()
	spool, err := turbine.OpenShredSpool(spoolDir, 0)
	require.NoError(t, err)
	var log strings.Builder
	for slot := uint64(200); slot <= 203; slot++ {
		gen := turbine.ShredGenerator{Slot: slot, ParentSlot: slot - 1, Version: 3}
		packets, _, _, _, err := gen.MakeShredsFromData(leader, bytes.Repeat([]byte{byte(slot)}, 3000), true, solana.Hash{}, 0, 0)
		require.NoError(t, err)
		for _, p := range packets {
			spool.Append(slot, p)
		}
		log.WriteString(strconv.FormatUint(slot, 10) + " " + solana.Hash{byte(slot)}.String() + "\n")
	}
	spool.Close()
	logPath := filepath.Join(t.TempDir(), "bankhash.log")
	require.NoError(t, os.WriteFile(logPath, []byte("# run:x\n"+log.String()), 0o644))

	// The run started from a snapshot at 198, but 199 is not in the spool.
	out := filepath.Join(t.TempDir(), "corpus")
	_, err = runFromSpoolCmd(t, out, "--spool", spoolDir, "--bank-hashes", logPath, "--checkpoint", "198")
	require.ErrorContains(t, err, "starts after slot 199, not after the checkpoint 198")
	require.NoDirExists(t, out)

	capture := filepath.Join(t.TempDir(), "run.pcapng.zst")
	text, err := runFromSpoolCmd(t, out, "--spool", spoolDir, "--bank-hashes", logPath, "--checkpoint", "199", "--to-slot", "202", "--capture", capture)
	require.NoError(t, err)
	require.Contains(t, text, "3 blocks 200..202 from checkpoint 199")
	require.Contains(t, text, "wrote 3 slots to "+capture)

	// The capture imports back to the same chain.
	f, err := os.Open(capture)
	require.NoError(t, err)
	defer f.Close()
	man, _, err := shredcap.ImportCorpus(f, filepath.Join(t.TempDir(), "again"), shredcap.ImportOptions{})
	require.NoError(t, err)
	require.Equal(t, []uint64{200, 201, 202}, man.Slots)
	require.Equal(t, uint16(3), man.ShredVersion)
}
