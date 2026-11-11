// Package corpuscmd converts between replay corpora and Firedancer's shredcap
// capture format.
package corpuscmd

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/Overclock-Validator/mithril/pkg/shredcap"
	"github.com/klauspost/compress/zstd"
	"github.com/spf13/cobra"
)

var (
	fromSlot, toSlot, snapshotSlot uint64

	CorpusCmd = cobra.Command{
		Use:   "corpus",
		Short: "Replay corpus tools",
		Long: `Convert between Mithril replay corpora and Firedancer shredcap captures.

A replay corpus is a directory with shreds/s<slot>.shreds, complete.idx and
manifest.json. A benchmark replays it offline (mithril run --offline) from the
manifest's checkpoint slot, which the node's AccountsDB must already hold, and
checks every block's bank hash against the manifest.`,
	}

	importCmd = cobra.Command{
		Use:   "import-shredcap CAPTURE OUTDIR",
		Short: "Make a replay corpus from a shredcap capture (.pcapng or .pcapng.zst)",
		Args:  cobra.ExactArgs(2),
		RunE:  runImport,
	}

	exportCmd = cobra.Command{
		Use:   "export-shredcap CORPUS OUTFILE",
		Short: "Write a replay corpus as a shredcap capture for firedancer-dev backtest (zstd-compressed when OUTFILE ends in .zst)",
		Args:  cobra.ExactArgs(2),
		RunE:  runExport,
	}
)

func init() {
	importCmd.Flags().Uint64Var(&fromSlot, "from-slot", 0, "First slot to import")
	importCmd.Flags().Uint64Var(&toSlot, "to-slot", 0, "Last slot to import (0 = no limit)")
	exportCmd.Flags().Uint64Var(&snapshotSlot, "snapshot-slot", 0, "Slot of the snapshot backtest starts from (0 = the corpus checkpoint); a capture whose first block is later starts with a placeholder shred for this slot")
	CorpusCmd.AddCommand(&importCmd, &exportCmd)
}

func runImport(cmd *cobra.Command, args []string) error {
	in, err := os.Open(args[0])
	if err != nil {
		return err
	}
	defer in.Close()
	man, stats, err := shredcap.ImportCorpus(in, args[1], shredcap.ImportOptions{
		FromSlot: fromSlot, ToSlot: toSlot, Source: filepath.Base(args[0]),
	})
	if err != nil {
		return err
	}
	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "corpus %s: %d blocks %d..%d from checkpoint %d, %.1f MiB of shreds\n",
		args[1], man.Blocks, man.Slots[0], man.Head, man.Checkpoint, float64(man.ShredBytes)/(1<<20))
	fmt.Fprintf(out, "read %d shred packets (%d duplicates, %d not shreds, %d outside the slot range), %d bank hashes, %d root records\n",
		stats.Shreds, stats.Duplicates, stats.Unparsed, stats.OutOfRange, stats.BankHashes, stats.Roots)
	report := func(what string, slots []uint64) {
		if len(slots) > 0 {
			fmt.Fprintf(out, "%d %s: %s\n", len(slots), what, sample(slots))
		}
	}
	report("incomplete slots left out", stats.Incomplete)
	report("complete slots off the replayed chain left out", stats.OffChain)
	report("chain slots without a bank hash (not checked)", stats.NoBankHash)
	report("chain slots whose data shred count differs from the bank hash record", stats.CountMismatch)
	report("chain slots whose parent is not the previous chain slot", stats.ParentBreaks)
	fmt.Fprintf(out, "the AccountsDB must hold slot %d before replaying this corpus\n", man.Checkpoint)
	return nil
}

func runExport(cmd *cobra.Command, args []string) error {
	f, err := os.OpenFile(args[1], os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	var w io.Writer = f
	var enc *zstd.Encoder
	if strings.HasSuffix(args[1], ".zst") {
		if enc, err = zstd.NewWriter(f); err != nil {
			return err
		}
		w = enc
	}
	n, err := shredcap.ExportCorpus(args[0], w, "mithril corpus export-shredcap", shredcap.ExportOptions{SnapshotSlot: snapshotSlot})
	if err != nil {
		return err
	}
	if enc != nil {
		if err := enc.Close(); err != nil {
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "wrote %d slots to %s\n", n, args[1])
	return nil
}

func sample(slots []uint64) string {
	const show = 10
	parts := make([]string, 0, show+1)
	for i, slot := range slots {
		if i == show {
			parts = append(parts, fmt.Sprintf("... (%d more)", len(slots)-show))
			break
		}
		parts = append(parts, fmt.Sprint(slot))
	}
	return strings.Join(parts, " ")
}
