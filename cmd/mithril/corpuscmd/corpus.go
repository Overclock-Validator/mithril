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
	spoolDirs                      []string
	bankHashLog, spoolCapture      string
	spoolCheckpoint                uint64

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

	fromSpoolCmd = cobra.Command{
		Use:   "from-spool OUTDIR",
		Short: "Make a replay corpus (and optionally a shredcap capture) from a node run's shred spool",
		Long: `Builds a replay corpus from the shreds a Mithril node kept and the bank hashes
its replay logged. Run the node with MITHRIL_SHRED_ARCHIVE_DIR set so it keeps
the shreds of every block it replayed instead of deleting them; pass that
archive and the node's spool directory (<blockstore>/catchup-spool) with
--spool, and the bankhash.log from its AccountsDB directory with --bank-hashes.

The corpus is the chain of shred-header parents ending at the highest complete
slot that has a logged bank hash, within --from-slot..--to-slot. The newest
slots of a run may not be final yet: set --to-slot some slots before the end
of the run. --checkpoint refuses a chain that does not start right after that
slot (the snapshot the run started from). --capture also writes the corpus as
a shredcap capture (zstd-compressed when the name ends in .zst).`,
		Args: cobra.ExactArgs(1),
		RunE: runFromSpool,
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
	fromSpoolCmd.Flags().StringArrayVar(&spoolDirs, "spool", nil, "Spool or archive directory holding s<slot>.shreds files (repeatable)")
	fromSpoolCmd.Flags().StringVar(&bankHashLog, "bank-hashes", "", "The node's bankhash.log (in its AccountsDB directory)")
	fromSpoolCmd.Flags().Uint64Var(&fromSlot, "from-slot", 0, "First slot to include")
	fromSpoolCmd.Flags().Uint64Var(&toSlot, "to-slot", 0, "Last slot to include (0 = no limit)")
	fromSpoolCmd.Flags().Uint64Var(&spoolCheckpoint, "checkpoint", 0, "Slot the chain must start right after (the snapshot slot); 0 = no check")
	fromSpoolCmd.Flags().StringVar(&spoolCapture, "capture", "", "Also write the corpus as this shredcap capture (.pcapng or .pcapng.zst)")
	_ = fromSpoolCmd.MarkFlagRequired("spool")
	_ = fromSpoolCmd.MarkFlagRequired("bank-hashes")
	exportCmd.Flags().Uint64Var(&snapshotSlot, "snapshot-slot", 0, "Slot of the snapshot backtest starts from (0 = the corpus checkpoint); a capture whose first block is later starts with a placeholder shred for this slot")
	CorpusCmd.AddCommand(&importCmd, &exportCmd, &fromSpoolCmd)
}

func runImport(cmd *cobra.Command, args []string) error {
	cmd.SilenceUsage = true
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
	printImport(cmd.OutOrStdout(), args[1], man, stats)
	return nil
}

func printImport(out io.Writer, dir string, man *shredcap.Manifest, stats shredcap.ImportStats) {
	fmt.Fprintf(out, "corpus %s: %d blocks %d..%d from checkpoint %d, %.1f MiB of shreds\n",
		dir, man.Blocks, man.Slots[0], man.Head, man.Checkpoint, float64(man.ShredBytes)/(1<<20))
	fmt.Fprintf(out, "read %d shred packets (%d duplicates, %d not shreds, %d outside the slot range), %d bank hashes, %d root records\n",
		stats.Shreds, stats.Duplicates, stats.Unparsed, stats.OutOfRange, stats.BankHashes, stats.Roots)
	report := func(what string, slots []uint64) {
		if len(slots) > 0 {
			fmt.Fprintf(out, "%d %s: %s\n", len(slots), what, sample(slots))
		}
	}
	report("slots completed by recovering data shreds from coding shreds", stats.RecoveredSlots)
	report("incomplete slots left out", stats.Incomplete)
	report("complete slots off the replayed chain left out", stats.OffChain)
	report("chain slots without a bank hash (not checked)", stats.NoBankHash)
	report("chain slots whose data shred count differs from the bank hash record", stats.CountMismatch)
	report("chain slots whose parent is not the previous chain slot", stats.ParentBreaks)
	fmt.Fprintf(out, "shred version %d\n", man.ShredVersion)
	fmt.Fprintf(out, "the AccountsDB must hold slot %d before replaying this corpus\n", man.Checkpoint)
}

func runFromSpool(cmd *cobra.Command, args []string) error {
	cmd.SilenceUsage = true
	f, err := os.Open(bankHashLog)
	if err != nil {
		return err
	}
	hashes, err := shredcap.ReadBankHashLog(f)
	f.Close()
	if err != nil {
		return err
	}
	man, stats, spoolStats, err := shredcap.ImportSpool(spoolDirs, hashes, args[0], shredcap.ImportOptions{
		FromSlot: fromSlot, ToSlot: toSlot, Source: "shred spool",
	})
	if err != nil {
		return err
	}
	if spoolCheckpoint != 0 && man.Checkpoint != spoolCheckpoint {
		_ = os.RemoveAll(args[0])
		return fmt.Errorf("the chain starts after slot %d, not after the checkpoint %d: a slot between is missing or incomplete in the spool (removed %s)", man.Checkpoint, spoolCheckpoint, args[0])
	}
	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "read %d spool files, %d logged bank hashes\n", spoolStats.SlotFiles, len(hashes))
	if len(spoolStats.Torn) > 0 {
		fmt.Fprintf(out, "%d slot files ended in a torn record (intact records kept): %s\n", len(spoolStats.Torn), sample(spoolStats.Torn))
	}
	printImport(out, args[0], man, stats)
	if spoolCapture == "" {
		return nil
	}
	return exportCapture(cmd, args[0], spoolCapture, 0)
}

func runExport(cmd *cobra.Command, args []string) error {
	cmd.SilenceUsage = true
	return exportCapture(cmd, args[0], args[1], snapshotSlot)
}

// exportCapture writes corpusDir as a shredcap capture at path.
func exportCapture(cmd *cobra.Command, corpusDir, path string, snapshot uint64) (err error) {
	args := []string{corpusDir, path}
	f, err := os.OpenFile(args[1], os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	defer func() {
		if err != nil {
			// Never leave a partial capture that a later import would read.
			_ = os.Remove(args[1])
		}
	}()
	var w io.Writer = f
	var enc *zstd.Encoder
	if strings.HasSuffix(args[1], ".zst") {
		if enc, err = zstd.NewWriter(f); err != nil {
			return err
		}
		w = enc
	}
	stats, err := shredcap.ExportCorpus(args[0], w, "mithril corpus export-shredcap", shredcap.ExportOptions{SnapshotSlot: snapshot})
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
	fmt.Fprintf(cmd.OutOrStdout(), "wrote %d slots to %s (%d needed data shreds recovered from coding shreds)\n", stats.Slots, args[1], stats.RecoveredSlots)
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
