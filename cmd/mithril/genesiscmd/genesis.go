package genesiscmd

import (
	"context"
	"fmt"
	"os"

	"github.com/Overclock-Validator/mithril/pkg/genesis"
	"github.com/Overclock-Validator/mithril/pkg/genesisinit"
	"github.com/spf13/cobra"
)

// NewCommand creates an independent command tree; --config here names the
// genesis specification and never initializes the live node's configuration.
func NewCommand() *cobra.Command {
	root := &cobra.Command{Use: "genesis", Short: "Create a pinned Alpenglow genesis and initialize its slot-0 bank"}
	var config, output, path, accounts string
	create := &cobra.Command{Use: "create --config <genesis.toml> --output <dir>", Short: "Create deterministic genesis.bin, genesis.tar.bz2 and a resolved summary", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		file, err := os.Open(config)
		if err != nil {
			return err
		}
		c, err := genesis.ParseConfig(file)
		closeErr := file.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		_, err = genesis.CreateFiles(cmd.Context(), c, output)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(cmd.OutOrStdout(), "Created %s using %s (%s)\n", output, genesis.Profile, genesis.AgaveRevision)
		return err
	}}
	create.Flags().StringVar(&config, "config", "", "Genesis TOML specification (explicit creation time and public keys)")
	create.Flags().StringVar(&output, "output", "", "New output directory")
	_ = create.MarkFlagRequired("config")
	_ = create.MarkFlagRequired("output")
	init := &cobra.Command{Use: "init --genesis <path> --accounts-path <dir>", Short: "Persist and verify the completed initial bank in an empty AccountsDB", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		g, _, err := genesis.ReadGenesisFromFile(path)
		if err != nil {
			return err
		}
		metadata, err := genesisinit.Initialize(cmd.Context(), g, accounts)
		if err != nil {
			return err
		}
		// Reopening verifies durable artifacts through the same public adapter that
		// future bootstrap will use, after the initialization owner has closed.
		db, _, err := genesisinit.Open(cmd.Context(), accounts)
		if err != nil {
			return err
		}
		if err = db.Shutdown(context.Background()); err != nil {
			return err
		}
		_, err = fmt.Fprintf(cmd.OutOrStdout(), "Initialized slot 0 in %s\nGenesis hash: %s\nBank hash: %s\nNext replay slot: %d\n", accounts, metadata.GenesisHash, metadata.BankHash, metadata.NextReplaySlot)
		return err
	}}
	init.Flags().StringVar(&path, "genesis", "", "Raw genesis.bin or genesis.tar.bz2")
	init.Flags().StringVar(&accounts, "accounts-path", "", "Empty or new AccountsDB directory")
	_ = init.MarkFlagRequired("genesis")
	_ = init.MarkFlagRequired("accounts-path")
	root.AddCommand(create, init)
	return root
}
