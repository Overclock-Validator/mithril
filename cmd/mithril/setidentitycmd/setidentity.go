// Package setidentitycmd implements `mithril set-identity`, the client for a
// running node's admin RPC socket.
package setidentitycmd

import (
	"errors"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/adminrpc"
	"github.com/Overclock-Validator/mithril/pkg/identity"
	"github.com/spf13/cobra"
)

type options struct {
	ledger                  string
	requireTower            bool
	requireVoteHistory      bool
	doNotRequireVoteHistory bool
}

// Command returns the set-identity command.
func Command() *cobra.Command {
	var opts options
	cmd := &cobra.Command{
		Use:   "set-identity --ledger <ledger> [--require-tower] [--require-vote-history] KEYPAIR",
		Short: "Set the identity of a running validator",
		Long: `Set the identity of a running validator through its admin RPC socket,
<ledger>/admin.rpc, as Agave's agave-validator set-identity does.

<ledger> is the running node's storage.shredstore directory (the
--ledger-path flag of mithril run). It holds the node's vote history, the
state Agave keeps in its ledger, and the node creates admin.rpc there while
it runs.

KEYPAIR is a Solana keygen JSON file. It is read here and its bytes are sent
with setIdentityFromBytes, so the node process does not need to be able to
read it. To take a validator out of voting, pass a keypair with no stake,
such as one made with solana-keygen new.

The node refuses to switch from one staked identity to another. With
--require-vote-history (the default), switching to the identity of a vote
account that has already voted requires that identity's vote history to be
present in <ledger>. --require-tower is accepted for Agave compatibility, but
Mithril keeps no tower, so it always fails.

The new identity applies only to the running process; set
validator.identity_keypair to keep it across restarts.`,
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return opts.run(cmd, args)
		},
	}
	cmd.Flags().StringVar(&opts.ledger, "ledger", "", "Ledger directory of the running node (its storage.shredstore)")
	cmd.Flags().BoolVar(&opts.requireTower, "require-tower", false, "Refuse unless saved tower state exists for the new identity (Mithril keeps none, so this always fails)")
	cmd.Flags().BoolVar(&opts.requireVoteHistory, "require-vote-history", true, "Refuse unless saved vote history exists for an identity whose vote account has voted")
	cmd.Flags().BoolVar(&opts.doNotRequireVoteHistory, "do-not-require-vote-history", false, "Same as --require-vote-history=false (Agave's spelling); dangerous")
	_ = cmd.MarkFlagRequired("ledger")
	return cmd
}

func (opts options) run(cmd *cobra.Command, args []string) error {
	if opts.doNotRequireVoteHistory && cmd.Flags().Changed("require-vote-history") && opts.requireVoteHistory {
		return errors.New("--require-vote-history and --do-not-require-vote-history conflict")
	}
	requireHistory := opts.requireVoteHistory && !opts.doNotRequireVoteHistory

	key, err := adminrpc.ReadKeypairFile(args[0])
	if err != nil {
		return fmt.Errorf("read identity keypair %s: %w", args[0], err)
	}

	client, err := adminrpc.Dial(opts.ledger)
	if err != nil {
		return err
	}
	defer client.Close()
	if err := client.SetIdentityFromBytes(key, opts.requireTower, requireHistory); err != nil {
		return fmt.Errorf("set-identity: %w", err)
	}
	newIdentity, err := identity.NewKeypair(key)
	if err != nil {
		// The node accepted it, so this cannot happen.
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Validator identity set to %s (%s)\n", newIdentity.PublicKey(), args[0])
	return nil
}
