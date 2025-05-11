//go:build !lite

package node

import (
	"fmt"
	"net/http"

	_ "net/http/pprof"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/snapshot"
	"github.com/spf13/cobra"
	"k8s.io/klog/v2"
)

var (
	Cmd = cobra.Command{
		Use:   "verifier",
		Short: "Run mithril verifier node",
		Run:   run,
	}

	loadFromSnapshot   bool
	loadFromAccountsDb bool
	updateAccountsDb   bool
	path               string
	outputDir          string
	rpcEndpoint        string
	startSlot          int64
	endSlot            int64
	pprofPort          int64
)

func init() {
	Cmd.Flags().BoolVarP(&loadFromSnapshot, "snapshot", "s", false, "Load from a full snapshot")
	Cmd.Flags().BoolVarP(&loadFromAccountsDb, "accountsdb", "a", false, "Load from AccountsDB")
	Cmd.Flags().BoolVarP(&updateAccountsDb, "update-accounts-db", "u", false, "Update accountsdb after execution")
	Cmd.Flags().StringVarP(&path, "path", "p", "", "Path of full snapshot or AccountsDB to load from")
	Cmd.Flags().StringVarP(&outputDir, "out", "o", "", "Output path for writing AccountsDB data to")
	Cmd.Flags().StringVarP(&rpcEndpoint, "rpc", "r", "", "URL for RPC endpoint")
	Cmd.Flags().Int64VarP(&startSlot, "startslot", "b", -1, "Block at which to begin replaying")
	Cmd.Flags().Int64VarP(&endSlot, "endslot", "e", -1, "Block at which to stop replaying, inclusive")
	Cmd.Flags().Int64Var(&pprofPort, "pprofport", -1, "Port to serve HTTP pprof endpoint")
}

func run(c *cobra.Command, args []string) {
	if pprofPort != -1 {
		pprofAddr := fmt.Sprintf("localhost:%d", pprofPort)
		mlog.Log.Infof("Starting HTTP server for pprof on %s", pprofAddr)
		go func() {
			mlog.Log.Errorf("HTTP pprof server exited: %v", http.ListenAndServe(pprofAddr, nil))
		}()
	}

	if !loadFromSnapshot && !loadFromAccountsDb {
		klog.Errorf("must specify either to load from a snapshot or from an existing AccountsDB")
		return
	}

	if startSlot < 0 {
		if loadFromAccountsDb {
			klog.Errorf("must specify a slot at which to begin replaying")
			return
		}
	}

	if endSlot != -1 && endSlot < startSlot {
		klog.Errorf("end slot cannot be lower than start block")
		return
	}

	if endSlot > 0 && startSlot < 0 {
		klog.Errorf("specified end block without providing start block")
		return
	}

	if startSlot > 0 && endSlot > 0 {
		updateAccountsDb = true
	} else if startSlot > 0 && endSlot == -1 {
		endSlot = startSlot
	}

	var err error
	var accountsDbDir string
	var accountsDb *accountsdb.AccountsDb
	var manifest *snapshot.SnapshotManifest

	if loadFromSnapshot {
		if path == "" || outputDir == "" {
			klog.Errorf("must specify snapshot path and directory path for writing generated AccountsDB")
			return
		}

		mlog.Log.Infof("building AccountsDB from snapshot at %s\n", path)

		// extract accountvecs from full snapshot, build accountsdb index, and write it all out to disk
		accountsDb, manifest, err = snapshot.BuildAccountsIndexFromSnapshot(path, outputDir)
		if err != nil {
			klog.Exitf("failed to populate new accounts db from snapshot %s: %s", path, err)
		}

		mlog.Log.Debugf("successfully created accounts db from snapshot %s", path)

		// just processing the snapshot - not executing blocks.
		if startSlot < 0 {
			return
		}

		accountsDbDir = outputDir
	} else if loadFromAccountsDb {
		if path == "" {
			klog.Fatalf("must specify an AccountsDB directory path to load from")
		}

		accountsDbDir = path
	}

	if rpcEndpoint == "" {
		rpcEndpoint = "https://api.mainnet-beta.solana.com"
	}

	if accountsDb == nil || manifest == nil {
		mlog.Log.Infof("loading from AccountsDB at %s", accountsDbDir)

		accountsDb, err = accountsdb.OpenDb(accountsDbDir)
		if err != nil {
			klog.Fatalf("unable to open accounts db %s\n", accountsDbDir)
		}

		manifest, err = snapshot.LoadManifestFromFile(fmt.Sprintf("%s/manifest", accountsDbDir))
		if err != nil {
			klog.Fatalf("unable to open manifest file")
		}
	}

	mlog.Log.Infof("initializing caches")
	accountsDb.InitCaches()

	replay.ReplayBlocks(accountsDb, accountsDbDir, manifest, uint64(startSlot), uint64(endSlot), rpcEndpoint, updateAccountsDb)
	mlog.Log.Infof("done replaying, closing DB")
	accountsDb.CloseDb()
}
