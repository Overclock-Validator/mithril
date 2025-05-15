package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/rpcclient"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/spf13/cobra"
	"k8s.io/klog/v2"
)

var (
	cmd = cobra.Command{
		Use:   "blockdl",
		Short: "Download blocks for mithril verification.",
		Run:   run,
	}

	outputDir   string
	rpcEndpoint string
	startSlot   uint64
	endSlot     uint64
	workers     uint64
)

func init() {
	klogFlags := flag.NewFlagSet("klog", flag.ExitOnError)
	klog.InitFlags(klogFlags)
	cmd.PersistentFlags().AddGoFlagSet(klogFlags)
	cmd.Flags().StringVarP(&outputDir, "outdir", "o", "", "Directory to write slot.gob files to.")
	cmd.Flags().StringVarP(&rpcEndpoint, "rpc", "r", "", "URL for RPC endpoint")
	cmd.Flags().Uint64VarP(&startSlot, "startslot", "b", 0, "Block at which to begin replaying")
	cmd.Flags().Uint64VarP(&endSlot, "endslot", "e", 0, "Block at which to stop replaying, inclusive")
	cmd.Flags().Uint64VarP(&workers, "workers", "w", 10, "Number of parallel downloaders")
}

func saveBlockToFile(filename string, b *rpc.GetBlockResult) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(b)
}

func run(c *cobra.Command, args []string) {
	var errs []string
	if outputDir == "" {
		errs = append(errs, "must have nonempty -outdir flag")
	} else {
		info, err := os.Stat(outputDir)
		if err != nil {
			errs = append(errs, fmt.Sprintf("error stat-ing outdir: %v", err))
		} else if !info.IsDir() {
			errs = append(errs, "outdir exists and is not a directory")
		}
	}
	if rpcEndpoint == "" {
		errs = append(errs, "must have nonempty -rpc flag")
	}
	if startSlot == 0 {
		errs = append(errs, "must set -startslot")
	}
	if endSlot == 0 || endSlot <= startSlot {
		errs = append(errs, "-endslot must be set and larger than -startslot")
	}
	if len(errs) > 0 {
		for _, err := range errs {
			mlog.Log.Errorf(err)
		}
		return
	}

	rpcc := rpcclient.NewRpcClient(rpcEndpoint)
	slotMu := &sync.Mutex{}
	slot := startSlot
	wg := &sync.WaitGroup{}
	wg.Add(int(workers))
	for i := 0; i < int(workers); i++ {
		go func() {
			defer wg.Done()
			for {
				slotMu.Lock()
				s := slot
				slot++
				slotMu.Unlock()
				if s > endSlot {
					return
				}

				if err := c.Context().Err(); err != nil {
					mlog.Log.Errorf("worker interrupted on slot=%d: %v, exiting...", s, err)
					return
				}
				blockResult, err := rpcc.GetBlockFinalized(s)
				if err == rpcclient.SlotSkipped {
					continue
				} else if err != nil {
					mlog.Log.Errorf("error fetching slot=%d: %v", s, err)
					continue
				}
				blockFilename := filepath.Join(filepath.Clean(outputDir), fmt.Sprintf("%d.json", s))
				saveBlockToFile(blockFilename, blockResult)
			}
		}()
	}
	wg.Wait()
}

func main() {
	mlog.Log.Infof("mithril block downloader")
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	cobra.CheckErr(cmd.ExecuteContext(ctx))
}
