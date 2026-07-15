package main

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/gagliardetto/solana-go/rpc"
)

func unmarshalBlockJSON(filename string) (*block.Block, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("reading file: %w", err)
	}

	gbr := &rpc.GetBlockResult{}
	if err := json.Unmarshal(data, gbr); err != nil {
		return nil, fmt.Errorf("unmarshaling JSON: %w", err)
	}

	b := block.FromBlockResult(gbr, 0, nil)
	return b, nil
}

func main() {
	if len(os.Args) < 2 {
		mlog.Log.Errorf("Usage: %s <block_json_file>", os.Args[0])
		os.Exit(1)
	}

	b, err := unmarshalBlockJSON(os.Args[1])
	if err != nil {
		panic(err)
	}

	mlog.Log.Infof("len(b.Transactions)=%d len(b.TxMetas)=%d\n", len(b.Transactions), len(b.TxMetas))
	topSortLevels := replay.TopsortPlanner(b)

	var sanityCheck []int
	for level, txs := range topSortLevels {
		mlog.Log.Infof("level=%d len(txs)=%d", level, len(txs))
		mlog.Log.Infof("txs=%v", txs)
		sanityCheck = append(sanityCheck, txs...)
	}
	slices.Sort(sanityCheck)
	for i, v := range sanityCheck {
		if i != v {
			panic(fmt.Sprintf("i=%d != v=%d", i, v))
		}
	}
}
