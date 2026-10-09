package rpcclient

import (
	"context"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

func (fetcher *RpcClient) GetTransactionMeta(sig solana.Signature) (*rpc.TransactionMeta, error) {
	maxSupportedTxVer := rpc.MaxSupportedTransactionVersion1
	tx, err := fetcher.client.GetTransaction(context.TODO(), sig, &rpc.GetTransactionOpts{
		Encoding:                       solana.EncodingJSON,
		MaxSupportedTransactionVersion: &maxSupportedTxVer,
	})

	if err != nil {
		return nil, err
	}

	return tx.Meta, nil
}
