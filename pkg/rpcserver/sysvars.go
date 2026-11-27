package rpcserver

import (
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/bankview"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

// readSysvar decodes a sysvar account as of the read's bank.
func readSysvar(r *bankview.Read, addr solana.PublicKey, into interface {
	UnmarshalWithDecoder(*bin.Decoder) error
}) error {
	acct, err := r.Account(addr)
	if err != nil {
		return err
	}
	if acct == nil {
		return fmt.Errorf("sysvar %s missing at slot %d", addr, r.View.Slot)
	}
	return into.UnmarshalWithDecoder(bin.NewBinDecoder(acct.Data))
}

// blockhashValidAt reports whether a transaction using blockhash would still
// be accepted by the read's bank: its own blockhash, or one of the recent
// blockhashes in the processing window.
func blockhashValidAt(r *bankview.Read, blockhash solana.Hash) (bool, error) {
	if r.View.Blockhash == blockhash {
		return true, nil
	}
	var recent sealevel.SysvarRecentBlockhashes
	if err := readSysvar(r, solana.PublicKey(sealevel.SysvarRecentBlockHashesAddr), &recent); err != nil {
		return false, err
	}
	for _, entry := range recent {
		if solana.Hash(entry.Blockhash) == blockhash {
			return true, nil
		}
	}
	return false, nil
}
