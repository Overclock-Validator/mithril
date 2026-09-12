package sealevel

import (
	"bytes"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/base58"
	bin "github.com/gagliardetto/binary"
)

const SysvarRecentBlockHashesAddrStr = "SysvarRecentB1ockHashes11111111111111111111"

var SysvarRecentBlockHashesAddr = base58.MustDecodeFromString(SysvarRecentBlockHashesAddrStr)

type RecentBlockHashesEntry struct {
	Blockhash     [32]byte
	FeeCalculator FeeCalculator
}

type SysvarRecentBlockhashes []RecentBlockHashesEntry

const (
	recentBlockhashesMaxEntries = 150
)

func (recentBlockhashes *SysvarRecentBlockhashes) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	numBlockhashes, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return err
	}

	for count := uint64(0); count < numBlockhashes; count++ {
		var recentBlockhashEntry RecentBlockHashesEntry
		hash, err := decoder.ReadBytes(32)
		if err != nil {
			return err
		}
		copy(recentBlockhashEntry.Blockhash[:], hash)

		recentBlockhashEntry.FeeCalculator.LamportsPerSignature, err = decoder.ReadUint64(bin.LE)
		if err != nil {
			return err
		}

		*recentBlockhashes = append(*recentBlockhashes, recentBlockhashEntry)
	}

	return nil
}

func (recentBlockhashes *SysvarRecentBlockhashes) MarshalWithEncoder(encoder *bin.Encoder) error {
	numBlockhashes := uint64(len(*recentBlockhashes))

	err := encoder.WriteUint64(numBlockhashes, bin.LE)
	if err != nil {
		return err
	}

	rbh := *recentBlockhashes
	for count := uint64(0); count < numBlockhashes; count++ {
		err = encoder.WriteBytes(rbh[count].Blockhash[:], false)
		if err != nil {
			return err
		}

		err = encoder.WriteUint64(rbh[count].FeeCalculator.LamportsPerSignature, bin.LE)
		if err != nil {
			return err
		}
	}

	return nil
}

func (recentBlockhashes *SysvarRecentBlockhashes) MustMarshal() []byte {
	data := new(bytes.Buffer)
	enc := bin.NewBinEncoder(data)

	err := recentBlockhashes.MarshalWithEncoder(enc)
	if err != nil {
		panic(fmt.Sprintf("unable to marshal RecentBlockhashes: %s", err))
	}

	return data.Bytes()
}

func CheckAcctForRecentBlockHashesSysvar(txCtx *TransactionCtx, instrCtx *InstructionCtx, instrAcctIdx uint64) error {
	idxInTx, err := instrCtx.IndexOfInstructionAccountInTransaction(instrAcctIdx)
	if err != nil {
		return err
	}
	pk, err := txCtx.KeyOfAccountAtIndex(idxInTx)
	if err != nil {
		return err
	}
	if pk == SysvarRecentBlockHashesAddr {
		return nil
	} else {
		return InstrErrInvalidArgument
	}
}

func (recentBlockhashes *SysvarRecentBlockhashes) MustUnmarshalWithDecoder(decoder *bin.Decoder) {
	err := recentBlockhashes.UnmarshalWithDecoder(decoder)
	if err != nil {
		panic(err.Error())
	}
}

func (recentBlockhashes *SysvarRecentBlockhashes) GetLatest() RecentBlockHashesEntry {
	rbh := *recentBlockhashes
	return rbh[0]
}

func (recentBlockhashes *SysvarRecentBlockhashes) PushLatest(latest [32]byte, lamportsPerSignature uint64) [32]byte {
	rbh := *recentBlockhashes

	newEntry := RecentBlockHashesEntry{Blockhash: latest, FeeCalculator: FeeCalculator{LamportsPerSignature: lamportsPerSignature}}
	var latestEvicted [32]byte

	if len(rbh) >= recentBlockhashesMaxEntries {
		latestEvicted = rbh[len(rbh)-1].Blockhash
		rbh = rbh[:len(rbh)-1]
		rbh = append([]RecentBlockHashesEntry{newEntry}, rbh...)
	} else {
		rbh = append([]RecentBlockHashesEntry{newEntry}, rbh...)
	}

	*recentBlockhashes = rbh
	return latestEvicted
}

func (recentBlockhashes *SysvarRecentBlockhashes) IsBlockhashAgeValid(hash [32]byte) bool {
	rbh := *recentBlockhashes

	for _, r := range rbh {
		if hash == r.Blockhash {
			return true
		}
	}

	return false
}

func ReadRecentBlockHashesSysvar(execCtx *ExecutionCtx) (SysvarRecentBlockhashes, error) {
	if execCtx != nil && execCtx.SlotCtx != nil {
		if bankSysvars := execCtx.SlotCtx.BankSysvars(); bankSysvars != nil {
			recentBlockhashes, ok := bankSysvars.RecentBlockhashes()
			if !ok {
				return SysvarRecentBlockhashes{}, InstrErrUnsupportedSysvar
			}
			return recentBlockhashes, nil
		}
	}

	if recentAcct, ok := localSysvarAccount(execCtx, SysvarRecentBlockHashesAddr); ok {
		if recentAcct.Lamports == 0 || len(recentAcct.Data) == 0 {
			return SysvarRecentBlockhashes{}, InstrErrUnsupportedSysvar
		}
		var recentBlockhashes SysvarRecentBlockhashes
		if err := recentBlockhashes.UnmarshalWithDecoder(bin.NewBinDecoder(recentAcct.Data)); err != nil {
			return SysvarRecentBlockhashes{}, InstrErrUnsupportedSysvar
		}
		return recentBlockhashes, nil
	}

	if SysvarCache.RecentBlockHashes.Sysvar != nil {
		return *SysvarCache.RecentBlockHashes.Sysvar, nil
	}
	return SysvarRecentBlockhashes{}, InstrErrUnsupportedSysvar
}
