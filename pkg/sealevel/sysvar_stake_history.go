package sealevel

import (
	"bytes"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/base58"
	bin "github.com/gagliardetto/binary"
)

const SysvarStakeHistoryAddrStr = "SysvarStakeHistory1111111111111111111111111"

var SysvarStakeHistoryAddr = base58.MustDecodeFromString(SysvarStakeHistoryAddrStr)

type StakeHistoryEntry struct {
	Effective    uint64
	Activating   uint64
	Deactivating uint64
}

type StakeHistoryPair struct {
	Epoch uint64
	Entry StakeHistoryEntry
}

type SysvarStakeHistory []StakeHistoryPair

const SysvarStakeHistoryMaxEntries = 512

func (sh *SysvarStakeHistory) MarshalWithEncoder(encoder *bin.Encoder) error {
	lenStakeHistory := len(*sh)

	err := encoder.WriteUint64(uint64(lenStakeHistory), bin.LE)
	if err != nil {
		err = fmt.Errorf("failed to serialize len of StakeHistory for StakeHistory sysvar: %w", err)
		panic(err)
	}

	for count := 0; count < lenStakeHistory; count++ {
		err = encoder.WriteUint64((*sh)[count].Epoch, bin.LE)
		if err != nil {
			return fmt.Errorf("failed to serialize Epoch for StakeHistory sysvar: %w", err)
		}

		err = encoder.WriteUint64((*sh)[count].Entry.Effective, bin.LE)
		if err != nil {
			return fmt.Errorf("failed to serialize Effective for StakeHistory sysvar: %w", err)
		}

		err = encoder.WriteUint64((*sh)[count].Entry.Activating, bin.LE)
		if err != nil {
			return fmt.Errorf("failed to serialize Activating for StakeHistory sysvar: %w", err)
		}

		err = encoder.WriteUint64((*sh)[count].Entry.Deactivating, bin.LE)
		if err != nil {
			return fmt.Errorf("failed to serialize Deactivating for StakeHistory sysvar: %w", err)
		}
	}

	return nil
}

func (sh *SysvarStakeHistory) MustMarshalWithEncoder(encoder *bin.Encoder) {
	err := sh.MarshalWithEncoder(encoder)
	if err != nil {
		panic(err)
	}
}

func (sh *SysvarStakeHistory) UnmarshalWithDecoder(decoder *bin.Decoder) (err error) {
	entriesLen, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return fmt.Errorf("failed to read length of entries when decoding SysvarStakeHistory: %w", err)
	}

	stakeHistory := SysvarStakeHistory{}

	for count := uint64(0); count < entriesLen; count++ {

		stakeHistoryPair := StakeHistoryPair{}
		stakeHistoryPair.Epoch, err = decoder.ReadUint64(bin.LE)
		if err != nil {
			return fmt.Errorf("failed to read Epoch when decoding SysvarStakeHistory: %w", err)
		}

		stakeHistoryPair.Entry.Effective, err = decoder.ReadUint64(bin.LE)
		if err != nil {
			return fmt.Errorf("failed to read Effective when decoding SysvarStakeHistory: %w", err)
		}

		stakeHistoryPair.Entry.Activating, err = decoder.ReadUint64(bin.LE)
		if err != nil {
			return fmt.Errorf("failed to read Activating when decoding SysvarStakeHistory: %w", err)
		}

		stakeHistoryPair.Entry.Deactivating, err = decoder.ReadUint64(bin.LE)
		if err != nil {
			return fmt.Errorf("failed to read Deactivating when decoding SysvarStakeHistory: %w", err)
		}

		stakeHistory = append(stakeHistory, stakeHistoryPair)
	}

	*sh = stakeHistory

	return
}

func (sh *SysvarStakeHistory) MustUnmarshalWithDecoder(decoder *bin.Decoder) {
	err := sh.UnmarshalWithDecoder(decoder)
	if err != nil {
		panic(err.Error())
	}
}

func (sh *SysvarStakeHistory) Get(epoch uint64) *StakeHistoryEntry {
	for _, pair := range *sh {
		if pair.Epoch == epoch {
			return &pair.Entry
		}
	}
	return nil
}

func (sh *SysvarStakeHistory) Update(epoch uint64, entry StakeHistoryEntry) {
	var found bool

	for count := 0; count < len(*sh); count++ {
		if (*sh)[count].Epoch == epoch {
			(*sh)[count].Entry = entry
			found = true
		}
	}

	if !found {
		if len(*sh) == SysvarStakeHistoryMaxEntries {
			*sh = (*sh)[:len(*sh)-1]
		}

		pair := StakeHistoryPair{Epoch: epoch, Entry: entry}
		*sh = append([]StakeHistoryPair{pair}, (*sh)...)
	}
}

func (sh *SysvarStakeHistory) String() string {
	var str string
	for _, s := range *sh {
		str += fmt.Sprintf("epoch: %d, effective: %d, activating: %d, deactivating: %d\n", s.Epoch, s.Entry.Effective, s.Entry.Activating, s.Entry.Deactivating)
	}
	return str
}

func ReadStakeHistorySysvar(execCtx *ExecutionCtx) (SysvarStakeHistory, error) {
	if execCtx != nil && execCtx.SlotCtx != nil {
		if bankSysvars := execCtx.SlotCtx.BankSysvars(); bankSysvars != nil {
			stakeHistory, ok := bankSysvars.StakeHistory()
			if !ok {
				return SysvarStakeHistory{}, InstrErrUnsupportedSysvar
			}
			return stakeHistory, nil
		}
	}

	if stakeHistoryAcct, ok := localSysvarAccount(execCtx, SysvarStakeHistoryAddr); ok {
		if stakeHistoryAcct.Lamports == 0 {
			return SysvarStakeHistory{}, InstrErrUnsupportedSysvar
		}
		var stakeHistory SysvarStakeHistory
		if err := stakeHistory.UnmarshalWithDecoder(bin.NewBinDecoder(stakeHistoryAcct.Data)); err != nil {
			return SysvarStakeHistory{}, InstrErrUnsupportedSysvar
		}
		return stakeHistory, nil
	}

	if SysvarCache.StakeHistory.Sysvar != nil {
		return *SysvarCache.StakeHistory.Sysvar, nil
	}
	return SysvarStakeHistory{}, InstrErrUnsupportedSysvar
}

func WriteStakeHistorySysvar(accts *accounts.Accounts, stakeHistory SysvarStakeHistory) {
	stakeHistSysvarAcct, err := (*accts).GetAccount(&SysvarStakeHistoryAddr)
	if err != nil {
		panic("failed to read StakeHistory sysvar account")
	}

	data := new(bytes.Buffer)
	enc := bin.NewBinEncoder(data)

	lenStakeHistory := len(stakeHistory)

	err = enc.WriteUint64(uint64(lenStakeHistory), bin.LE)
	if err != nil {
		err = fmt.Errorf("failed to serialize len of StakeHistory for StakeHistory sysvar: %w", err)
		panic(err)
	}

	for count := 0; count < lenStakeHistory; count++ {
		err = enc.WriteUint64(stakeHistory[count].Epoch, bin.LE)
		if err != nil {
			err = fmt.Errorf("failed to serialize Epoch for StakeHistory sysvar: %w", err)
			panic(err)
		}

		err = enc.WriteUint64(stakeHistory[count].Entry.Effective, bin.LE)
		if err != nil {
			err = fmt.Errorf("failed to serialize Effective for StakeHistory sysvar: %w", err)
			panic(err)
		}

		err = enc.WriteUint64(stakeHistory[count].Entry.Activating, bin.LE)
		if err != nil {
			err = fmt.Errorf("failed to serialize Activating for StakeHistory sysvar: %w", err)
			panic(err)
		}

		err = enc.WriteUint64(stakeHistory[count].Entry.Deactivating, bin.LE)
		if err != nil {
			err = fmt.Errorf("failed to serialize Deactivating for StakeHistory sysvar: %w", err)
			panic(err)
		}
	}

	stakeHistSysvarAcct.Data = data.Bytes()

	err = (*accts).SetAccount(&SysvarStakeHistoryAddr, stakeHistSysvarAcct)
	if err != nil {
		err = fmt.Errorf("failed to write newly serialized StakeHistory sysvar to sysvar account: %w", err)
		panic(err)
	}
}

func checkAcctForStakeHistorySysvar(txCtx *TransactionCtx, instrCtx *InstructionCtx, instrAcctIdx uint64) error {
	idxInTx, err := instrCtx.IndexOfInstructionAccountInTransaction(instrAcctIdx)
	if err != nil {
		return err
	}
	pk, err := txCtx.KeyOfAccountAtIndex(idxInTx)
	if err != nil {
		return err
	}
	if pk == SysvarStakeHistoryAddr {
		return nil
	} else {
		return InstrErrInvalidArgument
	}
}
