package sealevel

import (
	"bytes"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/base58"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	"github.com/Overclock-Validator/wide"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

const SysvarEpochRewardsAddrStr = "SysvarEpochRewards1111111111111111111111111"

var SysvarEpochRewardsAddr = base58.MustDecodeFromString(SysvarEpochRewardsAddrStr)

const SysvarEpochRewardsStructLen = 96

type SysvarEpochRewards struct {
	DistributionStartingBlockHeight uint64
	NumPartitions                   uint64
	ParentBlockhash                 [32]byte
	TotalPoints                     wide.Uint128
	TotalRewards                    uint64
	DistributedRewards              uint64
	Active                          bool
}

func (ser *SysvarEpochRewards) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	ser.DistributionStartingBlockHeight, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return fmt.Errorf("failed to read DistributionStartingBlockHeight when decoding SysvarEpochRewards: %w", err)
	}

	ser.NumPartitions, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return fmt.Errorf("failed to read NumPartitions when decoding SysvarEpochRewards: %w", err)
	}

	parentBlockhash, err := decoder.ReadBytes(32)
	if err != nil {
		return fmt.Errorf("failed to read ParentBlockhash when decoding SysvarEpochRewards: %w", err)
	}
	copy(ser.ParentBlockhash[:], parentBlockhash)

	ser.TotalPoints.Lo, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return fmt.Errorf("failed to read lower 8 bytes of TotalPoints when decoding SysvarEpochRewards: %w", err)
	}

	ser.TotalPoints.Hi, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return fmt.Errorf("failed to read upper 8 bytes of TotalPoints when decoding SysvarEpochRewards: %w", err)
	}

	ser.TotalRewards, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return fmt.Errorf("failed to read TotalRewards when decoding SysvarEpochRewards: %w", err)
	}

	ser.DistributedRewards, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return fmt.Errorf("failed to read DistributedRewards when decoding SysvarEpochRewards: %w", err)
	}

	ser.Active, err = ReadBool(decoder)
	return err
}

func (ser *SysvarEpochRewards) MarshalWithEncoder(encoder *bin.Encoder) error {
	var err error

	err = encoder.WriteUint64(ser.DistributionStartingBlockHeight, bin.LE)
	if err != nil {
		return fmt.Errorf("failed to write DistributionStartingBlockHeight when decoding SysvarEpochRewards: %w", err)
	}

	err = encoder.WriteUint64(ser.NumPartitions, bin.LE)
	if err != nil {
		return fmt.Errorf("failed to write NumPartitions when decoding SysvarEpochRewards: %w", err)
	}

	err = encoder.WriteBytes(ser.ParentBlockhash[:], false)
	if err != nil {
		return fmt.Errorf("failed to write ParentBlockhash when decoding SysvarEpochRewards: %w", err)
	}

	err = encoder.WriteUint64(ser.TotalPoints.Lo, bin.LE)
	if err != nil {
		return fmt.Errorf("failed to write lower 8 bytes of TotalPoints when decoding SysvarEpochRewards: %w", err)
	}

	err = encoder.WriteUint64(ser.TotalPoints.Hi, bin.LE)
	if err != nil {
		return fmt.Errorf("failed to write upper 8 bytes of TotalPoints when decoding SysvarEpochRewards: %w", err)
	}

	err = encoder.WriteUint64(ser.TotalRewards, bin.LE)
	if err != nil {
		return fmt.Errorf("failed to write TotalRewards when decoding SysvarEpochRewards: %w", err)
	}

	err = encoder.WriteUint64(ser.DistributedRewards, bin.LE)
	if err != nil {
		return fmt.Errorf("failed to write DistributedRewards when decoding SysvarEpochRewards: %w", err)
	}

	err = encoder.WriteBool(ser.Active)
	return err
}

func (sr *SysvarEpochRewards) MustUnmarshalWithDecoder(decoder *bin.Decoder) {
	err := sr.UnmarshalWithDecoder(decoder)
	if err != nil {
		panic(err.Error())
	}
}

func (sr *SysvarEpochRewards) MustMarshalWithEncoder(encoder *bin.Encoder) {
	err := sr.MarshalWithEncoder(encoder)
	if err != nil {
		panic(err.Error())
	}
}

func (sr *SysvarEpochRewards) Distribute(amount uint64) {
	if safemath.SaturatingAddU64(sr.DistributedRewards, amount) > sr.TotalRewards {
		panic("should be impossible")
	}

	sr.DistributedRewards += amount
}

func ReadEpochRewardsSysvar(execCtx *ExecutionCtx) (SysvarEpochRewards, error) {
	if execCtx != nil && execCtx.SlotCtx != nil {
		if bankSysvars := execCtx.SlotCtx.BankSysvars(); bankSysvars != nil {
			epochRewards, ok := bankSysvars.EpochRewards()
			if !ok {
				return SysvarEpochRewards{}, InstrErrUnsupportedSysvar
			}
			return epochRewards, nil
		}
	}

	if epochRewardsAcct, ok := localSysvarAccount(execCtx, SysvarEpochRewardsAddr); ok {
		if epochRewardsAcct.Lamports == 0 {
			return SysvarEpochRewards{}, InstrErrUnsupportedSysvar
		}
		var epochRewards SysvarEpochRewards
		if err := epochRewards.UnmarshalWithDecoder(bin.NewBinDecoder(epochRewardsAcct.Data)); err != nil {
			return SysvarEpochRewards{}, InstrErrUnsupportedSysvar
		}
		return epochRewards, nil
	}

	if SysvarCache.EpochRewards.Sysvar != nil {
		return *SysvarCache.EpochRewards.Sysvar, nil
	}
	return SysvarEpochRewards{}, InstrErrUnsupportedSysvar
}

func WriteEpochRewardsSysvar(accts *accounts.Accounts, epochRewards SysvarEpochRewards) {

	epochRewardsSysvarAcct, err := (*accts).GetAccount(&SysvarEpochRewardsAddr)
	if err != nil {
		panic("failed to read EpochRewards sysvar account")
	}

	data := new(bytes.Buffer)
	enc := bin.NewBinEncoder(data)

	err = epochRewards.MarshalWithEncoder(enc)

	epochRewardsSysvarAcct.Data = data.Bytes()

	err = (*accts).SetAccount(&SysvarEpochRewardsAddr, epochRewardsSysvarAcct)
	if err != nil {
		err = fmt.Errorf("failed write newly serialized EpochRewards sysvar to sysvar account: %w", err)
		panic(err)
	}
}

func (sr SysvarEpochRewards) String() string {
	fmtStr := "EpochRewards { distribution_starting_block_height: %d, num_partitions: %d, parent_blockhash: %s, total_points: %s, total_rewards: %d, distributed_rewards: %d, active: %t"
	str := fmt.Sprintf(fmtStr, sr.DistributionStartingBlockHeight, sr.NumPartitions, solana.HashFromBytes(sr.ParentBlockhash[:]), sr.TotalPoints, sr.TotalRewards, sr.DistributedRewards, sr.Active)
	return str
}
