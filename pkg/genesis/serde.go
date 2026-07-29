package genesis

import (
	"io"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/runtime"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

// Dumping ground for handwritten serialization boilerplate.
// To be removed when switching over to serde-generate.

func (g *Genesis) UnmarshalWithDecoder(decoder *bin.Decoder) (err error) {
	var raw struct {
		CreationTime   int64
		NumAccounts    uint64 `bin:"sizeof=Accounts"`
		Accounts       []AccountEntry
		NumBuiltins    uint64 `bin:"sizeof=Builtins"`
		Builtins       []BuiltinProgram
		NumRewardPools uint64 `bin:"sizeof=RewardPools"`
		RewardPools    []AccountEntry
		TicksPerSlot   uint64
		Padding00      uint64
		PohParams      runtime.PohParams
		Padding01      uint64
		Fees           runtime.FeeParams
		Rent           runtime.RentParams
		Inflation      runtime.InflationParams
		EpochSchedule  runtime.EpochSchedule
		ClusterID      uint32
	}
	if err = decoder.Decode(&raw); err != nil {
		return err
	}
	*g = Genesis{
		CreationTime:  time.Unix(raw.CreationTime, 0).UTC(),
		Accounts:      raw.Accounts,
		Builtins:      raw.Builtins,
		RewardPools:   raw.RewardPools,
		TicksPerSlot:  raw.TicksPerSlot,
		PohParams:     raw.PohParams,
		Fees:          raw.Fees,
		Rent:          raw.Rent,
		Inflation:     raw.Inflation,
		EpochSchedule: raw.EpochSchedule,
		ClusterID:     raw.ClusterID,
	}
	return nil
}

func (g *Genesis) MarshalWithEncoder(_ *bin.Encoder) (err error) {
	// TODO not implemented
	panic("not implemented")
}

// UnmarshalWithDecoder reads one genesis account entry.
//
// This deliberately does not delegate to accounts.Account.UnmarshalWithDecoder.
// That codec is the accountsdb/snapshot representation and leads with Slot
// (u64) and Key (32 bytes), which are Mithril's own bookkeeping and are absent
// from the genesis wire format. Delegating consumed 40 phantom bytes per entry,
// desynchronised the stream and failed the whole decode with "unexpected EOF"
// on the Accounts field.
//
// The genesis format is bincode over Agave's (Pubkey, Account) pair:
//
//	pubkey     [32]byte
//	lamports   u64
//	data       u64 length + bytes
//	owner      [32]byte
//	executable bool
//	rent_epoch u64
//
// Key is populated from the entry's own pubkey so downstream users of
// AccountEntry.Account see a fully-formed account; Slot stays zero, which is
// correct for genesis.
func (a *AccountEntry) UnmarshalWithDecoder(decoder *bin.Decoder) (err error) {
	if err = decoder.Decode(&a.Pubkey); err != nil {
		return err
	}
	a.Account.Key = solana.PublicKeyFromBytes(a.Pubkey[:])

	if a.Account.Lamports, err = decoder.ReadUint64(bin.LE); err != nil {
		return err
	}

	var dataLen uint64
	if dataLen, err = decoder.ReadUint64(bin.LE); err != nil {
		return err
	}
	if dataLen > uint64(decoder.Remaining()) {
		return io.ErrUnexpectedEOF
	}
	if a.Account.Data, err = decoder.ReadNBytes(int(dataLen)); err != nil {
		return err
	}

	var ownerBytes []byte
	if ownerBytes, err = decoder.ReadNBytes(solana.PublicKeyLength); err != nil {
		return err
	}
	copy(a.Account.Owner[:], ownerBytes)

	if a.Account.Executable, err = decoder.ReadBool(); err != nil {
		return err
	}

	a.Account.RentEpoch, err = decoder.ReadUint64(bin.LE)
	return err
}

func (a *AccountEntry) MarshalWihEncoder(encoder *bin.Encoder) (err error) {
	if err = encoder.WriteBytes(a.Pubkey[:], false); err != nil {
		return err
	}
	if err = encoder.WriteUint64(a.Account.Lamports, bin.LE); err != nil {
		return err
	}
	if err = encoder.WriteUint64(uint64(len(a.Account.Data)), bin.LE); err != nil {
		return err
	}
	if err = encoder.WriteBytes(a.Account.Data, false); err != nil {
		return err
	}
	if err = encoder.WriteBytes(a.Account.Owner[:], false); err != nil {
		return err
	}
	if err = encoder.WriteBool(a.Account.Executable); err != nil {
		return err
	}
	return encoder.WriteUint64(a.Account.RentEpoch, bin.LE)
}

func (b *BuiltinProgram) UnmarshalWithDecoder(decoder *bin.Decoder) (err error) {
	var strLen uint64
	if strLen, err = decoder.ReadUint64(bin.LE); err != nil {
		return err
	}
	if strLen > uint64(decoder.Remaining()) {
		return io.ErrUnexpectedEOF
	}
	var strBytes []byte
	if strBytes, err = decoder.ReadNBytes(int(strLen)); err != nil {
		return err
	}
	b.Key = string(strBytes)
	return decoder.Decode(&b.Pubkey)
}

func (*BuiltinProgram) MarshalWihEncoder(_ *bin.Encoder) (err error) {
	// TODO not implemented
	panic("not implemented")
}
