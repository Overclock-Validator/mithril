package sealevel

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/base58"
	bin "github.com/gagliardetto/binary"
)

const SysvarSlotHistoryAddrStr = "SysvarS1otHistory11111111111111111111111111"

var SysvarSlotHistoryAddr = base58.MustDecodeFromString(SysvarSlotHistoryAddrStr)

type SlotHistoryInner struct {
	BlocksLen uint64
	Blocks    []uint64
}

type SlotHistoryBitvec struct {
	Bits SlotHistoryInner
	Len  uint64
}

type SysvarSlotHistory struct {
	Bits     SlotHistoryBitvec
	NextSlot uint64
}

const (
	slotHistoryMaxEntries = 1024 * 1024
	bitsPerBlock          = 64
)

func (sh *SysvarSlotHistory) UnmarshalWithDecoder(decoder *bin.Decoder) (err error) {

	opt, err := decoder.ReadByte()

	if opt != 0 {
		sh.Bits.Bits.BlocksLen, err = decoder.ReadUint64(bin.LE)
		if err != nil {
			return fmt.Errorf("failed to read BlocksLen when decoding SysvarSlotHistory: %w", err)
		}

		// The bit vector is 16,384 words on mainnet and is decoded for every
		// bank, so it is read in one piece. A count the data cannot hold is
		// an error before anything is allocated for it.
		words := sh.Bits.Bits.BlocksLen
		if words > uint64(decoder.Remaining())/8 {
			return fmt.Errorf("failed to read a block when decoding Bitvec in SysvarSlotHistory: %d words in %d bytes", words, decoder.Remaining())
		}
		raw, err := decoder.ReadNBytes(int(words) * 8)
		if err != nil {
			return fmt.Errorf("failed to read a block when decoding Bitvec in SysvarSlotHistory: %w", err)
		}
		blocks := slices.Grow(sh.Bits.Bits.Blocks, int(words))
		for ; len(raw) >= 8; raw = raw[8:] {
			blocks = append(blocks, binary.LittleEndian.Uint64(raw))
		}
		sh.Bits.Bits.Blocks = blocks
	}

	sh.Bits.Len, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return fmt.Errorf("failed to read Len when decoding Bitvec in SysvarSlotHistory: %w", err)
	}

	sh.NextSlot, err = decoder.ReadUint64(bin.LE)
	if err != nil {
		return fmt.Errorf("failed to read NextSlot when decoding SysvarSlotHistory: %w", err)
	}

	return
}

func (sr *SysvarSlotHistory) MustUnmarshalWithDecoder(decoder *bin.Decoder) {
	err := sr.UnmarshalWithDecoder(decoder)
	if err != nil {
		panic(err.Error())
	}
}

// MustMarshal encodes the sysvar: an option byte, then, when the bit vector
// has words, their count and the words, then the bit length and the next
// slot, all little-endian. It runs for every bank, so it writes straight into
// a buffer of the final size.
func (sr *SysvarSlotHistory) MustMarshal() []byte {
	words := sr.Bits.Bits.BlocksLen
	if words == 0 {
		data := make([]byte, 1+8+8)
		binary.LittleEndian.PutUint64(data[1:], sr.Bits.Len)
		binary.LittleEndian.PutUint64(data[9:], sr.NextSlot)
		return data
	}
	_ = sr.Bits.Bits.Blocks[words-1] // panics, as before, if BlocksLen overstates the words held
	blocks := sr.Bits.Bits.Blocks[:words]
	data := make([]byte, 1+8+8*len(blocks)+8+8)
	data[0] = 1
	binary.LittleEndian.PutUint64(data[1:], words)
	at := data[9:]
	for _, block := range blocks {
		binary.LittleEndian.PutUint64(at, block)
		at = at[8:]
	}
	binary.LittleEndian.PutUint64(at, sr.Bits.Len)
	binary.LittleEndian.PutUint64(at[8:], sr.NextSlot)
	return data
}

func (sr *SysvarSlotHistory) Add(slot uint64) {
	slotHistory := *sr

	if slot > slotHistory.NextSlot && (slot-slotHistory.NextSlot) >= slotHistoryMaxEntries {
		for idx := uint64(0); idx < slotHistory.Bits.Bits.BlocksLen; idx++ {
			slotHistory.Bits.Bits.Blocks[idx] = 0
		}
	} else {
		for j := slotHistory.NextSlot; j < slot; j++ {
			blockIdx := (j / bitsPerBlock) % slotHistory.Bits.Bits.BlocksLen
			slotHistory.Bits.Bits.Blocks[blockIdx] &= ^(uint64(1) << (j % bitsPerBlock))
		}
	}

	blockIdx := (slot / bitsPerBlock) % slotHistory.Bits.Bits.BlocksLen
	slotHistory.Bits.Bits.Blocks[blockIdx] |= (uint64(1) << (slot % bitsPerBlock))
}

func (sr *SysvarSlotHistory) SetNextSlot(nextSlot uint64) {
	sr.NextSlot = nextSlot
}

func ReadSlotHistorySysvar(execCtx *ExecutionCtx) SysvarSlotHistory {
	if execCtx != nil && execCtx.SlotCtx != nil {
		if bankSysvars := execCtx.SlotCtx.BankSysvars(); bankSysvars != nil {
			slotHistory, ok := bankSysvars.SlotHistory()
			if !ok {
				panic("SlotHistory sysvar is absent from bank snapshot")
			}
			return slotHistory
		}
	}

	if slotHistoryAcct, ok := localSysvarAccount(execCtx, SysvarSlotHistoryAddr); ok {
		if slotHistoryAcct.Lamports == 0 {
			panic("SlotHistory sysvar account is absent")
		}
		var slotHistory SysvarSlotHistory
		slotHistory.MustUnmarshalWithDecoder(bin.NewBinDecoder(slotHistoryAcct.Data))
		return slotHistory
	}

	if SysvarCache.SlotHistory.Sysvar != nil {
		return *SysvarCache.SlotHistory.Sysvar
	}
	panic("failed to read SlotHistory sysvar account")
}

func WriteSlotHistorySysvar(accts *accounts.Accounts, slotHistory SysvarSlotHistory) {

	slotHistorySysvarAcct, err := (*accts).GetAccount(&SysvarSlotHistoryAddr)
	if err != nil {
		panic("failed to read EpochRewards sysvar account")
	}

	data := new(bytes.Buffer)
	enc := bin.NewBinEncoder(data)

	if slotHistory.Bits.Bits.BlocksLen != 0 {
		err = enc.WriteByte(1)
		if err != nil {
			err = fmt.Errorf("failed to serialize opt byte for SlotHistory sysvar: %w", err)
			panic(err)
		}

		err = enc.WriteUint64(slotHistory.Bits.Bits.BlocksLen, bin.LE)
		if err != nil {
			err = fmt.Errorf("failed to serialize BlocksLen for SlotHistory sysvar: %w", err)
			panic(err)
		}

		for count := 0; count < int(slotHistory.Bits.Bits.BlocksLen); count++ {
			err = enc.WriteUint64(slotHistory.Bits.Bits.Blocks[count], bin.LE)
			if err != nil {
				err = fmt.Errorf("failed to serialize a Block for SlotHistory sysvar: %w", err)
				panic(err)
			}
		}
	} else {
		err = enc.WriteByte(0)
		if err != nil {
			err = fmt.Errorf("failed to serialize opt byte for SlotHistory sysvar: %w", err)
			panic(err)
		}
	}

	err = enc.WriteUint64(slotHistory.Bits.Len, bin.LE)
	if err != nil {
		err = fmt.Errorf("failed to serialize Bits.Len for SlotHistory sysvar: %w", err)
		panic(err)
	}

	err = enc.WriteUint64(slotHistory.NextSlot, bin.LE)
	if err != nil {
		err = fmt.Errorf("failed to serialize NextSlot for SlotHistory sysvar: %w", err)
		panic(err)
	}

	slotHistorySysvarAcct.Data = data.Bytes()

	err = (*accts).SetAccount(&SysvarSlotHistoryAddr, slotHistorySysvarAcct)
	if err != nil {
		err = fmt.Errorf("failed to write newly serialized SlotHistory sysvar to sysvar account: %w", err)
		panic(err)
	}
}
