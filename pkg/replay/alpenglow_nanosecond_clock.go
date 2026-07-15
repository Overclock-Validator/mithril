package replay

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

const nanosecondClockDataLen = 8

// NanosecondClockBounds mirrors Agave BlockComponentProcessor::nanosecond_time_bounds:
// the footer producer time must satisfy lower <= t <= upper, where
// lower = parentNanos + 1 and upper = parentNanos + 2*elapsedSlotDurationNanos.
func NanosecondClockBounds(parentNanos int64, elapsedSlotDurationNanos uint64) (int64, int64) {
	lower := parentNanos
	if lower != math.MaxInt64 {
		lower++
	}

	offset := elapsedSlotDurationNanos
	if offset > uint64(math.MaxInt64)/2 {
		offset = uint64(math.MaxInt64) / 2
	}
	maxOffset := int64(offset * 2)

	upper := parentNanos
	if maxOffset > math.MaxInt64-parentNanos {
		upper = math.MaxInt64
	} else {
		upper += maxOffset
	}
	return lower, upper
}

// SkewBlockProducerTimeNanos clamps a candidate producer timestamp into the
// Alpenglow clock bounds, matching Agave's block_creation_loop::skew_block_producer_time_nanos.
func SkewBlockProducerTimeNanos(parentNanos, workingNanos int64, elapsedSlotDurationNanos uint64) int64 {
	lower, upper := NanosecondClockBounds(parentNanos, elapsedSlotDurationNanos)
	if workingNanos < lower {
		workingNanos = lower
	}
	if workingNanos > upper {
		workingNanos = upper
	}
	return workingNanos
}

// ReadNanosecondClockAt returns the parent bank's nanosecond clock for footer
// timestamp clamping. It reads the alpenclock PDA at the given slot, falling
// back to the Clock sysvar's unix_timestamp (seconds -> nanoseconds), mirroring
// Agave's bank.get_nanosecond_clock().unwrap_or_else(clock fallback).
func ReadNanosecondClockAt(acctsDb *accountsdb.AccountsDb, slot uint64) (int64, bool) {
	if acctsDb == nil {
		return 0, false
	}
	return readNanosecondClock(func(pubkey solana.PublicKey) (*accounts.Account, error) {
		return acctsDb.GetAccount(slot, pubkey)
	})
}

// ReadResolvedNanosecondClockAt reads through the active speculative overlay
// before falling back to durable AccountsDB. Local production must use this
// path because the selected parent is normally newer than the durable root.
func ReadResolvedNanosecondClockAt(acctsDb *accountsdb.AccountsDb, slot uint64) (int64, bool) {
	return readNanosecondClock(func(pubkey solana.PublicKey) (*accounts.Account, error) {
		return ResolveActiveAccount(acctsDb, slot, pubkey)
	})
}

func readNanosecondClock(load func(solana.PublicKey) (*accounts.Account, error)) (int64, bool) {
	if load == nil {
		return 0, false
	}
	if acct, err := load(NanosecondClockAccountAddr()); err == nil &&
		acct != nil && len(acct.Data) >= nanosecondClockDataLen {
		return int64(binary.LittleEndian.Uint64(acct.Data[:nanosecondClockDataLen])), true
	}

	clockAcct, err := load(sealevel.SysvarClockAddr)
	if err != nil || clockAcct == nil {
		return 0, false
	}
	var clock sealevel.SysvarClock
	if err := clock.UnmarshalWithDecoder(bin.NewBinDecoder(clockAcct.Data)); err != nil {
		return 0, false
	}
	return clock.UnixTimestamp * 1_000_000_000, true
}

func alpenglowFooterProducerTimeNanos(block *b.Block) (int64, bool, error) {
	if block == nil {
		return 0, false, nil
	}
	if block.FooterProducerTimeNanos > 0 {
		if block.FooterProducerTimeNanos > uint64(math.MaxInt64) {
			return 0, false, fmt.Errorf("footer producer time nanos %d overflows i64", block.FooterProducerTimeNanos)
		}
		return int64(block.FooterProducerTimeNanos), true, nil
	}
	if block.UnixTimestamp != 0 {
		return block.UnixTimestamp * 1_000_000_000, true, nil
	}
	return 0, false, nil
}

func encodeNanosecondClockData(nanos int64) []byte {
	out := make([]byte, nanosecondClockDataLen)
	binary.LittleEndian.PutUint64(out, uint64(nanos))
	return out
}

func updateAlpenglowNanosecondClockAccount(slotCtx *sealevel.SlotCtx, block *b.Block) error {
	nanos, ok, err := alpenglowFooterProducerTimeNanos(block)
	if err != nil {
		return fmt.Errorf("slot %d alpenglow nanosecond clock: %w", block.Slot, err)
	}
	if !ok {
		return nil
	}

	addr := NanosecondClockAccountAddr()
	acct, err := slotCtx.GetAccount(addr)
	if err != nil {
		acct = &accounts.Account{
			Key:       addr,
			Owner:     a.SystemProgramAddr,
			RentEpoch: 0,
		}
	}

	// Agave update_clock_from_footer uses Rent::default().minimum_balance(), not the on-chain
	// rent sysvar, and AccountSharedData::new (rent_epoch=0).
	defaultRent := sealevel.NewDefaultRentSysvar()
	acct.Owner = a.SystemProgramAddr
	acct.Executable = false
	acct.RentEpoch = 0
	acct.Lamports = defaultRent.MinimumBalance(nanosecondClockDataLen)
	acct.Data = encodeNanosecondClockData(nanos)

	if err := slotCtx.SetAccount(addr, acct); err != nil {
		return fmt.Errorf("slot %d alpenglow nanosecond clock: %w", block.Slot, err)
	}
	slotCtx.RecordModifiedAcct(addr)
	return nil
}
