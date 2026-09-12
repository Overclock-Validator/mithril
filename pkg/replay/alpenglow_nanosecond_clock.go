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

	var maxOffset int64
	if elapsedSlotDurationNanos > uint64(math.MaxInt64)/2 {
		maxOffset = math.MaxInt64
	} else {
		maxOffset = int64(elapsedSlotDurationNanos * 2)
	}

	upper := parentNanos
	if parentNanos > math.MaxInt64-maxOffset {
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
	if acct, err := acctsDb.GetAccount(slot, NanosecondClockAccountAddr()); err == nil &&
		acct != nil && len(acct.Data) >= nanosecondClockDataLen {
		return int64(binary.LittleEndian.Uint64(acct.Data[:nanosecondClockDataLen])), true
	}

	clockAcct, err := acctsDb.GetAccount(slot, sealevel.SysvarClockAddr)
	if err != nil || clockAcct == nil {
		return 0, false
	}
	var clock sealevel.SysvarClock
	if err := clock.UnmarshalWithDecoder(bin.NewBinDecoder(clockAcct.Data)); err != nil {
		return 0, false
	}
	return clock.UnixTimestamp * 1_000_000_000, true
}

// ReadNanosecondClockFromSlotCtx returns the exact parent-time anchor used for
// Alpenglow footer bounds. The parent account is pinned when the bank is built;
// a transaction in the child bank must not be able to change this value.
//
// Before the nanosecond account is populated, Agave falls back to the child
// bank's pre-footer Clock timestamp. Neither path consults a process-global or
// unrooted account view.
func ReadNanosecondClockFromSlotCtx(slotCtx *sealevel.SlotCtx) (int64, bool) {
	nanos, err := nanosecondClockAnchor(slotCtx)
	return nanos, err == nil
}

func nanosecondClockAnchor(slotCtx *sealevel.SlotCtx) (int64, error) {
	if slotCtx == nil {
		return 0, fmt.Errorf("missing slot context")
	}
	var nanoClockAcct *accounts.Account
	if slotCtx.ParentAccts != nil {
		acct, err := slotCtx.GetParentAccount(NanosecondClockAccountAddr())
		if err != nil {
			return 0, fmt.Errorf("parent nanosecond clock was not pinned: %w", err)
		}
		nanoClockAcct = acct
	} else if slotCtx.Accounts != nil {
		// Compatibility for isolated callers that predate parent snapshots.
		nanoClockAcct, _ = slotCtx.GetAccount(NanosecondClockAccountAddr())
	}
	if nanoClockAcct != nil && nanoClockAcct.Lamports > 0 && len(nanoClockAcct.Data) != 0 {
		if len(nanoClockAcct.Data) < nanosecondClockDataLen {
			return 0, fmt.Errorf("parent nanosecond clock has invalid data length %d", len(nanoClockAcct.Data))
		}
		return int64(binary.LittleEndian.Uint64(nanoClockAcct.Data[:nanosecondClockDataLen])), nil
	}
	if bankSysvars := slotCtx.BankSysvars(); bankSysvars != nil {
		if clock, ok := bankSysvars.Clock(); ok {
			return secondsToNanosecondsSaturating(clock.UnixTimestamp), nil
		}
	}
	return 0, fmt.Errorf("bank-local Clock sysvar is unavailable")
}

func secondsToNanosecondsSaturating(seconds int64) int64 {
	const nanosPerSecond = int64(1_000_000_000)
	if seconds > math.MaxInt64/nanosPerSecond {
		return math.MaxInt64
	}
	if seconds < math.MinInt64/nanosPerSecond {
		return math.MinInt64
	}
	return seconds * nanosPerSecond
}

// validateAlpenglowFooterNanosecondClock mirrors Agave's footer-time check.
// FooterProducerTimeNanos is a required wire value here: zero is numeric zero,
// not an invitation to fall back to the second-resolution compatibility field.
func validateAlpenglowFooterNanosecondClock(slotCtx *sealevel.SlotCtx, block *b.Block) error {
	if slotCtx == nil || block == nil {
		return fmt.Errorf("cannot validate Alpenglow footer nanosecond clock without bank state")
	}
	if !block.HasAlpenglowFooter {
		return fmt.Errorf("slot %d missing block footer", block.Slot)
	}
	if block.FooterProducerTimeNanos > uint64(math.MaxInt64) {
		return fmt.Errorf("slot %d footer nanosecond clock out of bounds: producer time %d overflows i64", block.Slot, block.FooterProducerTimeNanos)
	}
	parentNanos, err := nanosecondClockAnchor(slotCtx)
	if err != nil {
		return fmt.Errorf("slot %d footer nanosecond clock: %w", block.Slot, err)
	}

	var elapsed uint64
	if slotCtx.Slot > slotCtx.ParentSlot {
		slotGap := slotCtx.Slot - slotCtx.ParentSlot
		if slotGap > math.MaxUint64/uint64(alpenglowNsPerSlot) {
			elapsed = math.MaxUint64
		} else {
			elapsed = slotGap * uint64(alpenglowNsPerSlot)
		}
	}
	lower, upper := NanosecondClockBounds(parentNanos, elapsed)
	producerNanos := int64(block.FooterProducerTimeNanos)
	if producerNanos < lower || producerNanos > upper {
		return fmt.Errorf(
			"slot %d footer nanosecond clock out of bounds: producer=%d parent=%d bounds=[%d,%d]",
			block.Slot, producerNanos, parentNanos, lower, upper,
		)
	}
	return nil
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
