package replay

import (
	"bytes"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/Overclock-Validator/mithril/pkg/base58"
)

// CaptureBankHashes, when non-nil, holds the bank hash a backtest capture
// recorded for each slot. Replay checks every block it executes against it,
// after the footer check, and stops on the first slot that disagrees or has no
// record, as Firedancer's backtest does. A normal node leaves it nil.
var CaptureBankHashes map[uint64][32]byte

// ReplayReadyAt is when the replay loop was first about to fetch a block in
// this process (Unix nanoseconds, zero until then): the end of loading, from
// which a backtest times replay (Firedancer's replay_ready_time).
var ReplayReadyAt atomic.Int64

// SkipPhaseTimers turns off replay's per-transaction and instruction-dispatch
// phase timers (the detail behind TxLoop), as block production already does.
// Block-level intervals such as SlotReplay and TxLoop are unaffected; the
// per-transaction timing fields of the replay rows stay empty.
var SkipPhaseTimers bool

// ErrCaptureBankHash marks a block whose bank hash disagrees with the capture
// or is missing from it.
var ErrCaptureBankHash = errors.New("capture bank hash check failed")

func verifyCaptureBankHash(slot uint64, computed []byte) error {
	if CaptureBankHashes == nil {
		return nil
	}
	want, ok := CaptureBankHashes[slot]
	if !ok {
		return fmt.Errorf("%w: no bank hash available for slot %d", ErrCaptureBankHash, slot)
	}
	if !bytes.Equal(want[:], computed) {
		return fmt.Errorf("%w: Bank hash mismatch! slot=%d expected=%s, got=%s",
			ErrCaptureBankHash, slot, base58.Encode(want[:]), base58.Encode(computed))
	}
	return nil
}
