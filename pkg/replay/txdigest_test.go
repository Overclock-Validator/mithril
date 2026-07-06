package replay

import (
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func capSig(b byte) solana.Signature { var s solana.Signature; s[0] = b; return s }

// With no run capturing, the record path no-ops and drains return nothing —
// there is no sticky global left enabled from a prior run.
func TestTxCaptureInactiveByDefault(t *testing.T) {
	require.Nil(t, activeCapture.Load(), "no capture registry should be published at rest")
	assert.False(t, txCaptureActive())
	recordTxExecCapture(10, capSig(1), &txExecRecord{Fee: 5})
	assert.Nil(t, takeTxCaptures(10), "records must not be stored while inactive")
}

// A run's captures are isolated to that run: a second run cannot see or consume
// the first run's records, and unpublishing clears capture.
func TestTxCaptureIsRunLocal(t *testing.T) {
	stop1 := beginTxCapture()
	require.True(t, txCaptureActive())
	recordTxExecCapture(100, capSig(1), &txExecRecord{Fee: 11, Pre: []uint64{1}})
	recordTxExecCapture(100, capSig(2), &txExecRecord{Fee: 22})

	// A fresh run publishes its OWN registry — the first run's records are not
	// visible through it (no cross-run consumption / false digest data).
	stop2 := beginTxCapture()
	assert.Nil(t, takeTxCaptures(100), "second run must not see the first run's records")
	recordTxExecCapture(100, capSig(9), &txExecRecord{Fee: 99})
	got := takeTxCaptures(100)
	require.Len(t, got, 1)
	assert.Equal(t, uint64(99), got[capSig(9)].Fee)
	stop2()

	// Unpublishing the last active run turns capture off.
	assert.False(t, txCaptureActive(), "capture is off once the active run stops")
	recordTxExecCapture(100, capSig(3), &txExecRecord{Fee: 33})
	assert.Nil(t, takeTxCaptures(100), "no records after the run stopped")

	stop1() // idempotent CAS: does not clobber a different published registry
	assert.False(t, txCaptureActive())
}

// takeTxCaptures drains a slot exactly once; the janitor drops slots far below
// the newest so an unwound/abandoned slot cannot leak.
func TestTxCaptureDrainAndJanitor(t *testing.T) {
	stop := beginTxCapture()
	defer stop()

	recordTxExecCapture(200, capSig(1), &txExecRecord{Fee: 1})
	first := takeTxCaptures(200)
	require.Len(t, first, 1)
	assert.Nil(t, takeTxCaptures(200), "a slot drains exactly once")

	// A slot far below the newest is janitored on the next record.
	recordTxExecCapture(1000, capSig(2), &txExecRecord{Fee: 2})
	recordTxExecCapture(5000, capSig(3), &txExecRecord{Fee: 3}) // 1000 + 256 < 5000 -> 1000 evicted
	assert.Nil(t, takeTxCaptures(1000), "stale slot must be janitored")
	assert.Len(t, takeTxCaptures(5000), 1)
}
