package replay

import (
	"testing"
	"time"
)

// A claim wait splits the early execution at the turn: head start before it,
// the committing worker's wait after it.
func TestWorkAheadClaimWaitRecord(t *testing.T) {
	var none *workAheadClaims
	none.record(0, &workAheadEntry{}, time.Now(), -1) // nil-safe when off
	none.log(0)

	began := time.Now()
	e := &workAheadEntry{assumed: 2, usedKCU: 31, began: began, ended: began.Add(300 * time.Microsecond)}
	c := &workAheadClaims{}
	c.record(7, e, began.Add(100*time.Microsecond), 250_000)
	if len(c.waits) != 1 {
		t.Fatalf("waits %d", len(c.waits))
	}
	r := c.waits[0]
	if r.tx != 7 || r.assumed != 2 || r.kcu != 31 || r.headNs != 100_000 || r.waitNs != 200_000 || r.runNs != 300_000 || r.canonNs != 250_000 {
		t.Fatalf("record %+v", r)
	}
	c.log(1)
	if len(c.waits) != 0 {
		t.Fatal("log empties the block's records")
	}
}
