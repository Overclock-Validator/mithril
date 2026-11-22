package replay

import (
	"os"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
)

// MITHRIL_WORK_AHEAD_CLAIM_LOG=1 (diagnostic, log only): one line per valid
// early execution that a committing worker waited on at its turn, logged at
// block end so the committing path only appends.
//
//   - head-ns: from the early execution's start to the turn (its head start);
//   - wait-ns: from the turn to its end (the committing worker's wait);
//   - run-ns: its whole duration (head-ns + wait-ns);
//   - canon-ns: in verify mode, the canonical execution of the same
//     transaction at the turn, a fresh run's duration; -1 otherwise.
//
// Waiting beat a fresh start when wait-ns < canon-ns. run-ns / canon-ns above
// one means the early execution itself ran slower than a fresh one.
var workAheadClaimLog = os.Getenv("MITHRIL_WORK_AHEAD_CLAIM_LOG") == "1"

type workAheadClaimWait struct {
	tx, assumed                    int
	kcu                            int64
	headNs, waitNs, runNs, canonNs int64
}

type workAheadClaims struct {
	mu    sync.Mutex
	waits []workAheadClaimWait
}

// record notes a wait on e that began at turn; e.ended is set. Called with
// e.mu held.
func (c *workAheadClaims) record(tx int, e *workAheadEntry, turn time.Time, canonNs int64) {
	if c == nil {
		return
	}
	r := workAheadClaimWait{
		tx: tx, assumed: e.assumed, kcu: e.usedKCU,
		headNs: int64(turn.Sub(e.began)), waitNs: int64(e.ended.Sub(turn)), runNs: int64(e.ended.Sub(e.began)),
		canonNs: canonNs,
	}
	c.mu.Lock()
	c.waits = append(c.waits, r)
	c.mu.Unlock()
}

func (c *workAheadClaims) log(slot uint64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range c.waits {
		mlog.Log.Infof("work-ahead claim-wait slot %d tx %d assumed %d kcu %d head-ns %d wait-ns %d run-ns %d canon-ns %d",
			slot, r.tx, r.assumed, r.kcu, r.headNs, r.waitNs, r.runNs, r.canonNs)
	}
	c.waits = nil
}
