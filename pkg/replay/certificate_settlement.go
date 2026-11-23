package replay

import (
	"time"

	"github.com/gagliardetto/solana-go"
)

// certificateFlushCapture copies only slot hashes, never account-write lists.
// It is captured on the replay owner after draining the async fold worker.
type certificateFlushCapture struct {
	hashes map[uint64][32]byte
	held   int
	valid  bool
}

func captureCertificateFlush(tail *unrootedTail) certificateFlushCapture {
	held := tail.overlay.HeldSlots()
	if held > certificatePendingLimit || len(tail.bankhashes) != held {
		return certificateFlushCapture{}
	}
	hashes := make(map[uint64][32]byte, held)
	for slot, hash := range tail.bankhashes {
		hashes[slot] = hash
	}
	return certificateFlushCapture{hashes: hashes, held: held, valid: true}
}

// settleFlush observes only hashes removed by successful chunks of the existing
// flush. A later chunk may fail; its hashes and overlay layers remain retained.
func (r *CertificateOutcomes) settleFlush(before certificateFlushCapture, tail *unrootedTail, ids map[uint64]solana.Hash, through uint64, at time.Time) {
	if r == nil {
		return
	}
	if !before.valid {
		r.loseCoverage(at)
		return
	}
	removed := make(map[uint64][32]byte)
	for slot, hash := range before.hashes {
		if _, retained := tail.bankhashes[slot]; !retained {
			if slot > through {
				r.loseCoverage(at)
				return
			}
			removed[slot] = hash
		}
	}
	r.settleFold(removed, ids, before.held-tail.overlay.HeldSlots(), at)
}

// settleFold receives actual committed layer hashes before replay prunes the
// selected block IDs. A watermark alone is never sufficient evidence.
func (r *CertificateOutcomes) settleFold(hashes map[uint64][32]byte, ids map[uint64]solana.Hash, slots int, at time.Time) {
	if r == nil {
		return
	}
	if slots < 0 || slots > certificatePendingLimit || len(hashes) != slots {
		r.loseCoverage(at)
		return
	}
	evidence := make([]CertificateFoldEvidence, 0, slots)
	for slot, hash := range hashes {
		id, known := ids[slot]
		evidence = append(evidence, CertificateFoldEvidence{
			Slot: slot, Bankhash: solana.Hash(hash), BlockID: id, HasBlockID: known && !id.IsZero(),
		})
	}
	r.Settle(evidence, at)
}

func (r *CertificateOutcomes) loseCoverage(at time.Time) {
	r.coverageLost = true
	r.dropped++
	clear(r.pending)
	r.refresh(at)
}
