package scheduler

import (
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

const expiredHashHistoryCapacity = 1024

// expiryHistory belongs to the scheduler drain goroutine. It remembers only
// blockhashes observed to leave a continuous recent-blockhash window, never
// equating an unknown (possibly future) blockhash with an expired one.
type expiryHistory struct {
	previous sealevel.SysvarRecentBlockhashes // immutable bank snapshot view
	old      map[solana.Hash]struct{}
	ring     [expiredHashHistoryCapacity]solana.Hash
	count    int
	next     int
}

func (h *expiryHistory) expired(hash solana.Hash) bool {
	_, ok := h.old[hash]
	return ok
}

func (h *expiryHistory) observe(hashes sealevel.SysvarRecentBlockhashes) (newlyExpired bool) {
	if len(hashes) == 0 {
		return false
	}
	if len(h.previous) > 0 && hashes[0].Blockhash == h.previous[0].Blockhash {
		return false
	}
	current := make(map[solana.Hash]struct{}, len(hashes))
	for _, e := range hashes {
		current[e.Blockhash] = struct{}{}
	}
	continuous := false
	if len(h.previous) > 0 {
		_, continuous = current[h.previous[0].Blockhash]
	}
	if !continuous {
		// A fork, rewind or sampling gap longer than the window is not proof
		// of expiry. Start again rather than carrying old-branch exclusions.
		clear(h.old)
		h.count, h.next = 0, 0
	} else {
		for _, e := range h.previous {
			if _, valid := current[e.Blockhash]; !valid {
				h.remember(e.Blockhash)
				newlyExpired = true
			}
		}
	}
	h.previous = hashes
	return newlyExpired
}

func (h *expiryHistory) remember(hash solana.Hash) {
	if h.expired(hash) {
		return
	}
	if h.old == nil {
		h.old = make(map[solana.Hash]struct{}, expiredHashHistoryCapacity)
	}
	if h.count == len(h.ring) {
		delete(h.old, h.ring[h.next])
	} else {
		h.count++
	}
	h.old[hash] = struct{}{}
	h.ring[h.next] = hash
	h.next = (h.next + 1) % len(h.ring)
}
