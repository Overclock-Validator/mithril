package dashboard

import (
	"time"

	"github.com/gagliardetto/solana-go"
)

const replayDetailRemainingHeader = "Mithril-Detail-Remaining-Milliseconds"

func validReplayDetailTime(at time.Time) bool {
	_, offset := at.Zone()
	return !at.IsZero() && at.Year() >= 1 && at.Year() <= 9999 && offset == 0
}

func validReplayDetailHash(text string, nonzero bool) bool {
	if len(text) < 32 || len(text) > 44 {
		return false
	}
	hash, err := solana.HashFromBase58(text)
	return err == nil && hash.String() == text && (!nonzero || hash != (solana.Hash{}))
}
