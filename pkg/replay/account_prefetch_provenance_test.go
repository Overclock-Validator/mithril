package replay

import (
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// No worker is needed: inspect the offered job synchronously, so a rejected
// fallback cannot be hidden by asynchronous consumption or timing assertions.
func provenancePrefetcher() *accountPrefetcher {
	return &accountPrefetcher{
		refs:     make(map[uint64][]*turbine.StreamAccountRefs),
		streamed: make(map[uint64]struct{}),
		jobs:     make(chan accountPrefetchJob, 4),
		turn:     make(chan struct{}, 1),
	}
}

func TestAccountPrefetchStreamProvenanceSurvivesLostMarkers(t *testing.T) {
	for _, scenario := range []string{"offered before rewind", "expired marker", "missing summary"} {
		t.Run(scenario, func(t *testing.T) {
			p := provenancePrefetcher()
			blk := &b.Block{Slot: 13, FromLiveStream: true, Transactions: []*solana.Transaction{lookupTestTx(t)}}
			switch scenario {
			case "offered before rewind":
				p.OfferStreamRefs(13, &turbine.StreamAccountRefs{Keys: []solana.PublicKey{testPk(3)}})
				p.Started(13)
				p.Offer(blk) // consumes the marker even though this offer is late
			case "expired marker":
				p.OfferStreamRefs(13, &turbine.StreamAccountRefs{Keys: []solana.PublicKey{testPk(3)}})
				p.Started(14 + accountPrefetchStreamedKeep)
			}
			p.Started(11)
			require.Empty(t, p.streamed)
			require.Empty(t, p.refs)
			late := p.late.Load()
			p.Offer(blk)
			require.Equal(t, late+1, p.late.Load())
			require.Empty(t, p.jobs, "stream messages must not be traversed when immutable summaries are missing")
		})
	}
}

func TestAccountPrefetchStreamProvenanceKeepsSafeSources(t *testing.T) {
	p := provenancePrefetcher()
	p.Started(11)
	p.OfferStreamRefs(13, &turbine.StreamAccountRefs{Keys: []solana.PublicKey{testPk(3)}})
	p.Offer(&b.Block{Slot: 13, FromLiveStream: true, Transactions: []*solana.Transaction{lookupTestTx(t)}})
	require.Len(t, p.jobs, 1)
	require.Equal(t, []solana.PublicKey{testPk(3)}, (<-p.jobs).keys, "live blocks still use immutable summaries")
	p.Offer(&b.Block{Slot: 14, Transactions: []*solana.Transaction{lookupTestTx(t)}})
	require.Len(t, p.jobs, 1, "non-streamed blocks retain the pre-emission snapshot path")
	require.NotEmpty(t, (<-p.jobs).keys)
}
