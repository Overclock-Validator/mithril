package node

import (
	"errors"
	"math"
	"slices"
	"strconv"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
)

// captureVATEpochStakes groups an immutable VAT-filtered vote-weight snapshot
// by node identity in canonical base58 text order, not protocol rank order. It
// neither repeats admission nor applies BLS rank filtering.
func captureVATEpochStakes(snapshot epochstakes.Snapshot, known bool) (*dashboard.HeldEpochStakeSet, error) {
	if !known {
		return nil, errors.New("waiting_source")
	}
	switch snapshot.Selection {
	case epochstakes.SelectionVATFiltered:
	case epochstakes.SelectionUnfiltered:
		return nil, errors.New("not_applicable")
	default:
		return nil, errors.New("selection_unverified")
	}
	if snapshot.Generation == 0 {
		return nil, errors.New("source_invalid")
	}
	if len(snapshot.Stakes) > alpenglow.MaximumVATValidators {
		return nil, errors.New("response_limit")
	}
	weights := make(map[string]uint64, len(snapshot.Stakes))
	var total uint64
	for vote, stake := range snapshot.Stakes {
		meta := snapshot.VoteAccounts[vote]
		if vote.IsZero() || stake == 0 || meta == nil || meta.NodePubkey.IsZero() {
			return nil, errors.New("source_invalid")
		}
		identity := meta.NodePubkey.String()
		if math.MaxUint64-total < stake || math.MaxUint64-weights[identity] < stake {
			return nil, errors.New("source_invalid")
		}
		total += stake
		weights[identity] += stake
	}
	if total != snapshot.TotalStake {
		return nil, errors.New("source_invalid")
	}
	identities := make([]string, 0, len(weights))
	for identity := range weights {
		identities = append(identities, identity)
	}
	slices.Sort(identities)
	result := &dashboard.HeldEpochStakeSet{Generation: snapshot.Generation, TotalStake: total,
		Identities: identities, Stakes: make([]string, len(identities))}
	for i, identity := range identities {
		result.Stakes[i] = strconv.FormatUint(weights[identity], 10)
	}
	return result, nil
}
