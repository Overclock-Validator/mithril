package epochstakes

import (
	"encoding/json"
	"fmt"
)

// HasInflationCommissionMetadata distinguishes complete historical state from
// checkpoints written before commissions were retained. Explicit zero is known.
func HasInflationCommissionMetadata(data []byte) bool {
	var persisted PersistedEpochStakes
	if err := json.Unmarshal(data, &persisted); err != nil {
		return false
	}
	for _, vote := range persisted.VoteAccts {
		if vote == nil || vote.InflationRewardsCommissionBps == nil {
			return false
		}
	}
	return true
}

// RestoreInflationCommissionMetadata fills only missing commission fields from
// the same historical generation in the retained snapshot. It never substitutes
// current vote state or changes already persisted values, stake, or membership.
func RestoreInflationCommissionMetadata(data, historical []byte) ([]byte, error) {
	var persisted, source PersistedEpochStakes
	if err := json.Unmarshal(data, &persisted); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(historical, &source); err != nil {
		return nil, err
	}
	if persisted.Epoch != source.Epoch {
		return nil, fmt.Errorf("cannot restore commission metadata for epoch %d from epoch %d", persisted.Epoch, source.Epoch)
	}
	changed := false
	for pubkey, vote := range persisted.VoteAccts {
		if vote == nil || vote.InflationRewardsCommissionBps != nil {
			continue
		}
		if original := source.VoteAccts[pubkey]; original != nil && original.InflationRewardsCommissionBps != nil {
			vote.InflationRewardsCommissionBps = original.InflationRewardsCommissionBps
			changed = true
		}
	}
	if !changed {
		return data, nil
	}
	return json.Marshal(persisted)
}
