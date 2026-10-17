package replay

import (
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
)

type legacyEpochRewardPublisher struct{}

func (legacyEpochRewardPublisher) RecordEpochRewards(*b.Block) error      { return nil }
func (legacyEpochRewardPublisher) PrepareEpochRewards(uint64) error       { return nil }
func (legacyEpochRewardPublisher) SetRootedEpochRewardsSlot(uint64) error { return nil }
func (legacyEpochRewardPublisher) RewindEpochRewards(uint64) error        { return nil }

func TestEpochRewardPublisherDoesNotRequireCoverage(t *testing.T) {
	if _, ok := any(legacyEpochRewardPublisher{}).(epochRewardPublisher); !ok {
		t.Fatal("existing reward publishers must retain their callbacks without the optional coverage hook")
	}
}
