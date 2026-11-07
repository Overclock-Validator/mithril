package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/blockstream"
)

func TestNetworkDetailsStartupOptionSurvivesSourceReplacement(t *testing.T) {
	var absent *Progress
	absent.EnableNetworkDetails()
	var p Progress
	if p.networkDetails {
		t.Fatal("network details defaulted on")
	}
	p.EnableNetworkDetails()
	for range 2 {
		p.begin(1, nil)
		p.rewound()
		if !p.networkDetails {
			t.Fatal("attempt/rewind reset startup option")
		}
		s := &blockstream.BlockSource{}
		p.sourceStarted(s)
		if !p.networkDetails || p.source.Load() != s {
			t.Fatal("source replacement lost startup option")
		}
		p.sourceStarted(nil)
		if !p.networkDetails || p.source.Load() != nil {
			t.Fatal("source detach reset startup option")
		}
	}
}
