package txstatus

import (
	"errors"
	"testing"

	"github.com/gagliardetto/solana-go"
)

func TestResultCacheInvalidReset(t *testing.T) {
	c := resultTestCache(t)
	sig := resultTestSignature(1)
	resultTestPublish(t, c, resultTestID(1, 0), SignatureResult{Signature: sig})
	old, err := c.Lookup([]solana.Signature{sig})
	if err != nil || !old.Results[0].Found {
		t.Fatal("missing positive control", err)
	}
	if err := c.Reset(ResultBankID{}); !errors.Is(err, ErrResultLineage) {
		t.Fatal("invalid authoritative reset accepted", err)
	}
	if c.generation != old.Generation+1 || c.count != 1 || len(c.results) != 1 || !old.Results[0].Found {
		t.Fatal("invalid reset did not invalidate generation while retaining owned state")
	}
	if _, err := c.Lookup([]solana.Signature{sig}); !errors.Is(err, ErrResultUnavailable) {
		t.Fatal("invalid recovery parent left old positive view available", err)
	}
	parent := resultTestID(10, 1)
	if err := c.Reset(parent); err != nil || c.results != nil || c.count != 0 {
		t.Fatal("valid authoritative reset did not clear stale index", err)
	}
	view, err := c.Lookup([]solana.Signature{sig})
	if err != nil || view.Results[0].Found || view.Root != parent || view.Tip != parent || view.Generation <= old.Generation+1 {
		t.Fatal("valid reset did not restore cold misses at the new parent", view, err)
	}
	resultTestPublish(t, c, resultTestID(11, 1), SignatureResult{Signature: sig})
	view, err = c.Lookup([]solana.Signature{sig})
	if err != nil || !view.Results[0].Found || view.Results[0].Slot != 11 || view.Results[0].Finalized {
		t.Fatal("valid reset did not resume selected capture", view, err)
	}
}
