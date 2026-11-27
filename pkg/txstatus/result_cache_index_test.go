package txstatus

import (
	"errors"
	"testing"

	"github.com/gagliardetto/solana-go"
)

func TestResultCacheSingleMapLifecycle(t *testing.T) {
	c := resultTestCache(t)
	rootSig, orphanSig := resultTestSignature(1), resultTestSignature(2)
	root, orphan := resultTestID(10, 0), resultTestID(20, 0)
	resultTestPublish(t, c, root, SignatureResult{Signature: rootSig})
	if err := c.Root(root); err != nil {
		t.Fatal(err)
	}
	resultTestPublish(t, c, orphan, SignatureResult{Signature: orphanSig})
	view, err := c.Lookup([]solana.Signature{rootSig, orphanSig})
	if err != nil || !view.Results[0].Finalized || view.Results[1].Finalized {
		t.Fatal("selected root/suffix classification failed", view, err)
	}
	if err := c.Unwind(root); err != nil || len(c.results) != 1 {
		t.Fatal("orphan key remained indexed", err)
	}
	replacement := resultTestID(20, 1)
	resultTestPublish(t, c, replacement, SignatureResult{Signature: orphanSig})
	if err := c.Root(replacement); err != nil {
		t.Fatal(err)
	}
	view, err = c.Lookup([]solana.Signature{orphanSig})
	if err != nil || !view.Results[0].Found || !view.Results[0].Finalized || view.Root != replacement {
		t.Fatal("replacement branch was not selected exactly", view, err)
	}
	for slot := uint64(21); slot <= 320; slot++ {
		id := resultTestID(slot, 0)
		resultTestPublish(t, c, id, SignatureResult{Signature: resultTestSignature(slot)})
		if err := c.Root(id); err != nil {
			t.Fatal(err)
		}
	}
	view, err = c.Lookup([]solana.Signature{rootSig, orphanSig})
	if err != nil || view.Results[0].Found || view.Results[1].Found || len(c.results) != 300 || c.rooted != 300 {
		t.Fatal("root pruning left indexed keys or lost retained keys", view, err)
	}
	if err := c.Reset(resultTestID(400, 1)); err != nil || c.results != nil || c.count != 0 {
		t.Fatal("reset kept old index or its high-water allocation", err)
	}

	// A duplicate at the end cannot insert earlier new keys into the retained map.
	resultTestPublish(t, c, resultTestID(401, 1), SignatureResult{Signature: rootSig})
	input := ResultBank{ID: resultTestID(402, 1), Parent: c.tip(), Complete: true,
		Results: []SignatureResult{{Signature: orphanSig}, {Signature: rootSig}}}
	if err := c.Publish(input); !errors.Is(err, ErrResultInput) || len(c.results) != 1 || c.count != 1 {
		t.Fatal("conflict partially inserted the new bank", err)
	}
	if _, present := c.results[orphanSig]; present {
		t.Fatal("pre-conflict key survived rejected bank")
	}
}
