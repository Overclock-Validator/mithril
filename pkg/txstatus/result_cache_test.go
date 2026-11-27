package txstatus

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/gagliardetto/solana-go"
)

func resultTestID(slot uint64, fork byte) ResultBankID {
	id := ResultBankID{Slot: slot}
	binary.LittleEndian.PutUint64(id.Blockhash[:], slot)
	id.Blockhash[31] = fork + 1
	return id
}

func resultTestSignature(value uint64) solana.Signature {
	var signature solana.Signature
	binary.LittleEndian.PutUint64(signature[:], value)
	signature[63] = 1
	return signature
}

func resultTestCache(t *testing.T) *ResultCache {
	t.Helper()
	c, err := NewResultCache(resultTestID(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestResultCacheForwardChangeClassification(t *testing.T) {
	for _, mode := range []string{"stable", "publish", "gap", "root", "unwind", "reset", "invalidate", "lineage", "foreign", "generation-wrap", "generation-only", "tip-identity", "root-identity"} {
		t.Run(mode, func(t *testing.T) {
			cache := resultTestCache(t)
			resultTestPublish(t, cache, resultTestID(1, 0))
			if mode == "generation-wrap" {
				cache.generation = ^uint64(0)
			}
			view, err := cache.Lookup(nil)
			if err != nil {
				t.Fatal(err)
			}
			want := false
			switch mode {
			case "publish", "generation-wrap":
				resultTestPublish(t, cache, resultTestID(2, 0))
				want = true
			case "gap":
				if err := cache.Publish(ResultBank{ID: resultTestID(2, 0), Parent: cache.tip()}); !errors.Is(err, ErrResultUnavailable) {
					t.Fatal(err)
				}
				want = true
			case "root":
				if err := cache.Root(resultTestID(1, 0)); err != nil {
					t.Fatal(err)
				}
				want = true
			case "unwind":
				if err := cache.Unwind(resultTestID(0, 0)); err != nil {
					t.Fatal(err)
				}
				resultTestPublish(t, cache, resultTestID(2, 0))
			case "reset":
				if err := cache.Reset(resultTestID(0, 0)); err != nil {
					t.Fatal(err)
				}
				resultTestPublish(t, cache, resultTestID(2, 0))
			case "invalidate":
				cache.Invalidate()
			case "lineage":
				if err := cache.Publish(ResultBank{ID: resultTestID(2, 0), Parent: resultTestID(0, 0)}); !errors.Is(err, ErrResultLineage) {
					t.Fatal(err)
				}
			case "foreign":
				other := resultTestCache(t)
				resultTestPublish(t, other, resultTestID(1, 0))
				view, err = other.Lookup(nil)
				if err != nil {
					t.Fatal(err)
				}
				resultTestPublish(t, cache, resultTestID(2, 0))
			case "generation-only":
				view.Generation--
			case "tip-identity":
				view.Tip.Blockhash[0]++
				if err := cache.Root(resultTestID(1, 0)); err != nil {
					t.Fatal(err)
				}
			case "root-identity":
				view.Root.Blockhash[0]++
				resultTestPublish(t, cache, resultTestID(2, 0))
			}
			if got := cache.ChangedOnlyForward(view); got != want {
				t.Fatalf("forward classification = %t, want %t", got, want)
			}
		})
	}
}

func resultTestPublish(t *testing.T, c *ResultCache, id ResultBankID, results ...SignatureResult) {
	t.Helper()
	if err := c.Publish(ResultBank{ID: id, Parent: c.tip(), Results: results, Complete: true}); err != nil {
		t.Fatal(err)
	}
}

func TestResultCacheHistoryViewOwnsKnownOutcomesAcrossGap(t *testing.T) {
	c := resultTestCache(t)
	signature, missing := resultTestSignature(1), resultTestSignature(2)
	errorJSON := json.RawMessage(`{"InstructionError":[1,{"Custom":1}]}`)
	resultTestPublish(t, c, resultTestID(1, 0), SignatureResult{Signature: signature, Error: errorJSON})
	if err := c.Publish(ResultBank{ID: resultTestID(2, 0), Parent: c.tip()}); !errors.Is(err, ErrResultUnavailable) {
		t.Fatal(err)
	}
	query := []solana.Signature{signature, missing, signature}
	strict, err := c.Lookup(query)
	if !errors.Is(err, ErrResultUnavailable) || strict.Results != nil {
		t.Fatal("strict lookup exposed an uncertain partial batch", strict, err)
	}
	view, uncertain, err := c.LookupForHistory(query)
	if err != nil || !uncertain || len(view.Results) != 3 || !view.Results[0].Found ||
		view.Results[1].Found || !view.Results[2].Found || !bytes.Equal(view.Results[0].Error, errorJSON) {
		t.Fatal("history lookup lost known outcomes or uncertainty", view, uncertain, err)
	}
	view.Results[0].Error[0] = '!'
	if !bytes.Equal(view.Results[2].Error, errorJSON) {
		t.Fatal("duplicate output positions share mutable error bytes")
	}
	known, uncertain, err := c.LookupForHistory([]solana.Signature{signature})
	if err != nil || uncertain || !bytes.Equal(known.Results[0].Error, errorJSON) {
		t.Fatal("caller mutation changed retained outcome", known, uncertain, err)
	}
	if empty, uncertain, err := c.LookupForHistory(nil); err != nil || uncertain || empty.Results == nil {
		t.Fatal("empty history query became uncertain", empty, uncertain, err)
	}
	if view, uncertain, err := c.LookupForHistory(make([]solana.Signature, 257)); !errors.Is(err, ErrResultInput) || uncertain || view.Results != nil {
		t.Fatal("oversized history query bypassed input limit", view, uncertain, err)
	}
	for _, lineage := range []bool{false, true} {
		t.Run(map[bool]string{false: "invalidated", true: "bad-lineage"}[lineage], func(t *testing.T) {
			cache := resultTestCache(t)
			if lineage {
				if err := cache.Publish(ResultBank{ID: resultTestID(2, 0), Parent: resultTestID(1, 0)}); !errors.Is(err, ErrResultLineage) {
					t.Fatal(err)
				}
			} else {
				cache.Invalidate()
			}
			if view, uncertain, err := cache.LookupForHistory(query); !errors.Is(err, ErrResultUnavailable) || uncertain || view.Results != nil {
				t.Fatal("history view bypassed cache invalidation", view, uncertain, err)
			}
		})
	}
}

func TestResultCacheColdAndOwnedOutcomes(t *testing.T) {
	c := resultTestCache(t)
	sig := resultTestSignature(1)
	view, err := c.Lookup([]solana.Signature{sig})
	if err != nil || view.Results[0].Found || view.Tip != resultTestID(0, 0) || view.Root != view.Tip {
		t.Fatal("cold cache did not return an ordinary miss", view, err)
	}
	empty, err := c.Lookup(nil)
	if err != nil || empty.Results == nil || len(empty.Results) != 0 {
		t.Fatal("empty batch must encode as an empty array")
	}
	errorJSON := []byte(`{"InstructionError":[3,{"Custom":0}]}`)
	bank := ResultBank{ID: resultTestID(10, 0), Parent: c.tip(), Complete: true,
		Results: []SignatureResult{{Signature: sig, Error: errorJSON}, {Signature: resultTestSignature(2)}}}
	if err := c.Publish(bank); err != nil {
		t.Fatal(err)
	}
	generation, charged := c.generation, c.charged
	if err := c.Publish(bank); err != nil || c.generation != generation || c.charged != charged || c.count != 1 {
		t.Fatal("exact retry was not idempotent", err)
	}
	errorJSON[0] = '!'
	bank.Results[0].Signature = resultTestSignature(99)
	view, err = c.Lookup([]solana.Signature{sig, resultTestSignature(2), sig})
	if err != nil || !view.Results[0].Found || view.Results[0].Finalized || string(view.Results[0].Error) != `{"InstructionError":[3,{"Custom":0}]}` || view.Results[1].Error != nil || !bytes.Equal(view.Results[0].Error, view.Results[2].Error) {
		t.Fatal("incorrect owned outcome", view, err)
	}
	view.Results[0].Error[0] = '!'
	if view.Results[2].Error[0] != '{' {
		t.Fatal("duplicate output positions alias mutable error bytes")
	}
	if err := c.Root(bank.ID); err != nil {
		t.Fatal(err)
	}
	view, err = c.Lookup([]solana.Signature{sig})
	if err != nil || !view.Results[0].Finalized || view.Results[0].Error[0] != '{' {
		t.Fatal("caller mutation changed cache or root was not applied")
	}
	if _, err := json.Marshal(view); err != nil {
		t.Fatal("owned errors must remain JSON encodable", err)
	}
}

func TestResultCacheGapAgesWithProducedRoots(t *testing.T) {
	c := resultTestCache(t)
	sig, missing := resultTestSignature(1), resultTestSignature(9999)
	resultTestPublish(t, c, resultTestID(10, 0), SignatureResult{Signature: sig})
	gap := ResultBank{ID: resultTestID(1000, 0), Parent: c.tip()}
	if err := c.Publish(gap); !errors.Is(err, ErrResultUnavailable) {
		t.Fatal("missing capture was not recorded as unavailable")
	}
	if err := c.Root(gap.ID); err != nil || c.rooted != 2 {
		t.Fatal("slot distance was incorrectly counted as produced roots", err)
	}
	generation, charged := c.generation, c.charged
	if err := c.Root(gap.ID); err != nil || c.generation != generation || c.charged != charged || c.rooted != 2 {
		t.Fatal("repeated root changed accounting")
	}
	if _, err := c.Lookup([]solana.Signature{sig, sig}); err != nil {
		t.Fatal("known positive batch was blocked by unrelated gap", err)
	}
	for _, query := range [][]solana.Signature{{missing}, {sig, missing}, {missing, sig}} {
		if view, err := c.Lookup(query); !errors.Is(err, ErrResultUnavailable) || view.Results != nil {
			t.Fatal("gap returned a partially successful batch", view, err)
		}
	}
	for count := 3; count <= 301; count++ {
		id := resultTestID(uint64(count)*1000, 0)
		resultTestPublish(t, c, id)
		if err := c.Root(id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.Lookup([]solana.Signature{missing}); !errors.Is(err, ErrResultUnavailable) {
		t.Fatal("gap disappeared one produced root too soon")
	}
	id := resultTestID(302000, 0)
	resultTestPublish(t, c, id)
	if err := c.Root(id); err != nil {
		t.Fatal(err)
	}
	view, err := c.Lookup([]solana.Signature{missing})
	if err != nil || view.Results[0].Found || c.rooted != 300 || c.count != 300 || c.charged != resultScaffold {
		t.Fatal("aged gap did not restore ordinary misses", view, err)
	}
}

func TestResultCacheUnwindAndAuthoritativeReset(t *testing.T) {
	c := resultTestCache(t)
	a, b, orphan := resultTestID(10, 0), resultTestID(20, 0), resultTestSignature(2)
	resultTestPublish(t, c, a)
	if err := c.Root(a); err != nil {
		t.Fatal(err)
	}
	resultTestPublish(t, c, b, SignatureResult{Signature: orphan})
	if err := c.Publish(ResultBank{ID: resultTestID(30, 0), Parent: b}); !errors.Is(err, ErrResultUnavailable) {
		t.Fatal(err)
	}
	for _, invalid := range []ResultBankID{resultTestID(0, 0), resultTestID(20, 1), resultTestID(15, 0)} {
		before, retainedCount := c.generation, c.count
		if err := c.Unwind(invalid); !errors.Is(err, ErrResultLineage) {
			t.Fatal("unwind accepted a nonselected or rooted parent", invalid, err)
		}
		if _, err := c.Lookup([]solana.Signature{orphan}); !errors.Is(err, ErrResultUnavailable) || c.generation == before || c.count != retainedCount {
			t.Fatal("invalid unwind left positive views available or changed retained nodes")
		}
		if err := c.Reset(a); err != nil {
			t.Fatal(err)
		}
		resultTestPublish(t, c, b, SignatureResult{Signature: orphan})
		if err := c.Publish(ResultBank{ID: resultTestID(30, 0), Parent: b}); !errors.Is(err, ErrResultUnavailable) {
			t.Fatal(err)
		}
	}
	if err := c.Unwind(a); err != nil || c.count != 0 || c.rooted != 0 {
		t.Fatal("suffix outcomes and gap were not removed together", err)
	}
	view, err := c.Lookup([]solana.Signature{orphan})
	if err != nil || view.Results[0].Found {
		t.Fatal("orphan result or gap remained visible", view, err)
	}
	resultTestPublish(t, c, resultTestID(20, 1), SignatureResult{Signature: orphan})
	if err := c.Reset(resultTestID(5, 2)); err != nil {
		t.Fatal(err)
	}
	view, err = c.Lookup([]solana.Signature{orphan})
	if err != nil || view.Results[0].Found || view.Tip != resultTestID(5, 2) || view.Root != view.Tip || c.charged != resultScaffold {
		t.Fatal("authoritative rewind did not produce a clean pinned baseline")
	}
}

func TestResultCacheLimitsPreserveKnownBanks(t *testing.T) {
	c := resultTestCache(t)
	c.limit = resultScaffold + resultEntryBytes
	sig := resultTestSignature(1)
	resultTestPublish(t, c, resultTestID(1, 0), SignatureResult{Signature: sig})
	if err := c.Publish(ResultBank{ID: resultTestID(2, 0), Parent: c.tip(), Complete: true,
		Results: []SignatureResult{{Signature: resultTestSignature(2)}}}); !errors.Is(err, ErrResultUnavailable) {
		t.Fatal("byte limit not enforced", err)
	}
	if c.count != 2 || c.banks[1].complete || c.banks[1].signatures != nil || c.charged != c.limit {
		t.Fatal("byte refusal lost its fixed-scaffold marker or exceeded accounting")
	}
	if _, err := c.Lookup([]solana.Signature{sig}); err != nil {
		t.Fatal("byte refusal discarded a valid earlier bank", err)
	}
	resultTestPublish(t, c, resultTestID(3, 0))
	if _, err := c.Lookup([]solana.Signature{resultTestSignature(2)}); !errors.Is(err, ErrResultUnavailable) {
		t.Fatal("byte-refused result became a clean miss")
	}
	oversize := make([]SignatureResult, resultBankEntries+1)
	if allocations := testing.AllocsPerRun(10, func() { _, _ = resultCharge(oversize) }); allocations != 0 {
		t.Fatal("oversized-count preflight allocated", allocations)
	}
	for _, input := range [][]SignatureResult{oversize, {{Error: make([]byte, resultErrorBytes+1)}}} {
		if _, err := resultCharge(input); !errors.Is(err, ErrResultUnavailable) {
			t.Fatal("count/error envelope limit not enforced", err)
		}
	}
	for _, raw := range []string{"null", "true", "42", "[]", "{}", `""`, "bad"} {
		if _, err := resultCharge([]SignatureResult{{Error: json.RawMessage(raw)}}); !errors.Is(err, ErrResultInput) {
			t.Fatal("invalid execution-error envelope accepted", raw, err)
		}
	}
	if _, err := c.Lookup(make([]solana.Signature, 257)); !errors.Is(err, ErrResultInput) {
		t.Fatal("oversized query accepted")
	}
}

func TestResultCacheConflictsAndActiveCeiling(t *testing.T) {
	for _, kind := range []string{"within-bank", "across-bank", "retry-parent", "retry-outcome", "retry-oversize"} {
		t.Run(kind, func(t *testing.T) {
			c := resultTestCache(t)
			sig := resultTestSignature(1)
			bank := ResultBank{ID: resultTestID(1, 0), Parent: c.tip(), Complete: true, Results: []SignatureResult{{Signature: sig}}}
			if err := c.Publish(bank); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "within-bank":
				bank = ResultBank{ID: resultTestID(2, 0), Parent: c.tip(), Complete: true,
					Results: []SignatureResult{{Signature: resultTestSignature(2)}, {Signature: resultTestSignature(2)}}}
			case "across-bank":
				bank.ID, bank.Parent = resultTestID(2, 0), c.tip()
				bank.Results[0].Error = json.RawMessage(`"InvalidAccountIndex"`)
			case "retry-parent":
				bank.Parent = resultTestID(0, 1)
			case "retry-outcome":
				bank.Results[0].Error = json.RawMessage(`"InvalidAccountIndex"`)
			case "retry-oversize":
				bank.Results[0].Error = make([]byte, resultErrorBytes+1)
			}
			if err := c.Publish(bank); err == nil || len(c.banks[0].signatures) != 1 || c.results[sig].err != "" {
				t.Fatal("conflict changed an existing result", err)
			}
			if c.count == 2 && (c.banks[1].complete || len(c.banks[1].signatures) != 0) {
				t.Fatal("duplicate input published partial results")
			}
			if _, err := c.Lookup([]solana.Signature{sig}); !errors.Is(err, ErrResultUnavailable) {
				t.Fatal("explicit conflict returned the formerly known outcome", err)
			}
		})
	}
	c := resultTestCache(t)
	for slot := uint64(1); slot <= resultActiveBanks; slot++ {
		resultTestPublish(t, c, resultTestID(slot, 0))
	}
	if err := c.Publish(ResultBank{ID: resultTestID(resultActiveBanks+1, 0), Parent: c.tip(), Complete: true}); !errors.Is(err, ErrResultUnavailable) || c.count != resultActiveBanks {
		t.Fatal("active-bank overflow was not bounded", err)
	}
	if _, err := c.Lookup([]solana.Signature{resultTestSignature(99)}); !errors.Is(err, ErrResultUnavailable) {
		t.Fatal("lost active lineage became a cold miss")
	}
	if err := c.Reset(ResultBankID{}); err == nil {
		t.Fatal("invalid reset cleared unavailability")
	}
	if err := c.Reset(resultTestID(resultActiveBanks+1, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Lookup([]solana.Signature{resultTestSignature(99)}); err != nil {
		t.Fatal("authoritative reset did not recover", err)
	}
	full := resultTestCache(t)
	for slot := uint64(1); slot <= resultBankSlots; slot++ {
		id := resultTestID(slot, 0)
		resultTestPublish(t, full, id)
		if slot <= resultRoots {
			if err := full.Root(id); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := full.Root(resultTestID(resultRoots+1, 0)); err != nil || full.count != resultBankSlots-1 || full.rooted != resultRoots {
		t.Fatal("root did not free exactly one node in a full array", err)
	}
	resultTestPublish(t, full, resultTestID(resultBankSlots+1, 0))
	if full.count != resultBankSlots || full.charged != resultScaffold {
		t.Fatal("full array rollover lost bounded accounting")
	}
	maximum := resultTestCache(t)
	resultTestPublish(t, maximum, resultTestID(^uint64(0), 0))
	if err := maximum.Root(resultTestID(^uint64(0), 0)); err != nil {
		t.Fatal("maximum slot was not accepted without arithmetic overflow", err)
	}
}

func TestResultCacheRootIdentityAndConcurrentViews(t *testing.T) {
	c := resultTestCache(t)
	resultTestPublish(t, c, resultTestID(10, 0))
	if err := c.Root(resultTestID(10, 0)); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []ResultBankID{resultTestID(10, 1), resultTestID(9, 0), resultTestID(100, 0)} {
		probe := resultTestCache(t)
		sig := resultTestSignature(1)
		resultTestPublish(t, probe, resultTestID(10, 0), SignatureResult{Signature: sig})
		if err := probe.Root(resultTestID(10, 0)); err != nil {
			t.Fatal(err)
		}
		old, err := probe.Lookup([]solana.Signature{sig})
		if err != nil || !old.Results[0].Found {
			t.Fatal("missing old positive control")
		}
		if err := probe.Root(invalid); !errors.Is(err, ErrResultLineage) || probe.generation == old.Generation || probe.count != 1 {
			t.Fatal("invalid root did not invalidate old generation while retaining nodes", err)
		}
		if _, err := probe.Lookup([]solana.Signature{sig}); !errors.Is(err, ErrResultUnavailable) {
			t.Fatal("invalid authoritative root left positives available")
		}
		if err := probe.Reset(resultTestID(10, 0)); err != nil {
			t.Fatal(err)
		}
		if view, err := probe.Lookup([]solana.Signature{sig}); err != nil || view.Results[0].Found {
			t.Fatal("authoritative reset did not restore cold misses")
		}
	}
	stop := make(chan struct{})
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			select {
			case <-stop:
				return
			default:
				view, err := c.Lookup([]solana.Signature{resultTestSignature(1)})
				if err != nil || view.Root.Slot > view.Tip.Slot {
					t.Error("mixed mutation view", view, err)
					return
				}
			}
		}
	}()
	defer readers.Wait()
	defer close(stop)
	for slot := uint64(11); slot < 620; slot++ {
		id := resultTestID(slot, 0)
		resultTestPublish(t, c, id)
		if err := c.Root(id); err != nil {
			t.Fatal(err)
		}
	}
}
