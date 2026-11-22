package node

import (
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
)

func TestCacheWindowSmallCopy(t *testing.T) {
	var sampler cacheActivityWindow
	at := time.Now()
	v := accountsdb.CacheActivityObservation{Instance: 1, Generation: 1, FixedSmallCopyKnown: true}
	if sampler.observe(v, true, at, at) != nil {
		t.Fatal("one sample produced an interval")
	}
	at = at.Add(time.Second)
	v.FixedSmallCopyBytes = 200
	window := sampler.observe(v, true, at, at)
	if window == nil || window.FixedSmallPayloadBytesCopied == nil || *window.FixedSmallPayloadBytesCopied != 200 {
		t.Fatalf("copy delta: got %+v, want 200 bytes", window)
	}
	at = at.Add(time.Second)
	v.FixedSmallCopyKnown = false
	window = sampler.observe(v, true, at, at)
	if window == nil || window.FixedSmallPayloadBytesCopied != nil {
		t.Fatalf("unavailable copy counter changed the lookup window or published a value: %+v", window)
	}
	at = at.Add(time.Second)
	v.Generation++
	v.FixedSmallCopyKnown = true
	if sampler.observe(v, true, at, at) != nil {
		t.Fatal("cache generation change reused the previous interval")
	}
	at = at.Add(time.Second)
	window = sampler.observe(v, true, at, at)
	if window == nil || window.FixedSmallPayloadBytesCopied == nil || *window.FixedSmallPayloadBytesCopied != 0 {
		t.Fatalf("measured zero: got %+v, want a reported zero", window)
	}
}
