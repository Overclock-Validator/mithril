package consensus

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/gagliardetto/solana-go"
)

func lifecycleID(slot, hash uint64) alpenglow.BlockID {
	var id solana.Hash
	binary.LittleEndian.PutUint64(id[:8], hash)
	return alpenglow.BlockID{Slot: slot, Hash: id}
}

func lifecycleEngine(t *testing.T, enabled, stakes bool) *AlpenglowObserverEngine {
	t.Helper()
	e, err := NewEngine(Config{MonitoringEnabled: enabled})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.SetAlpenglowEpochLookup(func(uint64) uint64 { return 1 })
	if stakes {
		if err := e.SetAlpenglowValidatorSet(testAlpenglowValidatorSet()); err != nil {
			t.Fatal(err)
		}
	}
	return e
}

func lifecycleSignedCertificate(t *testing.T, kind alpenglow.CertificateType, id alpenglow.BlockID) alpenglow.Certificate {
	t.Helper()
	c := alpenglow.Certificate{Type: kind, Slot: id.Slot, Bitmap: testAlpenglowSignerBitmap(), At: time.Unix(1, 0)}
	if kind.HasBlock() {
		c.BlockHash = id.Hash
	}
	c.Signature = testAlpenglowCertificateSignature(t, c)
	return c
}
