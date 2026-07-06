package replay

import (
	"errors"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/rpcclient"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func vsig(b byte) solana.Signature { var s solana.Signature; s[0] = b; return s }
func vhash(b byte) solana.Hash     { var h solana.Hash; h[0] = b; return h }

// fakeVerificationSource serves canned per-slot attested views.
type fakeVerificationSource struct {
	blocks  map[uint64]*verifiedBlock
	skipped map[uint64]bool
	errs    map[uint64]error
	calls   int
}

func (f *fakeVerificationSource) FetchFinalized(slot uint64) (*verifiedBlock, error) {
	f.calls++
	if f.errs[slot] != nil {
		return nil, f.errs[slot]
	}
	if f.skipped[slot] {
		return nil, rpcclient.SlotSkipped
	}
	if vb, ok := f.blocks[slot]; ok {
		return vb, nil
	}
	return nil, errors.New("block not available yet")
}

// digestFor builds a matching (digest, attested view) pair for one tx.
func digestFor(slot uint64, blockhash solana.Hash, sig solana.Signature, fee uint64, failed bool, pre, post []uint64, mask []byte) (*SlotDigest, *verifiedBlock) {
	n := uint16(len(pre))
	td := TxDigest{NumAccts: n, SkipMask: mask}
	copy(td.SigPrefix[:], sig[:8])
	td.RecordHash = txRecordHash(sig, fee, failed, n, mask, pre, post)
	d := &SlotDigest{Slot: slot, Blockhash: blockhash, Txs: []TxDigest{td}}
	vb := &verifiedBlock{Blockhash: blockhash, Txs: []verifierTx{{Sig: sig, Fee: fee, Failed: failed, Pre: pre, Post: post}}}
	return d, vb
}

// The record hash is deterministic, sensitive to every compared field, and
// insensitive to masked balances.
func TestTxRecordHashProperties(t *testing.T) {
	sig := vsig(1)
	mask := []byte{0b00000010} // index 1 masked
	pre := []uint64{100, 555, 300}
	post := []uint64{90, 555, 310}

	h1 := txRecordHash(sig, 5000, false, 3, mask, pre, post)
	h2 := txRecordHash(sig, 5000, false, 3, mask, pre, post)
	assert.Equal(t, h1, h2, "deterministic")

	assert.NotEqual(t, h1, txRecordHash(sig, 5001, false, 3, mask, pre, post), "fee changes hash")
	assert.NotEqual(t, h1, txRecordHash(sig, 5000, true, 3, mask, pre, post), "status changes hash")
	pre2 := []uint64{101, 555, 300}
	assert.NotEqual(t, h1, txRecordHash(sig, 5000, false, 3, mask, pre2, post), "unmasked pre-balance changes hash")

	// Masked index differs -> hash identical (index 1 is not comparable).
	preMasked := []uint64{100, 999999, 300}
	assert.Equal(t, h1, txRecordHash(sig, 5000, false, 3, mask, preMasked, post), "masked balance is ignored")

	// Failed txs ignore post-balances entirely (mirrors RPC-mode checks).
	hf1 := txRecordHash(sig, 5000, true, 3, mask, pre, post)
	hf2 := txRecordHash(sig, 5000, true, 3, mask, pre, []uint64{1, 2, 3})
	assert.Equal(t, hf1, hf2, "post ignored for failed txs")
}

func TestCompareSlotDigest(t *testing.T) {
	sig := vsig(7)
	d, vb := digestFor(100, vhash(0xAA), sig, 5000, false, []uint64{10, 20}, []uint64{5, 25}, []byte{0})
	assert.Nil(t, compareSlotDigest(d, vb), "matching digest verifies clean")

	vb.Txs[0].Fee = 5001
	div := compareSlotDigest(d, vb)
	require.NotNil(t, div, "fee mismatch diverges")
	assert.Equal(t, "tx_record", div.Kind)
	assert.Equal(t, 0, div.TxIndex)

	vb.Txs[0].Fee = 5000
	vb.Txs = append(vb.Txs, verifierTx{Sig: vsig(9)})
	div = compareSlotDigest(d, vb)
	require.NotNil(t, div, "tx count mismatch diverges")
	assert.Equal(t, "tx_count", div.Kind)
}

func newTestVerifier(src blockVerificationSource) *TrailingVerifier {
	return newTrailingVerifier(src, VerifierConfig{Enabled: true, LagSlots: 4, MaxRPS: 1000, Required: true})
}

// Watermark advances contiguously as slots verify, oldest-first.
func TestVerifierWatermarkAdvancesContiguously(t *testing.T) {
	src := &fakeVerificationSource{blocks: map[uint64]*verifiedBlock{}, skipped: map[uint64]bool{}, errs: map[uint64]error{}}
	v := newTestVerifier(src)

	for slot := uint64(100); slot <= 102; slot++ {
		d, vb := digestFor(slot, vhash(byte(slot)), vsig(byte(slot)), 5000, false, []uint64{1}, []uint64{2}, []byte{0})
		v.Record(d)
		src.blocks[slot] = vb
	}
	v.SetExecutedTip(200) // all eligible past the lag

	assert.Equal(t, uint64(99), v.VerifiedWatermark(), "floor anchors below first recorded slot")
	for i := 0; i < 3; i++ {
		v.verifyNext()
	}
	assert.Equal(t, uint64(102), v.VerifiedWatermark())
	assert.Nil(t, v.Failure())
}

// A mid-window unverified slot holds the watermark even when later slots verified.
func TestVerifierWatermarkHeldByGap(t *testing.T) {
	src := &fakeVerificationSource{blocks: map[uint64]*verifiedBlock{}, skipped: map[uint64]bool{}, errs: map[uint64]error{}}
	v := newTestVerifier(src)

	for slot := uint64(100); slot <= 102; slot++ {
		d, vb := digestFor(slot, vhash(byte(slot)), vsig(byte(slot)), 5000, false, []uint64{1}, []uint64{2}, []byte{0})
		v.Record(d)
		if slot != 101 {
			src.blocks[slot] = vb
		} else {
			src.errs[101] = errors.New("rpc outage")
		}
	}
	v.SetExecutedTip(200)

	for i := 0; i < 6; i++ {
		v.verifyNext()
	}
	assert.Equal(t, uint64(100), v.VerifiedWatermark(), "gap at 101 holds the watermark")
	assert.Nil(t, v.Failure(), "transient errors are not divergences")
}

// A different blockhash is a sibling question: requeued, watermark held, no failure.
func TestVerifierSiblingBlockhashRequeuesNotFails(t *testing.T) {
	src := &fakeVerificationSource{blocks: map[uint64]*verifiedBlock{}, skipped: map[uint64]bool{}, errs: map[uint64]error{}}
	v := newTestVerifier(src)

	d, vb := digestFor(100, vhash(0xAA), vsig(1), 5000, false, []uint64{1}, []uint64{2}, []byte{0})
	vb.Blockhash = vhash(0xBB) // canonical block is a different sibling
	v.Record(d)
	src.blocks[100] = vb
	v.SetExecutedTip(200)

	for i := 0; i < 5; i++ {
		v.verifyNext()
	}
	assert.Nil(t, v.Failure(), "sibling mismatch is not an execution divergence")
	assert.Equal(t, uint64(99), v.VerifiedWatermark(), "watermark held while identity is unresolved")
}

// Execution divergence (same block, different results) fails exactly once.
func TestVerifierExecutionDivergenceFails(t *testing.T) {
	src := &fakeVerificationSource{blocks: map[uint64]*verifiedBlock{}, skipped: map[uint64]bool{}, errs: map[uint64]error{}}
	v := newTestVerifier(src)

	d, vb := digestFor(100, vhash(0xAA), vsig(1), 5000, false, []uint64{1}, []uint64{2}, []byte{0})
	vb.Txs[0].Post = []uint64{999} // attested post-balance differs
	v.Record(d)
	src.blocks[100] = vb
	v.SetExecutedTip(200)

	v.verifyNext()
	div := v.Failure()
	require.NotNil(t, div)
	assert.Equal(t, uint64(100), div.Slot)
	assert.Equal(t, "tx_record", div.Kind)
	assert.Equal(t, uint64(99), v.VerifiedWatermark(), "nothing folds past a divergence")
}

// Skip agreement verifies; skip disagreement needs 3 confirmations to fail.
func TestVerifierSkipHandling(t *testing.T) {
	src := &fakeVerificationSource{blocks: map[uint64]*verifiedBlock{}, skipped: map[uint64]bool{100: true}, errs: map[uint64]error{}}
	v := newTestVerifier(src)
	v.RecordSkip(100)
	v.SetExecutedTip(200)
	v.verifyNext()
	assert.Equal(t, uint64(100), v.VerifiedWatermark(), "agreed skip verifies")

	// Disagreement: we executed, RPC says skipped.
	v2 := newTestVerifier(src)
	d, _ := digestFor(101, vhash(1), vsig(1), 5000, false, []uint64{1}, []uint64{2}, []byte{0})
	src.skipped[101] = true
	v2.Record(d)
	v2.SetExecutedTip(200)
	for i := 0; i < 2; i++ {
		v2.verifyNext()
		v2.mu.Lock()
		if pd := v2.pending[101]; pd != nil {
			pd.nextTry = pd.nextTry.Add(-time.Hour) // bypass the confirm backoff
		}
		v2.mu.Unlock()
	}
	assert.Nil(t, v2.Failure(), "needs 3 confirmations")
	v2.verifyNext()
	div := v2.Failure()
	require.NotNil(t, div)
	assert.Equal(t, "skip_mismatch", div.Kind)
}

// The lag keeps fresh slots unverified until the tip moves past them.
func TestVerifierRespectsLag(t *testing.T) {
	src := &fakeVerificationSource{blocks: map[uint64]*verifiedBlock{}, skipped: map[uint64]bool{}, errs: map[uint64]error{}}
	v := newTestVerifier(src) // lag 4

	d, vb := digestFor(100, vhash(1), vsig(1), 5000, false, []uint64{1}, []uint64{2}, []byte{0})
	v.Record(d)
	src.blocks[100] = vb
	// tip = 103: slot 100 > 103-4 -> not yet eligible
	v.SetExecutedTip(103)
	v.verifyNext()
	assert.Equal(t, 0, src.calls, "slot within the lag window is not verified yet")

	v.SetExecutedTip(104)
	v.verifyNext()
	assert.Equal(t, 1, src.calls)
	assert.Equal(t, uint64(100), v.VerifiedWatermark())
}
