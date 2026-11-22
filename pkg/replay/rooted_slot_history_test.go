package replay

import (
	"encoding/base64"
	"encoding/binary"
	"math"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func rootedHistoryBank(t testing.TB, slot uint64, made ...uint64) (*sealevel.BankSysvars, *state.ResumeContext) {
	t.Helper()
	// Encode the protocol account directly so the observer's offsets and range
	// checks are not merely compared with the same production helper.
	raw := make([]byte, 1+8+rootedHistoryBitmapBytes+16)
	raw[0] = 1
	binary.LittleEndian.PutUint64(raw[1:9], RootedSlotHistoryCapacity/64)
	binary.LittleEndian.PutUint64(raw[len(raw)-16:len(raw)-8], RootedSlotHistoryCapacity)
	binary.LittleEndian.PutUint64(raw[len(raw)-8:], slot+1)
	for _, s := range made {
		raw[9+(s%RootedSlotHistoryCapacity)/8] |= 1 << (s % 8)
	}
	bank, err := sealevel.NewBankSysvars(slot, &accounts.Account{Key: sealevel.SysvarSlotHistoryAddr, Lamports: 1, Data: raw})
	require.NoError(t, err)
	return bank, &state.ResumeContext{Slot: slot, Bankhash: solana.Hash(testHash(1)).String()}
}

func TestRootedSlotHistoryEpochGeometry(t *testing.T) {
	normal := sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}
	warmup := sealevel.SysvarEpochSchedule{SlotsPerEpoch: 128, Warmup: true, FirstNormalEpoch: 2, FirstNormalSlot: 96}
	for _, tc := range []struct {
		name     string
		slot     uint64
		schedule *sealevel.SysvarEpochSchedule
		want     RootedHistoryEpoch
	}{
		{"missing", 20, nil, RootedHistoryEpoch{}},
		{"first", 32, &normal, RootedHistoryEpoch{true, 1, 32, 32}},
		{"last", 63, &normal, RootedHistoryEpoch{true, 1, 32, 32}},
		{"warmup first", 0, &warmup, RootedHistoryEpoch{true, 0, 0, 32}},
		{"warmup last", 31, &warmup, RootedHistoryEpoch{true, 0, 0, 32}},
		{"warmup next", 32, &warmup, RootedHistoryEpoch{true, 1, 32, 64}},
		{"warmup normal boundary", 96, &warmup, RootedHistoryEpoch{true, 2, 96, 128}},
		{"retained prefix expired", RootedSlotHistoryCapacity + 20, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 2 * RootedSlotHistoryCapacity}, RootedHistoryEpoch{true, 0, 0, 2 * RootedSlotHistoryCapacity}},
		{"zero divisor", 20, &sealevel.SysvarEpochSchedule{}, RootedHistoryEpoch{}},
		{"invalid warmup", 20, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 128, Warmup: true, FirstNormalEpoch: 2}, RootedHistoryEpoch{}},
		{"invalid nonwarmup", 20, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32, FirstNormalEpoch: 1}, RootedHistoryEpoch{}},
		{"overflowing end", math.MaxUint64 - 1, &normal, RootedHistoryEpoch{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bank, ctx := rootedHistoryBank(t, tc.slot, tc.slot)
			if tc.schedule != nil {
				var err error
				bank, err = bank.WithAccounts(&accounts.Account{Key: sealevel.SysvarEpochScheduleAddr, Lamports: 1,
					Data: marshalEpochScheduleForParentLoader(t, *tc.schedule)})
				require.NoError(t, err)
			}
			h := captureRootedSlotHistory(tc.slot, ctx, bank, time.Now())
			require.True(t, h.valid(), "bad epoch geometry must not erase valid slot history")
			require.Equal(t, tc.want, h.Epoch)
		})
	}
}

func TestRootedSlotHistoryProtocolRangeAndWrap(t *testing.T) {
	at := time.Unix(100, 123)
	for _, slot := range []uint64{0, 20, RootedSlotHistoryCapacity - 1, RootedSlotHistoryCapacity,
		RootedSlotHistoryCapacity + 19, 3*RootedSlotHistoryCapacity + 3, math.MaxUint64 - 1} {
		bank, ctx := rootedHistoryBank(t, slot, slot)
		h := captureRootedSlotHistory(slot, ctx, bank, at)
		require.Equal(t, slot, h.BankSlot)
		require.Equal(t, at, h.ObservedAt)
		require.Equal(t, slot+1, h.NextSlot)
		oldest := uint64(0)
		if slot+1 > RootedSlotHistoryCapacity {
			oldest = slot + 1 - RootedSlotHistoryCapacity
		}
		require.Equal(t, oldest, h.OldestSlot)
		for _, s := range []uint64{0, oldest, slot, slot + 1, math.MaxUint64} {
			produced, known := h.Check(s)
			require.Equal(t, s >= oldest && s <= slot, known)
			if known {
				require.Equal(t, s == slot, produced)
			}
		}
		if oldest > 0 {
			_, known := h.Check(oldest - 1)
			require.False(t, known)
		}
		encoded, ok := h.BitmapBase64()
		require.True(t, ok)
		require.Len(t, encoded, base64.StdEncoding.EncodedLen(rootedHistoryBitmapBytes))
		decoded, err := base64.StdEncoding.DecodeString(encoded)
		require.NoError(t, err)
		require.Len(t, decoded, rootedHistoryBitmapBytes)
		decoded[(slot%RootedSlotHistoryCapacity)/8] = 0
		produced, known := h.Check(slot)
		require.True(t, produced && known, "encoded copies must not mutate the source")
	}
	bank, ctx := rootedHistoryBank(t, 20, 0, 2, 20)
	h := captureRootedSlotHistory(20, ctx, bank, at)
	for s := uint64(0); s <= 20; s++ {
		produced, known := h.Check(s)
		require.True(t, known)
		require.Equal(t, s == 0 || s == 2 || s == 20, produced)
	}
	var absent RootedSlotHistory
	_, known := absent.Check(0)
	require.False(t, known)
	_, ok := absent.BitmapBase64()
	require.False(t, ok)
}

func TestRootedSlotHistoryRejectsMissingMalformedAndMismatchedBank(t *testing.T) {
	at := time.Unix(100, 0)
	for _, name := range []string{"missing bank", "missing context", "context slot", "bank slot", "bad hash", "zero hash",
		"zero date", "missing history", "next slot", "bit length", "blocks length", "option", "trailing bytes", "current bit", "slot overflow"} {
		t.Run(name, func(t *testing.T) {
			slot, observed := uint64(20), at
			bank, ctx := rootedHistoryBank(t, slot, slot)
			switch name {
			case "missing bank":
				bank = nil
			case "missing context":
				ctx = nil
			case "context slot":
				ctx.Slot++
			case "bank slot":
				bank, _ = rootedHistoryBank(t, slot+1, slot+1)
			case "bad hash":
				ctx.Bankhash = "bad"
			case "zero hash":
				ctx.Bankhash = (solana.Hash{}).String()
			case "zero date":
				observed = time.Time{}
			case "missing history":
				bank, _ = sealevel.NewBankSysvars(slot)
			case "slot overflow":
				slot = math.MaxUint64
				bank, ctx = rootedHistoryBank(t, slot, slot)
			default:
				raw, ok := bank.RawView(solana.PublicKey(sealevel.SysvarSlotHistoryAddr))
				require.True(t, ok)
				raw = append([]byte(nil), raw...)
				switch name {
				case "next slot":
					binary.LittleEndian.PutUint64(raw[len(raw)-8:], slot)
				case "bit length":
					binary.LittleEndian.PutUint64(raw[len(raw)-16:len(raw)-8], RootedSlotHistoryCapacity-1)
				case "blocks length":
					binary.LittleEndian.PutUint64(raw[1:9], RootedSlotHistoryCapacity/64-1)
					raw = append(raw[:9+(RootedSlotHistoryCapacity/64-1)*8], raw[len(raw)-16:]...)
				case "option":
					raw[0] = 2
				case "trailing bytes":
					raw = append(raw, 0)
				case "current bit":
					raw[9+slot/8] = 0
				}
				var err error
				bank, err = sealevel.NewBankSysvars(slot, &accounts.Account{Key: sealevel.SysvarSlotHistoryAddr, Lamports: 1, Data: raw})
				require.NoError(t, err)
			}
			require.Equal(t, RootedSlotHistory{}, captureRootedSlotHistory(slot, ctx, bank, observed))
		})
	}
}

func TestRootedSlotHistoryOwnsBitmap(t *testing.T) {
	bank, ctx := rootedHistoryBank(t, 20, 0, 2, 20)
	at := time.Unix(100, 0)
	h := captureRootedSlotHistory(20, ctx, bank, at)
	raw, ok := bank.RawView(solana.PublicKey(sealevel.SysvarSlotHistoryAddr))
	require.True(t, ok)
	// Fault injection after capture deliberately violates the borrowed-view
	// contract. The monitoring copy must still own its earlier bytes.
	clear(raw)
	ctx.Bankhash = "changed"
	for _, s := range []uint64{0, 2, 20} {
		produced, known := h.Check(s)
		require.True(t, produced && known)
	}
	other := h
	other.BankSlot++
	other.ObservedAt = at.Add(time.Hour)
	require.Equal(t, uint64(20), h.BankSlot)
	require.Equal(t, at, h.ObservedAt)
	require.Equal(t, solana.Hash(testHash(1)), h.Bankhash)
	require.Zero(t, testing.AllocsPerRun(1000, func() { h.Check(2) }))
}

func rootedHistoryTail(t *testing.T, fc *fakeCommitter, enabled bool, slots ...uint64) *unrootedTail {
	t.Helper()
	tail := asyncTestTail(fc, slots...)
	if enabled {
		tail.rootedHistoryOwner = diagnosticProgress(t).diagnostics.Load()
	}
	for i, slot := range slots {
		made := append([]uint64{0}, slots[:i+1]...)
		bank, ctx := rootedHistoryBank(t, slot, made...)
		ctx.Bankhash = solana.Hash(tail.bankhashes[slot]).String()
		tail.SetContext(slot, ctx, bank)
	}
	return tail
}

func TestRootedSlotHistorySuccessfulFoldPrefix(t *testing.T) {
	for _, tc := range []struct {
		name string
		fail uint64
		want uint64
	}{{"success", 0, 18}, {"partial success", 17, 16}, {"first failure", 15, 0}} {
		t.Run(tc.name, func(t *testing.T) {
			fc := &fakeCommitter{durable: accounts.NewMemAccounts(), failOn: tc.fail}
			tail := rootedHistoryTail(t, fc, true, 15, 16, 17, 18)
			before := time.Now()
			through, ctx, err := tail.flush(18)
			if tc.fail == 0 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Equal(t, tc.want, through)
			require.Equal(t, tc.want, tail.rootedHistory.BankSlot)
			if through == 0 {
				require.True(t, tail.rootedHistory.ObservedAt.IsZero())
				return
			}
			h := tail.rootedHistory
			require.Equal(t, ctx.Bankhash, h.Bankhash.String())
			require.False(t, h.ObservedAt.Before(before))
			require.False(t, h.ObservedAt.After(time.Now()))
			require.NotContains(t, tail.bankSysvars, through)
			for _, slot := range []uint64{0, 15, through} {
				produced, known := h.Check(slot)
				require.True(t, produced && known, "pre-prune bank history was lost")
			}
		})
	}
}

func TestRootedSlotHistoryWaitsForAsyncApply(t *testing.T) {
	fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
	tail := rootedHistoryTail(t, fc, true, 5, 6, 7)
	job, err := tail.buildFoldJob(7, false)
	require.NoError(t, err)
	require.Equal(t, uint64(6), job.through)
	require.True(t, tail.rootedHistory.ObservedAt.IsZero())
	require.NoError(t, runFoldJob(fc, job))
	require.True(t, tail.rootedHistory.ObservedAt.IsZero(), "worker commit is not loop application")
	ctx := tail.applyFoldJob(job)
	require.Equal(t, uint64(6), tail.rootedHistory.BankSlot)
	require.Equal(t, ctx.Bankhash, tail.rootedHistory.Bankhash.String())
	require.NotContains(t, tail.bankSysvars, uint64(6))
	require.Contains(t, tail.bankSysvars, uint64(7))
	produced, known := tail.rootedHistory.Check(0)
	require.True(t, produced && known)
}

func TestRootedSlotHistoryDisabledAndFailureDoNotChangeFolds(t *testing.T) {
	for _, async := range []bool{false, true} {
		var first []uint64
		for _, enabled := range []bool{false, true} {
			fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
			tail := rootedHistoryTail(t, fc, enabled, 5, 6, 7)
			if async {
				job, err := tail.buildFoldJob(7, false)
				require.NoError(t, err)
				require.NoError(t, runFoldJob(fc, job))
				tail.applyFoldJob(job)
			} else {
				_, _, err := tail.promote(7)
				require.NoError(t, err)
			}
			require.Equal(t, []uint64{5, 6}, fc.committed)
			require.Equal(t, 1, tail.overlay.HeldSlots())
			if !enabled {
				first = append([]uint64(nil), fc.committed...)
				require.True(t, tail.rootedHistory.ObservedAt.IsZero())
				require.Zero(t, testing.AllocsPerRun(1000, func() { tail.observeRootedSlotHistory(6, nil) }))
			} else {
				require.Equal(t, first, fc.committed)
				require.Equal(t, uint64(6), tail.rootedHistory.BankSlot)
			}
		}
	}
	fc := &fakeCommitter{durable: accounts.NewMemAccounts(), failOn: 5}
	tail := rootedHistoryTail(t, fc, true, 5, 6)
	job, err := tail.buildFoldJob(6, false)
	require.NoError(t, err)
	require.Error(t, runFoldJob(fc, job))
	require.True(t, tail.rootedHistory.ObservedAt.IsZero())
	require.Equal(t, 2, tail.overlay.HeldSlots())
	require.Empty(t, fc.committed)
	// A malformed context or missing bank cannot turn monitoring into a fold
	// failure, and must not reuse an earlier observation for the new frontier.
	for _, name := range []string{"missing bank", "context hash", "missing hash"} {
		t.Run(name, func(t *testing.T) {
			fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
			tail := rootedHistoryTail(t, fc, true, 5, 6)
			job, err := tail.buildFoldJob(6, false)
			require.NoError(t, err)
			switch name {
			case "missing bank":
				delete(tail.bankSysvars, 6)
			case "context hash":
				job.ctx.Bankhash = solana.Hash(testHash(99)).String()
			case "missing hash":
				delete(job.bankhashes, 6)
			}
			require.NoError(t, runFoldJob(fc, job))
			require.Same(t, job.ctx, tail.applyFoldJob(job))
			require.Equal(t, []uint64{5, 6}, fc.committed)
			require.True(t, tail.rootedHistory.ObservedAt.IsZero())
		})
	}
}

func TestRootedSlotHistoryDiagnosticPublicationAndReset(t *testing.T) {
	p := diagnosticProgress(t)
	s, _ := p.Snapshot()
	bank, ctx := rootedHistoryBank(t, 20, 0, 2, 20)
	h := captureRootedSlotHistory(20, ctx, bank, s.UpdatedAt.Add(time.Millisecond))
	committed := h.Bankhash
	owner := p.diagnostics.Load()
	observation := rootedSlotHistoryObservation{RootedSlotHistory: h, owner: owner, generation: owner.generation}
	p.recordRootedSlotHistory(20, ctx, committed, observation)
	d, ok := p.ReplayDiagnosticsSnapshot()
	require.True(t, ok)
	require.Empty(t, d.Records, "publication must not require a completed row")
	require.Equal(t, uint64(20), d.RootedHistory.BankSlot)
	require.Equal(t, h.ObservedAt, d.RootedHistory.ObservedAt)
	light, ok := p.RootedHistorySnapshot()
	require.True(t, ok)
	require.Equal(t, d.RunID, light.RunID)
	require.Equal(t, d.Generation, light.Generation)
	require.Equal(t, d.Attempt, light.Attempt)
	require.Equal(t, d.Rewinds, light.Rewinds)
	require.Equal(t, d.StartedAt, light.StartedAt)
	require.Equal(t, d.ResetAt, light.ResetAt)
	require.Equal(t, h, light.History)
	require.Zero(t, testing.AllocsPerRun(100, func() { _, _ = p.RootedHistorySnapshot() }))
	after, _ := p.Snapshot()
	require.Equal(t, s, after, "observation changed replay progress or readiness")
	d.RootedHistory.BankSlot++
	next, _ := p.ReplayDiagnosticsSnapshot()
	require.Equal(t, uint64(20), next.RootedHistory.BankSlot)
	p.rewound()
	_, ok = p.RootedHistorySnapshot()
	require.False(t, ok, "reset must invalidate the lightweight history too")
	d, _ = p.ReplayDiagnosticsSnapshot()
	require.True(t, d.RootedHistory.ObservedAt.IsZero())
	_, oldKnown := next.RootedHistory.Check(0)
	require.True(t, oldKnown, "a detached prior capture was mutated by reset")
	// A wall clock alone cannot identify the attempt: an old capture with a
	// future date still needs to fail its explicit source-generation fence.
	observation.ObservedAt = time.Now().Add(time.Hour)
	p.begin(30, nil)
	p.recordRootedSlotHistory(20, ctx, committed, observation)
	d, _ = p.ReplayDiagnosticsSnapshot()
	require.True(t, d.RootedHistory.ObservedAt.IsZero(), "old attempt capture was reused")

	s, _ = p.Snapshot()
	h = captureRootedSlotHistory(20, ctx, bank, s.UpdatedAt.Add(time.Millisecond))
	observation = rootedSlotHistoryObservation{RootedSlotHistory: h, owner: owner, generation: owner.generation}
	for _, bad := range []solana.Hash{{}, solana.Hash(testHash(99))} {
		p.recordRootedSlotHistory(20, ctx, committed, observation)
		p.recordRootedSlotHistory(20, ctx, bad, observation)
		d, _ = p.ReplayDiagnosticsSnapshot()
		require.True(t, d.RootedHistory.ObservedAt.IsZero(), "mismatched committed hash became a history binding")
	}
	backward := observation
	backward.ObservedAt = s.StartedAt.Add(-time.Second)
	p.recordRootedSlotHistory(20, ctx, committed, backward)
	d, _ = p.ReplayDiagnosticsSnapshot()
	require.Equal(t, backward.ObservedAt, d.RootedHistory.ObservedAt, "a wall-clock correction erased the same-generation bank fact")
	var absent *Progress
	var disabled Progress
	require.Zero(t, testing.AllocsPerRun(1000, func() {
		absent.recordRootedSlotHistory(20, ctx, committed, observation)
		disabled.recordRootedSlotHistory(20, ctx, committed, observation)
	}))
}
