package replay

import (
	"encoding/binary"
	"math"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestNanosecondClockAccountAddr(t *testing.T) {
	require.Equal(t, "GaGQ2vyb3xuUwjKiq9tQQWoqhzWYh7LbiLXvqG2sUzzG", NanosecondClockAccountAddr().String())
}

func TestEncodeNanosecondClockData(t *testing.T) {
	nanos := int64(1782235622348189724)
	data := encodeNanosecondClockData(nanos)
	require.Len(t, data, nanosecondClockDataLen)
	require.Equal(t, nanos, int64(binary.LittleEndian.Uint64(data)))
}

func TestAlpenglowNanosecondClockUsesDefaultRentAndRentEpochZero(t *testing.T) {
	slotCtx := &sealevel.SlotCtx{
		Slot: 3768180,
		Accounts: func() accounts.Accounts {
			accts := accounts.NewMemAccountsWithLen(1)
			return accts
		}(),
		AcctMapsMu:    &sync.Mutex{},
		ModifiedAccts: make(map[solana.PublicKey]bool),
		WritableAccts: make(map[solana.PublicKey]bool),
	}
	block := &b.Block{
		Slot:                    3768180,
		FooterProducerTimeNanos: 1782240542728820757,
	}
	require.NoError(t, updateAlpenglowNanosecondClockAccount(slotCtx, block))
	acct, err := slotCtx.GetAccount(NanosecondClockAccountAddr())
	require.NoError(t, err)
	require.Equal(t, uint64(0), acct.RentEpoch)
	defaultRent := sealevel.NewDefaultRentSysvar()
	require.Equal(t, defaultRent.MinimumBalance(nanosecondClockDataLen), acct.Lamports)
	require.Equal(t, encodeNanosecondClockData(1782240542728820757), acct.Data)
}

func TestAlpenglowFooterProducerTimeNanosPrefersFooterNanos(t *testing.T) {
	block := &b.Block{
		UnixTimestamp:           1,
		FooterProducerTimeNanos: 1782235622348189724,
	}
	nanos, ok, err := alpenglowFooterProducerTimeNanos(block)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(1782235622348189724), nanos)
}

func TestNanosecondClockBounds(t *testing.T) {
	const slotNanos = 400_000_000
	parent := int64(1782240542_000000000)

	// One-slot gap: upper = parent + 2*400ms, lower = parent + 1.
	lower, upper := NanosecondClockBounds(parent, slotNanos)
	require.Equal(t, parent+1, lower)
	require.Equal(t, parent+2*slotNanos, upper)

	// Multi-slot gap scales the upper bound.
	_, upper5 := NanosecondClockBounds(parent, 5*slotNanos)
	require.Equal(t, parent+2*5*slotNanos, upper5)

	// Agave saturates the doubled elapsed duration to the full i64 maximum.
	_, saturatedUpper := NanosecondClockBounds(0, math.MaxUint64)
	require.Equal(t, int64(math.MaxInt64), saturatedUpper)
}

func TestSkewBlockProducerTimeNanosClampsBothEnds(t *testing.T) {
	const slotNanos = 400_000_000
	parent := int64(1782240542_000000000)
	lower, upper := NanosecondClockBounds(parent, slotNanos)

	// Wall clock far ahead (leader just caught up) clamps down to the upper bound.
	require.Equal(t, upper, SkewBlockProducerTimeNanos(parent, parent+10*slotNanos, slotNanos))

	// Wall clock behind/equal to parent clamps up to parent+1.
	require.Equal(t, lower, SkewBlockProducerTimeNanos(parent, parent, slotNanos))
	require.Equal(t, lower, SkewBlockProducerTimeNanos(parent, parent-slotNanos, slotNanos))

	// In-bounds value passes through unchanged.
	inBounds := parent + slotNanos
	require.Equal(t, inBounds, SkewBlockProducerTimeNanos(parent, inBounds, slotNanos))
}

func TestValidateAlpenglowFooterNanosecondClockBounds(t *testing.T) {
	const (
		parentSlot  = uint64(100)
		workingSlot = uint64(101)
		parentNanos = int64(1_782_240_542_000_000_000)
	)
	parentAccts := accounts.NewMemAccounts()
	nanoClock := &accounts.Account{
		Key:      NanosecondClockAccountAddr(),
		Lamports: 1,
		Data:     encodeNanosecondClockData(parentNanos),
	}
	require.NoError(t, parentAccts.SetAccountWithoutLock(nanoClock.Key, nanoClock))
	clock := sealevel.SysvarClock{Slot: workingSlot, UnixTimestamp: parentNanos / 1_000_000_000}
	clockAcct := &accounts.Account{Key: sealevel.SysvarClockAddr, Lamports: 1, Data: clock.MustMarshal()}
	bankSysvars, err := sealevel.NewBankSysvars(workingSlot, clockAcct)
	require.NoError(t, err)
	slotCtx := &sealevel.SlotCtx{
		Slot:        workingSlot,
		ParentSlot:  parentSlot,
		ParentAccts: parentAccts,
	}
	require.NoError(t, slotCtx.PublishBankSysvars(bankSysvars))

	lower, upper := NanosecondClockBounds(parentNanos, uint64(alpenglowNsPerSlot))
	for _, tc := range []struct {
		name      string
		producer  uint64
		wantError bool
	}{
		{name: "inclusive lower", producer: uint64(lower)},
		{name: "inclusive upper", producer: uint64(upper)},
		{name: "zero", producer: 0, wantError: true},
		{name: "equal parent", producer: uint64(parentNanos), wantError: true},
		{name: "above upper", producer: uint64(upper + 1), wantError: true},
		{name: "i64 overflow", producer: uint64(math.MaxInt64) + 1, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			block := &b.Block{
				Slot:                    workingSlot,
				ParentSlot:              parentSlot,
				HasAlpenglowFooter:      true,
				FooterProducerTimeNanos: tc.producer,
			}
			err := validateAlpenglowFooterNanosecondClock(slotCtx, block)
			if tc.wantError {
				require.ErrorContains(t, err, "nanosecond clock out of bounds")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNanosecondClockAnchorUsesPinnedParentAndBankClockFallback(t *testing.T) {
	const (
		parentNanos = int64(1_782_240_542_000_000_000)
		childNanos  = parentNanos + 123
	)
	parentAccts := accounts.NewMemAccounts()
	currentAccts := accounts.NewMemAccounts()
	parentNano := &accounts.Account{Key: NanosecondClockAccountAddr(), Lamports: 1, Data: encodeNanosecondClockData(parentNanos)}
	childNano := &accounts.Account{Key: NanosecondClockAccountAddr(), Lamports: 1, Data: encodeNanosecondClockData(childNanos)}
	require.NoError(t, parentAccts.SetAccountWithoutLock(parentNano.Key, parentNano))
	require.NoError(t, currentAccts.SetAccountWithoutLock(childNano.Key, childNano))
	clock := sealevel.SysvarClock{Slot: 101, UnixTimestamp: 1234}
	clockAcct := &accounts.Account{Key: sealevel.SysvarClockAddr, Lamports: 1, Data: clock.MustMarshal()}
	bankSysvars, err := sealevel.NewBankSysvars(101, clockAcct)
	require.NoError(t, err)
	slotCtx := &sealevel.SlotCtx{Slot: 101, ParentSlot: 100, Accounts: currentAccts, ParentAccts: parentAccts}
	require.NoError(t, slotCtx.PublishBankSysvars(bankSysvars))

	got, ok := ReadNanosecondClockFromSlotCtx(slotCtx)
	require.True(t, ok)
	require.Equal(t, parentNanos, got)

	// A prefunded but not-yet-populated PDA is still an account before-image,
	// but it supplies no time value. Agave falls back to the bank-start Clock.
	prefunded := &accounts.Account{Key: NanosecondClockAccountAddr(), Lamports: 1}
	require.NoError(t, parentAccts.SetAccountWithoutLock(prefunded.Key, prefunded))
	got, ok = ReadNanosecondClockFromSlotCtx(slotCtx)
	require.True(t, ok)
	require.Equal(t, int64(1_234_000_000_000), got)
}
