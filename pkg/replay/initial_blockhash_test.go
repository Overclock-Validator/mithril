package replay

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"math"
	"os"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/stretchr/testify/require"
)

func TestSnapshotStartupNonceSlot452018181(t *testing.T) {
	var fixture struct {
		Slot, Epoch     uint64
		SnapshotSlot    uint64      `json:"snapshot_slot"`
		ParentBankhash  solana.Hash `json:"parent_bankhash"`
		ParentBlockhash solana.Hash `json:"parent_blockhash"`
		Blockhash       solana.Hash
		Signatures      uint64            `json:"num_signatures"`
		BrokenHash      solana.Hash       `json:"broken_bankhash"`
		ExpectedHash    solana.Hash       `json:"expected_bankhash"`
		BrokenLtHash    []byte            `json:"broken_lthash"`
		BrokenNonce     *accounts.Account `json:"broken_nonce"`
		ExpectedNonce   *accounts.Account `json:"expected_nonce"`
	}
	raw, err := os.ReadFile("testdata/slot_452018181_snapshot_nonce.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &fixture))
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000, LeaderScheduleSlotOffset: 432000, Warmup: true, FirstNormalEpoch: 14, FirstNormalSlot: 524256}
	key := solana.PublicKey{0xC7}
	db := epochBoundaryStakeDB(t, fixture.SnapshotSlot, []sealevel.Delegation{{VoterPubkey: key, StakeLamports: 1000, ActivationEpoch: math.MaxUint64, DeactivationEpoch: math.MaxUint64}})
	previousManage := global.ManageLeaderSchedule()
	previousBlockhash := global.LatestBlockHash()
	global.SetManageLeaderSchedule(false)
	global.PutEpochStakes(fixture.Epoch+1, map[solana.PublicKey]uint64{key: 1000}, nil, 1000)
	t.Cleanup(func() {
		global.SetManageLeaderSchedule(previousManage)
		global.SetLatestBlockHash(previousBlockhash)
		global.ClearEpochStakes(fixture.Epoch + 1)
	})
	ms := &state.MithrilState{
		ManifestParentSlot: fixture.SnapshotSlot, ManifestParentBankhash: fixture.ParentBankhash.String(),
		ManifestEvictedBlockhash: solana.Hash{2}.String(), ManifestFeeRateGovernor: &state.ManifestFeeRateGovernorSeed{},
		ManifestRecentBlockhashes: []state.BlockhashEntry{{Blockhash: fixture.ParentBlockhash.String()}, {Blockhash: solana.Hash{3}.String()}},
	}
	// Start with a genuinely unconfigured block. Older startup tests supplied
	// LastBlockhash themselves, masking the snapshot initialization omission.
	block := &b.Block{Slot: fixture.Slot, Epoch: fixture.Epoch, Blockhash: fixture.Blockhash}
	require.NoError(t, configureInitialBlock(db, block, ms, schedule))
	require.Equal(t, [32]byte(fixture.ParentBlockhash), global.LatestBlockHash())
	require.NotEqual(t, fixture.ParentBankhash, fixture.ParentBlockhash)
	require.NotEqual(t, fixture.Blockhash, fixture.ParentBlockhash)

	env, cleanup := newCommitTestSlotCtx()
	defer cleanup()
	block.Features, block.FeeRateGovernor = env.Features, env.FeeRateGovernor
	ctx := newSlotCtx(block, env.Accounts, accounts.NewMemAccounts(), nil, nil, 0)
	recent := sealevel.SysvarRecentBlockhashes{{Blockhash: fixture.ParentBlockhash, FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: 5000}}}
	sealevel.SysvarCache.RecentBlockHashes.Sysvar = &recent
	nonceKey, payer := solana.PublicKey{0xD7}, txfixture.PayerPubkey()
	initialNonce := solana.Hash{0xAA}
	nonce := sealevel.NonceStateVersions{Type: sealevel.NonceVersionCurrent, Current: sealevel.NonceData{
		IsInitialized: true, Authority: payer, DurableNonce: initialNonce, FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: 5000},
	}}
	data, err := nonce.Marshal()
	require.NoError(t, err)
	require.NoError(t, ctx.SetAccount(nonceKey, &accounts.Account{Key: nonceKey, Lamports: 10_000_000, Data: data, Owner: addresses.SystemProgramAddr, RentEpoch: math.MaxUint64}))
	tx, err := solana.NewTransaction([]solana.Instruction{system.NewAdvanceNonceAccountInstruction(nonceKey, solana.SysVarRecentBlockHashesPubkey, payer).Build()}, initialNonce, solana.TransactionPayer(payer))
	require.NoError(t, err)
	privateKey := txfixture.PayerPrivateKey()
	_, err = tx.Sign(func(pk solana.PublicKey) *solana.PrivateKey {
		if pk == payer {
			return &privateKey
		}
		return nil
	})
	require.NoError(t, err)
	var wg sync.WaitGroup
	fee, _, err := ProcessTransaction(ctx, &wg, tx, nil, nil, nil, false)
	wg.Wait()
	require.NoError(t, err)
	require.NotNil(t, fee)
	advanced, err := ctx.GetAccount(nonceKey)
	require.NoError(t, err)
	actual, err := sealevel.UnmarshalNonceStateVersions(advanced.Data)
	require.NoError(t, err)
	expected, err := sealevel.UnmarshalNonceStateVersions(fixture.ExpectedNonce.Data)
	require.NoError(t, err)
	require.Equal(t, expected.State().DurableNonce, actual.State().DurableNonce)

	// The captured bank differed in exactly one account: transaction 92's
	// successful nonce advance. Apply the nonce produced through fresh-start
	// configuration to that captured account and recover the certified hash.
	lt := new(lthash.LtHash).InitWithHash(fixture.BrokenLtHash)
	bankHash := func() solana.Hash {
		input := append([]byte(nil), fixture.ParentBankhash[:]...)
		input = binary.LittleEndian.AppendUint64(input, fixture.Signatures)
		input = append(input, fixture.Blockhash[:]...)
		inner := sha256.Sum256(input)
		return solana.Hash(sha256.Sum256(append(inner[:], lt.Hash()...)))
	}
	require.Equal(t, fixture.BrokenHash, bankHash())
	repaired := fixture.BrokenNonce.Clone()
	captured, err := sealevel.UnmarshalNonceStateVersions(repaired.Data)
	require.NoError(t, err)
	captured.State().DurableNonce = actual.State().DurableNonce
	repaired.Data, err = captured.Marshal()
	require.NoError(t, err)
	require.Equal(t, fixture.ExpectedNonce, repaired)
	lt.Sub(new(lthash.LtHash).InitWithAcct(fixture.BrokenNonce))
	lt.Add(new(lthash.LtHash).InitWithAcct(repaired))
	require.Equal(t, fixture.ExpectedHash, bankHash())
}

func TestSnapshotStartupRejectsMissingOrMalformedParentBlockhash(t *testing.T) {
	for _, tc := range []struct {
		name    string
		entries []state.BlockhashEntry
	}{
		{"absent", nil},
		{"empty", []state.BlockhashEntry{{}}},
		{"invalid base58", []state.BlockhashEntry{{Blockhash: "not-a-hash"}}},
		{"wrong length", []state.BlockhashEntry{{Blockhash: "123"}}},
		{"zero", []state.BlockhashEntry{{Blockhash: solana.Hash{}.String()}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &state.MithrilState{ManifestParentSlot: 452018180, ManifestParentBankhash: solana.Hash{1}.String(), ManifestRecentBlockhashes: tc.entries}
			// Even a prefilled candidate cannot replace missing snapshot metadata.
			block := &b.Block{LastBlockhash: solana.Hash{99}}
			require.Error(t, configureInitialBlock(nil, block, s, &sealevel.SysvarEpochSchedule{}))
		})
	}
}
