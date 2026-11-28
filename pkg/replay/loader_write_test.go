package replay

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"math"
	"os"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestLoaderWriteSlot470788792(t *testing.T) {
	var fixture struct {
		Slot         uint64
		Transaction  string
		Accounts     []accounts.Account
		Fee          uint64
		ComputeUnits uint64 `json:"compute_units"`
		HashEvidence struct {
			ParentBankhash solana.Hash        `json:"parent_bankhash"`
			Blockhash      solana.Hash        `json:"blockhash"`
			Signatures     uint64             `json:"num_signatures"`
			BrokenHash     solana.Hash        `json:"broken_bankhash"`
			ExpectedHash   solana.Hash        `json:"expected_bankhash"`
			BrokenLtHash   []byte             `json:"broken_lthash"`
			BrokenAccounts []accounts.Account `json:"broken_accounts"`
		} `json:"hash_evidence"`
	}
	raw, err := os.ReadFile("testdata/slot_470788792_loader_write.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &fixture))
	wire, err := base64.StdEncoding.DecodeString(fixture.Transaction)
	require.NoError(t, err)
	tx, err := solana.TransactionFromBytes(wire)
	require.NoError(t, err)
	require.NoError(t, tx.VerifySignatures())
	require.Equal(t, solana.MessageVersionV1, tx.Message.GetVersion())

	ctx, cleanup := newCommitTestSlotCtx()
	defer cleanup()
	ctx.Slot = fixture.Slot
	ctx.Features.EnableFeature(features.EnableTxV1, 0)
	for _, acct := range fixture.Accounts {
		require.NoError(t, ctx.SetAccount(acct.Key, acct.Clone()))
	}
	require.NoError(t, ctx.SetAccount(addresses.BpfLoaderUpgradeableAddr, &accounts.Account{
		Key: addresses.BpfLoaderUpgradeableAddr, Owner: addresses.NativeLoaderAddr,
		Lamports: 1, Executable: true, RentEpoch: math.MaxUint64,
	}))
	recent := sealevel.SysvarRecentBlockhashes{{
		Blockhash:     tx.Message.RecentBlockhash,
		FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: 5000},
	}}
	sealevel.SysvarCache.RecentBlockHashes.Sysvar = &recent
	ctx.LastBlockhash = tx.Message.RecentBlockhash

	// The captured v1 transaction's oversized loader Write must fail before
	// mutating its buffer or executing the following 5000-lamport transfer.
	// Fees still apply to the payer when the first instruction fails.
	var sigverify sync.WaitGroup
	fee, usedCU, processErr := ProcessTransaction(ctx, &sigverify, tx, nil, nil, nil, false)
	sigverify.Wait()
	require.ErrorIs(t, processErr, sealevel.InstrErrInvalidInstructionData)
	require.NotNil(t, fee)
	require.Equal(t, fixture.Fee, fee.TotalFee)
	require.Equal(t, fixture.ComputeUnits, usedCU)
	for i, before := range fixture.Accounts {
		after, err := ctx.GetAccount(before.Key)
		require.NoError(t, err)
		expected := before.Clone()
		if i == 0 {
			expected.Lamports -= fixture.Fee
		}
		require.Equal(t, expected, after)
	}

	// Replace only these three transaction effects in the captured complete
	// end-of-slot LtHash. The actual replay output must recover the certified
	// footer exactly, including all other transactions, sysvars and rewards.
	evidence := fixture.HashEvidence
	require.Len(t, evidence.BrokenLtHash, 2048)
	lt := new(lthash.LtHash).InitWithHash(evidence.BrokenLtHash)
	bankHash := func() solana.Hash {
		input := append([]byte(nil), evidence.ParentBankhash[:]...)
		input = binary.LittleEndian.AppendUint64(input, evidence.Signatures)
		input = append(input, evidence.Blockhash[:]...)
		inner := sha256.Sum256(input)
		return solana.Hash(sha256.Sum256(append(inner[:], lt.Hash()...)))
	}
	require.Equal(t, evidence.BrokenHash, bankHash())
	for _, broken := range evidence.BrokenAccounts {
		fixed, err := ctx.GetAccount(broken.Key)
		require.NoError(t, err)
		lt.Sub(new(lthash.LtHash).InitWithAcct(&broken))
		lt.Add(new(lthash.LtHash).InitWithAcct(fixed))
	}
	require.Equal(t, evidence.ExpectedHash, bankHash())
}
