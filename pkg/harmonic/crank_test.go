package harmonic

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/blockprod"
	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	blockenginepb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/blockengine"
	"github.com/Overclock-Validator/mithril/pkg/txverify"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The mainnet tip programs, and the inputs of Harmonic Major's crank in
// transaction tVoDkAas...PxttW (slot 447985272, epoch 1037).
var (
	mainnetTipPayment      = solana.MustPublicKeyFromBase58("T1pyyaTNZsKv2WcRAB8oVnk93mLJw2XzjtVYqCsaHqt")
	mainnetTipDistribution = solana.MustPublicKeyFromBase58("4R3gSG8BpU4t19KYj8CfnbtRpnT8gtk4dvTHxVRwc2r7")
	majorVote              = solana.MustPublicKeyFromBase58("MajorF3gAYEmUhqkoRXoL546Zim8nMa82tuUTz9LkmE")
	majorMerkleAuthority   = solana.MustPublicKeyFromBase58("8F4jGUmxF36vQ6yabnsxX6AQVXdKBhs8kGSUuRKSg8Xt")
	// majorTDA is Harmonic Major's tip distribution account for epoch 1037.
	majorTDA = solana.MustPublicKeyFromBase58("BufWFj7nbLcgEq7GN67ntPGJTRpepDDP3rA4c3WnRLGt")
	// The tip receiver and block builder the crank replaced.
	majorOldReceiver = solana.MustPublicKeyFromBase58("FeHQ4R45LncZ2jXdYmPuZpVzM8YFHot6sANqbKUmjkPX")
	majorOldBuilder  = solana.MustPublicKeyFromBase58("shftkxnsXmqAkmLgz9Mn7bNB5Fr6mKgFc58kFHfVikj")
	// otherBuilder is fee info naming the block builder already in place.
	otherBuilder = FeeInfo{Pubkey: majorOldBuilder, Commission: 3}
)

// harmonicMajorCrank is transaction tVoDkAas...PxttW: a compute budget, then
// Harmonic Major initializing its tip distribution account, routing tips to
// it with itself as block builder at 3%, and the FBA memo.
const harmonicMajorCrank = "ASxom6nw/Z3iXqzjx8uTxv1HcP2Ks8RjZnmqZAUhD20Du29vpFRAJF+SERsg+DgI0Uoo3qS+Xi83ohQWvg0l9wgBAAUVgLeLcjD8+FglNNlJnga0pOs4bkzc6zTVeve4QlrBTqV4Uhyxec67hYm1VqLV7JTSSYaC/fm7KvWtZOSRzEFT2vGH7IfR90XLOgM4SiamntoMotGqD0HkJBY3fpH/W10xsU4N5V6fuoY5br/VSM/4ySAR6se3W6qbLZxqhvWhcUGI8f+jot/mF73E41cyUaMi4/yugeWkVzkOZHUcAKRl4rwrVwZe8d1mVDC+YGumWWwClTAbre+LWvxBAUFQ9BJ0iQd9VaW7EzB2Prdn9V7Ad7QaDQdffeHXP7rKPGPVVHG/lxtZEItbhaBPsJPx4htOP9TEyPSH3Qm5V1J2nw3YwyAmEB7CAyiWSjKrqxNsVAW5HzrjjuT2TLa96Hm4aDjSBoU4E1+QmgRpJJcD4xsLIzzYvKtaM6BEUVsC8WtuSbX3+ZoJQvrHiAVJQ6u8+Uax+bkWy+EK7c+plnZlNxyigNmQpc8umUDWUzX0Mtn+XQ3Sq6myPWCcJxYkGkx0QXbWDP0G3GcM+JGk0PvEOwY6JyH6YvyUBuw1pM48s3zaYWyiEpu+ZRlk81P2fGTpAMNrXt6MDmxYuHl7z6bTHV7M9YC4i3Iw/PhYJTTZSZ4GtKTrOG5M3Os01Xr3uEJawU6lBUXDkRcvkc62zsNrOYLVMPhvlR+AWi+pPPN0WAbGFSkDBkZv5SEXMv/srbpyw5vnvIzlu8X3EmssQ5s6QAAAAAaqCVSLUEdq1GL5H4mjAVAzJk/Jq9UnACCp0UIzR0L7BUpTWpkpIQZNJOhxYNo4fHw1td28kruB5B+oQEEFRI0AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADK8B8f95T8sn0WK6FHyWCqexPsACofWZ8R3DxbR0fycRk/Uk/arvshwYWKLA7MkSIQx+fLazpisW2BQT9+jPJoFEAAFAtD7AQAUBQkNDwATK3i/GbZvMbM3a5wn6b8s4ocbhqDkf2GsZbAAwy/v3JwieWFXTYHOS+MAAP8RDQoLDQwBAgMEBQYHCAAIRWMWRwvnVo8RDQoNDAABAgMEBQYHCAAQhlAmiaUVcnsDAAAAAAAAABIAA0ZCQQ=="

type fakeTipState struct {
	accounts  map[solana.PublicKey]*accounts.Account
	epoch     uint64
	blockhash solana.Hash
}

func (f *fakeTipState) ReadAccount(pubkey solana.PublicKey) (*accounts.Account, bool) {
	acct, ok := f.accounts[pubkey]
	return acct, ok
}

func (f *fakeTipState) Epoch() uint64              { return f.epoch }
func (f *fakeTipState) LastBlockhash() solana.Hash { return f.blockhash }

func tipConfigData(receiver, builder solana.PublicKey, commission uint64) []byte {
	data := append([]byte(nil), tipPaymentConfigDiscriminator...)
	data = append(data, receiver[:]...)
	data = append(data, builder[:]...)
	data = binary.LittleEndian.AppendUint64(data, commission)
	return append(data, 254, 255, 254, 255, 255, 252, 255, 252, 255)
}

func owned(owner solana.PublicKey, data []byte) *accounts.Account {
	return &accounts.Account{Lamports: 1_000_000, Owner: owner, Data: data}
}

// majorCrank returns a crank with the mainnet programs and Harmonic Major's
// vote account, signed by identity, and a bank state at epoch 1037 in which
// tips go to another tip receiver and block builder.
func majorCrank(t *testing.T, identity solana.PrivateKey) (*TipCrank, *fakeTipState) {
	t.Helper()
	crank, err := NewTipCrank(TipConfig{
		TipPaymentProgram:         mainnetTipPayment,
		TipDistributionProgram:    mainnetTipDistribution,
		MerkleRootUploadAuthority: majorMerkleAuthority,
		VoteAccount:               majorVote,
		Identity:                  identity,
	})
	require.NoError(t, err)
	state := &fakeTipState{epoch: 1037, blockhash: solana.Hash{7}, accounts: map[solana.PublicKey]*accounts.Account{
		crank.TipPaymentConfigAddress(): owned(mainnetTipPayment, tipConfigData(majorOldReceiver, majorOldBuilder, 3)),
		majorTDA:                        owned(mainnetTipDistribution, nil),
	}}
	return crank, state
}

func newIdentity(t *testing.T) solana.PrivateKey {
	t.Helper()
	key, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)
	return key
}

// instructionKeys resolves an instruction's account indexes to pubkeys.
func instructionKeys(tx *solana.Transaction, ix solana.CompiledInstruction) []solana.PublicKey {
	keys := make([]solana.PublicKey, len(ix.Accounts))
	for i, idx := range ix.Accounts {
		keys[i] = tx.Message.AccountKeys[idx]
	}
	return keys
}

func requireWellFormed(t *testing.T, tx *solana.Transaction, identity solana.PublicKey) {
	t.Helper()
	seen := map[solana.PublicKey]bool{}
	for _, key := range tx.Message.AccountKeys {
		require.Falsef(t, seen[key], "duplicate account key %s", key)
		seen[key] = true
	}
	require.Equal(t, identity, tx.Message.AccountKeys[0], "the identity pays")
	require.NoError(t, txverify.SanitizeTransaction(tx))
	require.NoError(t, txverify.VerifyTransaction(tx), "signed by the identity")
}

func TestTipAddressesMatchMainnet(t *testing.T) {
	crank, _ := majorCrank(t, newIdentity(t))
	assert.Equal(t, solana.MustPublicKeyFromBase58("HgzT81VF1xZ3FT9Eq1pHhea7Wcfq2bv4tWTP3VvJ8Y9D"), crank.TipPaymentConfigAddress())
	want := [tipAccounts]solana.PublicKey{}
	for i, s := range []string{
		"96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5", "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
		"Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY", "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
		"DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh", "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
		"DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL", "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
	} {
		want[i] = solana.MustPublicKeyFromBase58(s)
	}
	assert.Equal(t, want, crank.TipAccounts(), "tip accounts 0..7, in order")
	tda, _, err := crank.TipDistributionAccount(1037)
	require.NoError(t, err)
	assert.Equal(t, majorTDA, tda)
}

func TestTipDiscriminatorsAreAnchorSighashes(t *testing.T) {
	sighash := func(preimage string) []byte {
		sum := sha256.Sum256([]byte(preimage))
		return sum[:8]
	}
	assert.Equal(t, sighash("account:Config"), tipPaymentConfigDiscriminator)
	assert.Equal(t, sighash("global:change_tip_receiver"), changeTipReceiverDiscriminator)
	assert.Equal(t, sighash("global:change_block_builder"), changeBlockBuilderDiscriminator)
	assert.Equal(t, sighash("global:initialize_tip_distribution_account"), initTipDistributionDiscriminator)
}

// TestTipCrankMatchesHarmonicMajorsCrank builds the crank from the inputs of
// Harmonic Major's and compares the instructions: same programs, accounts and
// data. Harmonic Major sent them in one transaction after a compute budget;
// ours are an initialization and a change, signed by a test key in its place.
func TestTipCrankMatchesHarmonicMajorsCrank(t *testing.T) {
	raw, err := base64.StdEncoding.DecodeString(harmonicMajorCrank)
	require.NoError(t, err)
	mainnet, err := solana.TransactionFromBytes(raw)
	require.NoError(t, err)
	require.Len(t, mainnet.Message.Instructions, 5)
	majorIdentity := mainnet.Message.AccountKeys[0]

	key := newIdentity(t)
	identity := key.PublicKey()
	crank, state := majorCrank(t, key)
	delete(state.accounts, majorTDA)
	txs, err := crank.Build(state, FeeInfo{Pubkey: identity, Commission: 3}, "FBA")
	require.NoError(t, err)
	require.Len(t, txs, 2, "initialize, then change")
	for _, tx := range txs {
		requireWellFormed(t, tx, identity)
		assert.Equal(t, solana.Hash{7}, tx.Message.RecentBlockhash)
	}
	init, change := txs[0], txs[1]
	require.Len(t, init.Message.Instructions, 1)
	require.Len(t, change.Message.Instructions, 3)

	type instruction struct {
		tx *solana.Transaction
		ix solana.CompiledInstruction
	}
	ours := []instruction{{init, init.Message.Instructions[0]}}
	for _, ix := range change.Message.Instructions {
		ours = append(ours, instruction{change, ix})
	}
	for i, want := range mainnet.Message.Instructions[1:] {
		got := ours[i]
		assert.Equal(t, mainnet.Message.AccountKeys[want.ProgramIDIndex], got.tx.Message.AccountKeys[got.ix.ProgramIDIndex])
		wantKeys := instructionKeys(mainnet, want)
		for j := range wantKeys {
			if wantKeys[j] == majorIdentity {
				wantKeys[j] = identity
			}
		}
		assert.Equalf(t, wantKeys, instructionKeys(got.tx, got.ix), "instruction %d accounts", i+1)
		assert.Equalf(t, []byte(want.Data), []byte(got.ix.Data), "instruction %d data", i+1)
	}

	// The tip payment program needs every account of the change writable.
	for _, key := range change.Message.AccountKeys {
		if key == mainnetTipPayment || key == memoProgram {
			continue
		}
		writable, err := change.Message.IsWritable(key)
		require.NoError(t, err)
		assert.Truef(t, writable, "%s is writable", key)
	}
}

func TestTipCrankIsEmptyWhenTipsAlreadyRouteHere(t *testing.T) {
	crank, state := majorCrank(t, newIdentity(t))
	state.accounts[crank.TipPaymentConfigAddress()] = owned(mainnetTipPayment, tipConfigData(majorTDA, majorOldBuilder, 3))
	txs, err := crank.Build(state, otherBuilder, "FBA")
	require.NoError(t, err)
	assert.Empty(t, txs)

	for name, fee := range map[string]FeeInfo{
		"new block builder": {Pubkey: majorOldReceiver, Commission: 3},
		"new commission":    {Pubkey: majorOldBuilder, Commission: 4},
	} {
		txs, err := crank.Build(state, fee, "FBA")
		require.NoError(t, err)
		assert.Lenf(t, txs, 1, "%s needs a change", name)
	}
}

func TestTipCrankInitializesThisEpochsTipDistributionAccount(t *testing.T) {
	crank, state := majorCrank(t, newIdentity(t))
	delete(state.accounts, majorTDA)
	txs, err := crank.Build(state, otherBuilder, "MRV")
	require.NoError(t, err)
	require.Len(t, txs, 2, "initialize, then change")
	tdaWritable, _ := txs[0].Message.IsWritable(majorTDA)
	voteWritable, _ := txs[0].Message.IsWritable(majorVote)
	assert.True(t, tdaWritable)
	assert.False(t, voteWritable)
	assert.Equal(t, "MRV", string(txs[1].Message.Instructions[2].Data))

	// Lamports alone do not make the account exist.
	state.accounts[majorTDA] = owned(solana.SystemProgramID, nil)
	txs, err = crank.Build(state, otherBuilder, "MRV")
	require.NoError(t, err)
	assert.Len(t, txs, 2)

	// Tips already point at the account, which is missing: only initialize.
	delete(state.accounts, majorTDA)
	state.accounts[crank.TipPaymentConfigAddress()] = owned(mainnetTipPayment, tipConfigData(majorTDA, majorOldBuilder, 3))
	txs, err = crank.Build(state, otherBuilder, "MRV")
	require.NoError(t, err)
	require.Len(t, txs, 1)
	assert.Equal(t, mainnetTipDistribution, txs[0].Message.AccountKeys[txs[0].Message.Instructions[0].ProgramIDIndex])
}

// TestTipCrankValidatorAsItsOwnBlockBuilder covers the identity being the
// block builder: the instructions reference the identity's existing key
// instead of listing it twice.
func TestTipCrankValidatorAsItsOwnBlockBuilder(t *testing.T) {
	key := newIdentity(t)
	identity := key.PublicKey()
	crank, state := majorCrank(t, key)

	txs, err := crank.Build(state, FeeInfo{Pubkey: identity, Commission: 0}, "FBA")
	require.NoError(t, err)
	require.Len(t, txs, 1)
	tx := txs[0]
	requireWellFormed(t, tx, identity)
	changeBuilder := tx.Message.Instructions[1]
	assert.Equal(t, uint16(0), changeBuilder.Accounts[3], "the new block builder is the identity at index 0")
	assert.Equal(t, uint16(0), changeBuilder.Accounts[12], "as is the signer")
	assert.True(t, tx.Message.IsSigner(identity))

	// The previous slot's leader made itself the builder; now it is us.
	state.accounts[crank.TipPaymentConfigAddress()] = owned(mainnetTipPayment, tipConfigData(majorOldReceiver, identity, 0))
	txs, err = crank.Build(state, otherBuilder, "FBA")
	require.NoError(t, err)
	require.Len(t, txs, 1)
	requireWellFormed(t, txs[0], identity)
	assert.Equal(t, uint16(0), txs[0].Message.Instructions[0].Accounts[3], "the old block builder is the identity at index 0")
}

func TestTipCrankRefusesToBuildFromBadState(t *testing.T) {
	crank, _ := majorCrank(t, newIdentity(t))
	fee := otherBuilder
	configKey := crank.TipPaymentConfigAddress()
	cases := map[string]func(*fakeTipState) FeeInfo{
		"no fee info":                func(*fakeTipState) FeeInfo { return FeeInfo{} },
		"tip payment config missing": func(s *fakeTipState) FeeInfo { delete(s.accounts, configKey); return fee },
		"config wrong owner": func(s *fakeTipState) FeeInfo {
			s.accounts[configKey] = owned(solana.SystemProgramID, tipConfigData(majorOldReceiver, majorOldBuilder, 3))
			return fee
		},
		"config wrong discriminator": func(s *fakeTipState) FeeInfo {
			data := tipConfigData(majorOldReceiver, majorOldBuilder, 3)
			data[0] ^= 1
			s.accounts[configKey] = owned(mainnetTipPayment, data)
			return fee
		},
		"config truncated": func(s *fakeTipState) FeeInfo {
			s.accounts[configKey] = owned(mainnetTipPayment, tipConfigData(majorOldReceiver, majorOldBuilder, 3)[:80])
			return fee
		},
		"config zero receiver": func(s *fakeTipState) FeeInfo {
			s.accounts[configKey] = owned(mainnetTipPayment, tipConfigData(solana.PublicKey{}, majorOldBuilder, 3))
			return fee
		},
	}
	for name, setup := range cases {
		_, state := majorCrank(t, crank.cfg.Identity)
		txs, err := crank.Build(state, setup(state), "FBA")
		assert.Errorf(t, err, "%s", name)
		assert.Emptyf(t, txs, "%s", name)
	}

	_, err := NewTipCrank(TipConfig{TipPaymentProgram: mainnetTipPayment, TipDistributionProgram: mainnetTipDistribution,
		VoteAccount: majorVote, CommissionBps: 10_001, Identity: newIdentity(t)})
	assert.Error(t, err, "commission over 100%")
}

func TestMemoForStrategy(t *testing.T) {
	assert.Equal(t, "FBA", MemoForStrategy(blockenginepb.SchedulingStrategy_SCHEDULING_STRATEGY_FBA))
	assert.Equal(t, "MRV", MemoForStrategy(blockenginepb.SchedulingStrategy_SCHEDULING_STRATEGY_MREV))
	assert.Equal(t, "FIF", MemoForStrategy(blockenginepb.SchedulingStrategy_SCHEDULING_STRATEGY_FIFO))
	assert.Equal(t, "???", MemoForStrategy(blockenginepb.SchedulingStrategy_SCHEDULING_STRATEGY_UNSPECIFIED))
}

type fixedFees struct {
	fee FeeInfo
	ok  bool
}

func (f fixedFees) FeeInfo() (FeeInfo, bool) { return f.fee, f.ok }
func (f fixedFees) Strategy() blockenginepb.SchedulingStrategy {
	return blockenginepb.SchedulingStrategy_SCHEDULING_STRATEGY_FBA
}

var _ FeeSource = (*BlockEngineSession)(nil)

func TestExecutorRunsTheTipCrankFromTheSlotsBank(t *testing.T) {
	identity := newIdentity(t)
	crank, _ := majorCrank(t, identity)
	installTips := func(env *blockprod.TestEnv, receiver solana.PublicKey) {
		tda, _, err := crank.TipDistributionAccount(env.Bank.Epoch())
		require.NoError(t, err)
		if receiver.IsZero() {
			receiver = tda
		}
		require.NoError(t, env.SlotCtx.SetAccount(crank.TipPaymentConfigAddress(), owned(mainnetTipPayment, tipConfigData(receiver, majorOldBuilder, 3))))
		require.NoError(t, env.SlotCtx.SetAccount(tda, owned(mainnetTipDistribution, nil)))
	}

	t.Run("tips already route here: the block runs without a crank", func(t *testing.T) {
		h := startExecutor(t, true, crank.Builder(fixedFees{otherBuilder, true}))
		env := gatedBank(t, costmodel.Limits{})
		installTips(env, solana.PublicKey{})
		h.startSlot(10, env, time.Hour)
		h.send(t, bundleEvent(10, 1, false, transfer(t, 1)))
		h.sync(t)
		assert.Len(t, env.Bank.ForgedTransactions(), 1)
		assert.Equal(t, ModeHarmonic, h.end(t, 10).Mode)
	})
	t.Run("a crank that cannot land falls back", func(t *testing.T) {
		// The test bank has no tip programs deployed, so the change cannot
		// execute; the block must not start.
		h := startExecutor(t, true, crank.Builder(fixedFees{otherBuilder, true}))
		env := gatedBank(t, costmodel.Limits{})
		installTips(env, majorOldReceiver)
		h.startSlot(10, env, time.Hour)
		h.send(t, bundleEvent(10, 1, false, transfer(t, 1)))
		h.sync(t)
		assert.Empty(t, env.Bank.ForgedTransactions())
		report := h.end(t, 10)
		assert.Equal(t, ModeFallback, report.Mode)
		assert.Equal(t, FlagFailed, report.Flags)
	})
	t.Run("no fee info falls back", func(t *testing.T) {
		h := startExecutor(t, true, crank.Builder(fixedFees{}))
		env := gatedBank(t, costmodel.Limits{})
		installTips(env, solana.PublicKey{})
		h.startSlot(10, env, time.Hour)
		h.send(t, bundleEvent(10, 1, false, transfer(t, 1)))
		h.sync(t)
		assert.Empty(t, env.Bank.ForgedTransactions())
		assert.True(t, env.Bank.TPUAllowed())
	})
}
