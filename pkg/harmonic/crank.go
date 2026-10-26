package harmonic

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/blockprod"
	blockenginepb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/blockengine"
	"github.com/Overclock-Validator/mithril/pkg/identity"
	"github.com/gagliardetto/solana-go"
)

var (
	memoProgram = solana.MustPublicKeyFromBase58("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr")

	// Anchor discriminators: the first 8 bytes of sha256("account:Config")
	// and of sha256("global:<instruction>").
	tipPaymentConfigDiscriminator     = []byte{0x9b, 0x0c, 0xaa, 0xe0, 0x1e, 0xfa, 0xcc, 0x82}
	changeTipReceiverDiscriminator    = []byte{0x45, 0x63, 0x16, 0x47, 0x0b, 0xe7, 0x56, 0x8f}
	changeBlockBuilderDiscriminator   = []byte{0x86, 0x50, 0x26, 0x89, 0xa5, 0x15, 0x72, 0x7b}
	initTipDistributionDiscriminator  = []byte{0x78, 0xbf, 0x19, 0xb6, 0x6f, 0x31, 0xb3, 0x37}
	errNoFeeInfo                      = errors.New("no valid block builder fee info")
	errTipPaymentProgramUninitialized = errors.New("tip payment program is not initialized")
)

const (
	tipAccounts = 8
	// tipPaymentConfigSize is the discriminator, tip receiver, block builder,
	// commission and nine bumps.
	tipPaymentConfigSize = 8 + 32 + 32 + 8 + 9
	maxCommissionBps     = 10_000
)

// TipConfig configures the tip crank.
type TipConfig struct {
	TipPaymentProgram         solana.PublicKey
	TipDistributionProgram    solana.PublicKey
	MerkleRootUploadAuthority solana.PublicKey
	VoteAccount               solana.PublicKey
	// CommissionBps is the validator's commission on tips, 0 to 10000.
	CommissionBps uint16
	// Identity pays for and signs the crank; it must be an authorized voter
	// of VoteAccount. Each Build uses the identity current when it starts.
	Identity *identity.Holder
}

// TipPaymentConfig is the tip payment program's config account.
type TipPaymentConfig struct {
	TipReceiver   solana.PublicKey
	BlockBuilder  solana.PublicKey
	CommissionPct uint64
}

// ParseTipPaymentConfig decodes the tip payment config account's data.
func ParseTipPaymentConfig(data []byte) (TipPaymentConfig, error) {
	if len(data) < tipPaymentConfigSize {
		return TipPaymentConfig{}, fmt.Errorf("tip payment config has %d bytes, want %d", len(data), tipPaymentConfigSize)
	}
	if !bytes.Equal(data[:8], tipPaymentConfigDiscriminator) {
		return TipPaymentConfig{}, fmt.Errorf("tip payment config discriminator %x", data[:8])
	}
	cfg := TipPaymentConfig{
		TipReceiver:   solana.PublicKeyFromBytes(data[8:40]),
		BlockBuilder:  solana.PublicKeyFromBytes(data[40:72]),
		CommissionPct: binary.LittleEndian.Uint64(data[72:80]),
	}
	if cfg.TipReceiver.IsZero() || cfg.BlockBuilder.IsZero() {
		return TipPaymentConfig{}, errors.New("tip payment config holds a zero pubkey")
	}
	return cfg, nil
}

// TipState is the bank state the crank reads.
type TipState interface {
	ReadAccount(pubkey solana.PublicKey) (*accounts.Account, bool)
	Epoch() uint64
	LastBlockhash() solana.Hash
}

// TipCrank builds the transactions that route a slot's tips to this
// validator's tip distribution account with the block engine's block builder
// and commission, before any block bundle executes.
type TipCrank struct {
	cfg                TipConfig
	config             solana.PublicKey
	distributionConfig solana.PublicKey
	tipAccounts        [tipAccounts]solana.PublicKey
}

func NewTipCrank(cfg TipConfig) (*TipCrank, error) {
	switch {
	case cfg.TipPaymentProgram.IsZero(), cfg.TipDistributionProgram.IsZero(), cfg.VoteAccount.IsZero():
		return nil, errors.New("tip crank needs both tip programs and the vote account")
	case cfg.CommissionBps > maxCommissionBps:
		return nil, fmt.Errorf("tip commission %d bps is over %d", cfg.CommissionBps, maxCommissionBps)
	case cfg.Identity == nil:
		return nil, errors.New("tip crank needs the identity key")
	}
	c := &TipCrank{cfg: cfg}
	var err error
	if c.config, _, err = solana.FindProgramAddress([][]byte{[]byte("CONFIG_ACCOUNT")}, cfg.TipPaymentProgram); err != nil {
		return nil, err
	}
	if c.distributionConfig, _, err = solana.FindProgramAddress([][]byte{[]byte("CONFIG_ACCOUNT")}, cfg.TipDistributionProgram); err != nil {
		return nil, err
	}
	for i := range c.tipAccounts {
		seed := []byte(fmt.Sprintf("TIP_ACCOUNT_%d", i))
		if c.tipAccounts[i], _, err = solana.FindProgramAddress([][]byte{seed}, cfg.TipPaymentProgram); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// TipPaymentConfigAddress is the tip payment program's config account.
func (c *TipCrank) TipPaymentConfigAddress() solana.PublicKey {
	return c.config
}

// TipAccounts are the tip payment program's eight tip accounts.
func (c *TipCrank) TipAccounts() [tipAccounts]solana.PublicKey {
	return c.tipAccounts
}

// TipDistributionAccount is this validator's tip distribution account for
// epoch, and its bump.
func (c *TipCrank) TipDistributionAccount(epoch uint64) (solana.PublicKey, uint8, error) {
	var epochLE [8]byte
	binary.LittleEndian.PutUint64(epochLE[:], epoch)
	return solana.FindProgramAddress([][]byte{[]byte("TIP_DISTRIBUTION_ACCOUNT"), c.cfg.VoteAccount[:], epochLE[:]}, c.cfg.TipDistributionProgram)
}

// Build returns the crank for the slot whose bank is state, or no
// transactions when the tip state is already right. memo is the 3-byte
// strategy code recorded when the tip receiver changes.
func (c *TipCrank) Build(state TipState, fee FeeInfo, memo string) ([]*solana.Transaction, error) {
	if fee.Pubkey.IsZero() {
		return nil, errNoFeeInfo
	}
	configAcct, ok := state.ReadAccount(c.config)
	if !ok || solana.PublicKeyFromBytes(configAcct.Owner[:]) != c.cfg.TipPaymentProgram {
		return nil, errTipPaymentProgramUninitialized
	}
	config, err := ParseTipPaymentConfig(configAcct.Data)
	if err != nil {
		return nil, err
	}
	tda, bump, err := c.TipDistributionAccount(state.Epoch())
	if err != nil {
		return nil, err
	}
	// Anyone can send lamports to the PDA, so it only exists once the tip
	// distribution program owns it.
	tdaAcct, ok := state.ReadAccount(tda)
	needInit := !ok || solana.PublicKeyFromBytes(tdaAcct.Owner[:]) != c.cfg.TipDistributionProgram
	needChange := config.TipReceiver != tda || config.BlockBuilder != fee.Pubkey || config.CommissionPct != fee.Commission

	signer := c.cfg.Identity.Current()
	payer := signer.PublicKey()
	var instructions [][]solana.Instruction
	if needInit {
		instructions = append(instructions, []solana.Instruction{c.initTipDistributionAccount(tda, bump, payer)})
	}
	if needChange {
		instructions = append(instructions, []solana.Instruction{
			c.changeTipReceiver(config, tda, payer),
			c.changeBlockBuilder(config, tda, fee, payer),
			solana.NewInstruction(memoProgram, solana.AccountMetaSlice{}, []byte(memo)),
		})
	}
	key := solana.PrivateKey(signer.PrivateKey())
	txs := make([]*solana.Transaction, 0, len(instructions))
	for _, ixs := range instructions {
		tx, err := solana.NewTransaction(ixs, state.LastBlockhash(), solana.TransactionPayer(payer))
		if err != nil {
			return nil, err
		}
		if _, err := tx.Sign(func(solana.PublicKey) *solana.PrivateKey { return &key }); err != nil {
			return nil, err
		}
		txs = append(txs, tx)
	}
	return txs, nil
}

func (c *TipCrank) initTipDistributionAccount(tda solana.PublicKey, bump uint8, payer solana.PublicKey) solana.Instruction {
	data := append([]byte(nil), initTipDistributionDiscriminator...)
	data = append(data, c.cfg.MerkleRootUploadAuthority[:]...)
	data = binary.LittleEndian.AppendUint16(data, c.cfg.CommissionBps)
	data = append(data, bump)
	return solana.NewInstruction(c.cfg.TipDistributionProgram, solana.AccountMetaSlice{
		solana.NewAccountMeta(c.distributionConfig, false, false),
		solana.NewAccountMeta(tda, true, false),
		solana.NewAccountMeta(c.cfg.VoteAccount, false, false),
		solana.NewAccountMeta(payer, true, true),
		solana.NewAccountMeta(solana.SystemProgramID, false, false),
	}, data)
}

// tipPaymentAccounts is the layout both tip payment instructions use: the
// config, three instruction-specific accounts, the tip accounts, and the
// signer, all writable.
func (c *TipCrank) tipPaymentAccounts(first, second, third, payer solana.PublicKey) solana.AccountMetaSlice {
	metas := solana.AccountMetaSlice{
		solana.NewAccountMeta(c.config, true, false),
		solana.NewAccountMeta(first, true, false),
		solana.NewAccountMeta(second, true, false),
		solana.NewAccountMeta(third, true, false),
	}
	for _, tip := range c.tipAccounts {
		metas = append(metas, solana.NewAccountMeta(tip, true, false))
	}
	return append(metas, solana.NewAccountMeta(payer, true, true))
}

func (c *TipCrank) changeTipReceiver(config TipPaymentConfig, tda, payer solana.PublicKey) solana.Instruction {
	return solana.NewInstruction(c.cfg.TipPaymentProgram,
		c.tipPaymentAccounts(config.TipReceiver, tda, config.BlockBuilder, payer),
		append([]byte(nil), changeTipReceiverDiscriminator...))
}

func (c *TipCrank) changeBlockBuilder(config TipPaymentConfig, tda solana.PublicKey, fee FeeInfo, payer solana.PublicKey) solana.Instruction {
	data := append([]byte(nil), changeBlockBuilderDiscriminator...)
	data = binary.LittleEndian.AppendUint64(data, fee.Commission)
	// The tip receiver changed in the instruction before.
	return solana.NewInstruction(c.cfg.TipPaymentProgram,
		c.tipPaymentAccounts(tda, config.BlockBuilder, fee.Pubkey, payer), data)
}

// MemoForStrategy is the 3-byte code a Harmonic crank records for strategy.
func MemoForStrategy(strategy blockenginepb.SchedulingStrategy) string {
	switch strategy {
	case blockenginepb.SchedulingStrategy_SCHEDULING_STRATEGY_FBA:
		return "FBA"
	case blockenginepb.SchedulingStrategy_SCHEDULING_STRATEGY_MREV:
		return "MRV"
	case blockenginepb.SchedulingStrategy_SCHEDULING_STRATEGY_FIFO:
		return "FIF"
	default:
		return "???"
	}
}

// FeeSource supplies the block builder fee info and scheduling strategy.
type FeeSource interface {
	FeeInfo() (FeeInfo, bool)
	Strategy() blockenginepb.SchedulingStrategy
}

// Builder adapts the crank to the executor, with fee info and strategy from
// fees. Without valid fee info the crank fails, so the slot falls back.
func (c *TipCrank) Builder(fees FeeSource) CrankBuilder {
	return func(bank *blockprod.WorkingBank) ([]*solana.Transaction, error) {
		fee, ok := fees.FeeInfo()
		if !ok {
			return nil, errNoFeeInfo
		}
		return c.Build(bank, fee, MemoForStrategy(fees.Strategy()))
	}
}
