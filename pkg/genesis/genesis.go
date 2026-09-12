package genesis

import (
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/runtime"
)

// Genesis contains the genesis state of a Solana ledger.
type Genesis struct {
	CreationTime  time.Time
	Accounts      []AccountEntry
	Builtins      []BuiltinProgram
	RewardPools   []AccountEntry
	TicksPerSlot  uint64
	Unused        uint64
	PohParams     runtime.PohParams
	Unused2       uint64
	Fees          runtime.FeeParams
	Rent          runtime.RentParams
	Inflation     runtime.InflationParams
	EpochSchedule runtime.EpochSchedule
	ClusterID     uint32
}

type AccountEntry struct {
	Pubkey [32]byte
	accounts.Account
}

type BuiltinProgram struct {
	Key    string
	Pubkey [32]byte
}

func (g *Genesis) FillAccounts(state accounts.Accounts) {
	for _, acc := range g.Accounts {
		state.SetAccount(&acc.Pubkey, &acc.Account)
	}
}
