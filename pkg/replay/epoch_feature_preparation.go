package replay

import (
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	bin "github.com/gagliardetto/binary"
)

// prepareEpochBoundaryFeatureScan restores the inputs consumed by feature
// migrations before their account writes are staged. At startup those same
// migrations may already have run provisionally; on a fork the legacy Rent
// cache may still describe the abandoned sibling. Both must start from the
// selected parent, and the child receives each migration's lamport delta once.
func prepareEpochBoundaryFeatureScan(ctx *ReplayCtx, parentCapitalization, parentSlot uint64, parentSysvars *sealevel.BankSysvars, source sealevel.AccountReader) error {
	if ctx == nil {
		return fmt.Errorf("epoch feature preparation requires a replay context")
	}
	var rentAccount *accounts.Account
	if parentSysvars != nil {
		if parentSysvars.Slot() != parentSlot {
			return fmt.Errorf("epoch feature parent sysvars slot %d does not match parent %d", parentSysvars.Slot(), parentSlot)
		}
		var found bool
		rentAccount, found = parentSysvars.CloneAccount(sealevel.SysvarRentAddr)
		if !found {
			return fmt.Errorf("epoch feature parent %d has no Rent sysvar", parentSlot)
		}
	} else {
		if source == nil {
			return fmt.Errorf("epoch feature parent %d has no sysvar source", parentSlot)
		}
		var err error
		rentAccount, err = source.GetAccount(parentSlot, sealevel.SysvarRentAddr)
		if err != nil || rentAccount == nil {
			return fmt.Errorf("load epoch feature parent %d Rent: %v", parentSlot, err)
		}
		rentAccount = rentAccount.Clone()
	}
	var rent sealevel.SysvarRent
	if err := rent.UnmarshalWithDecoder(bin.NewBinDecoder(rentAccount.Data)); err != nil {
		return fmt.Errorf("decode epoch feature parent %d Rent: %w", parentSlot, err)
	}
	ctx.Capitalization = parentCapitalization
	sealevel.SysvarCache.Rent.Sysvar = &rent
	sealevel.SysvarCache.Rent.Acct = rentAccount
	return nil
}
