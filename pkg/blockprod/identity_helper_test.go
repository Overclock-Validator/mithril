package blockprod

import (
	"crypto/ed25519"

	"github.com/Overclock-Validator/mithril/pkg/identity"
	"github.com/gagliardetto/solana-go"
)

func identityHolder(key solana.PrivateKey) *identity.Holder {
	holder, err := identity.NewHolder(ed25519.PrivateKey(key))
	if err != nil {
		panic(err)
	}
	return holder
}
