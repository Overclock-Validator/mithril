package alpenglow

import (
	"crypto/ed25519"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/identity"
)

func identityHolder(t testing.TB, key ed25519.PrivateKey) *identity.Holder {
	t.Helper()
	holder, err := identity.NewHolder(key)
	if err != nil {
		t.Fatalf("identity holder: %v", err)
	}
	return holder
}
