package rpcserver

import (
	"encoding/binary"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
)

// storeBankHashForTest writes a bank hash row directly. Production writes
// these only inside an accounts fold.
func storeBankHashForTest(db *accountsdb.AccountsDb, slot uint64, bankHash []byte) error {
	var slotBytes [8]byte
	binary.LittleEndian.PutUint64(slotBytes[:], slot)
	return db.BankHashStore.Set(slotBytes[:], bankHash, nil)
}
