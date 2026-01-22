package util

import (
	"encoding/binary"
	"fmt"
	"regexp"
	"slices"
	"sort"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/zeebo/blake3"
)

// Got to be a valid hostname as per Let's Encrypt, ie 'localhost' is not valid.
// For more info, read https://letsencrypt.org/docs/certificates-for-localhost/.
var validHostnameRegexp = regexp.MustCompile(`^(?i)[a-z0-9-]+(\.[a-z0-9-]+)+\.?$`)

// IsValidHostname returns true if the hostname is valid.
//
// It uses a simple regular expression to check the hostname validity.
func IsValidHostname(hostname string) bool {
	return validHostnameRegexp.MatchString(hostname)
}

func AlignUp(unaligned uint64, align uint64) uint64 {
	mask := align - 1
	alignedVal := unaligned + (-unaligned & mask)
	return alignedVal
}

func PubkeyCmp(a solana.PublicKey, b solana.PublicKey) bool {
	for i := uint64(0); i < 4; i++ {
		a1 := binary.BigEndian.Uint64(a[8*i:])
		b1 := binary.BigEndian.Uint64(b[8*i:])
		if a1 != b1 {
			return a1 < b1
		}
	}
	return false
}

func PubkeyCmpByteSlice(a []byte, b []byte) bool {
	for i := uint64(0); i < 4; i++ {
		a1 := binary.BigEndian.Uint64(a[8*i:])
		b1 := binary.BigEndian.Uint64(b[8*i:])
		if a1 != b1 {
			return a1 < b1
		}
	}
	return false
}

func DedupePubkeys(pubkeys []solana.PublicKey) []solana.PublicKey {
	sort.SliceStable(pubkeys, func(i, j int) bool {
		return PubkeyCmp(pubkeys[i], pubkeys[j])
	})

	sortedPubkeys := slices.Compact(pubkeys)
	return sortedPubkeys
}

var empty32Bytes [32]byte

func CalculateAcctHash(acct accounts.Account) []byte {
	if acct.Lamports == 0 {
		return empty32Bytes[:]
	}

	hasher := blake3.New()

	var lamportBytes [8]byte
	binary.LittleEndian.PutUint64(lamportBytes[:], acct.Lamports)
	_, _ = hasher.Write(lamportBytes[:])

	var rentEpochBytes [8]byte
	binary.LittleEndian.PutUint64(rentEpochBytes[:], acct.RentEpoch)
	_, _ = hasher.Write(rentEpochBytes[:])

	_, _ = hasher.Write(acct.Data)

	if acct.Executable {
		_, _ = hasher.Write([]byte{1})
	} else {
		_, _ = hasher.Write([]byte{0})
	}

	_, _ = hasher.Write(acct.Owner[:])
	_, _ = hasher.Write(acct.Key[:])

	return hasher.Sum(nil)
}

// this logs the function name as well.
func VerboseHandleError(err error) (b bool) {
	if err != nil {
		// pc, filename, line, _ := runtime.Caller(1)

		//mlog.Log.Debugf("[error] in %s[%s:%d] %v", runtime.FuncForPC(pc).Name(), filename, line, err)
		b = true
	}
	return
}

func PrettyPrintAcct(acct *accounts.Account) string {
	return fmt.Sprintf("acct - slot: %d, pubkey: %s, owner: %s, lamports: %d, executable: %t, rent epoch: %d, data len: %d, data hash: %s\n", acct.Slot, acct.Key, solana.PublicKeyFromBytes(acct.Owner[:]), acct.Lamports, acct.Executable, acct.RentEpoch, len(acct.Data), solana.HashFromBytes(CalculateAcctHash(*acct)))
}

func PrettyPrintAcctWithAcctData(acct *accounts.Account) string {
	var dataHexString string
	for _, c := range acct.Data {
		dataHexString += fmt.Sprintf("%x ", c)
	}
	return fmt.Sprintf("acct - slot: %d, pubkey: %s, owner: %s, lamports: %d, executable: %t, rent epoch: %d, data len: %d\ndata: %s\n", acct.Slot, acct.Key, solana.PublicKeyFromBytes(acct.Owner[:]), acct.Lamports, acct.Executable, acct.RentEpoch, len(acct.Data), dataHexString)
}

func ReverseBytesInPlace(s []byte) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func ReverseBytes(s []byte) []byte {
	newBytes := make([]byte, len(s))
	copy(newBytes, s)
	slices.Reverse(newBytes)
	return newBytes
}

// Some parts of epoch transition handling make StoreAccounts calls directly to
// accountsDb before ProcessBlock. updateLoadedAccounts helps mirror those
// changes to the result of the account loader.
func UpdateLoadedAccounts(
	loadedAccts map[solana.PublicKey]*accounts.Account,
	storeAccounts []*accounts.Account,
) {
	if loadedAccts == nil {
		return
	}
	for _, a := range storeAccounts {
		if a == nil {
			continue
		}
		loadedAccts[a.Key] = a
	}
}
