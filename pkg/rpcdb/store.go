// Package rpcdb stores the personal RPC indexes: per-address transaction
// history, transaction records, token accounts owned by watched wallets, and
// the watch registry.
//
// Only finalized data is written. Every key is derived from the data itself
// (address, slot, position, signature), so writing a finalized slot again
// after a crash produces the same database: recovery is idempotent.
//
// Integers are big-endian so key order is numeric order. Slot and position
// are stored inverted in address-history keys so a forward scan is newest
// first.
package rpcdb

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"github.com/gagliardetto/solana-go"
)

const (
	prefixAddress     = 'a' // a | addr32 | ^slot u64 | ^pos u32 -> sig64 | flags
	prefixTransaction = 't' // t | sig64 -> encoded TxRecord
	prefixOwner       = 'o' // o | owner32 | program u8 | mint32 | account32 -> empty
	prefixOwnerByAcct = 'r' // r | account32 -> owner32 | program u8 | mint32
	prefixWatch       = 'w' // w | tenant u32 | addr32 -> kind u8 | since u64
	prefixEnumerated  = 'e' // e | owner32 -> watermark u64 when the owner's token accounts were enumerated
	prefixMeta        = 'm' // m | name
)

var (
	metaGenesis   = []byte("mgenesis")
	metaWatermark = []byte("mwatermark")
	metaVersion   = []byte("mversion")
)

const formatVersion = 1

// ErrGenesisMismatch means the database was written for another cluster.
var ErrGenesisMismatch = errors.New("rpcdb belongs to a different genesis")

// Store is a Pebble database of personal RPC indexes.
type Store struct {
	db *pebble.DB
}

// Open opens or creates the store at dir and binds it to genesis.
func Open(dir string, genesis solana.Hash) (*Store, error) {
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("open rpcdb: %w", err)
	}
	s := &Store{db: db}
	if err := s.checkIdentity(genesis); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) checkIdentity(genesis solana.Hash) error {
	stored, ok, err := s.get(metaGenesis)
	if err != nil {
		return err
	}
	if ok {
		if string(stored) != string(genesis[:]) {
			return ErrGenesisMismatch
		}
		version, _, err := s.get(metaVersion)
		if err != nil {
			return err
		}
		if len(version) != 1 || version[0] != formatVersion {
			return fmt.Errorf("rpcdb format %v is not supported (want %d)", version, formatVersion)
		}
		return nil
	}
	b := s.db.NewBatch()
	_ = b.Set(metaGenesis, genesis[:], nil)
	_ = b.Set(metaVersion, []byte{formatVersion}, nil)
	return b.Commit(pebble.Sync)
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

func (s *Store) get(key []byte) ([]byte, bool, error) {
	v, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	out := append([]byte(nil), v...)
	return out, true, closer.Close()
}

// Watermark is the highest finalized slot whose records are durable.
func (s *Store) Watermark() (uint64, error) {
	v, ok, err := s.get(metaWatermark)
	if err != nil || !ok {
		return 0, err
	}
	if len(v) != 8 {
		return 0, fmt.Errorf("corrupt watermark")
	}
	return binary.BigEndian.Uint64(v), nil
}

// Flags describe one transaction for an address entry.
type Flags uint8

const (
	// Failed marks a transaction that landed with an error.
	Failed Flags = 1 << iota
	// Partial marks a record indexed from block results only, without
	// captured balances or logs.
	Partial
)

// TxRecord is one captured transaction.
type TxRecord struct {
	Signature solana.Signature
	Slot      uint64
	Position  uint32 // index of the transaction within its block
	BlockTime int64
	BlockID   solana.Hash
	Flags     Flags
	// Addresses are the watched addresses this transaction is indexed under.
	Addresses []solana.PublicKey
	// Wire is the transaction as received from shreds.
	Wire []byte
	// Meta is the encoded transaction status metadata; empty when Partial.
	Meta []byte
}

// TokenAccount is one token account owned by a watched wallet.
type TokenAccount struct {
	Owner   solana.PublicKey
	Program uint8 // 0 SPL Token, 1 Token-2022
	Mint    solana.PublicKey
	Account solana.PublicKey
}

// Batch collects writes for one or more finalized slots.
type Batch struct {
	b *pebble.Batch
}

// NewBatch starts a write batch.
func (s *Store) NewBatch() *Batch { return &Batch{b: s.db.NewBatch()} }

// PutTransaction indexes rec under each of its addresses.
func (b *Batch) PutTransaction(rec *TxRecord) error {
	value := encodeTxRecord(rec)
	if err := b.b.Set(txKey(&rec.Signature), value, nil); err != nil {
		return err
	}
	entry := make([]byte, 65)
	copy(entry, rec.Signature[:])
	entry[64] = byte(rec.Flags)
	for i := range rec.Addresses {
		if err := b.b.Set(addressKey(&rec.Addresses[i], rec.Slot, rec.Position), entry, nil); err != nil {
			return err
		}
	}
	return nil
}

// PutTokenAccount records a token account owned by a watched wallet.
func (b *Batch) PutTokenAccount(ta TokenAccount) error {
	if err := b.b.Set(ownerKey(ta), nil, nil); err != nil {
		return err
	}
	v := make([]byte, 65)
	copy(v[0:32], ta.Owner[:])
	v[32] = ta.Program
	copy(v[33:65], ta.Mint[:])
	return b.b.Set(ownerByAccountKey(&ta.Account), v, nil)
}

// PutTokenAccountCandidate records that owner held account in some stored
// version, without changing the account's current-owner entry. Enumeration
// stores every version it finds this way, a superset that readers check
// against the bank they read.
func (b *Batch) PutTokenAccountCandidate(ta TokenAccount) error {
	return b.b.Set(ownerKey(ta), nil, nil)
}

// DeleteTokenAccount removes a token account that was closed or reassigned.
func (b *Batch) DeleteTokenAccount(ta TokenAccount) error {
	if err := b.b.Delete(ownerKey(ta), nil); err != nil {
		return err
	}
	return b.b.Delete(ownerByAccountKey(&ta.Account), nil)
}

func ownerByAccountKey(acct *solana.PublicKey) []byte {
	k := make([]byte, 33)
	k[0] = prefixOwnerByAcct
	copy(k[1:], acct[:])
	return k
}

// TokenAccountByAccount returns the recorded owner row for a token account.
func (s *Store) TokenAccountByAccount(acct solana.PublicKey) (TokenAccount, bool, error) {
	v, ok, err := s.get(ownerByAccountKey(&acct))
	if err != nil || !ok {
		return TokenAccount{}, ok, err
	}
	if len(v) != 65 {
		return TokenAccount{}, false, fmt.Errorf("corrupt owner-by-account entry")
	}
	ta := TokenAccount{Program: v[32], Account: acct}
	copy(ta.Owner[:], v[0:32])
	copy(ta.Mint[:], v[33:65])
	return ta, true, nil
}

// SetWatermark records that every finalized slot up to slot is durable.
func (b *Batch) SetWatermark(slot uint64) error {
	var v [8]byte
	binary.BigEndian.PutUint64(v[:], slot)
	return b.b.Set(metaWatermark, v[:], nil)
}

// PutWatch persists a watch registration.
func (b *Batch) PutWatch(tenant uint32, addr solana.PublicKey, kind uint8, since uint64) error {
	v := make([]byte, 9)
	v[0] = kind
	binary.BigEndian.PutUint64(v[1:], since)
	return b.b.Set(watchKey(tenant, &addr), v, nil)
}

// DeleteWatch removes a watch registration.
func (b *Batch) DeleteWatch(tenant uint32, addr solana.PublicKey) error {
	return b.b.Delete(watchKey(tenant, &addr), nil)
}

// PutEnumerated records that owner's token accounts were enumerated from the
// account store; watermark is the store's watermark when the scan began.
func (b *Batch) PutEnumerated(owner solana.PublicKey, watermark uint64) error {
	var v [8]byte
	binary.BigEndian.PutUint64(v[:], watermark)
	return b.b.Set(enumeratedKey(&owner), v[:], nil)
}

// DeleteEnumerated forgets that owner was enumerated, so it is scanned again.
func (b *Batch) DeleteEnumerated(owner solana.PublicKey) error {
	return b.b.Delete(enumeratedKey(&owner), nil)
}

func enumeratedKey(owner *solana.PublicKey) []byte {
	k := make([]byte, 33)
	k[0] = prefixEnumerated
	copy(k[1:], owner[:])
	return k
}

// Enumerated returns the owners whose token accounts were enumerated.
func (s *Store) Enumerated() (map[solana.PublicKey]uint64, error) {
	lower := []byte{prefixEnumerated}
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: keyUpperBound(lower)})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	out := map[solana.PublicKey]uint64{}
	for ok := iter.First(); ok; ok = iter.Next() {
		k, v := iter.Key(), iter.Value()
		if len(k) != 33 || len(v) != 8 {
			return nil, fmt.Errorf("corrupt enumeration entry")
		}
		out[solana.PublicKey(k[1:33])] = binary.BigEndian.Uint64(v)
	}
	return out, iter.Error()
}

// Commit writes the batch durably.
func (b *Batch) Commit() error { return b.b.Commit(pebble.Sync) }

func txKey(sig *solana.Signature) []byte {
	k := make([]byte, 65)
	k[0] = prefixTransaction
	copy(k[1:], sig[:])
	return k
}

func addressKey(addr *solana.PublicKey, slot uint64, pos uint32) []byte {
	k := make([]byte, 45)
	k[0] = prefixAddress
	copy(k[1:33], addr[:])
	binary.BigEndian.PutUint64(k[33:41], ^slot)
	binary.BigEndian.PutUint32(k[41:45], ^pos)
	return k
}

func ownerKey(ta TokenAccount) []byte {
	k := make([]byte, 98)
	k[0] = prefixOwner
	copy(k[1:33], ta.Owner[:])
	k[33] = ta.Program
	copy(k[34:66], ta.Mint[:])
	copy(k[66:98], ta.Account[:])
	return k
}

func watchKey(tenant uint32, addr *solana.PublicKey) []byte {
	k := make([]byte, 37)
	k[0] = prefixWatch
	binary.BigEndian.PutUint32(k[1:5], tenant)
	copy(k[5:37], addr[:])
	return k
}

// Transaction returns the record for sig.
func (s *Store) Transaction(sig solana.Signature) (*TxRecord, bool, error) {
	v, ok, err := s.get(txKey(&sig))
	if err != nil || !ok {
		return nil, ok, err
	}
	rec, err := decodeTxRecord(v)
	if err != nil {
		return nil, false, err
	}
	rec.Signature = sig
	return rec, true, nil
}

// SignatureEntry is one row of an address's history.
type SignatureEntry struct {
	Signature solana.Signature
	Slot      uint64
	Position  uint32
	Flags     Flags
}

// SignaturesForAddress returns addr's history newest first. before and until
// are exclusive bounds given as signatures, as in getSignaturesForAddress; a
// bound that is not in the store is an error.
func (s *Store) SignaturesForAddress(addr solana.PublicKey, before, until *solana.Signature, limit int) ([]SignatureEntry, error) {
	lower := addressKey(&addr, ^uint64(0), ^uint32(0)) // newest possible
	upper := make([]byte, 33)
	upper[0] = prefixAddress
	copy(upper[1:], addr[:])
	upper = keyUpperBound(upper)
	skipFirst := false
	if before != nil {
		rec, ok, err := s.Transaction(*before)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("before signature %s is not indexed", before)
		}
		lower = addressKey(&addr, rec.Slot, rec.Position)
		skipFirst = true
	}
	var untilKey []byte
	if until != nil {
		rec, ok, err := s.Transaction(*until)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("until signature %s is not indexed", until)
		}
		untilKey = addressKey(&addr, rec.Slot, rec.Position)
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []SignatureEntry
	for ok := iter.First(); ok && len(out) < limit; ok = iter.Next() {
		key := iter.Key()
		if skipFirst && string(key) == string(lower) {
			continue
		}
		if untilKey != nil && string(key) >= string(untilKey) {
			break
		}
		v := iter.Value()
		if len(key) != 45 || len(v) != 65 {
			return nil, fmt.Errorf("corrupt address entry")
		}
		var e SignatureEntry
		copy(e.Signature[:], v[:64])
		e.Flags = Flags(v[64])
		e.Slot = ^binary.BigEndian.Uint64(key[33:41])
		e.Position = ^binary.BigEndian.Uint32(key[41:45])
		out = append(out, e)
	}
	return out, iter.Error()
}

// TokenAccounts returns the token accounts recorded for owner.
func (s *Store) TokenAccounts(owner solana.PublicKey) ([]TokenAccount, error) {
	lower := make([]byte, 33)
	lower[0] = prefixOwner
	copy(lower[1:], owner[:])
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: keyUpperBound(lower)})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []TokenAccount
	for ok := iter.First(); ok; ok = iter.Next() {
		k := iter.Key()
		if len(k) != 98 {
			return nil, fmt.Errorf("corrupt owner entry")
		}
		var ta TokenAccount
		copy(ta.Owner[:], k[1:33])
		ta.Program = k[33]
		copy(ta.Mint[:], k[34:66])
		copy(ta.Account[:], k[66:98])
		out = append(out, ta)
	}
	return out, iter.Error()
}

// Watch is one persisted watch registration.
type Watch struct {
	Tenant uint32
	Addr   solana.PublicKey
	Kind   uint8
	Since  uint64
}

// Watches returns every persisted registration.
func (s *Store) Watches() ([]Watch, error) {
	lower := []byte{prefixWatch}
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: keyUpperBound(lower)})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []Watch
	for ok := iter.First(); ok; ok = iter.Next() {
		k, v := iter.Key(), iter.Value()
		if len(k) != 37 || len(v) != 9 {
			return nil, fmt.Errorf("corrupt watch entry")
		}
		w := Watch{Tenant: binary.BigEndian.Uint32(k[1:5]), Kind: v[0], Since: binary.BigEndian.Uint64(v[1:])}
		copy(w.Addr[:], k[5:37])
		out = append(out, w)
	}
	return out, iter.Error()
}

// keyUpperBound returns the smallest key greater than every key with prefix.
func keyUpperBound(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		end[i]++
		if end[i] != 0 {
			return end[:i+1]
		}
	}
	return nil
}

// Record encoding, version 1:
// version u8 | slot u64 | pos u32 | blockTime i64 | blockID 32 | flags u8 |
// nAddr u16 | addrs 32*n | wireLen u32 | wire | metaLen u32 | meta
const recordVersion = 1

func encodeTxRecord(rec *TxRecord) []byte {
	n := 1 + 8 + 4 + 8 + 32 + 1 + 2 + 32*len(rec.Addresses) + 4 + len(rec.Wire) + 4 + len(rec.Meta)
	out := make([]byte, 0, n)
	out = append(out, recordVersion)
	out = binary.BigEndian.AppendUint64(out, rec.Slot)
	out = binary.BigEndian.AppendUint32(out, rec.Position)
	out = binary.BigEndian.AppendUint64(out, uint64(rec.BlockTime))
	out = append(out, rec.BlockID[:]...)
	out = append(out, byte(rec.Flags))
	out = binary.BigEndian.AppendUint16(out, uint16(len(rec.Addresses)))
	for i := range rec.Addresses {
		out = append(out, rec.Addresses[i][:]...)
	}
	out = binary.BigEndian.AppendUint32(out, uint32(len(rec.Wire)))
	out = append(out, rec.Wire...)
	out = binary.BigEndian.AppendUint32(out, uint32(len(rec.Meta)))
	return append(out, rec.Meta...)
}

func decodeTxRecord(v []byte) (*TxRecord, error) {
	bad := fmt.Errorf("corrupt transaction record")
	if len(v) < 60 || v[0] != recordVersion {
		return nil, bad
	}
	rec := &TxRecord{}
	p := 1
	rec.Slot = binary.BigEndian.Uint64(v[p:])
	p += 8
	rec.Position = binary.BigEndian.Uint32(v[p:])
	p += 4
	rec.BlockTime = int64(binary.BigEndian.Uint64(v[p:]))
	p += 8
	copy(rec.BlockID[:], v[p:p+32])
	p += 32
	rec.Flags = Flags(v[p])
	p++
	nAddr := int(binary.BigEndian.Uint16(v[p:]))
	p += 2
	if len(v) < p+32*nAddr+4 {
		return nil, bad
	}
	rec.Addresses = make([]solana.PublicKey, nAddr)
	for i := range rec.Addresses {
		copy(rec.Addresses[i][:], v[p:p+32])
		p += 32
	}
	read := func() ([]byte, bool) {
		if len(v) < p+4 {
			return nil, false
		}
		n := int(binary.BigEndian.Uint32(v[p:]))
		p += 4
		if len(v) < p+n {
			return nil, false
		}
		out := append([]byte(nil), v[p:p+n]...)
		p += n
		return out, true
	}
	var ok bool
	if rec.Wire, ok = read(); !ok {
		return nil, bad
	}
	if rec.Meta, ok = read(); !ok {
		return nil, bad
	}
	if p != len(v) {
		return nil, bad
	}
	return rec, nil
}
