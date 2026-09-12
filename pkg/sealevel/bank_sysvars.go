package sealevel

import (
	"bytes"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

// bankSysvarID is the stable index of a bank-owned sysvar. Instructions is not
// included: that sysvar is synthesized separately for each transaction.
type bankSysvarID uint8

const (
	bankSysvarClock bankSysvarID = iota
	bankSysvarRent
	bankSysvarEpochSchedule
	bankSysvarEpochRewards
	bankSysvarSlotHashes
	bankSysvarStakeHistory
	bankSysvarLastRestartSlot
	bankSysvarRecentBlockhashes
	bankSysvarSlotHistory
	bankSysvarFees
	bankSysvarCount
)

type bankSysvarMask uint16

// BankSysvars is an immutable, bank-owned view of all cached sysvars. It owns
// the account pointers in accounts and the decoded values below. Slice-bearing
// decoded values are immutable views; callers that update a sysvar must first
// copy it and publish a derived snapshot.
//
// A derived snapshot shallowly shares all unchanged accounts and decoded slice
// backing arrays with its parent. This is safe because there is no mutating API
// on BankSysvars and keeps per-slot work proportional to the sysvars that
// actually changed.
type BankSysvars struct {
	slot     uint64
	accounts [bankSysvarCount]*accounts.Account
	decoded  bankSysvarMask

	clock             SysvarClock
	rent              SysvarRent
	epochSchedule     SysvarEpochSchedule
	epochRewards      SysvarEpochRewards
	slotHashes        SysvarSlotHashes
	stakeHistory      SysvarStakeHistory
	lastRestartSlot   SysvarLastRestartSlot
	recentBlockhashes SysvarRecentBlockhashes
	slotHistory       SysvarSlotHistory
	fees              SysvarFees
}

// SysvarAccountLoader supplies an account template while converting the
// process-global legacy cache at bootstrap. found=false represents a sysvar
// that does not exist on this cluster (most commonly the deprecated Fees
// sysvar).
type SysvarAccountLoader func(solana.PublicKey) (acct *accounts.Account, found bool, err error)

func bankSysvarIDForAddress(pubkey solana.PublicKey) (bankSysvarID, bool) {
	switch [32]byte(pubkey) {
	case SysvarClockAddr:
		return bankSysvarClock, true
	case SysvarRentAddr:
		return bankSysvarRent, true
	case SysvarEpochScheduleAddr:
		return bankSysvarEpochSchedule, true
	case SysvarEpochRewardsAddr:
		return bankSysvarEpochRewards, true
	case SysvarSlotHashesAddr:
		return bankSysvarSlotHashes, true
	case SysvarStakeHistoryAddr:
		return bankSysvarStakeHistory, true
	case SysvarLastRestartSlotAddr:
		return bankSysvarLastRestartSlot, true
	case SysvarRecentBlockHashesAddr:
		return bankSysvarRecentBlockhashes, true
	case SysvarSlotHistoryAddr:
		return bankSysvarSlotHistory, true
	case SysvarFeesAddr:
		return bankSysvarFees, true
	default:
		return 0, false
	}
}

func bankSysvarAddress(id bankSysvarID) solana.PublicKey {
	switch id {
	case bankSysvarClock:
		return solana.PublicKey(SysvarClockAddr)
	case bankSysvarRent:
		return solana.PublicKey(SysvarRentAddr)
	case bankSysvarEpochSchedule:
		return solana.PublicKey(SysvarEpochScheduleAddr)
	case bankSysvarEpochRewards:
		return solana.PublicKey(SysvarEpochRewardsAddr)
	case bankSysvarSlotHashes:
		return solana.PublicKey(SysvarSlotHashesAddr)
	case bankSysvarStakeHistory:
		return solana.PublicKey(SysvarStakeHistoryAddr)
	case bankSysvarLastRestartSlot:
		return solana.PublicKey(SysvarLastRestartSlotAddr)
	case bankSysvarRecentBlockhashes:
		return solana.PublicKey(SysvarRecentBlockHashesAddr)
	case bankSysvarSlotHistory:
		return solana.PublicKey(SysvarSlotHistoryAddr)
	case bankSysvarFees:
		return solana.PublicKey(SysvarFeesAddr)
	default:
		return solana.PublicKey{}
	}
}

// IsBankSysvarAccount reports whether pubkey has a bank-owned cache entry.
func IsBankSysvarAccount(pubkey solana.PublicKey) bool {
	_, ok := bankSysvarIDForAddress(pubkey)
	return ok
}

// RangeBankSysvarAddresses visits the complete registry in stable order,
// including entries that are absent from a particular bank snapshot.
func RangeBankSysvarAddresses(fn func(solana.PublicKey) error) error {
	if fn == nil {
		return nil
	}
	for id := bankSysvarID(0); id < bankSysvarCount; id++ {
		if err := fn(bankSysvarAddress(id)); err != nil {
			return err
		}
	}
	return nil
}

// NewBankSysvars constructs a snapshot and defensively clones every input.
func NewBankSysvars(slot uint64, accts ...*accounts.Account) (*BankSysvars, error) {
	owned := make([]*accounts.Account, len(accts))
	for i, acct := range accts {
		if acct != nil {
			owned[i] = acct.Clone()
		}
	}
	return NewBankSysvarsOwned(slot, owned...)
}

// NewBankSysvarsOwned constructs a snapshot by adopting the supplied account
// pointers. The caller must pass fresh accounts and must not mutate them after
// this call.
func NewBankSysvarsOwned(slot uint64, accts ...*accounts.Account) (*BankSysvars, error) {
	snapshot := &BankSysvars{slot: slot}
	if err := snapshot.applyOwnedAccounts(accts...); err != nil {
		return nil, err
	}
	return snapshot, nil
}

// Derive creates a child-bank snapshot, cloning all updated accounts.
func (s *BankSysvars) Derive(slot uint64, updates ...*accounts.Account) (*BankSysvars, error) {
	owned := make([]*accounts.Account, len(updates))
	for i, acct := range updates {
		if acct != nil {
			owned[i] = acct.Clone()
		}
	}
	return s.DeriveOwned(slot, owned...)
}

// DeriveOwned creates a child-bank snapshot and adopts fresh updated account
// pointers. Unchanged entries remain shared with the parent snapshot.
func (s *BankSysvars) DeriveOwned(slot uint64, updates ...*accounts.Account) (*BankSysvars, error) {
	var next BankSysvars
	if s != nil {
		next = *s
	}
	next.slot = slot
	if err := next.applyOwnedAccounts(updates...); err != nil {
		return nil, err
	}
	return &next, nil
}

// WithAccounts returns a same-bank snapshot with cloned account updates.
func (s *BankSysvars) WithAccounts(updates ...*accounts.Account) (*BankSysvars, error) {
	return s.Derive(s.Slot(), updates...)
}

// WithOwnedAccounts returns a same-bank snapshot and adopts fresh account
// updates.
func (s *BankSysvars) WithOwnedAccounts(updates ...*accounts.Account) (*BankSysvars, error) {
	return s.DeriveOwned(s.Slot(), updates...)
}

// Without returns a snapshot in which the listed optional sysvars are absent.
func (s *BankSysvars) Without(pubkeys ...solana.PublicKey) *BankSysvars {
	var next BankSysvars
	if s != nil {
		next = *s
	}
	for _, pubkey := range pubkeys {
		if id, ok := bankSysvarIDForAddress(pubkey); ok {
			next.accounts[id] = nil
			next.decoded &^= 1 << id
			next.clearValue(id)
		}
	}
	return &next
}

func (s *BankSysvars) applyOwnedAccounts(accts ...*accounts.Account) error {
	for _, acct := range accts {
		if acct == nil {
			return fmt.Errorf("nil bank sysvar account")
		}
		id, ok := bankSysvarIDForAddress(acct.Key)
		if !ok {
			return fmt.Errorf("account %s is not a cached bank sysvar", acct.Key)
		}

		s.accounts[id] = nil
		s.decoded &^= 1 << id
		s.clearValue(id)
		// A zero-lamport account is a tombstone and therefore absent from the
		// runtime sysvar cache.
		if acct.Lamports == 0 {
			continue
		}
		if err := s.decodeValue(id, acct.Data); err != nil {
			return fmt.Errorf("decode bank sysvar %s: %w", acct.Key, err)
		}
		s.accounts[id] = acct
		s.decoded |= 1 << id
	}
	return nil
}

func (s *BankSysvars) clearValue(id bankSysvarID) {
	switch id {
	case bankSysvarClock:
		s.clock = SysvarClock{}
	case bankSysvarRent:
		s.rent = SysvarRent{}
	case bankSysvarEpochSchedule:
		s.epochSchedule = SysvarEpochSchedule{}
	case bankSysvarEpochRewards:
		s.epochRewards = SysvarEpochRewards{}
	case bankSysvarSlotHashes:
		s.slotHashes = nil
	case bankSysvarStakeHistory:
		s.stakeHistory = nil
	case bankSysvarLastRestartSlot:
		s.lastRestartSlot = SysvarLastRestartSlot{}
	case bankSysvarRecentBlockhashes:
		s.recentBlockhashes = nil
	case bankSysvarSlotHistory:
		s.slotHistory = SysvarSlotHistory{}
	case bankSysvarFees:
		s.fees = SysvarFees{}
	}
}

func (s *BankSysvars) decodeValue(id bankSysvarID, data []byte) error {
	decoder := bin.NewBinDecoder(data)
	switch id {
	case bankSysvarClock:
		return s.clock.UnmarshalWithDecoder(decoder)
	case bankSysvarRent:
		return s.rent.UnmarshalWithDecoder(decoder)
	case bankSysvarEpochSchedule:
		return s.epochSchedule.UnmarshalWithDecoder(decoder)
	case bankSysvarEpochRewards:
		return s.epochRewards.UnmarshalWithDecoder(decoder)
	case bankSysvarSlotHashes:
		return s.slotHashes.UnmarshalWithDecoder(decoder)
	case bankSysvarStakeHistory:
		return s.stakeHistory.UnmarshalWithDecoder(decoder)
	case bankSysvarLastRestartSlot:
		return s.lastRestartSlot.UnmarshalWithDecoder(decoder)
	case bankSysvarRecentBlockhashes:
		return s.recentBlockhashes.UnmarshalWithDecoder(decoder)
	case bankSysvarSlotHistory:
		return s.slotHistory.UnmarshalWithDecoder(decoder)
	case bankSysvarFees:
		return s.fees.UnmarshalWithDecoder(decoder)
	default:
		return fmt.Errorf("unknown bank sysvar id %d", id)
	}
}

// Slot is the bank slot represented by this snapshot.
func (s *BankSysvars) Slot() uint64 {
	if s == nil {
		return 0
	}
	return s.slot
}

func (s *BankSysvars) hasDecoded(id bankSysvarID) bool {
	return s != nil && s.decoded&(1<<id) != 0
}

// ValidateForExecution verifies the sysvars that every supported bank must
// carry before transaction execution begins. EpochRewards and Fees are
// intentionally optional because their accounts are feature/lifecycle
// dependent; the remaining entries are part of the current runtime contract.
//
// Constructors permit partial snapshots for bootstrap and focused tests. A
// production replay or leader bank must call this once at its publication
// boundary, keeping the hot typed readers free of repeated validation work.
func (s *BankSysvars) ValidateForExecution() error {
	if s == nil {
		return fmt.Errorf("bank sysvar snapshot is nil")
	}
	required := [...]bankSysvarID{
		bankSysvarClock,
		bankSysvarRent,
		bankSysvarEpochSchedule,
		bankSysvarSlotHashes,
		bankSysvarStakeHistory,
		bankSysvarLastRestartSlot,
		bankSysvarRecentBlockhashes,
		bankSysvarSlotHistory,
	}
	for _, id := range required {
		if s.accounts[id] == nil || !s.hasDecoded(id) {
			return fmt.Errorf("required bank sysvar %s is absent at slot %d", bankSysvarAddress(id), s.slot)
		}
	}
	return nil
}

// AccountView returns an immutable borrowed account. It is intended for
// installing shared, read-only entries into SlotCtx account maps; callers must
// copy before mutation.
func (s *BankSysvars) AccountView(pubkey solana.PublicKey) (*accounts.Account, bool) {
	if s == nil {
		return nil, false
	}
	id, ok := bankSysvarIDForAddress(pubkey)
	if !ok || s.accounts[id] == nil {
		return nil, false
	}
	return s.accounts[id], true
}

// CloneAccount returns a mutable copy of a cached sysvar account.
func (s *BankSysvars) CloneAccount(pubkey solana.PublicKey) (*accounts.Account, bool) {
	acct, ok := s.AccountView(pubkey)
	if !ok {
		return nil, false
	}
	return acct.Clone(), true
}

// RawView returns immutable account data without an allocation.
func (s *BankSysvars) RawView(pubkey solana.PublicKey) ([]byte, bool) {
	acct, ok := s.AccountView(pubkey)
	if !ok {
		return nil, false
	}
	return acct.Data, true
}

// RangeAccountViews visits every present sysvar in stable registry order. The
// account pointers are immutable borrowed views.
func (s *BankSysvars) RangeAccountViews(fn func(solana.PublicKey, *accounts.Account) error) error {
	if s == nil || fn == nil {
		return nil
	}
	for id := bankSysvarID(0); id < bankSysvarCount; id++ {
		if acct := s.accounts[id]; acct != nil {
			if err := fn(bankSysvarAddress(id), acct); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *BankSysvars) Clock() (SysvarClock, bool) {
	if s == nil {
		return SysvarClock{}, false
	}
	return s.clock, s.hasDecoded(bankSysvarClock)
}

func (s *BankSysvars) Rent() (SysvarRent, bool) {
	if s == nil {
		return SysvarRent{}, false
	}
	return s.rent, s.hasDecoded(bankSysvarRent)
}

func (s *BankSysvars) EpochSchedule() (SysvarEpochSchedule, bool) {
	if s == nil {
		return SysvarEpochSchedule{}, false
	}
	return s.epochSchedule, s.hasDecoded(bankSysvarEpochSchedule)
}

func (s *BankSysvars) EpochRewards() (SysvarEpochRewards, bool) {
	if s == nil {
		return SysvarEpochRewards{}, false
	}
	return s.epochRewards, s.hasDecoded(bankSysvarEpochRewards)
}

// SlotHashes returns an immutable slice view.
func (s *BankSysvars) SlotHashes() (SysvarSlotHashes, bool) {
	if s == nil {
		return nil, false
	}
	return s.slotHashes, s.hasDecoded(bankSysvarSlotHashes)
}

// StakeHistory returns an immutable slice view.
func (s *BankSysvars) StakeHistory() (SysvarStakeHistory, bool) {
	if s == nil {
		return nil, false
	}
	return s.stakeHistory, s.hasDecoded(bankSysvarStakeHistory)
}

func (s *BankSysvars) LastRestartSlot() (SysvarLastRestartSlot, bool) {
	if s == nil {
		return SysvarLastRestartSlot{}, false
	}
	return s.lastRestartSlot, s.hasDecoded(bankSysvarLastRestartSlot)
}

// RecentBlockhashes returns an immutable slice view.
func (s *BankSysvars) RecentBlockhashes() (SysvarRecentBlockhashes, bool) {
	if s == nil {
		return nil, false
	}
	return s.recentBlockhashes, s.hasDecoded(bankSysvarRecentBlockhashes)
}

// SlotHistory returns an immutable view. In particular, Bits.Bits.Blocks must
// not be modified by callers.
func (s *BankSysvars) SlotHistory() (SysvarSlotHistory, bool) {
	if s == nil {
		return SysvarSlotHistory{}, false
	}
	return s.slotHistory, s.hasDecoded(bankSysvarSlotHistory)
}

func (s *BankSysvars) Fees() (SysvarFees, bool) {
	if s == nil {
		return SysvarFees{}, false
	}
	return s.fees, s.hasDecoded(bankSysvarFees)
}

// SnapshotLegacySysvarCache converts the process-global cache into one
// immutable bank snapshot. It exists only for bootstrap/resume migration. A
// typed legacy value is authoritative over potentially stale durable bytes, so
// it is marshaled over a cloned account template before publication.
func SnapshotLegacySysvarCache(slot uint64, load SysvarAccountLoader) (*BankSysvars, error) {
	owned := make([]*accounts.Account, 0, bankSysvarCount)
	for id := bankSysvarID(0); id < bankSysvarCount; id++ {
		acct, value, hasValue := legacySysvarEntry(id)
		if acct != nil {
			acct = acct.Clone()
		} else if load != nil {
			loaded, found, err := load(bankSysvarAddress(id))
			if err != nil {
				return nil, err
			}
			if found && loaded != nil {
				acct = loaded.Clone()
			}
		}
		if acct == nil {
			if hasValue {
				return nil, fmt.Errorf("legacy sysvar %s has a decoded value but no account template", bankSysvarAddress(id))
			}
			continue
		}
		if hasValue {
			data, err := marshalLegacySysvarValue(id, value)
			if err != nil {
				return nil, err
			}
			acct.Data = data
		}
		owned = append(owned, acct)
	}
	return NewBankSysvarsOwned(slot, owned...)
}

func legacySysvarEntry(id bankSysvarID) (*accounts.Account, any, bool) {
	switch id {
	case bankSysvarClock:
		return SysvarCache.Clock.Acct, SysvarCache.Clock.Sysvar, SysvarCache.Clock.Sysvar != nil
	case bankSysvarRent:
		return SysvarCache.Rent.Acct, SysvarCache.Rent.Sysvar, SysvarCache.Rent.Sysvar != nil
	case bankSysvarEpochSchedule:
		return SysvarCache.EpochSchedule.Acct, SysvarCache.EpochSchedule.Sysvar, SysvarCache.EpochSchedule.Sysvar != nil
	case bankSysvarEpochRewards:
		return SysvarCache.EpochRewards.Acct, SysvarCache.EpochRewards.Sysvar, SysvarCache.EpochRewards.Sysvar != nil
	case bankSysvarSlotHashes:
		return SysvarCache.SlotHashes.Acct, SysvarCache.SlotHashes.Sysvar, SysvarCache.SlotHashes.Sysvar != nil
	case bankSysvarStakeHistory:
		return SysvarCache.StakeHistory.Acct, SysvarCache.StakeHistory.Sysvar, SysvarCache.StakeHistory.Sysvar != nil
	case bankSysvarLastRestartSlot:
		return SysvarCache.LastRestartSlot.Acct, SysvarCache.LastRestartSlot.Sysvar, SysvarCache.LastRestartSlot.Sysvar != nil
	case bankSysvarRecentBlockhashes:
		return SysvarCache.RecentBlockHashes.Acct, SysvarCache.RecentBlockHashes.Sysvar, SysvarCache.RecentBlockHashes.Sysvar != nil
	case bankSysvarSlotHistory:
		return SysvarCache.SlotHistory.Acct, SysvarCache.SlotHistory.Sysvar, SysvarCache.SlotHistory.Sysvar != nil
	case bankSysvarFees:
		return SysvarCache.Fees.Acct, SysvarCache.Fees.Sysvar, SysvarCache.Fees.Sysvar != nil
	default:
		return nil, nil, false
	}
}

func marshalLegacySysvarValue(id bankSysvarID, value any) ([]byte, error) {
	switch id {
	case bankSysvarClock:
		return value.(*SysvarClock).MustMarshal(), nil
	case bankSysvarRent:
		return value.(*SysvarRent).MustMarshal(), nil
	case bankSysvarEpochSchedule:
		v := value.(*SysvarEpochSchedule)
		var data bytes.Buffer
		enc := bin.NewBinEncoder(&data)
		if err := enc.WriteUint64(v.SlotsPerEpoch, bin.LE); err != nil {
			return nil, err
		}
		if err := enc.WriteUint64(v.LeaderScheduleSlotOffset, bin.LE); err != nil {
			return nil, err
		}
		if err := enc.WriteBool(v.Warmup); err != nil {
			return nil, err
		}
		if err := enc.WriteUint64(v.FirstNormalEpoch, bin.LE); err != nil {
			return nil, err
		}
		if err := enc.WriteUint64(v.FirstNormalSlot, bin.LE); err != nil {
			return nil, err
		}
		return data.Bytes(), nil
	case bankSysvarEpochRewards:
		var data bytes.Buffer
		err := value.(*SysvarEpochRewards).MarshalWithEncoder(bin.NewBinEncoder(&data))
		return data.Bytes(), err
	case bankSysvarSlotHashes:
		return value.(*SysvarSlotHashes).MustMarshal(), nil
	case bankSysvarStakeHistory:
		var data bytes.Buffer
		err := value.(*SysvarStakeHistory).MarshalWithEncoder(bin.NewBinEncoder(&data))
		return data.Bytes(), err
	case bankSysvarLastRestartSlot:
		var data bytes.Buffer
		err := bin.NewBinEncoder(&data).WriteUint64(value.(*SysvarLastRestartSlot).LastRestartSlot, bin.LE)
		return data.Bytes(), err
	case bankSysvarRecentBlockhashes:
		return value.(*SysvarRecentBlockhashes).MustMarshal(), nil
	case bankSysvarSlotHistory:
		return value.(*SysvarSlotHistory).MustMarshal(), nil
	case bankSysvarFees:
		var data bytes.Buffer
		err := bin.NewBinEncoder(&data).WriteUint64(value.(*SysvarFees).FeeCalculator.LamportsPerSignature, bin.LE)
		return data.Bytes(), err
	default:
		return nil, fmt.Errorf("unknown legacy bank sysvar id %d", id)
	}
}
