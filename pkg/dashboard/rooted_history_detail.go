package dashboard

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

const (
	// MaxRootedHistoryBytes bounds the complete document, not existing status
	// or replay pages. The protocol bitmap encodes to 174,764 base64 characters.
	MaxRootedHistoryBytes = 192 << 10
	// RootedHistoryCapacity is the protocol bitmap's retained slot count.
	RootedHistoryCapacity = 1024 * 1024
	// RootedHistoryFreshFor bounds a node-side copy's monotonic lease.
	RootedHistoryFreshFor = 30 * time.Second
)

// RootedHistoryDetail preserves the exact SlotHistory of a folded bank.
// CapturedAt is the node's copy time; ObservedAt is the original fold-owner
// observation. Neither timestamp defines a cluster tip or an elapsed duration.
type RootedHistoryDetail struct {
	SchemaVersion int                 `json:"schema_version"`
	Family        string              `json:"family"`
	Scope         string              `json:"scope"`
	State         string              `json:"state"`
	Reason        string              `json:"reason"`
	Binding       ReplayDetailBinding `json:"binding"`
	CapturedAt    time.Time           `json:"captured_at"`
	ObservedAt    time.Time           `json:"observed_at"`
	BankSlot      uint64              `json:"bank_slot,string"`
	Bankhash      string              `json:"bankhash"`
	OldestSlot    uint64              `json:"oldest_slot,string"`
	NextSlot      uint64              `json:"next_slot,string"`
	Capacity      int                 `json:"capacity"`
	Encoding      string              `json:"encoding"`
	Bitmap        string              `json:"bitmap"`
	Epoch         *RootedHistoryEpoch `json:"epoch,omitempty"`
}

// RootedHistoryEpoch describes this bank's epoch, not wall-time progress or a
// current cluster epoch. History retention can cover only part of its prefix.
type RootedHistoryEpoch struct {
	Number    uint64 `json:"number,string"`
	FirstSlot uint64 `json:"first_slot,string"`
	Slots     uint64 `json:"slots,string"`
}

// RootedHistoryCapture retains an original monotonic copy deadline. It is
// separate from both displayed wall dates and the bank's persistent history.
type RootedHistoryCapture struct {
	Detail     RootedHistoryDetail
	FreshUntil time.Time
}

// RootedHistorySampler copies a bounded immutable replay observation. It must
// not scan accounts or wait for a root, and is never called by HTTP viewers.
type RootedHistorySampler func(time.Time) (RootedHistoryCapture, error)

func validateRootedHistoryDetail(d RootedHistoryDetail) error {
	if d.SchemaVersion != 1 || d.Family != "rooted_history" || d.Scope != "rooted_bank_slot_history" ||
		d.State != "fresh" || d.Reason != "" || !validReplayDetailBinding(d.Binding) ||
		!validReplayDetailTime(d.CapturedAt) || !validReplayDetailTime(d.ObservedAt) ||
		!validReplayDetailHash(d.Bankhash, true) || d.NextSlot == 0 || d.BankSlot != d.NextSlot-1 ||
		d.Capacity != RootedHistoryCapacity || d.Encoding != "base64_le_lsb0" ||
		len(d.Bitmap) != base64.StdEncoding.EncodedLen(RootedHistoryCapacity/8) || strings.ContainsAny(d.Bitmap, "\r\n") {
		return errors.New("source_invalid")
	}
	oldest := uint64(0)
	if d.NextSlot > RootedHistoryCapacity {
		oldest = d.NextSlot - RootedHistoryCapacity
	}
	if d.OldestSlot != oldest {
		return errors.New("source_invalid")
	}
	if e := d.Epoch; e != nil && (e.Slots == 0 || e.FirstSlot > d.BankSlot ||
		d.BankSlot-e.FirstSlot >= e.Slots || e.Slots > ^uint64(0)-e.FirstSlot ||
		e.Number > e.FirstSlot || (e.Number == 0) != (e.FirstSlot == 0)) {
		return errors.New("source_invalid")
	}
	bitmap, err := base64.StdEncoding.Strict().DecodeString(d.Bitmap)
	if err != nil || len(bitmap) != RootedHistoryCapacity/8 || bitmap[(d.BankSlot%RootedHistoryCapacity)/8]&(1<<(d.BankSlot%8)) == 0 {
		return errors.New("source_invalid")
	}
	// Binding generations establish lifetime. A backward wall-clock adjustment
	// must not turn an original observation date into fabricated missing slots.
	return nil
}

func sameRootedHistoryBank(a, b RootedHistoryDetail) bool {
	return a.Bankhash == b.Bankhash && a.Bitmap == b.Bitmap && a.ObservedAt.Equal(b.ObservedAt) &&
		(a.Epoch == nil && b.Epoch == nil || a.Epoch != nil && b.Epoch != nil && *a.Epoch == *b.Epoch)
}

func decodeRootedHistoryDetail(body []byte) (RootedHistoryDetail, error) {
	var d RootedHistoryDetail
	if len(body) > MaxRootedHistoryBytes || json.Unmarshal(body, &d) != nil || validateRootedHistoryDetail(d) != nil {
		return RootedHistoryDetail{}, errors.New("source_invalid")
	}
	canonical, err := json.Marshal(d)
	if err != nil || !bytes.Equal(body, canonical) {
		return RootedHistoryDetail{}, errors.New("source_invalid")
	}
	return d, nil
}
