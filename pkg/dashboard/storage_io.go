package dashboard

import "time"

// StorageIODetail separates process accounting from the shared mounted block
// layer. Paths and device topology are intentionally not part of this API.
type StorageIODetail struct {
	Scope      string          `json:"scope"`
	StartedAt  time.Time       `json:"started_at"`
	CapturedAt time.Time       `json:"captured_at"`
	Process    ProcessIODetail `json:"process"`
	Root       BlockIODetail   `json:"root"`
	Accounts   BlockIODetail   `json:"accounts"`
	Ledger     BlockIODetail   `json:"ledger"`
}

// ProcessIODetail distinguishes unsupported, unreadable and measured-zero data.
type ProcessIODetail struct {
	Supported  bool               `json:"supported"`
	StartedAt  time.Time          `json:"started_at"`
	CapturedAt time.Time          `json:"captured_at"`
	Counters   *ProcessIOCounts   `json:"counters"`
	Window     *ProcessIOInterval `json:"window"`
}

// ProcessIOCounts keeps cancelled-write accounting independent of write bytes.
// Character counts include cached/non-disk I/O; write bytes attribute dirtying,
// not completed storage. Linux includes waited-for children in these totals.
type ProcessIOCounts struct {
	ReadCharacters      uint64 `json:"read_characters,string"`
	WriteCharacters     uint64 `json:"write_characters,string"`
	ReadSyscalls        uint64 `json:"read_syscalls,string"`
	WriteSyscalls       uint64 `json:"write_syscalls,string"`
	ReadBytes           uint64 `json:"read_bytes,string"`
	WriteBytes          uint64 `json:"write_bytes,string"`
	CancelledWriteBytes uint64 `json:"cancelled_write_bytes,string"`
}

// ProcessIOInterval holds independent deltas, not a net-write or disk-rate sum.
type ProcessIOInterval struct {
	StartedAt           time.Time       `json:"started_at"`
	EndedAt             time.Time       `json:"ended_at"`
	DurationNanoseconds uint64          `json:"duration_nanoseconds,string"`
	Counters            ProcessIOCounts `json:"counters"`
}

// BlockIODetail describes the configured role's mounted block layer. Device
// numbers associate duplicate roles in this capture, not across device reuse.
// No rates or physical-drive health are inferred from them.
type BlockIODetail struct {
	Supported  bool           `json:"supported"`
	StartedAt  time.Time      `json:"started_at"`
	CapturedAt time.Time      `json:"captured_at"`
	Device     *BlockIODevice `json:"device"`
	Counters   *BlockIOCounts `json:"counters"`
}

type BlockIODevice struct {
	Major      uint32 `json:"major"`
	Minor      uint32 `json:"minor"`
	HasDiscard bool   `json:"has_discard"`
	HasFlush   bool   `json:"has_flush"`
}

// BlockIOCounts uses native 512-byte sectors and aggregate milliseconds.
// InFlight is a gauge; time sums can exceed wall time. These are shared-device
// values, never exclusive node writes. Duplicate role rows must not be summed.
type BlockIOCounts struct {
	ReadsCompleted       uint64 `json:"reads_completed,string"`
	ReadsMerged          uint64 `json:"reads_merged,string"`
	ReadSectors          uint64 `json:"read_sectors,string"`
	ReadMilliseconds     uint64 `json:"read_milliseconds,string"`
	WritesCompleted      uint64 `json:"writes_completed,string"`
	WritesMerged         uint64 `json:"writes_merged,string"`
	WriteSectors         uint64 `json:"write_sectors,string"`
	WriteMilliseconds    uint64 `json:"write_milliseconds,string"`
	InFlight             uint64 `json:"in_flight,string"`
	BusyMilliseconds     uint64 `json:"busy_milliseconds,string"`
	WeightedMilliseconds uint64 `json:"weighted_milliseconds,string"`
	DiscardsCompleted    uint64 `json:"discards_completed,string"`
	DiscardsMerged       uint64 `json:"discards_merged,string"`
	DiscardSectors       uint64 `json:"discard_sectors,string"`
	DiscardMilliseconds  uint64 `json:"discard_milliseconds,string"`
	FlushesCompleted     uint64 `json:"flushes_completed,string"`
	FlushMilliseconds    uint64 `json:"flush_milliseconds,string"`
}

func validStorageIO(d *StorageIODetail, capturedAt time.Time) bool {
	if d == nil {
		return true
	}
	if d.Scope != "linux_process_and_mounted_block_layer" || !validReplayDetailTime(d.StartedAt) ||
		!validReplayDetailTime(d.CapturedAt) || d.CapturedAt.Before(d.StartedAt) ||
		d.CapturedAt.Sub(d.StartedAt) > 5*time.Second || d.CapturedAt.After(capturedAt.Add(time.Second)) ||
		capturedAt.Sub(d.StartedAt) >= StorageDetailFreshFor {
		return false
	}
	validCapture := func(supported bool, start, end time.Time) bool {
		if !supported {
			return start.IsZero() && end.IsZero()
		}
		return validReplayDetailTime(start) && validReplayDetailTime(end) && !start.Before(d.StartedAt) &&
			!end.Before(start) && !end.After(d.CapturedAt)
	}
	p := d.Process
	if !validCapture(p.Supported, p.StartedAt, p.CapturedAt) || !p.Supported && (p.Counters != nil || p.Window != nil) ||
		p.Counters == nil && p.Window != nil || p.Counters != nil && p.CapturedAt.Sub(p.StartedAt) > time.Second {
		return false
	}
	if w := p.Window; w != nil {
		if !validReplayDetailTime(w.StartedAt) || !validReplayDetailTime(w.EndedAt) ||
			!w.EndedAt.Equal(p.CapturedAt) || w.DurationNanoseconds == 0 || w.DurationNanoseconds > uint64(30*time.Second) ||
			!processIODeltaWithin(*p.Counters, w.Counters) {
			return false
		}
	}
	for _, b := range [3]BlockIODetail{d.Root, d.Accounts, d.Ledger} {
		if !validCapture(b.Supported, b.StartedAt, b.CapturedAt) || (b.Device == nil) != (b.Counters == nil) ||
			!b.Supported && b.Device != nil {
			return false
		}
		if b.Device != nil {
			c := b.Counters
			if b.CapturedAt.Sub(b.StartedAt) > time.Second || b.Device.Major == 0 || !b.Device.HasDiscard && (c.DiscardsCompleted != 0 || c.DiscardsMerged != 0 || c.DiscardSectors != 0 || c.DiscardMilliseconds != 0) ||
				!b.Device.HasFlush && (c.FlushesCompleted != 0 || c.FlushMilliseconds != 0) {
				return false
			}
		}
	}
	return true
}

func processIODeltaWithin(total, delta ProcessIOCounts) bool {
	return delta.ReadCharacters <= total.ReadCharacters && delta.WriteCharacters <= total.WriteCharacters &&
		delta.ReadSyscalls <= total.ReadSyscalls && delta.WriteSyscalls <= total.WriteSyscalls &&
		delta.ReadBytes <= total.ReadBytes && delta.WriteBytes <= total.WriteBytes &&
		delta.CancelledWriteBytes <= total.CancelledWriteBytes
}
