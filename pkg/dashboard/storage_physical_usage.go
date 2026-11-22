package dashboard

import "time"

// StoragePhysicalUsage is a historical bounded filesystem-block observation.
// It is not an atomic filesystem snapshot, logical live bytes or fragmentation.
type StoragePhysicalUsage struct {
	Scope               string    `json:"scope"`
	StartedAt           time.Time `json:"started_at"`
	CompletedAt         time.Time `json:"completed_at"`
	DurationNanoseconds uint64    `json:"duration_nanoseconds,string"`
	AllocatedBytes      uint64    `json:"allocated_bytes,string"`
	LogicalFileBytes    uint64    `json:"logical_file_bytes,string"`
	RegularFiles        uint64    `json:"regular_files,string"`
	Directories         uint64    `json:"directories,string"`
}

func validStoragePhysicalUsage(p *StoragePhysicalUsage, opened, captured time.Time) bool {
	if p == nil {
		return true
	}
	if p.Scope != "primary_db_tree_and_secondary_account_trees" || !validReplayDetailTime(p.StartedAt) || !validReplayDetailTime(p.CompletedAt) || p.StartedAt.Before(opened) || !p.CompletedAt.After(p.StartedAt) || p.CompletedAt.After(captured) || captured.Sub(p.CompletedAt) >= 75*time.Second || p.DurationNanoseconds == 0 || p.DurationNanoseconds > uint64(5*time.Second) || p.Directories == 0 || p.AllocatedBytes%512 != 0 || p.RegularFiles > 1_000_000 || p.Directories > 1_000_000-p.RegularFiles {
		return false
	}
	wall, elapsed := p.CompletedAt.Sub(p.StartedAt), time.Duration(p.DurationNanoseconds)
	return wall-elapsed <= 250*time.Millisecond && elapsed-wall <= 250*time.Millisecond
}
