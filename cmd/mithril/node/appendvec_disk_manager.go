package node

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
)

const (
	defaultAppendVecCompactionIntervalSeconds = 30
	defaultEmergencyMinDeadFraction           = 0.20
	appendVecDiskMiB                          = uint64(1 << 20)
	appendVecDiskGiB                          = uint64(1 << 30)
)

type appendVecDiskProbe func(string) (accountsdb.AppendVecFilesystemSpace, error)

type appendVecCompactionRunner func(context.Context, accountsdb.CompactionConfig) (accountsdb.CompactStats, error)

type appendVecDiskPolicyConfig struct {
	AccountsPath             string
	CompactionEnabled        bool
	ReserveEnforced          bool
	MinFreeBytes             uint64 // zero selects an adaptive filesystem-size default
	TargetFreeBytes          uint64 // zero selects an adaptive filesystem-size default
	EmergencyMinDeadFraction float64
	NormalCompaction         accountsdb.CompactionConfig
}

// appendVecDiskManager serializes pressure compaction and fold disk admission.
// A successful admission retains cycleMu until CommitBatch returns, so two
// concurrent callers cannot both spend the same free-space reserve.
type appendVecDiskManager struct {
	ctx    context.Context
	cancel context.CancelFunc

	accountsPath      string
	compactionEnabled bool
	reserveEnforced   bool
	minFreeBytes      uint64
	targetFreeBytes   uint64
	scratchFreeBytes  uint64
	normalConfig      accountsdb.CompactionConfig
	emergencyConfig   accountsdb.CompactionConfig
	probe             appendVecDiskProbe
	compact           appendVecCompactionRunner

	cycleMu sync.Mutex
	fatalMu sync.Mutex
	fatal   error
}

func newAppendVecDiskManager(
	ctx context.Context,
	cancel context.CancelFunc,
	config appendVecDiskPolicyConfig,
	probe appendVecDiskProbe,
	compact appendVecCompactionRunner,
) (*appendVecDiskManager, error) {
	if ctx == nil {
		return nil, errors.New("appendvec disk manager: nil context")
	}
	if cancel == nil {
		return nil, errors.New("appendvec disk manager: nil cancellation callback")
	}
	if config.AccountsPath == "" {
		return nil, errors.New("appendvec disk manager: empty AccountsDB path")
	}
	if probe == nil {
		probe = accountsdb.ReadAppendVecFilesystemSpace
	}
	if compact == nil {
		return nil, errors.New("appendvec disk manager: nil compaction callback")
	}
	if config.EmergencyMinDeadFraction <= 0 || config.EmergencyMinDeadFraction > 1 {
		return nil, fmt.Errorf(
			"appendvec disk manager: emergency minimum dead fraction %v is outside (0,1]",
			config.EmergencyMinDeadFraction,
		)
	}
	space, err := probe(config.AccountsPath)
	if err != nil {
		return nil, fmt.Errorf("appendvec disk manager: initial filesystem probe: %w", err)
	}
	minFree, targetFree, scratchFree, err := resolveAppendVecDiskThresholds(
		space.TotalBytes,
		config.MinFreeBytes,
		config.TargetFreeBytes,
	)
	if err != nil {
		return nil, err
	}
	normal := config.NormalCompaction
	normal.NonBlocking = true
	normal.MinOutputFreeBytes = scratchFree
	normal.ContinueOnOutputSpacePressure = true
	emergency := config.NormalCompaction
	emergency.NonBlocking = false
	emergency.MaxSourceBytes = 0
	emergency.MinOutputFreeBytes = scratchFree
	emergency.ContinueOnOutputSpacePressure = true
	emergency.MinDeadFraction = config.EmergencyMinDeadFraction
	return &appendVecDiskManager{
		ctx:               ctx,
		cancel:            cancel,
		accountsPath:      config.AccountsPath,
		compactionEnabled: config.CompactionEnabled,
		reserveEnforced:   config.ReserveEnforced,
		minFreeBytes:      minFree,
		targetFreeBytes:   targetFree,
		scratchFreeBytes:  scratchFree,
		normalConfig:      normal,
		emergencyConfig:   emergency,
		probe:             probe,
		compact:           compact,
	}, nil
}

func resolveAppendVecDiskThresholds(total, configuredMin, configuredTarget uint64) (uint64, uint64, uint64, error) {
	if total == 0 {
		return 0, 0, 0, errors.New("appendvec disk manager: filesystem reported zero capacity")
	}
	minFree := configuredMin
	if minFree == 0 {
		// Reserve 5% on large filesystems, rising toward one eighth on small
		// development volumes, with a 16 GiB absolute contribution ceiling.
		minFree = max(total/20, min(16*appendVecDiskGiB, total/8))
	}
	targetFree := configuredTarget
	if targetFree == 0 {
		// Begin low-priority reclamation well before admission reaches the hard
		// reserve. This is 10% on large volumes and up to one quarter on small
		// volumes, with a 32 GiB absolute contribution ceiling.
		targetFree = max(total/10, min(32*appendVecDiskGiB, total/4))
	}
	if minFree == 0 || minFree >= total {
		return 0, 0, 0, fmt.Errorf(
			"appendvec disk manager: minimum free bytes %d must be in [1,%d)",
			minFree,
			total,
		)
	}
	if targetFree <= minFree || targetFree >= total {
		return 0, 0, 0, fmt.Errorf(
			"appendvec disk manager: target free bytes %d must be greater than minimum %d and below filesystem capacity %d",
			targetFree,
			minFree,
			total,
		)
	}
	scratchFree := max(uint64(64<<20), total/100)
	scratchFree = min(scratchFree, appendVecDiskGiB)
	// Keep emergency compaction possible below the fold reserve while retaining
	// at least half the reserve for metadata and unrelated filesystem writers.
	scratchFree = min(scratchFree, minFree/2)
	if scratchFree == 0 {
		scratchFree = 1
	}
	return minFree, targetFree, scratchFree, nil
}

func (manager *appendVecDiskManager) MinFreeBytes() uint64    { return manager.minFreeBytes }
func (manager *appendVecDiskManager) TargetFreeBytes() uint64 { return manager.targetFreeBytes }

// AdmitFold is installed as AccountsDb's first-step disk admission. When the
// prospective fold would consume the hard reserve it performs synchronous
// emergency compaction. This is deliberate backpressure at disk pressure, not
// part of the normal replay path.
func (manager *appendVecDiskManager) AdmitFold(requiredBytes uint64) (func(), error) {
	if manager == nil {
		return nil, errors.New("appendvec disk manager: nil fold admission")
	}
	if err := manager.Err(); err != nil {
		return nil, err
	}
	if !manager.reserveEnforced {
		return func() {}, nil
	}
	manager.cycleMu.Lock()
	releaseOnce := sync.Once{}
	release := func() { releaseOnce.Do(manager.cycleMu.Unlock) }
	if err := manager.ensureFreeLocked(requiredBytes); err != nil {
		release()
		return nil, err
	}
	return release, nil
}

func (manager *appendVecDiskManager) ensureFreeLocked(requiredBytes uint64) error {
	requiredAvailable := saturatingAddDiskBytes(manager.minFreeBytes, requiredBytes)
	passCandidates := 0
	passScanned := 0
	for {
		if err := manager.ctx.Err(); err != nil {
			return err
		}
		space, err := manager.probe(manager.accountsPath)
		if err != nil {
			return fmt.Errorf("appendvec disk manager: admission filesystem probe: %w", err)
		}
		if space.AvailableBytes >= requiredAvailable {
			return nil
		}
		pressure := &accountsdb.AppendVecDiskPressureError{
			Operation:      "fold admission reserve",
			AvailableBytes: space.AvailableBytes,
			RequiredBytes:  requiredAvailable,
		}
		if !manager.compactionEnabled {
			return pressure
		}

		stats, compactErr := manager.compact(manager.ctx, manager.emergencyConfig)
		if compactErr != nil {
			return fmt.Errorf("appendvec disk manager: emergency compaction: %w", compactErr)
		}
		if stats.BytesReclaimed > 0 {
			// The candidate set changed. Recheck capacity and begin a fresh pass if
			// additional reclamation is still required.
			passCandidates = 0
			passScanned = 0
			continue
		}
		if passCandidates == 0 {
			passCandidates = stats.EligibleCandidates
		}
		passScanned += stats.CandidatesScanned
		if stats.PassComplete || stats.CandidatesScanned == 0 || passCandidates == 0 || passScanned >= passCandidates {
			return pressure
		}
	}
}

// RunNormalCycle performs at most one soft-budget cycle when free space is
// below the target. Both mutexes use TryLock semantics: healthy-background
// maintenance never waits ahead of a fold or another pressure cycle.
func (manager *appendVecDiskManager) RunNormalCycle(ctx context.Context) (accountsdb.CompactStats, error) {
	stats := accountsdb.CompactStats{}
	if manager == nil {
		return stats, errors.New("appendvec disk manager: nil normal cycle")
	}
	if !manager.compactionEnabled {
		return stats, nil
	}
	if err := manager.Err(); err != nil {
		return stats, err
	}
	if ctx == nil {
		return stats, errors.New("appendvec disk manager: nil normal-cycle context")
	}
	space, err := manager.probe(manager.accountsPath)
	if err != nil {
		return stats, fmt.Errorf("appendvec disk manager: normal filesystem probe: %w", err)
	}
	if space.AvailableBytes >= manager.targetFreeBytes {
		return stats, nil
	}
	if !manager.cycleMu.TryLock() {
		return stats, nil
	}
	defer manager.cycleMu.Unlock()
	// Admission may have restored space before this cycle acquired the gate.
	space, err = manager.probe(manager.accountsPath)
	if err != nil {
		return stats, fmt.Errorf("appendvec disk manager: locked normal filesystem probe: %w", err)
	}
	if space.AvailableBytes >= manager.targetFreeBytes {
		return stats, nil
	}
	stats, err = manager.compact(ctx, manager.normalConfig)
	if errors.Is(err, accountsdb.ErrCompactionBusy) {
		return accountsdb.CompactStats{}, nil
	}
	if errors.Is(err, accountsdb.ErrAppendVecDiskPressure) && stats.OutputSpaceDeferrals > 0 {
		// Routine maintenance is advisory: a source that cannot preserve scratch
		// headroom is deferred and the pre-fold hard admission remains the
		// authority for synchronous reclamation or a fail-closed halt.
		return stats, nil
	}
	if err != nil {
		return stats, fmt.Errorf("appendvec disk manager: normal compaction: %w", err)
	}
	return stats, nil
}

func (manager *appendVecDiskManager) Fail(err error) {
	if manager == nil || err == nil || errors.Is(err, context.Canceled) {
		return
	}
	manager.fatalMu.Lock()
	if manager.fatal == nil {
		manager.fatal = err
		manager.cancel()
	}
	manager.fatalMu.Unlock()
}

func (manager *appendVecDiskManager) Err() error {
	if manager == nil {
		return nil
	}
	manager.fatalMu.Lock()
	defer manager.fatalMu.Unlock()
	return manager.fatal
}

func saturatingAddDiskBytes(left, right uint64) uint64 {
	if right > ^uint64(0)-left {
		return ^uint64(0)
	}
	return left + right
}

func logAppendVecCompactionStats(stats accountsdb.CompactStats) {
	if stats.CandidatesScanned == 0 && stats.BytesReclaimed == 0 &&
		stats.SourceSizeDeferrals == 0 && stats.OutputSpaceDeferrals == 0 {
		return
	}
	mlog.Log.Infof(
		"appendvec compaction pressure cycle: eligible=%d scanned=%d/%s largest-source=%s oversized-deferred=%d/%s output-space-deferred=%d compacted=%d deleted=%d moved=%s reclaimed=%s pass-complete=%t",
		stats.EligibleCandidates,
		stats.CandidatesScanned,
		formatDiskBytes(uint64(max(stats.ScannedBytes, 0))),
		formatDiskBytes(uint64(max(stats.MaxSourceBytes, 0))),
		stats.SourceSizeDeferrals,
		formatDiskBytes(uint64(max(stats.MaxDeferredSourceBytes, 0))),
		stats.OutputSpaceDeferrals,
		stats.FilesCompacted,
		stats.FilesDeleted,
		formatDiskBytes(uint64(max(stats.LiveBytesMoved, 0))),
		formatDiskBytes(uint64(max(stats.BytesReclaimed, 0))),
		stats.PassComplete,
	)
}

func formatDiskBytes(bytes uint64) string {
	switch {
	case bytes >= 1<<40:
		return fmt.Sprintf("%.2fTiB", float64(bytes)/float64(uint64(1)<<40))
	case bytes >= 1<<30:
		return fmt.Sprintf("%.2fGiB", float64(bytes)/float64(uint64(1)<<30))
	case bytes >= 1<<20:
		return fmt.Sprintf("%.2fMiB", float64(bytes)/float64(uint64(1)<<20))
	case bytes >= 1<<10:
		return fmt.Sprintf("%.2fKiB", float64(bytes)/float64(uint64(1)<<10))
	default:
		return fmt.Sprintf("%dB", bytes)
	}
}
