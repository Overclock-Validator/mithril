package accountsdb

import (
	"errors"
	"fmt"
	"sync"
)

const (
	// A checkpoint build temporarily owns the 64-byte exact record file, the
	// StreamHash output, its descriptor, and StreamHash's partition scratch.
	// With the pinned StreamHash format each scratch entry is 22 bytes and its
	// writer regions use a 1.2x-plus-eight-sigma skew allowance. Together with
	// the final PTRHash this is comfortably below the explicit 256-byte envelope
	// for every supported worker/build geometry; 4 MiB absorbs fixed regions and
	// metadata for small builds. A StreamHash or checkpoint-format update must
	// re-establish this bound; exact-size validation before publication is the
	// second line of defence for durable artifacts.
	checkpointBuildReservationFixedBytes     = uint64(4 << 20)
	checkpointBuildReservationBytesPerRecord = uint64(256)
)

var (
	ErrCheckpointResourceBudget = errors.New(
		"accountsdb: delta-checkpoint resource budget exhausted",
	)
	ErrCheckpointResourceAccounting = errors.New(
		"accountsdb: delta-checkpoint resource accounting invariant violated",
	)
)

type checkpointResourceBudgetStats struct {
	SelectedBytes               uint64
	BuildReservedBytes          uint64
	ObsoleteBytes               uint64
	PhysicalBytes               uint64
	MaxSelectedBytes            uint64
	MaxPhysicalBytes            uint64
	SelectedHighWaterBytes      uint64
	BuildReservedHighWaterBytes uint64
	ObsoleteHighWaterBytes      uint64
	PhysicalHighWaterBytes      uint64
	ReservationRejects          uint64
}

type checkpointBuildReservation struct {
	budget                 *checkpointResourceBudget
	shardID                uint32
	bytes                  uint64
	selectedGrowthReserved uint64
	previousSelectedBytes  uint64
	validatedArtifactBytes uint64
	validated              bool
	finished               bool
}

// checkpointResourceBudget accounts immutable delta artifacts independently
// of Go heap accounting. selectedByShard describes the durable root. A build
// reservation is charged before active RAM is frozen. Replaced artifacts move
// from selected to obsolete and remain physically charged until the generation
// resource reports that close and deletion have both completed successfully.
type checkpointResourceBudget struct {
	mu sync.Mutex

	maxSelectedBytes uint64
	maxPhysicalBytes uint64
	selectedByShard  []uint64
	selectedBytes    uint64
	buildBytes       uint64
	selectedGrowth   uint64
	obsoleteBytes    uint64

	selectedHighWater  uint64
	buildHighWater     uint64
	obsoleteHighWater  uint64
	physicalHighWater  uint64
	reservationRejects uint64
	changed            chan struct{}
}

func newCheckpointResourceBudget(
	maxSelectedBytes uint64,
	maxPhysicalBytes uint64,
	selectedByShard []uint64,
) (*checkpointResourceBudget, error) {
	if maxSelectedBytes == 0 || maxPhysicalBytes < maxSelectedBytes {
		return nil, fmt.Errorf(
			"%w: invalid limits selected=%d physical=%d",
			ErrCheckpointResourceBudget, maxSelectedBytes, maxPhysicalBytes,
		)
	}
	selected := uint64(0)
	for shardID, bytes := range selectedByShard {
		var overflow bool
		selected, overflow = checkedCheckpointAdd(selected, bytes)
		if overflow {
			return nil, fmt.Errorf(
				"%w: selected checkpoint bytes overflow at shard %d",
				ErrCheckpointResourceAccounting, shardID,
			)
		}
	}
	if selected > maxSelectedBytes {
		return nil, fmt.Errorf(
			"%w: root selects %d bytes, maximum selected bytes is %d",
			ErrCheckpointResourceBudget, selected, maxSelectedBytes,
		)
	}
	if selected > maxPhysicalBytes {
		return nil, fmt.Errorf(
			"%w: root selects %d physical bytes, maximum is %d",
			ErrCheckpointResourceBudget, selected, maxPhysicalBytes,
		)
	}
	budget := &checkpointResourceBudget{
		maxSelectedBytes:  maxSelectedBytes,
		maxPhysicalBytes:  maxPhysicalBytes,
		selectedByShard:   append([]uint64(nil), selectedByShard...),
		selectedBytes:     selected,
		selectedHighWater: selected,
		physicalHighWater: selected,
		changed:           make(chan struct{}),
	}
	return budget, nil
}

func checkpointSelectedBytesFromCatalog(catalog *RootIndexCatalog) ([]uint64, error) {
	if catalog == nil {
		return nil, fmt.Errorf("%w: nil root catalog", ErrCheckpointResourceAccounting)
	}
	selected := make([]uint64, len(catalog.Shards))
	for shardID := range catalog.Shards {
		bytes, err := checkpointSelectedShardBytes(catalog.Shards[shardID])
		if err != nil {
			return nil, fmt.Errorf("accountsdb: account checkpoint shard %d: %w", shardID, err)
		}
		selected[shardID] = bytes
	}
	return selected, nil
}

func checkpointSelectedShardBytes(shard IndexCatalogShard) (uint64, error) {
	if shard.DeltaGeneration == 0 {
		return 0, nil
	}
	return checkedCheckpointSum(
		shard.DeltaIndex.Size,
		shard.DeltaRecords.Size,
		deltaCheckpointDescriptorSize,
	)
}

func checkpointArtifactSetBytes(artifacts ShardedDeltaCheckpointArtifactSet) (uint64, error) {
	return checkedCheckpointSum(
		artifacts.Index.Size,
		artifacts.Records.Size,
		artifacts.Descriptor.Size,
	)
}

func checkpointBuildReservationBytes(recordCount uint64) (uint64, error) {
	variable, overflow := multiplyUint64(recordCount, checkpointBuildReservationBytesPerRecord)
	if overflow || variable > ^uint64(0)-checkpointBuildReservationFixedBytes {
		return 0, fmt.Errorf(
			"%w: checkpoint reservation for %d records overflows uint64",
			ErrCheckpointResourceAccounting, recordCount,
		)
	}
	return checkpointBuildReservationFixedBytes + variable, nil
}

func (budget *checkpointResourceBudget) reserveBuild(
	shardID uint32,
	bytes uint64,
) (*checkpointBuildReservation, error) {
	if budget == nil {
		return nil, nil
	}
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if int(shardID) >= len(budget.selectedByShard) || bytes == 0 {
		return nil, fmt.Errorf(
			"%w: invalid checkpoint reservation shard=%d bytes=%d",
			ErrCheckpointResourceAccounting, shardID, bytes,
		)
	}
	previous := budget.selectedByShard[shardID]
	growth := uint64(0)
	if bytes > previous {
		growth = bytes - previous
	}
	projectedGrowth, overflow := checkedCheckpointAdd(budget.selectedGrowth, growth)
	projectedSelected, selectedOverflow := checkedCheckpointAdd(budget.selectedBytes, projectedGrowth)
	physical := budget.physicalBytesLocked()
	projectedPhysical, physicalOverflow := checkedCheckpointAdd(physical, bytes)
	if overflow || selectedOverflow || physicalOverflow ||
		projectedSelected > budget.maxSelectedBytes || projectedPhysical > budget.maxPhysicalBytes {
		if budget.reservationRejects != ^uint64(0) {
			budget.reservationRejects++
		}
		return nil, fmt.Errorf(
			"%w: shard=%d request=%d selected=%d+growth=%d/%d physical=%d/%d",
			ErrCheckpointResourceBudget,
			shardID,
			bytes,
			budget.selectedBytes,
			projectedGrowth,
			budget.maxSelectedBytes,
			physical,
			budget.maxPhysicalBytes,
		)
	}
	budget.buildBytes += bytes
	budget.selectedGrowth = projectedGrowth
	reservation := &checkpointBuildReservation{
		budget:                 budget,
		shardID:                shardID,
		bytes:                  bytes,
		selectedGrowthReserved: growth,
		previousSelectedBytes:  previous,
	}
	budget.updateHighWaterLocked()
	budget.signalChangedLocked()
	return reservation, nil
}

// validatePublication is deliberately separate from commit. Every condition
// capable of returning an error is checked before the root selector rename;
// commitPublication is then an infallible accounting transition at the same
// durable commit point.
func (budget *checkpointResourceBudget) validatePublication(
	reservation *checkpointBuildReservation,
	shardID uint32,
	artifactBytes uint64,
) error {
	if budget == nil || reservation == nil || reservation.budget != budget {
		return fmt.Errorf("%w: missing or foreign build reservation", ErrCheckpointResourceAccounting)
	}
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if reservation.finished || reservation.shardID != shardID || artifactBytes == 0 {
		return fmt.Errorf(
			"%w: invalid publication reservation state for shard %d",
			ErrCheckpointResourceAccounting, shardID,
		)
	}
	if reservation.validated {
		if reservation.validatedArtifactBytes == artifactBytes {
			return nil
		}
		return fmt.Errorf(
			"%w: shard %d artifact size changed from validated %d to %d",
			ErrCheckpointResourceAccounting,
			shardID,
			reservation.validatedArtifactBytes,
			artifactBytes,
		)
	}
	if budget.selectedByShard[shardID] != reservation.previousSelectedBytes {
		return fmt.Errorf(
			"%w: shard %d selected bytes changed from reserved %d to %d",
			ErrCheckpointResourceAccounting,
			shardID,
			reservation.previousSelectedBytes,
			budget.selectedByShard[shardID],
		)
	}
	if artifactBytes > reservation.bytes {
		return fmt.Errorf(
			"%w: checkpoint artifacts need %d bytes but reservation is %d",
			ErrCheckpointResourceAccounting, artifactBytes, reservation.bytes,
		)
	}
	withoutPrevious := budget.selectedBytes - reservation.previousSelectedBytes
	projected, overflow := checkedCheckpointAdd(withoutPrevious, artifactBytes)
	if overflow || projected > budget.maxSelectedBytes {
		return fmt.Errorf(
			"%w: checkpoint publication selects %d/%d bytes",
			ErrCheckpointResourceBudget, projected, budget.maxSelectedBytes,
		)
	}
	reservation.validatedArtifactBytes = artifactBytes
	reservation.validated = true
	return nil
}

func (budget *checkpointResourceBudget) commitPublication(
	reservation *checkpointBuildReservation,
) uint64 {
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if reservation == nil || reservation.budget != budget || reservation.finished || !reservation.validated {
		panic("accountsdb: checkpoint budget committed an unvalidated reservation")
	}
	shardID := reservation.shardID
	oldBytes := reservation.previousSelectedBytes
	budget.buildBytes -= reservation.bytes
	budget.selectedGrowth -= reservation.selectedGrowthReserved
	budget.selectedBytes -= oldBytes
	budget.selectedBytes += reservation.validatedArtifactBytes
	budget.obsoleteBytes += oldBytes
	budget.selectedByShard[shardID] = reservation.validatedArtifactBytes
	reservation.finished = true
	budget.updateHighWaterLocked()
	budget.signalChangedLocked()
	return oldBytes
}

func (budget *checkpointResourceBudget) releaseReservation(reservation *checkpointBuildReservation) {
	if budget == nil || reservation == nil {
		return
	}
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if reservation.budget != budget || reservation.finished {
		return
	}
	budget.buildBytes -= reservation.bytes
	budget.selectedGrowth -= reservation.selectedGrowthReserved
	reservation.finished = true
	budget.signalChangedLocked()
}

func (budget *checkpointResourceBudget) resetReservationValidation(
	reservation *checkpointBuildReservation,
) {
	if budget == nil || reservation == nil {
		return
	}
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if reservation.budget != budget || reservation.finished {
		return
	}
	reservation.validated = false
	reservation.validatedArtifactBytes = 0
}

func (budget *checkpointResourceBudget) validateRebase(shardID uint32, artifactBytes uint64) error {
	if budget == nil {
		return nil
	}
	budget.mu.Lock()
	defer budget.mu.Unlock()
	selectedBytes := uint64(0)
	if int(shardID) < len(budget.selectedByShard) {
		selectedBytes = budget.selectedByShard[shardID]
	}
	if int(shardID) >= len(budget.selectedByShard) || artifactBytes == 0 ||
		selectedBytes != artifactBytes {
		return fmt.Errorf(
			"%w: rebase shard %d accounts %d bytes, selected budget has %d",
			ErrCheckpointResourceAccounting,
			shardID,
			artifactBytes,
			selectedBytes,
		)
	}
	return nil
}

func (budget *checkpointResourceBudget) commitRebase(shardID uint32) uint64 {
	if budget == nil {
		return 0
	}
	budget.mu.Lock()
	defer budget.mu.Unlock()
	oldBytes := budget.selectedByShard[shardID]
	if oldBytes == 0 {
		panic("accountsdb: checkpoint budget rebased an unselected shard")
	}
	budget.selectedByShard[shardID] = 0
	budget.selectedBytes -= oldBytes
	budget.obsoleteBytes += oldBytes
	budget.updateHighWaterLocked()
	budget.signalChangedLocked()
	return oldBytes
}

func (budget *checkpointResourceBudget) releaseObsolete(bytes uint64) error {
	if budget == nil || bytes == 0 {
		return nil
	}
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if bytes > budget.obsoleteBytes {
		return fmt.Errorf(
			"%w: release %d obsolete bytes from %d",
			ErrCheckpointResourceAccounting, bytes, budget.obsoleteBytes,
		)
	}
	budget.obsoleteBytes -= bytes
	budget.signalChangedLocked()
	return nil
}

func (budget *checkpointResourceBudget) changedChannel() <-chan struct{} {
	if budget == nil {
		return nil
	}
	budget.mu.Lock()
	changed := budget.changed
	budget.mu.Unlock()
	return changed
}

func (budget *checkpointResourceBudget) snapshot() checkpointResourceBudgetStats {
	if budget == nil {
		return checkpointResourceBudgetStats{}
	}
	budget.mu.Lock()
	defer budget.mu.Unlock()
	return checkpointResourceBudgetStats{
		SelectedBytes:               budget.selectedBytes,
		BuildReservedBytes:          budget.buildBytes,
		ObsoleteBytes:               budget.obsoleteBytes,
		PhysicalBytes:               budget.physicalBytesLocked(),
		MaxSelectedBytes:            budget.maxSelectedBytes,
		MaxPhysicalBytes:            budget.maxPhysicalBytes,
		SelectedHighWaterBytes:      budget.selectedHighWater,
		BuildReservedHighWaterBytes: budget.buildHighWater,
		ObsoleteHighWaterBytes:      budget.obsoleteHighWater,
		PhysicalHighWaterBytes:      budget.physicalHighWater,
		ReservationRejects:          budget.reservationRejects,
	}
}

func (budget *checkpointResourceBudget) physicalBytesLocked() uint64 {
	return budget.selectedBytes + budget.buildBytes + budget.obsoleteBytes
}

func (budget *checkpointResourceBudget) updateHighWaterLocked() {
	budget.selectedHighWater = max(budget.selectedHighWater, budget.selectedBytes)
	budget.buildHighWater = max(budget.buildHighWater, budget.buildBytes)
	budget.obsoleteHighWater = max(budget.obsoleteHighWater, budget.obsoleteBytes)
	budget.physicalHighWater = max(budget.physicalHighWater, budget.physicalBytesLocked())
}

func (budget *checkpointResourceBudget) signalChangedLocked() {
	close(budget.changed)
	budget.changed = make(chan struct{})
}

func checkedCheckpointSum(values ...uint64) (uint64, error) {
	total := uint64(0)
	for _, value := range values {
		var overflow bool
		total, overflow = checkedCheckpointAdd(total, value)
		if overflow {
			return 0, fmt.Errorf("%w: checkpoint artifact byte sum overflows uint64", ErrCheckpointResourceAccounting)
		}
	}
	return total, nil
}

func checkedCheckpointAdd(left, right uint64) (uint64, bool) {
	if right > ^uint64(0)-left {
		return 0, true
	}
	return left + right, false
}
