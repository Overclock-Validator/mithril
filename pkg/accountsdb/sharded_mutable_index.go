package accountsdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gagliardetto/solana-go"
	"golang.org/x/sys/unix"
)

const (
	// ShardedDeltaIndexJournalFileName is deliberately distinct from the V1
	// journal. Opening one format as the other must fail closed rather than
	// silently discarding coverage information.
	ShardedDeltaIndexJournalFileName        = "accounts_delta_v2.journal"
	ShardedDeltaIndexJournalRewriteFileName = ShardedDeltaIndexJournalFileName + ".rewrite.partial"
	ShardedDeltaCheckpointDirName           = "accounts_delta_shards"

	DefaultShardedMutableMaxHotKeys        = uint64(2 << 20)
	DefaultShardedMutableMaxHotBytes       = uint64(512 << 20)
	DefaultShardedMutableBytesPerKey       = uint64(128)
	DefaultShardedMutableBytesPerRetired   = uint64(48)
	DefaultShardedMutableSealKeys          = uint64(8 << 10)
	DefaultShardedMutableSealAge           = DefaultProductionAccountIndexSealMaxAge
	DefaultShardedMutableRebaseKeys        = uint64(64 << 10)
	DefaultShardedMutableJournalRewriteAt  = uint64(512 << 20)
	defaultShardedMutableRebaseConcurrency = 1
	shardedMutableSealPressureDivisor      = uint64(2)
	shardedMutableSealRetryInitial         = time.Second
	shardedMutableSealRetryMaximum         = time.Minute
	shardedMutableRebaseRetryInitial       = time.Second
	shardedMutableRebaseRetryMaximum       = time.Minute

	// ShardedMutableAbsoluteMaxFrameBytes bounds both normal Apply work and
	// hostile recovery input independently from the legacy 1GiB wire-format
	// ceiling. Fold input is newest-wins deduplicated before it reaches this
	// layer; 64MiB still permits just under one million mutations in one epoch.
	ShardedMutableAbsoluteMaxFrameBytes = uint64(64 << 20)
	shardedMutableReplayChunkBytes      = 64 << 10
)

// DefaultShardedMutableMaxConcurrentSeals leaves roughly four scheduler
// threads per checkpoint build on large validators while bounding I/O fanout.
func DefaultShardedMutableMaxConcurrentSeals() int {
	return min(8, max(1, runtime.GOMAXPROCS(0)/4))
}

var (
	ErrShardedMutableClosed   = errors.New("accountsdb: sharded mutable account index is closed")
	ErrShardedMutableCapacity = errors.New(
		"accountsdb: sharded mutable account-index hot capacity exhausted",
	)
	ErrShardedMutableDurability = errors.New(
		"accountsdb: sharded mutable account-index publications must be durable",
	)
	errShardedMutableCandidateDrain = errors.New(
		"accountsdb: failed checkpoint candidate could not be safely drained",
	)
	errShardedMutableCandidateCleanup = errors.New(
		"accountsdb: failed checkpoint candidate could not be safely reclaimed",
	)
)

// ShardedMutableImmutablePin is an optional externally-owned immutable base
// view. AcquireImmutable is called while the mutable epoch is read-locked, so
// a caller can never observe frozen RAM being removed before it has pinned the
// root generation whose durable publication made that removal safe.
type ShardedMutableImmutablePin interface {
	Close() error
}

// ShardedMutableCheckpointPublication describes a newly built exact delta.
// PublishCheckpoint must atomically and durably select Next in the external
// root catalog before returning nil. The callback must call Next.Retain before
// installing this same handle into a root generation, and that generation's
// close callback must call Release. It must never close Checkpoint directly.
type ShardedMutableCheckpointPublication struct {
	ShardID                  uint32
	Directory                string
	Previous                 *ShardedDeltaCheckpointHandle
	Next                     *ShardedDeltaCheckpointHandle
	CoveredSequence          uint64
	ProposedCoveredSequences []uint64
	resourceReservation      *checkpointBuildReservation
	artifactBytes            uint64
}

// ShardedMutableRebaseRequest asks the owner to merge Checkpoint into the
// immutable base for ShardID. Returning nil promises that a new root catalog
// with that merge is already durable and visible. Only then does this layer
// remove its checkpoint. Hot mutations newer than CoveredSequence stay here.
type ShardedMutableRebaseRequest struct {
	ShardID          uint32
	Checkpoint       *ShardedDeltaCheckpointHandle
	CoveredSequence  uint64
	CoveredSequences []uint64
}

type ShardedMutableIndexCallbacks struct {
	PublishCheckpoint func(context.Context, ShardedMutableCheckpointPublication) error
	RequestRebase     func(context.Context, ShardedMutableRebaseRequest) error
	AcquireImmutable  func() (ShardedMutableImmutablePin, error)
}

// ShardedMutableIndexConfig fixes both the memory envelope and maintenance
// cadence. MaxHotBytes is charged deterministically using BytesPerKey and
// BytesPerRetired; these deliberately conservative accounting constants avoid
// pretending Go exposes an exact per-map-entry allocation size.
type ShardedMutableIndexConfig struct {
	Directory string
	Router    PersistentIndexShardRouter

	// CoveredSequences is the effective (base or delta) durable coverage for
	// every shard. InitialCheckpoints are borrowed shared handles; Open retains
	// each one for its mutable owner and the caller keeps its existing owner.
	CoveredSequences []uint64
	// BaseCoveredSequences is the physical immutable-base frontier. It is
	// distinct from effective coverage when an exact delta is selected and is
	// used to ensure small checkpoints cannot indefinitely pin retirement WAL.
	BaseCoveredSequences []uint64
	InitialCheckpoints   []*ShardedDeltaCheckpointHandle
	checkpointBudget     *checkpointResourceBudget
	// StartupRetirementPruneThrough is a fresh-process-only watermark. During
	// replay, retirement markers at or below it are validated but not retained,
	// then the WAL is durably rewritten before maintenance starts. The caller
	// must derive it from the global minimum *base* coverage; effective delta
	// coverage alone is not sufficient for appendvec retirement safety.
	StartupRetirementPruneThrough uint64

	MaxHotKeys           uint64
	MaxHotBytes          uint64
	BytesPerKey          uint64
	BytesPerRetired      uint64
	SealKeys             uint64
	SealMaxAge           time.Duration
	RebaseKeys           uint64
	JournalRewriteBytes  uint64
	CheckpointWorkers    int
	MaxConcurrentSeals   int
	MaxConcurrentRebases int

	Callbacks ShardedMutableIndexCallbacks
}

type shardedMutableShard struct {
	active               map[solana.PublicKey]deltaIndexValue
	frozen               map[solana.PublicKey]deltaIndexValue
	activeSince          time.Time
	frozenSeq            uint64
	coveredSeq           uint64
	baseCoveredSeq       uint64
	checkpoint           *ShardedDeltaCheckpointHandle
	sealReservation      *checkpointBuildReservation
	sealProposedCoverage []uint64
	sealing              bool
	sealErr              error
	sealFailures         uint32
	sealRetryAt          time.Time
	rebasing             bool
	rebaseErr            error
	rebaseFailures       uint32
	rebaseRetryAt        time.Time
}

type shardedMutableCheckpointBuilder func(
	context.Context,
	string,
	*DeltaCheckpoint,
	map[solana.PublicKey]deltaIndexValue,
	uint64,
	int,
) (*DeltaCheckpoint, error)

// ShardedDeltaCheckpointHandle pins a DeltaCheckpoint independently from the mutable
// state lock. The owner contributes one reference. Final Close runs in a
// goroutine, keeping both lookup and publication latency independent of mmap
// teardown.
type ShardedDeltaCheckpointHandle struct {
	checkpoint *DeltaCheckpoint
	refs       atomic.Int64
	done       chan struct{}
	errMu      sync.Mutex
	err        error
}

func NewShardedDeltaCheckpointHandle(checkpoint *DeltaCheckpoint) (*ShardedDeltaCheckpointHandle, error) {
	if checkpoint == nil {
		return nil, errors.New("accountsdb: nil delta checkpoint for shared handle")
	}
	ref := &ShardedDeltaCheckpointHandle{checkpoint: checkpoint, done: make(chan struct{})}
	ref.refs.Store(1)
	return ref, nil
}

// Retain adds one independently releasable owner.
func (ref *ShardedDeltaCheckpointHandle) Retain() error {
	if ref == nil {
		return errors.New("accountsdb: retain nil sharded delta checkpoint handle")
	}
	for {
		refs := ref.refs.Load()
		if refs == 0 {
			return errors.New("accountsdb: retain released sharded delta checkpoint handle")
		}
		if ref.refs.CompareAndSwap(refs, refs+1) {
			return nil
		}
	}
}

// Release relinquishes one owner. Final mmap teardown is asynchronous.
func (ref *ShardedDeltaCheckpointHandle) Release() error {
	if ref == nil {
		return nil
	}
	remaining := ref.refs.Add(-1)
	if remaining < 0 {
		panic("accountsdb: sharded delta checkpoint reference underflow")
	}
	if remaining != 0 {
		return nil
	}
	go func() {
		err := ref.checkpoint.Close()
		ref.errMu.Lock()
		ref.err = err
		close(ref.done)
		ref.errMu.Unlock()
	}()
	return nil
}

func (ref *ShardedDeltaCheckpointHandle) Checkpoint() *DeltaCheckpoint {
	if ref == nil || ref.refs.Load() == 0 {
		return nil
	}
	return ref.checkpoint
}

func (ref *ShardedDeltaCheckpointHandle) Done() <-chan struct{} {
	if ref == nil {
		done := make(chan struct{})
		close(done)
		return done
	}
	return ref.done
}

func (ref *ShardedDeltaCheckpointHandle) Err() error {
	if ref == nil {
		return nil
	}
	ref.errMu.Lock()
	defer ref.errMu.Unlock()
	return ref.err
}

type ShardedMutableShardStats struct {
	ShardID                   uint32
	ActiveKeys                uint64
	FrozenKeys                uint64
	CheckpointKeys            uint64
	CoveredSequence           uint64
	BaseCoveredSequence       uint64
	CheckpointCoveredSequence uint64
	OldestActiveAge           time.Duration
	Sealing                   bool
	LastSealError             string
	SealFailures              uint32
	SealRetryPending          bool
	SealRetryAfter            time.Duration
	Rebasing                  bool
	LastRebaseError           string
	RebaseFailures            uint32
	RebaseRetryPending        bool
	RebaseRetryAfter          time.Duration
}

type ShardedMutableAccountIndexStats struct {
	HotKeys                          uint64
	HotBytes                         uint64
	RetiredAppendVecs                uint64
	MaxHotKeys                       uint64
	MaxHotBytes                      uint64
	JournalSequence                  uint64
	JournalBytes                     uint64
	SealInProgress                   bool
	SealsInProgress                  uint64
	MaxConcurrentSeals               uint64
	RebasesInProgress                uint64
	RewriteInProgress                bool
	SealCount                        uint64
	RebaseCount                      uint64
	RewriteCount                     uint64
	MaintenanceErrors                uint64
	CheckpointSelectedBytes          uint64
	CheckpointBuildReservedBytes     uint64
	CheckpointObsoleteBytes          uint64
	CheckpointPhysicalBytes          uint64
	MaxCheckpointSelectedBytes       uint64
	MaxCheckpointPhysicalBytes       uint64
	CheckpointSelectedHighWaterBytes uint64
	CheckpointBuildHighWaterBytes    uint64
	CheckpointObsoleteHighWaterBytes uint64
	CheckpointPhysicalHighWaterBytes uint64
	CheckpointReservationRejects     uint64
	CheckpointPressureRebases        uint64
	FatalError                       string
	Shards                           []ShardedMutableShardStats
}

// ShardedMutableAccountIndex is a bounded exact overlay above sharded
// immutable bases. A single write mutex serializes the one global WAL. A short
// state RWMutex provides atomic visibility for cross-shard frames; immutable
// probes never execute while it is held.
type ShardedMutableAccountIndex struct {
	config ShardedMutableIndexConfig
	router PersistentIndexShardRouter
	shards []shardedMutableShard

	writeMu sync.Mutex
	stateMu sync.RWMutex
	// retired records the original WAL sequence of each marker. Callers may
	// prune only through a globally base-covered sequence after old root views
	// drain; retaining the sequence prevents this safety state growing forever.
	retired                   map[retiredAppendVec]uint64
	meta                      foldMeta
	hasMeta                   bool
	hotKeys                   uint64
	hotBytes                  uint64
	checkpointPressure        bool
	checkpointPressureRebases uint64
	seq                       uint64
	offset                    int64
	file                      *os.File
	path                      string
	closed                    bool
	closing                   bool
	poison                    error

	sealsInProgress   int
	rebasesInProgress int
	sealCount         uint64
	rebaseCount       uint64
	rewriteCount      uint64
	maintenanceErrors uint64
	rewriting         bool
	rewriteBaseline   uint64
	progress          chan struct{}
	wakeMaintenance   chan struct{}
	ctx               context.Context
	cancel            context.CancelFunc
	background        sync.WaitGroup
	buildCheckpoint   shardedMutableCheckpointBuilder

	// These durations are fixed production policy rather than persisted format
	// or operator tuning. Tests shorten them before making a shard eligible.
	sealRetryInitial   time.Duration
	sealRetryMaximum   time.Duration
	rebaseRetryInitial time.Duration
	rebaseRetryMaximum time.Duration

	startupRetirementPruned bool
}

// ShardedMutableCheckpointDirectory returns the private build directory for a
// persistent shard. Delta artifact generation numbers are local to this
// directory and never collide across shards.
func ShardedMutableCheckpointDirectory(root string, shardID uint32) string {
	return filepath.Join(root, ShardedDeltaCheckpointDirName, fmt.Sprintf("%04d", shardID))
}

// InitializeShardedMutableAccountIndex creates a new V2 WAL at sequence zero.
func InitializeShardedMutableAccountIndex(directory string) error {
	return InitializeShardedMutableAccountIndexAtSequence(directory, 0)
}

// InitializeShardedMutableAccountIndexAtSequence creates a new V2 WAL whose
// next frame follows baseSequence. Snapshot bootstrap must pass at least the
// maximum shard coverage so a new frame can never be mistaken for old state.
func InitializeShardedMutableAccountIndexAtSequence(directory string, baseSequence uint64) (retErr error) {
	if directory == "" {
		return errors.New("accountsdb: empty sharded mutable account-index directory")
	}
	if err := os.MkdirAll(filepath.Join(directory, ShardedDeltaCheckpointDirName), 0o755); err != nil {
		return fmt.Errorf("accountsdb: create sharded delta checkpoint root: %w", err)
	}
	path := filepath.Join(directory, ShardedDeltaIndexJournalFileName)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("accountsdb: create sharded mutable journal: %w", err)
	}
	keep := false
	defer func() {
		if !keep {
			_ = file.Close()
			_ = os.Remove(path)
		}
	}()
	if err := writeFullAt(file, encodeDeltaJournalHeader(baseSequence, 0), 0); err != nil {
		return fmt.Errorf("accountsdb: write sharded mutable journal header: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("accountsdb: sync sharded mutable journal: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("accountsdb: close sharded mutable journal: %w", err)
	}
	if err := fsyncDir(directory); err != nil {
		return fmt.Errorf("accountsdb: sync sharded mutable journal directory: %w", err)
	}
	keep = true
	return nil
}

func normalizeShardedMutableConfig(config ShardedMutableIndexConfig) (ShardedMutableIndexConfig, error) {
	if config.Directory == "" {
		return config, errors.New("accountsdb: empty sharded mutable account-index directory")
	}
	if config.Router.Count() == 0 {
		return config, errors.New("accountsdb: uninitialized sharded mutable account-index router")
	}
	shardCount := config.Router.Count()
	if config.CoveredSequences == nil {
		config.CoveredSequences = make([]uint64, shardCount)
	}
	if len(config.CoveredSequences) != shardCount {
		return config, fmt.Errorf(
			"accountsdb: %d sharded mutable coverage values for %d shards",
			len(config.CoveredSequences), shardCount,
		)
	}
	if config.BaseCoveredSequences == nil {
		// Direct users of the mutable layer historically supplied only effective
		// coverage. Treat it as base coverage for compatibility. The production
		// owner always supplies the exact, separately persisted base frontier.
		config.BaseCoveredSequences = append([]uint64(nil), config.CoveredSequences...)
	}
	if len(config.BaseCoveredSequences) != shardCount {
		return config, fmt.Errorf(
			"accountsdb: %d sharded mutable base coverage values for %d shards",
			len(config.BaseCoveredSequences), shardCount,
		)
	}
	for shardID := range config.BaseCoveredSequences {
		if config.BaseCoveredSequences[shardID] > config.CoveredSequences[shardID] {
			return config, fmt.Errorf(
				"accountsdb: shard %d base coverage %d exceeds effective coverage %d",
				shardID,
				config.BaseCoveredSequences[shardID],
				config.CoveredSequences[shardID],
			)
		}
	}
	if config.InitialCheckpoints == nil {
		config.InitialCheckpoints = make([]*ShardedDeltaCheckpointHandle, shardCount)
	}
	if len(config.InitialCheckpoints) != shardCount {
		return config, fmt.Errorf(
			"accountsdb: %d initial delta checkpoints for %d shards",
			len(config.InitialCheckpoints), shardCount,
		)
	}
	seenCheckpoints := make(map[*DeltaCheckpoint]uint32)
	for i, handle := range config.InitialCheckpoints {
		if handle == nil {
			continue
		}
		checkpoint := handle.Checkpoint()
		if checkpoint == nil {
			return config, fmt.Errorf("accountsdb: shard %d has a released initial delta checkpoint handle", i)
		}
		if prior, duplicate := seenCheckpoints[checkpoint]; duplicate {
			return config, fmt.Errorf(
				"accountsdb: shards %d and %d share one initial delta checkpoint",
				prior, i,
			)
		}
		seenCheckpoints[checkpoint] = uint32(i)
		if checkpoint.CoveredSeq() > config.CoveredSequences[i] {
			return config, fmt.Errorf(
				"accountsdb: shard %d checkpoint physically covers sequence %d beyond configured logical coverage %d",
				i, checkpoint.CoveredSeq(), config.CoveredSequences[i],
			)
		}
	}
	if config.MaxHotKeys == 0 {
		config.MaxHotKeys = DefaultShardedMutableMaxHotKeys
	}
	if config.MaxHotBytes == 0 {
		config.MaxHotBytes = DefaultShardedMutableMaxHotBytes
	}
	if config.BytesPerKey == 0 {
		config.BytesPerKey = DefaultShardedMutableBytesPerKey
	}
	if config.BytesPerRetired == 0 {
		config.BytesPerRetired = DefaultShardedMutableBytesPerRetired
	}
	if config.SealKeys == 0 {
		config.SealKeys = DefaultShardedMutableSealKeys
	}
	if config.SealMaxAge == 0 {
		config.SealMaxAge = DefaultShardedMutableSealAge
	}
	if config.SealMaxAge < 0 {
		return config, fmt.Errorf("accountsdb: negative sharded mutable seal age %s", config.SealMaxAge)
	}
	if config.RebaseKeys == 0 {
		config.RebaseKeys = DefaultShardedMutableRebaseKeys
	}
	if config.JournalRewriteBytes == 0 {
		config.JournalRewriteBytes = DefaultShardedMutableJournalRewriteAt
	}
	if config.CheckpointWorkers == 0 {
		config.CheckpointWorkers = runtime.GOMAXPROCS(0)
	}
	if config.CheckpointWorkers < 0 {
		return config, fmt.Errorf("accountsdb: negative checkpoint worker count %d", config.CheckpointWorkers)
	}
	if config.MaxConcurrentSeals == 0 {
		config.MaxConcurrentSeals = min(DefaultShardedMutableMaxConcurrentSeals(), shardCount)
	}
	if config.MaxConcurrentSeals < 1 || config.MaxConcurrentSeals > shardCount {
		return config, fmt.Errorf(
			"accountsdb: maximum concurrent seals must be in [1,%d] (got %d)",
			shardCount, config.MaxConcurrentSeals,
		)
	}
	if config.MaxConcurrentRebases == 0 {
		config.MaxConcurrentRebases = defaultShardedMutableRebaseConcurrency
	}
	if config.MaxConcurrentRebases < 1 || config.MaxConcurrentRebases > shardCount {
		return config, fmt.Errorf(
			"accountsdb: maximum concurrent rebases must be in [1,%d] (got %d)",
			shardCount, config.MaxConcurrentRebases,
		)
	}
	if config.MaxHotKeys == 0 || config.BytesPerKey > config.MaxHotBytes {
		return config, fmt.Errorf(
			"accountsdb: invalid sharded mutable memory envelope: keys=%d bytes=%d bytes-per-key=%d",
			config.MaxHotKeys, config.MaxHotBytes, config.BytesPerKey,
		)
	}
	if config.Callbacks.PublishCheckpoint == nil {
		return config, errors.New("accountsdb: nil durable sharded delta publication callback")
	}
	if config.RebaseKeys != 0 && config.Callbacks.RequestRebase == nil {
		return config, errors.New("accountsdb: nil sharded delta rebase callback")
	}
	return config, nil
}

// OpenShardedMutableAccountIndex opens and replays the V2 journal. It never
// creates a missing journal: loss after appendvec retirement is not
// recoverable and must require an explicit snapshot/bootstrap decision.
func OpenShardedMutableAccountIndex(config ShardedMutableIndexConfig) (*ShardedMutableAccountIndex, error) {
	config, err := normalizeShardedMutableConfig(config)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(config.Directory, ShardedDeltaIndexJournalFileName)
	file, openedInfo, err := openStableRegularFileReadWrite(path)
	if err != nil {
		return nil, fmt.Errorf("accountsdb: open sharded mutable journal: %w", err)
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("accountsdb: lock sharded mutable journal: %w", err)
	}
	// Revalidate only after owning the WAL lock. This closes the final path-swap
	// window before recovery is allowed to truncate a repairable torn tail.
	if err := validateStableRegularFile(file, path, openedInfo); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("accountsdb: validate locked sharded mutable journal: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	idx := &ShardedMutableAccountIndex{
		config:             config,
		router:             config.Router,
		shards:             make([]shardedMutableShard, config.Router.Count()),
		retired:            make(map[retiredAppendVec]uint64),
		file:               file,
		path:               path,
		progress:           make(chan struct{}),
		wakeMaintenance:    make(chan struct{}, 1),
		ctx:                ctx,
		cancel:             cancel,
		buildCheckpoint:    BuildDeltaCheckpoint,
		sealRetryInitial:   shardedMutableSealRetryInitial,
		sealRetryMaximum:   shardedMutableSealRetryMaximum,
		rebaseRetryInitial: shardedMutableRebaseRetryInitial,
		rebaseRetryMaximum: shardedMutableRebaseRetryMaximum,
	}
	for i := range idx.shards {
		idx.shards[i].active = make(map[solana.PublicKey]deltaIndexValue)
		idx.shards[i].coveredSeq = config.CoveredSequences[i]
		idx.shards[i].baseCoveredSeq = config.BaseCoveredSequences[i]
		if config.InitialCheckpoints[i] != nil {
			if err := config.InitialCheckpoints[i].Retain(); err != nil {
				for prior := 0; prior < i; prior++ {
					if idx.shards[prior].checkpoint != nil {
						_ = idx.shards[prior].checkpoint.Release()
					}
				}
				cancel()
				_ = file.Close()
				return nil, fmt.Errorf("accountsdb: retain shard %d initial checkpoint: %w", i, err)
			}
			idx.shards[i].checkpoint = config.InitialCheckpoints[i]
		}
	}
	if err := idx.openAndReplay(); err != nil {
		cancel()
		for i := range idx.shards {
			if idx.shards[i].checkpoint != nil {
				_ = idx.shards[i].checkpoint.Release()
			}
		}
		_ = file.Close()
		return nil, err
	}
	if idx.config.StartupRetirementPruneThrough > idx.seq {
		cancel()
		for i := range idx.shards {
			if idx.shards[i].checkpoint != nil {
				_ = idx.shards[i].checkpoint.Release()
			}
		}
		_ = file.Close()
		return nil, fmt.Errorf(
			"accountsdb: startup retirement prune sequence %d exceeds journal tail %d",
			idx.config.StartupRetirementPruneThrough,
			idx.seq,
		)
	}
	if idx.startupRetirementPruned {
		// No worker exists yet, but take writeMu to preserve the documented
		// rewriteJournalLocked contract and make future startup refactors safe.
		idx.writeMu.Lock()
		err := idx.rewriteJournalLocked(
			context.Background(),
			&idx.config.StartupRetirementPruneThrough,
		)
		idx.writeMu.Unlock()
		if err != nil {
			cancel()
			for i := range idx.shards {
				if idx.shards[i].checkpoint != nil {
					_ = idx.shards[i].checkpoint.Release()
				}
			}
			_ = idx.file.Close()
			return nil, fmt.Errorf("accountsdb: prune replayed retirement markers: %w", err)
		}
		idx.rewriteCount++
	}
	// Start age-based sealing only after replay has produced a complete,
	// validated state and any crash-surviving safe retirements are durably
	// absent from its replacement WAL.
	idx.background.Add(1)
	go idx.maintenanceLoop()
	idx.stateMu.Lock()
	idx.maybeScheduleRebasesLocked()
	idx.maybeScheduleSealLocked(time.Now(), false, nil)
	idx.maybeScheduleRewriteLocked()
	idx.stateMu.Unlock()
	return idx, nil
}

func (idx *ShardedMutableAccountIndex) openAndReplay() error {
	info, err := idx.file.Stat()
	if err != nil {
		return fmt.Errorf("accountsdb: stat sharded mutable journal: %w", err)
	}
	if info.Size() < deltaJournalHeaderSize {
		return fmt.Errorf("accountsdb: sharded mutable journal length %d is shorter than its header", info.Size())
	}
	header := make([]byte, deltaJournalHeaderSize)
	if _, err := idx.file.ReadAt(header, 0); err != nil {
		return fmt.Errorf("accountsdb: read sharded mutable journal header: %w", err)
	}
	if !bytes.Equal(header[:8], deltaJournalMagic[:]) {
		return fmt.Errorf("accountsdb: invalid sharded mutable journal magic %x", header[:8])
	}
	if version := uint32FromLE(header[8:12]); version != deltaJournalVersion {
		return fmt.Errorf("accountsdb: unsupported sharded mutable journal version %d", version)
	}
	if size := uint32FromLE(header[12:16]); size != deltaJournalHeaderSize {
		return fmt.Errorf("accountsdb: invalid sharded mutable journal header size %d", size)
	}
	if got, want := checksumDelta(header[:28]), uint32FromLE(header[28:32]); got != want {
		return fmt.Errorf("accountsdb: sharded mutable journal header CRC mismatch: got %08x want %08x", got, want)
	}

	baseSeq := uint64FromLE(header[16:24])
	stateFrameCount := uint32FromLE(header[24:28])
	offset := int64(deltaJournalHeaderSize)
	lastSeq := baseSeq
	stateFramesSeen := uint32(0)
	frameHeader := make([]byte, deltaFrameHeaderSize)
	for offset < info.Size() {
		remaining := info.Size() - offset
		if remaining < deltaFrameHeaderSize {
			if stateFramesSeen < stateFrameCount {
				return fmt.Errorf("accountsdb: compact sharded mutable state is torn after %d of %d frames", stateFramesSeen, stateFrameCount)
			}
			if err := truncateAndSync(idx.file, offset); err != nil {
				return fmt.Errorf("accountsdb: truncate torn sharded mutable frame header: %w", err)
			}
			break
		}
		if _, err := idx.file.ReadAt(frameHeader, offset); err != nil {
			return fmt.Errorf("accountsdb: read sharded mutable frame header at %d: %w", offset, err)
		}
		if !bytes.Equal(frameHeader[:8], deltaFrameMagic[:]) {
			return fmt.Errorf("accountsdb: invalid sharded mutable frame magic at %d", offset)
		}
		if version := uint32FromLE(frameHeader[8:12]); version != deltaJournalVersion {
			return fmt.Errorf("accountsdb: unsupported sharded mutable frame version %d at %d", version, offset)
		}
		count := uint32FromLE(frameHeader[56:60])
		frameLen := uint64FromLE(frameHeader[16:24])
		wantLen := uint64(deltaFrameHeaderSize) + uint64(count)*deltaMutationSize
		if frameLen != wantLen || frameLen > deltaMaxFrameBytes {
			return fmt.Errorf("accountsdb: invalid sharded mutable frame length %d for %d mutations at %d", frameLen, count, offset)
		}
		tooManyForConfiguredFrame := uint64(count) > idx.maxFrameMutations() &&
			idx.config.StartupRetirementPruneThrough == 0
		if tooManyForConfiguredFrame || frameLen > ShardedMutableAbsoluteMaxFrameBytes {
			return fmt.Errorf(
				"%w during recovery: frame at %d has %d mutations/%d bytes; maximum is %d/%d",
				ErrShardedMutableCapacity,
				offset,
				count,
				frameLen,
				idx.maxFrameMutations(),
				ShardedMutableAbsoluteMaxFrameBytes,
			)
		}
		if frameLen > uint64(remaining) {
			if stateFramesSeen < stateFrameCount {
				return fmt.Errorf("accountsdb: compact sharded mutable state frame %d of %d is torn", stateFramesSeen+1, stateFrameCount)
			}
			laterMagic, scanErr := journalHasLaterFrameMagic(idx.file, offset+deltaFrameHeaderSize, info.Size())
			if scanErr != nil {
				return scanErr
			}
			if laterMagic {
				return fmt.Errorf("accountsdb: corrupt interior sharded mutable frame length at %d", offset)
			}
			if err := truncateAndSync(idx.file, offset); err != nil {
				return fmt.Errorf("accountsdb: truncate torn sharded mutable frame at %d: %w", offset, err)
			}
			break
		}
		seq := uint64FromLE(frameHeader[24:32])
		if lastSeq == ^uint64(0) || seq != lastSeq+1 {
			return fmt.Errorf("accountsdb: sharded mutable frame sequence %d does not follow %d", seq, lastSeq)
		}
		flags := uint32FromLE(frameHeader[12:16])
		if flags&^(deltaFrameHasFoldMeta|deltaFrameIsState) != 0 {
			return fmt.Errorf("accountsdb: sharded mutable frame %d has unknown flags %#x", seq, flags)
		}
		expectState := stateFramesSeen < stateFrameCount
		isState := flags&deltaFrameIsState != 0
		if isState != expectState {
			return fmt.Errorf("accountsdb: sharded mutable frame %d state flag=%t, want %t", seq, isState, expectState)
		}
		if isState {
			stateFramesSeen++
		}
		stage, err := idx.readAndStageReplayFrame(
			idx.file,
			offset+deltaFrameHeaderSize,
			frameHeader,
			count,
			seq,
			isState,
		)
		if err != nil {
			return fmt.Errorf("accountsdb: validate sharded mutable frame %d: %w", seq, err)
		}
		if err := idx.applyReplayStage(stage); err != nil {
			return fmt.Errorf("accountsdb: apply sharded mutable recovery frame %d: %w", seq, err)
		}
		if flags&deltaFrameHasFoldMeta != 0 {
			idx.meta = foldMeta{
				BatchSeq:    uint64FromLE(frameHeader[32:40]),
				ThroughSlot: uint64FromLE(frameHeader[40:48]),
				FileId:      uint64FromLE(frameHeader[48:56]),
			}
			idx.hasMeta = true
		}
		lastSeq = seq
		offset += int64(frameLen)
	}
	if stateFramesSeen != stateFrameCount {
		return fmt.Errorf("accountsdb: compact sharded mutable state has %d of %d frames", stateFramesSeen, stateFrameCount)
	}
	maxCoverage := uint64(0)
	for i := range idx.shards {
		maxCoverage = max(maxCoverage, idx.shards[i].coveredSeq)
	}
	if lastSeq < maxCoverage {
		return fmt.Errorf(
			"accountsdb: sharded mutable journal tail %d precedes maximum shard coverage %d",
			lastSeq, maxCoverage,
		)
	}
	if idx.hotKeys > idx.config.MaxHotKeys || idx.hotBytes > idx.config.MaxHotBytes {
		return fmt.Errorf(
			"%w during recovery: keys=%d/%d bytes=%d/%d",
			ErrShardedMutableCapacity, idx.hotKeys, idx.config.MaxHotKeys, idx.hotBytes, idx.config.MaxHotBytes,
		)
	}
	idx.seq = lastSeq
	idx.offset = offset
	idx.rewriteBaseline = uint64(offset)
	return nil
}

type shardedReplayStage struct {
	accounts         map[solana.PublicKey]deltaIndexValue
	retired          map[retiredAppendVec]uint64
	prunedRetirement bool
}

// readAndStageReplayFrame streams CRC and structural validation in fixed-size
// chunks. The returned maps contain only the final newest-wins effect of this
// frame and remain bounded by the configured memory envelope. No live index
// state is touched until the complete frame has passed its CRC.
func (idx *ShardedMutableAccountIndex) readAndStageReplayFrame(
	file *os.File,
	payloadOffset int64,
	header []byte,
	count uint32,
	sequence uint64,
	state bool,
) (shardedReplayStage, error) {
	stage := shardedReplayStage{
		accounts: make(map[solana.PublicKey]deltaIndexValue),
		retired:  make(map[retiredAppendVec]uint64),
	}
	crc := crc32.Update(0, deltaCRC, header[:60])
	const recordsPerChunk = shardedMutableReplayChunkBytes / deltaMutationSize
	buffer := make([]byte, shardedMutableReplayChunkBytes)
	remaining := uint64(count)
	ordinal := uint64(0)
	for remaining != 0 {
		records := min(remaining, uint64(recordsPerChunk))
		chunk := buffer[:records*deltaMutationSize]
		n, err := file.ReadAt(chunk, payloadOffset+int64(ordinal*deltaMutationSize))
		if err != nil && !errors.Is(err, io.EOF) {
			return shardedReplayStage{}, fmt.Errorf("read payload chunk at mutation %d: %w", ordinal, err)
		}
		if n != len(chunk) {
			return shardedReplayStage{}, fmt.Errorf(
				"read payload chunk at mutation %d: %w",
				ordinal,
				io.ErrUnexpectedEOF,
			)
		}
		crc = crc32.Update(crc, deltaCRC, chunk)
		mutations, err := decodeDeltaMutations(chunk, uint32(records))
		if err != nil {
			return shardedReplayStage{}, fmt.Errorf("decode mutation %d: %w", ordinal, err)
		}
		for i := range mutations {
			mutation := mutations[i]
			switch mutation.Kind {
			case deltaMutationLive, deltaMutationTombstone:
				shardID := idx.router.Shard(mutation.Key)
				if sequence > idx.shards[shardID].coveredSeq {
					stage.accounts[mutation.Key] = mutation.Value
				}
			case deltaMutationRetire:
				marker := retiredAppendVec{Slot: mutation.Value.Entry.Slot, FileID: mutation.Value.Entry.FileId}
				markerSequence := sequence
				if state && mutation.Value.Entry.Offset != 0 {
					markerSequence = mutation.Value.Entry.Offset
				}
				if markerSequence <= idx.config.StartupRetirementPruneThrough &&
					idx.config.StartupRetirementPruneThrough != 0 {
					stage.prunedRetirement = true
					continue
				}
				stage.retired[marker] = max(stage.retired[marker], markerSequence)
			}
			if !idx.frameSetsFit(uint64(len(stage.accounts)), uint64(len(stage.retired))) {
				return shardedReplayStage{}, fmt.Errorf(
					"%w: frame staging exceeds configured key/byte envelope",
					ErrShardedMutableCapacity,
				)
			}
		}
		ordinal += records
		remaining -= records
	}
	if want := uint32FromLE(header[60:64]); crc != want {
		return shardedReplayStage{}, fmt.Errorf("CRC mismatch: got %08x want %08x", crc, want)
	}
	return stage, nil
}

func (idx *ShardedMutableAccountIndex) applyReplayStage(stage shardedReplayStage) error {
	var addKeys, addBytes uint64
	for key := range stage.accounts {
		shard := &idx.shards[idx.router.Shard(key)]
		if _, exists := shard.active[key]; !exists {
			addKeys++
			addBytes += idx.config.BytesPerKey
		}
	}
	for marker := range stage.retired {
		if _, exists := idx.retired[marker]; !exists {
			addBytes += idx.config.BytesPerRetired
		}
	}
	if !idx.capacityFitsLocked(addKeys, addBytes) {
		return fmt.Errorf(
			"%w: replay would require keys=%d/%d bytes=%d/%d",
			ErrShardedMutableCapacity,
			idx.hotKeys+addKeys,
			idx.config.MaxHotKeys,
			idx.hotBytes+addBytes,
			idx.config.MaxHotBytes,
		)
	}

	now := time.Now()
	for key, value := range stage.accounts {
		shard := &idx.shards[idx.router.Shard(key)]
		if _, exists := shard.active[key]; !exists {
			idx.hotKeys++
			idx.hotBytes += idx.config.BytesPerKey
			if shard.activeSince.IsZero() {
				shard.activeSince = now
			}
		}
		shard.active[key] = value
	}
	for marker, markerSequence := range stage.retired {
		if prior, exists := idx.retired[marker]; !exists {
			idx.retired[marker] = markerSequence
			idx.hotBytes += idx.config.BytesPerRetired
		} else if markerSequence > prior {
			idx.retired[marker] = markerSequence
		}
	}
	idx.startupRetirementPruned = idx.startupRetirementPruned || stage.prunedRetirement
	return nil
}

// Tiny endian/CRC wrappers keep journal parsing visually auditable beside the
// inherited V1 format without duplicating its encoders.
func uint32FromLE(data []byte) uint32 {
	return uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24
}
func uint64FromLE(data []byte) uint64 {
	return uint64(uint32FromLE(data)) | uint64(uint32FromLE(data[4:]))<<32
}
func checksumDelta(data []byte) uint32 {
	return crc32.Checksum(data, deltaCRC)
}

func checksumDeltaFrame(header, payload []byte) uint32 {
	crc := crc32.Update(0, deltaCRC, header[:60])
	return crc32.Update(crc, deltaCRC, payload)
}

func retireDeltaMutationAtSequence(retired retiredAppendVec, sequence uint64) deltaIndexMutation {
	mutation := retireDeltaMutation(retired.Slot, retired.FileID)
	mutation.Value.Entry.Offset = sequence
	return mutation
}

type shardedFrameFootprint struct {
	// These exact sets are prepared once before Apply can wait for capacity and
	// are thereafter immutable. Reusing them on every wake avoids rebuilding
	// two potentially multi-million-entry dedupe maps while holding stateMu.
	accountKeys []solana.PublicKey
	retirements []retiredAppendVec
	bytes       uint64
}

func (idx *ShardedMutableAccountIndex) maxFrameMutations() uint64 {
	// The largest possible frame under the configured byte envelope consists
	// entirely of whichever exact entry kind has the smaller accounting cost.
	// Raw duplicates do not get an exception: upstream folds are deduplicated,
	// and allowing duplicate-heavy input would reopen an allocation/CPU DoS.
	minimumCharge := min(idx.config.BytesPerKey, idx.config.BytesPerRetired)
	byBudget := idx.config.MaxHotBytes / minimumCharge
	absolute := (ShardedMutableAbsoluteMaxFrameBytes - deltaFrameHeaderSize) / deltaMutationSize
	return min(byBudget, absolute)
}

func (idx *ShardedMutableAccountIndex) validateFrameMutationCount(count int) error {
	if count < 0 || uint64(count) > idx.maxFrameMutations() {
		return fmt.Errorf(
			"%w: one atomic frame has %d mutations; configured recovery-safe maximum is %d",
			ErrShardedMutableCapacity,
			count,
			idx.maxFrameMutations(),
		)
	}
	return nil
}

func (idx *ShardedMutableAccountIndex) frameFootprint(mutations []deltaIndexMutation) (shardedFrameFootprint, error) {
	keyCapacity := len(mutations)
	if idx.config.MaxHotKeys < uint64(keyCapacity) {
		keyCapacity = int(idx.config.MaxHotKeys)
	}
	footprint := shardedFrameFootprint{
		accountKeys: make([]solana.PublicKey, 0, keyCapacity),
	}
	keys := make(map[solana.PublicKey]struct{}, keyCapacity)
	retired := make(map[retiredAppendVec]struct{})
	for i := range mutations {
		switch mutation := mutations[i]; mutation.Kind {
		case deltaMutationLive, deltaMutationTombstone:
			if _, duplicate := keys[mutation.Key]; duplicate {
				continue
			}
			keys[mutation.Key] = struct{}{}
			footprint.accountKeys = append(footprint.accountKeys, mutation.Key)
			if !idx.frameSetsFit(uint64(len(footprint.accountKeys)), uint64(len(footprint.retirements))) {
				return footprint, fmt.Errorf(
					"%w: one atomic frame exceeds the configured key/byte envelope",
					ErrShardedMutableCapacity,
				)
			}
		case deltaMutationRetire:
			marker := retiredAppendVec{Slot: mutation.Value.Entry.Slot, FileID: mutation.Value.Entry.FileId}
			if _, duplicate := retired[marker]; duplicate {
				continue
			}
			retired[marker] = struct{}{}
			footprint.retirements = append(footprint.retirements, marker)
			if !idx.frameSetsFit(uint64(len(footprint.accountKeys)), uint64(len(footprint.retirements))) {
				return footprint, fmt.Errorf(
					"%w: one atomic frame exceeds the configured retirement-byte envelope",
					ErrShardedMutableCapacity,
				)
			}
		default:
			return footprint, fmt.Errorf("accountsdb: invalid sharded mutable mutation kind %d", mutation.Kind)
		}
	}
	accountKeys := uint64(len(footprint.accountKeys))
	retirements := uint64(len(footprint.retirements))
	keyBytes, overflow := multiplyUint64(accountKeys, idx.config.BytesPerKey)
	if overflow {
		return footprint, fmt.Errorf("%w: one frame key-byte accounting overflow", ErrShardedMutableCapacity)
	}
	retiredBytes, overflow := multiplyUint64(retirements, idx.config.BytesPerRetired)
	if overflow || keyBytes > ^uint64(0)-retiredBytes {
		return footprint, fmt.Errorf("%w: one frame byte accounting overflow", ErrShardedMutableCapacity)
	}
	footprint.bytes = keyBytes + retiredBytes
	if accountKeys > idx.config.MaxHotKeys || footprint.bytes > idx.config.MaxHotBytes {
		return footprint, fmt.Errorf(
			"%w: one atomic frame needs %d/%d account keys and %d/%d bytes",
			ErrShardedMutableCapacity,
			accountKeys, idx.config.MaxHotKeys,
			footprint.bytes, idx.config.MaxHotBytes,
		)
	}
	return footprint, nil
}

func (idx *ShardedMutableAccountIndex) frameSetsFit(keys, retired uint64) bool {
	if keys > idx.config.MaxHotKeys || keys > idx.config.MaxHotBytes/idx.config.BytesPerKey {
		return false
	}
	keyBytes := keys * idx.config.BytesPerKey
	if idx.config.BytesPerRetired == 0 {
		return keyBytes <= idx.config.MaxHotBytes
	}
	return retired <= (idx.config.MaxHotBytes-keyBytes)/idx.config.BytesPerRetired
}

func multiplyUint64(left, right uint64) (uint64, bool) {
	if left != 0 && right > ^uint64(0)/left {
		return 0, true
	}
	return left * right, false
}

func (idx *ShardedMutableAccountIndex) projectedGrowthLocked(footprint shardedFrameFootprint) (keys, bytes uint64) {
	for _, key := range footprint.accountKeys {
		shard := &idx.shards[idx.router.Shard(key)]
		if _, exists := shard.active[key]; !exists {
			keys++
			bytes += idx.config.BytesPerKey
		}
	}
	for _, marker := range footprint.retirements {
		if _, exists := idx.retired[marker]; !exists {
			bytes += idx.config.BytesPerRetired
		}
	}
	return keys, bytes
}

func (idx *ShardedMutableAccountIndex) capacityFitsLocked(addKeys, addBytes uint64) bool {
	return idx.hotKeys <= idx.config.MaxHotKeys && idx.hotBytes <= idx.config.MaxHotBytes &&
		addKeys <= idx.config.MaxHotKeys-idx.hotKeys &&
		addBytes <= idx.config.MaxHotBytes-idx.hotBytes
}

// Apply publishes one multi-shard atomic epoch. The durable argument exists
// to ease migration from MutableAccountIndex, but false is rejected: this V2
// implementation never makes a RAM mutation visible before fdatasync/fsync.
func (idx *ShardedMutableAccountIndex) Apply(
	mutations []deltaIndexMutation,
	meta *foldMeta,
	durable bool,
) error {
	if idx == nil {
		return errors.New("accountsdb: nil sharded mutable account index")
	}
	if !durable {
		return ErrShardedMutableDurability
	}
	if err := idx.validateFrameMutationCount(len(mutations)); err != nil {
		return err
	}
	mutations = append([]deltaIndexMutation(nil), mutations...)
	var metaCopy *foldMeta
	if meta != nil {
		copied := *meta
		metaCopy = &copied
	}
	if len(mutations) == 0 && metaCopy == nil {
		return nil
	}
	footprint, err := idx.frameFootprint(mutations)
	if err != nil {
		return err
	}

	for {
		idx.writeMu.Lock()
		idx.stateMu.Lock()
		if idx.closed || idx.closing {
			idx.stateMu.Unlock()
			idx.writeMu.Unlock()
			return ErrShardedMutableClosed
		}
		if idx.poison != nil {
			err := idx.poison
			idx.stateMu.Unlock()
			idx.writeMu.Unlock()
			return fmt.Errorf("accountsdb: sharded mutable index is poisoned: %w", err)
		}
		addKeys, addBytes := idx.projectedGrowthLocked(footprint)
		if idx.capacityFitsLocked(addKeys, addBytes) {
			idx.stateMu.Unlock()
			break
		}

		now := time.Now()
		idx.maybeScheduleSealLocked(now, true, nil)
		wait := idx.progress
		budgetWait := idx.config.checkpointBudget.changedChannel()
		budgetStats := idx.config.checkpointBudget.snapshot()
		progressing := idx.sealsInProgress != 0 || idx.rewriting
		var sealErr error
		var nextSealRetry time.Duration
		for i := range idx.shards {
			shard := &idx.shards[i]
			progressing = progressing || shard.rebasing
			sealErr = errors.Join(sealErr, shard.sealErr)
			if !shard.sealRetryAt.IsZero() {
				remaining := max(time.Duration(0), shard.sealRetryAt.Sub(now))
				if nextSealRetry == 0 || remaining < nextSealRetry {
					nextSealRetry = remaining
				}
			}
		}
		var rebaseErr error
		for i := range idx.shards {
			rebaseErr = errors.Join(rebaseErr, idx.shards[i].rebaseErr)
		}
		hotKeys, hotBytes := idx.hotKeys, idx.hotBytes
		checkpointPressure := idx.checkpointPressure
		idx.stateMu.Unlock()
		idx.writeMu.Unlock()
		progressing = progressing || (checkpointPressure &&
			(budgetStats.BuildReservedBytes != 0 || budgetStats.ObsoleteBytes != 0))
		if !progressing {
			if sealErr != nil {
				return fmt.Errorf(
					"%w after checkpoint failure (next retry in %s): %v",
					ErrShardedMutableCapacity,
					nextSealRetry.Round(time.Millisecond),
					sealErr,
				)
			}
			if rebaseErr != nil {
				return fmt.Errorf("%w after rebase failure: %v", ErrShardedMutableCapacity, rebaseErr)
			}
			if checkpointPressure {
				return fmt.Errorf(
					"%w: checkpoint budget selected=%d/%d physical=%d/%d rejects=%d",
					ErrShardedMutableCapacity,
					budgetStats.SelectedBytes,
					budgetStats.MaxSelectedBytes,
					budgetStats.PhysicalBytes,
					budgetStats.MaxPhysicalBytes,
					budgetStats.ReservationRejects,
				)
			}
			return fmt.Errorf(
				"%w: current keys=%d/%d bytes=%d/%d",
				ErrShardedMutableCapacity, hotKeys, idx.config.MaxHotKeys, hotBytes, idx.config.MaxHotBytes,
			)
		}
		select {
		case <-wait:
		case <-budgetWait:
		}
	}
	defer idx.writeMu.Unlock()

	idx.stateMu.RLock()
	if idx.seq == ^uint64(0) {
		idx.stateMu.RUnlock()
		return errors.New("accountsdb: sharded mutable journal sequence exhausted")
	}
	sequence := idx.seq + 1
	offset := idx.offset
	idx.stateMu.RUnlock()
	frame, err := encodeDeltaFrame(sequence, mutations, metaCopy, false)
	if err != nil {
		return err
	}
	if uint64(len(frame)) > ShardedMutableAbsoluteMaxFrameBytes {
		return fmt.Errorf("%w: encoded frame is %d bytes", ErrShardedMutableCapacity, len(frame))
	}
	if err := writeFullAt(idx.file, frame, offset); err != nil {
		idx.stateMu.Lock()
		idx.poison = err
		idx.signalProgressLocked()
		idx.stateMu.Unlock()
		return fmt.Errorf("accountsdb: append sharded mutable frame %d: %w", sequence, err)
	}
	if err := idx.file.Sync(); err != nil {
		idx.stateMu.Lock()
		idx.poison = err
		idx.signalProgressLocked()
		idx.stateMu.Unlock()
		return fmt.Errorf("accountsdb: sync sharded mutable frame %d: %w", sequence, err)
	}

	idx.stateMu.Lock()
	idx.applyPublishedFrameLocked(mutations, sequence)
	if metaCopy != nil {
		idx.meta = *metaCopy
		idx.hasMeta = true
	}
	idx.seq = sequence
	idx.offset += int64(len(frame))
	idx.signalProgressLocked()
	idx.maybeScheduleSealLocked(time.Now(), false, nil)
	idx.maybeScheduleRewriteLocked()
	idx.stateMu.Unlock()
	idx.wakeMaintenanceLoop()
	return nil
}

func (idx *ShardedMutableAccountIndex) applyPublishedFrameLocked(mutations []deltaIndexMutation, sequence uint64) {
	now := time.Now()
	for i := range mutations {
		mutation := mutations[i]
		switch mutation.Kind {
		case deltaMutationLive, deltaMutationTombstone:
			shard := &idx.shards[idx.router.Shard(mutation.Key)]
			if _, exists := shard.active[mutation.Key]; !exists {
				idx.hotKeys++
				idx.hotBytes += idx.config.BytesPerKey
				if shard.activeSince.IsZero() {
					shard.activeSince = now
				}
			}
			shard.active[mutation.Key] = mutation.Value
		case deltaMutationRetire:
			marker := retiredAppendVec{Slot: mutation.Value.Entry.Slot, FileID: mutation.Value.Entry.FileId}
			if _, exists := idx.retired[marker]; !exists {
				idx.hotBytes += idx.config.BytesPerRetired
			}
			idx.retired[marker] = sequence
		}
	}
}

func (idx *ShardedMutableAccountIndex) signalProgressLocked() {
	close(idx.progress)
	idx.progress = make(chan struct{})
}

func (idx *ShardedMutableAccountIndex) wakeMaintenanceLoop() {
	select {
	case idx.wakeMaintenance <- struct{}{}:
	default:
	}
}

func (idx *ShardedMutableAccountIndex) Lookup(key solana.PublicKey) (deltaIndexValue, bool) {
	value, found, _ := idx.LookupWithError(key)
	return value, found
}

// LookupWithError pins a checkpoint while holding stateMu and releases the
// lock before probing its mmap. Thus checkpoint publication never waits for a
// long pointer chase and the checkpoint cannot close under the probe.
func (idx *ShardedMutableAccountIndex) LookupWithError(key solana.PublicKey) (deltaIndexValue, bool, error) {
	if idx == nil {
		return deltaIndexValue{}, false, nil
	}
	shardID := idx.router.Shard(key)
	idx.stateMu.RLock()
	shard := &idx.shards[shardID]
	if value, found := shard.active[key]; found {
		idx.stateMu.RUnlock()
		return value, true, nil
	}
	if value, found := shard.frozen[key]; found {
		idx.stateMu.RUnlock()
		return value, true, nil
	}
	checkpoint := shard.checkpoint
	if checkpoint != nil {
		if err := checkpoint.Retain(); err != nil {
			idx.stateMu.RUnlock()
			return deltaIndexValue{}, false, err
		}
	}
	idx.stateMu.RUnlock()
	if checkpoint == nil {
		return deltaIndexValue{}, false, nil
	}
	defer checkpoint.Release()
	return checkpoint.Checkpoint().lookupPinned(key)
}

// ShardedMutableLookupSnapshot is a key-scoped exact epoch. Hot answers are
// copied and every checkpoint needed by an unresolved key is retained while
// stateMu is held. It is intentionally key-scoped: copying a billion-key map
// merely to avoid a lock would defeat the memory design.
//
// LookupAt may be called concurrently for different positions. Close must not
// race those calls.
type ShardedMutableLookupSnapshot struct {
	keys        []solana.PublicKey
	values      []deltaIndexValue
	resolved    []uint64
	checkpoints []*ShardedDeltaCheckpointHandle
	router      PersistentIndexShardRouter
	immutable   ShardedMutableImmutablePin
	closed      atomic.Bool
	closeOnce   sync.Once
}

// NewSnapshot captures exactly the requested keys. AcquireImmutable, when
// configured, runs before stateMu is released and its returned root/base pin
// is exposed through ImmutablePin.
func (idx *ShardedMutableAccountIndex) NewSnapshot(keys []solana.PublicKey) (*ShardedMutableLookupSnapshot, error) {
	if idx == nil {
		return &ShardedMutableLookupSnapshot{}, nil
	}
	snapshot := &ShardedMutableLookupSnapshot{
		keys:        append([]solana.PublicKey(nil), keys...),
		values:      make([]deltaIndexValue, len(keys)),
		resolved:    make([]uint64, (len(keys)+63)/64),
		checkpoints: make([]*ShardedDeltaCheckpointHandle, idx.router.Count()),
		router:      idx.router,
	}
	idx.stateMu.RLock()
	if idx.closed {
		idx.stateMu.RUnlock()
		return nil, ErrShardedMutableClosed
	}
	for job, key := range snapshot.keys {
		shardID := idx.router.Shard(key)
		shard := &idx.shards[shardID]
		value, found := shard.active[key]
		if !found {
			value, found = shard.frozen[key]
		}
		if found {
			snapshot.values[job] = value
			snapshot.resolved[job>>6] |= uint64(1) << (job & 63)
			continue
		}
		if shard.checkpoint != nil && snapshot.checkpoints[shardID] == nil {
			if err := shard.checkpoint.Retain(); err != nil {
				idx.stateMu.RUnlock()
				snapshot.releaseCheckpoints()
				return nil, err
			}
			snapshot.checkpoints[shardID] = shard.checkpoint
		}
	}
	if acquire := idx.config.Callbacks.AcquireImmutable; acquire != nil {
		pin, err := acquire()
		if err != nil {
			idx.stateMu.RUnlock()
			snapshot.releaseCheckpoints()
			return nil, fmt.Errorf("accountsdb: pin immutable account-index generation: %w", err)
		}
		if pin == nil {
			idx.stateMu.RUnlock()
			snapshot.releaseCheckpoints()
			return nil, errors.New("accountsdb: immutable account-index pin callback returned nil")
		}
		snapshot.immutable = pin
	}
	idx.stateMu.RUnlock()
	return snapshot, nil
}

// ImmutablePin returns the caller-defined root/base view captured with this
// mutable epoch. It remains owned by the snapshot and is closed by Close.
func (snapshot *ShardedMutableLookupSnapshot) ImmutablePin() ShardedMutableImmutablePin {
	if snapshot == nil || snapshot.closed.Load() {
		return nil
	}
	return snapshot.immutable
}

func (snapshot *ShardedMutableLookupSnapshot) LookupAt(job int) (deltaIndexValue, bool, error) {
	if snapshot == nil || snapshot.closed.Load() {
		return deltaIndexValue{}, false, errors.New("accountsdb: sharded mutable lookup snapshot is closed")
	}
	if job < 0 || job >= len(snapshot.keys) {
		return deltaIndexValue{}, false, fmt.Errorf("accountsdb: sharded mutable snapshot position %d out of range", job)
	}
	if snapshot.resolved[job>>6]&(uint64(1)<<(job&63)) != 0 {
		return snapshot.values[job], true, nil
	}
	key := snapshot.keys[job]
	checkpoint := snapshot.checkpoints[snapshot.router.Shard(key)]
	if checkpoint == nil {
		return deltaIndexValue{}, false, nil
	}
	return checkpoint.Checkpoint().lookupPinned(key)
}

func (snapshot *ShardedMutableLookupSnapshot) releaseCheckpoints() {
	for i, checkpoint := range snapshot.checkpoints {
		if checkpoint != nil {
			_ = checkpoint.Release()
			snapshot.checkpoints[i] = nil
		}
	}
}

func (snapshot *ShardedMutableLookupSnapshot) Close() error {
	if snapshot == nil {
		return nil
	}
	var closeErr error
	snapshot.closeOnce.Do(func() {
		snapshot.closed.Store(true)
		snapshot.releaseCheckpoints()
		if snapshot.immutable != nil {
			closeErr = snapshot.immutable.Close()
			snapshot.immutable = nil
		}
	})
	return closeErr
}

// LookupBatch resolves a coherent mutable epoch without holding stateMu
// across checkpoint probes. Every output position is overwritten.
func (idx *ShardedMutableAccountIndex) LookupBatch(
	ctx context.Context,
	keys []solana.PublicKey,
	values []deltaIndexValue,
	found []bool,
) error {
	if ctx == nil {
		return errors.New("accountsdb: nil sharded mutable batch context")
	}
	if len(values) != len(keys) || len(found) != len(keys) {
		return fmt.Errorf(
			"accountsdb: sharded mutable batch buffer mismatch: keys=%d values=%d found=%d",
			len(keys), len(values), len(found),
		)
	}
	clear(values)
	clear(found)
	if err := ctx.Err(); err != nil {
		return err
	}
	if idx == nil {
		return nil
	}
	snapshot, err := idx.NewSnapshot(keys)
	if err != nil {
		return err
	}
	defer snapshot.Close()
	return runBatchStaticRanges(ctx, len(keys), func(workerCtx context.Context, start, end int) error {
		for job := start; job < end; job++ {
			if job&255 == 0 {
				if err := workerCtx.Err(); err != nil {
					return err
				}
			}
			value, ok, err := snapshot.LookupAt(job)
			if err != nil {
				return fmt.Errorf("accountsdb: sharded mutable batch lookup %d: %w", job, err)
			}
			values[job], found[job] = value, ok
		}
		return nil
	})
}

func (idx *ShardedMutableAccountIndex) ReadFoldMeta() (foldMeta, bool) {
	if idx == nil {
		return foldMeta{}, false
	}
	idx.stateMu.RLock()
	meta, found := idx.meta, idx.hasMeta
	idx.stateMu.RUnlock()
	return meta, found
}

func (idx *ShardedMutableAccountIndex) IsRetired(slot, fileID uint64) bool {
	if idx == nil {
		return false
	}
	idx.stateMu.RLock()
	_, found := idx.retired[retiredAppendVec{Slot: slot, FileID: fileID}]
	idx.stateMu.RUnlock()
	return found
}

func (idx *ShardedMutableAccountIndex) Len() int {
	if idx == nil {
		return 0
	}
	idx.stateMu.RLock()
	length := idx.hotKeys
	for i := range idx.shards {
		if idx.shards[i].checkpoint != nil {
			length += idx.shards[i].checkpoint.Checkpoint().Len()
		}
	}
	idx.stateMu.RUnlock()
	if length > uint64(^uint(0)>>1) {
		return int(^uint(0) >> 1)
	}
	return int(length)
}

func (idx *ShardedMutableAccountIndex) Stats() ShardedMutableAccountIndexStats {
	if idx == nil {
		return ShardedMutableAccountIndexStats{}
	}
	now := time.Now()
	idx.stateMu.RLock()
	stats := ShardedMutableAccountIndexStats{
		HotKeys:                   idx.hotKeys,
		HotBytes:                  idx.hotBytes,
		RetiredAppendVecs:         uint64(len(idx.retired)),
		MaxHotKeys:                idx.config.MaxHotKeys,
		MaxHotBytes:               idx.config.MaxHotBytes,
		JournalSequence:           idx.seq,
		JournalBytes:              uint64(idx.offset),
		SealInProgress:            idx.sealsInProgress != 0,
		SealsInProgress:           uint64(idx.sealsInProgress),
		MaxConcurrentSeals:        uint64(idx.config.MaxConcurrentSeals),
		RebasesInProgress:         uint64(idx.rebasesInProgress),
		RewriteInProgress:         idx.rewriting,
		SealCount:                 idx.sealCount,
		RebaseCount:               idx.rebaseCount,
		RewriteCount:              idx.rewriteCount,
		MaintenanceErrors:         idx.maintenanceErrors,
		CheckpointPressureRebases: idx.checkpointPressureRebases,
		Shards:                    make([]ShardedMutableShardStats, len(idx.shards)),
	}
	checkpointStats := idx.config.checkpointBudget.snapshot()
	stats.CheckpointSelectedBytes = checkpointStats.SelectedBytes
	stats.CheckpointBuildReservedBytes = checkpointStats.BuildReservedBytes
	stats.CheckpointObsoleteBytes = checkpointStats.ObsoleteBytes
	stats.CheckpointPhysicalBytes = checkpointStats.PhysicalBytes
	stats.MaxCheckpointSelectedBytes = checkpointStats.MaxSelectedBytes
	stats.MaxCheckpointPhysicalBytes = checkpointStats.MaxPhysicalBytes
	stats.CheckpointSelectedHighWaterBytes = checkpointStats.SelectedHighWaterBytes
	stats.CheckpointBuildHighWaterBytes = checkpointStats.BuildReservedHighWaterBytes
	stats.CheckpointObsoleteHighWaterBytes = checkpointStats.ObsoleteHighWaterBytes
	stats.CheckpointPhysicalHighWaterBytes = checkpointStats.PhysicalHighWaterBytes
	stats.CheckpointReservationRejects = checkpointStats.ReservationRejects
	if idx.poison != nil {
		stats.FatalError = idx.poison.Error()
	}
	for i := range idx.shards {
		shard := &idx.shards[i]
		shardStats := ShardedMutableShardStats{
			ShardID:             uint32(i),
			ActiveKeys:          uint64(len(shard.active)),
			FrozenKeys:          uint64(len(shard.frozen)),
			CoveredSequence:     shard.coveredSeq,
			BaseCoveredSequence: shard.baseCoveredSeq,
			Sealing:             shard.sealing,
			SealFailures:        shard.sealFailures,
			Rebasing:            shard.rebasing,
			RebaseFailures:      shard.rebaseFailures,
		}
		if !shard.activeSince.IsZero() {
			shardStats.OldestActiveAge = now.Sub(shard.activeSince)
		}
		if shard.checkpoint != nil {
			shardStats.CheckpointKeys = shard.checkpoint.Checkpoint().Len()
			shardStats.CheckpointCoveredSequence = shard.checkpoint.Checkpoint().CoveredSeq()
		}
		if shard.sealErr != nil {
			shardStats.LastSealError = shard.sealErr.Error()
		}
		if !shard.sealRetryAt.IsZero() {
			shardStats.SealRetryPending = true
			shardStats.SealRetryAfter = max(time.Duration(0), shard.sealRetryAt.Sub(now))
		}
		if shard.rebaseErr != nil {
			shardStats.LastRebaseError = shard.rebaseErr.Error()
		}
		if !shard.rebaseRetryAt.IsZero() {
			shardStats.RebaseRetryPending = true
			shardStats.RebaseRetryAfter = max(time.Duration(0), shard.rebaseRetryAt.Sub(now))
		}
		stats.Shards[i] = shardStats
	}
	idx.stateMu.RUnlock()
	return stats
}

func (idx *ShardedMutableAccountIndex) CoveredSequences() []uint64 {
	if idx == nil {
		return nil
	}
	idx.stateMu.RLock()
	covered := make([]uint64, len(idx.shards))
	for i := range idx.shards {
		covered[i] = idx.shards[i].coveredSeq
	}
	idx.stateMu.RUnlock()
	return covered
}

func (idx *ShardedMutableAccountIndex) MinimumCoveredSequence() uint64 {
	if idx == nil {
		return 0
	}
	idx.stateMu.RLock()
	minimum := ^uint64(0)
	for i := range idx.shards {
		minimum = min(minimum, idx.shards[i].coveredSeq)
	}
	idx.stateMu.RUnlock()
	if minimum == ^uint64(0) {
		return 0
	}
	return minimum
}

func (idx *ShardedMutableAccountIndex) maintenanceLoop() {
	defer idx.background.Done()
	baseInterval := idx.config.SealMaxAge / 4
	if baseInterval <= 0 || baseInterval > time.Second {
		baseInterval = time.Second
	}
	if baseInterval < 10*time.Millisecond {
		baseInterval = 10 * time.Millisecond
	}
	timer := time.NewTimer(baseInterval)
	defer timer.Stop()
	for {
		select {
		case <-idx.ctx.Done():
			return
		case <-idx.wakeMaintenance:
		case <-timer.C:
		}
		now := time.Now()
		nextDelay := baseInterval
		idx.stateMu.Lock()
		if !idx.closing {
			idx.maybeScheduleRebasesLocked()
			idx.maybeScheduleSealLocked(now, false, nil)
			idx.maybeScheduleRewriteLocked()
			nextDelay = idx.nextMaintenanceRetryDelayLocked(now, baseInterval)
		}
		idx.stateMu.Unlock()
		resetShardedMutableMaintenanceTimer(timer, nextDelay)
	}
}

// nextMaintenanceRetryDelayLocked lets the one maintenance timer serve every
// shard's independent seal and rebase deadline. Due retries have already been
// offered to their bounded schedulers above. If all slots are occupied, task
// completion wakes this same loop, so retaining the ordinary interval avoids
// a zero-duration busy loop.
func (idx *ShardedMutableAccountIndex) nextMaintenanceRetryDelayLocked(
	now time.Time,
	fallback time.Duration,
) time.Duration {
	if idx.poison != nil {
		return fallback
	}
	delay := fallback
	if idx.sealsInProgress < idx.config.MaxConcurrentSeals {
		for i := range idx.shards {
			shard := &idx.shards[i]
			if shard.sealing || shard.rebasing || len(shard.frozen) == 0 ||
				shard.sealErr == nil || shard.sealRetryAt.IsZero() {
				continue
			}
			remaining := shard.sealRetryAt.Sub(now)
			if remaining <= 0 {
				return time.Millisecond
			}
			if remaining < delay {
				delay = remaining
			}
		}
	}
	if idx.rebasesInProgress < idx.config.MaxConcurrentRebases {
		for i := range idx.shards {
			shard := &idx.shards[i]
			if shard.rebasing || shard.sealing || len(shard.frozen) != 0 || shard.checkpoint == nil ||
				shard.rebaseErr == nil || shard.rebaseRetryAt.IsZero() {
				continue
			}
			remaining := shard.rebaseRetryAt.Sub(now)
			if remaining <= 0 {
				return time.Millisecond
			}
			if remaining < delay {
				delay = remaining
			}
		}
	}
	return delay
}

func resetShardedMutableMaintenanceTimer(timer *time.Timer, delay time.Duration) {
	if delay <= 0 {
		delay = time.Millisecond
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(delay)
}

// maybeScheduleSealLocked fills the bounded build pool with the largest
// eligible shards. forceShard, when non-nil, limits selection to that shard.
// force ignores size/age thresholds. Global pressure makes all non-empty
// shards eligible well before Apply reaches the hard memory ceiling; frozen
// generations remain charged until their durable publications complete.
func (idx *ShardedMutableAccountIndex) maybeScheduleSealLocked(
	now time.Time,
	force bool,
	forceShard *uint32,
) bool {
	if idx.closing || idx.poison != nil {
		return false
	}
	scheduled := idx.maybeScheduleSealRetriesLocked(now, forceShard)
	for idx.sealsInProgress < idx.config.MaxConcurrentSeals {
		pressure := idx.sealPressureLocked()
		selected := -1
		selectedKeys := -1
		for i := range idx.shards {
			if forceShard != nil && uint32(i) != *forceShard {
				continue
			}
			shard := &idx.shards[i]
			if shard.sealing || shard.rebasing || len(shard.frozen) != 0 || len(shard.active) == 0 {
				continue
			}
			eligible := force || pressure || uint64(len(shard.active)) >= idx.config.SealKeys
			if !eligible && idx.config.SealMaxAge > 0 && !shard.activeSince.IsZero() {
				eligible = now.Sub(shard.activeSince) >= idx.config.SealMaxAge
			}
			if !eligible {
				continue
			}
			if len(shard.active) > selectedKeys {
				selected, selectedKeys = i, len(shard.active)
			}
		}
		if selected < 0 {
			break
		}

		shard := &idx.shards[selected]
		reservation, err := idx.reserveShardCheckpointBuildLocked(uint32(selected), shard)
		if err != nil {
			if errors.Is(err, ErrCheckpointResourceBudget) {
				// Leave the authoritative map active and charged. A pressure rebase
				// below the ordinary per-shard threshold is the only operation that
				// can create selected/physical checkpoint headroom.
				idx.checkpointPressure = true
				if idx.maybeScheduleRebasesLocked() {
					scheduled = true
				}
				break
			}
			idx.poison = fmt.Errorf("accountsdb: reserve shard %d checkpoint build: %w", selected, err)
			idx.maintenanceErrors++
			break
		}
		shard.sealReservation = reservation
		idx.checkpointPressure = false
		shard.frozen = shard.active
		shard.frozenSeq = idx.seq
		shard.active = make(map[solana.PublicKey]deltaIndexValue)
		shard.activeSince = time.Time{}
		proposedCoverage := make([]uint64, len(idx.shards))
		for i := range idx.shards {
			proposedCoverage[i] = idx.shards[i].coveredSeq
			candidate := &idx.shards[i]
			if i == selected || (len(candidate.active) == 0 && len(candidate.frozen) == 0 &&
				!candidate.sealing && !candidate.rebasing) {
				proposedCoverage[i] = max(proposedCoverage[i], idx.seq)
			}
		}
		shard.sealProposedCoverage = proposedCoverage
		shard.sealErr = nil
		idx.resetShardSealBackoffLocked(shard)
		idx.startShardSealLocked(uint32(selected))
		scheduled = true
	}
	if scheduled {
		idx.signalProgressLocked()
	}
	return scheduled
}

func (idx *ShardedMutableAccountIndex) reserveShardCheckpointBuildLocked(
	shardID uint32,
	shard *shardedMutableShard,
) (*checkpointBuildReservation, error) {
	if idx.config.checkpointBudget == nil {
		return nil, nil
	}
	recordCount := uint64(len(shard.active))
	if shard.checkpoint != nil {
		checkpoint := shard.checkpoint.Checkpoint()
		if checkpoint == nil {
			return nil, fmt.Errorf("%w: shard %d checkpoint handle is released", ErrCheckpointResourceAccounting, shardID)
		}
		if checkpoint.Len() > ^uint64(0)-recordCount {
			return nil, fmt.Errorf("%w: shard %d checkpoint record count overflow", ErrCheckpointResourceAccounting, shardID)
		}
		recordCount += checkpoint.Len()
	}
	reservationBytes, err := checkpointBuildReservationBytes(recordCount)
	if err != nil {
		return nil, err
	}
	return idx.config.checkpointBudget.reserveBuild(shardID, reservationBytes)
}

// maybeScheduleSealRetriesLocked gives failed frozen epochs priority over new
// freezes. Frozen RAM is the exact authoritative state until its original cut
// is durably published, and retrying it first both bounds memory and prevents
// a busy validator from starving recovery behind newly dirty shards.
func (idx *ShardedMutableAccountIndex) maybeScheduleSealRetriesLocked(
	now time.Time,
	forceShard *uint32,
) bool {
	scheduled := false
	for idx.sealsInProgress < idx.config.MaxConcurrentSeals {
		selected := -1
		selectedKeys := -1
		for i := range idx.shards {
			if forceShard != nil && uint32(i) != *forceShard {
				continue
			}
			shard := &idx.shards[i]
			if shard.sealing || shard.rebasing || len(shard.frozen) == 0 ||
				shard.sealErr == nil || shard.sealRetryAt.IsZero() || now.Before(shard.sealRetryAt) {
				continue
			}
			if len(shard.frozen) > selectedKeys {
				selected, selectedKeys = i, len(shard.frozen)
			}
		}
		if selected < 0 {
			break
		}
		idx.startShardSealLocked(uint32(selected))
		scheduled = true
	}
	return scheduled
}

func (idx *ShardedMutableAccountIndex) startShardSealLocked(shardID uint32) {
	shard := &idx.shards[shardID]
	shard.sealing = true
	shard.sealRetryAt = time.Time{}
	idx.sealsInProgress++
	builder := idx.buildCheckpoint
	if builder == nil {
		builder = BuildDeltaCheckpoint
	}
	idx.background.Add(1)
	go idx.sealFrozenShard(
		shardID,
		shard.frozen,
		shard.frozenSeq,
		shard.checkpoint,
		append([]uint64(nil), shard.sealProposedCoverage...),
		shard.sealReservation,
		idx.checkpointWorkersPerSeal(),
		builder,
	)
}

func (idx *ShardedMutableAccountIndex) sealPressureLocked() bool {
	keyThreshold := idx.config.MaxHotKeys/shardedMutableSealPressureDivisor +
		idx.config.MaxHotKeys%shardedMutableSealPressureDivisor
	byteThreshold := idx.config.MaxHotBytes/shardedMutableSealPressureDivisor +
		idx.config.MaxHotBytes%shardedMutableSealPressureDivisor
	return idx.hotKeys >= keyThreshold || idx.hotBytes >= byteThreshold
}

func (idx *ShardedMutableAccountIndex) checkpointWorkersPerSeal() int {
	return max(1, idx.config.CheckpointWorkers/idx.config.MaxConcurrentSeals)
}

func (idx *ShardedMutableAccountIndex) sealFrozenShard(
	shardID uint32,
	frozen map[solana.PublicKey]deltaIndexValue,
	coveredSequence uint64,
	old *ShardedDeltaCheckpointHandle,
	proposedCoverage []uint64,
	reservation *checkpointBuildReservation,
	workers int,
	buildCheckpoint shardedMutableCheckpointBuilder,
) {
	defer idx.background.Done()
	directory := ShardedMutableCheckpointDirectory(idx.config.Directory, shardID)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		idx.finishFailedSeal(
			shardID,
			coveredSequence,
			old,
			fmt.Errorf("accountsdb: create shard %d checkpoint directory: %w", shardID, err),
		)
		return
	}
	beforeBuild, err := snapshotDeltaCheckpointBuildEntries(directory)
	if err != nil {
		idx.finishFailedSeal(
			shardID,
			coveredSequence,
			old,
			fmt.Errorf("accountsdb: inventory shard %d checkpoint directory: %w", shardID, err),
		)
		return
	}
	var oldCheckpoint *DeltaCheckpoint
	if old != nil {
		oldCheckpoint = old.Checkpoint()
	}
	next, err := buildCheckpoint(
		idx.ctx,
		directory,
		oldCheckpoint,
		frozen,
		coveredSequence,
		workers,
	)
	if err != nil {
		if !isFatalShardedMutableSealError(err) {
			cleanupErr := fatalShardedMutableCandidateCleanup(
				cleanupRetryableFailedSealArtifacts(idx.config.Directory, directory, beforeBuild),
			)
			err = errors.Join(
				err,
				cleanupErr,
			)
		}
		idx.finishFailedSeal(shardID, coveredSequence, old, err)
		return
	}
	nextRef, handleErr := NewShardedDeltaCheckpointHandle(next)
	if handleErr != nil {
		_ = next.Close()
		idx.finishFailedSeal(shardID, coveredSequence, old, handleErr)
		return
	}
	artifactBytes, sizeErr := measureDeltaCheckpointArtifactBytes(directory, next)
	if sizeErr == nil && idx.config.checkpointBudget != nil {
		sizeErr = idx.config.checkpointBudget.validatePublication(reservation, shardID, artifactBytes)
	}
	if sizeErr != nil {
		_ = nextRef.Release()
		<-nextRef.Done()
		drainErr := nextRef.Err()
		var cleanupErr error
		if drainErr == nil {
			cleanupErr = fatalShardedMutableCandidateCleanup(
				cleanupRetryableFailedSealArtifacts(idx.config.Directory, directory, beforeBuild),
			)
		} else {
			// A failed close/unmap leaves the candidate's lifetime uncertain. Keep
			// its artifacts for startup orphan collection instead of unlinking a
			// potentially live mapping.
			drainErr = fmt.Errorf("%w: %v", errShardedMutableCandidateDrain, drainErr)
		}
		failure := errors.Join(
			fmt.Errorf("accountsdb: shard %d built checkpoint size: %w", shardID, sizeErr),
			drainErr,
			cleanupErr,
		)
		if isFatalShardedMutableSealError(failure) {
			idx.config.checkpointBudget.releaseReservation(reservation)
		} else {
			idx.config.checkpointBudget.resetReservationValidation(reservation)
		}
		idx.finishFailedSeal(
			shardID,
			coveredSequence,
			old,
			failure,
		)
		return
	}
	publication := ShardedMutableCheckpointPublication{
		ShardID:                  shardID,
		Directory:                directory,
		Previous:                 old,
		Next:                     nextRef,
		CoveredSequence:          coveredSequence,
		ProposedCoveredSequences: append([]uint64(nil), proposedCoverage...),
		resourceReservation:      reservation,
		artifactBytes:            artifactBytes,
	}
	if err := idx.config.Callbacks.PublishCheckpoint(idx.ctx, publication); err != nil {
		err = fmt.Errorf("accountsdb: durably publish shard %d checkpoint: %w", shardID, err)
		idx.config.checkpointBudget.resetReservationValidation(reservation)
		fatalPublication := isFatalShardedMutableSealError(err)
		_ = nextRef.Release()
		// Even a fatal rejection must drain Next before this background task is
		// considered complete. If the callback rejected before constructing a
		// managed generation, no other owner exists to keep its mmap teardown inside
		// ProductionAccountIndex's store-lock lifetime. Fatal failures retain their
		// artifacts for startup reconciliation; retryable failures additionally
		// reclaim them once the mapping is proven closed.
		<-nextRef.Done()
		drainErr := nextRef.Err()
		var cleanupErr error
		if drainErr != nil {
			drainErr = fmt.Errorf("%w: %v", errShardedMutableCandidateDrain, drainErr)
		} else if !fatalPublication {
			// A retryable callback contractually did not select Next. Wait for any
			// temporary derived-resource owner to release it before unlinking the
			// unselected generation, otherwise persistent ENOSPC/CAS failure could
			// consume unbounded disk one retry at a time.
			cleanupErr = fatalShardedMutableCandidateCleanup(
				cleanupRetryableFailedSealArtifacts(idx.config.Directory, directory, beforeBuild),
			)
		}
		err = errors.Join(err, drainErr, cleanupErr)
		idx.finishFailedSeal(
			shardID,
			coveredSequence,
			old,
			err,
		)
		return
	}
	idx.stateMu.Lock()
	shard := &idx.shards[shardID]
	if shard.frozenSeq != coveredSequence || !shard.sealing || shard.checkpoint != old {
		idx.poison = fmt.Errorf(
			"accountsdb: shard %d checkpoint publication no longer matches frozen epoch %d",
			shardID, coveredSequence,
		)
		idx.sealsInProgress--
		idx.maintenanceErrors++
		shard.sealing = false
		idx.signalProgressLocked()
		idx.stateMu.Unlock()
		_ = nextRef.Release()
		return
	}
	shard.checkpoint = nextRef
	shard.sealReservation = nil
	for i, coverage := range proposedCoverage {
		idx.shards[i].coveredSeq = max(idx.shards[i].coveredSeq, coverage)
		if uint32(i) != shardID && idx.shards[i].checkpoint == nil {
			idx.shards[i].baseCoveredSeq = max(idx.shards[i].baseCoveredSeq, coverage)
		}
	}
	removedKeys := uint64(len(shard.frozen))
	idx.hotKeys -= removedKeys
	idx.hotBytes -= removedKeys * idx.config.BytesPerKey
	shard.frozen = nil
	shard.frozenSeq = 0
	shard.sealProposedCoverage = nil
	shard.sealing = false
	shard.sealErr = nil
	idx.resetShardSealBackoffLocked(shard)
	shard.rebaseErr = nil
	idx.resetShardRebaseBackoffLocked(shard)
	idx.sealsInProgress--
	idx.sealCount++
	idx.signalProgressLocked()
	idx.maybeScheduleRebasesLocked()
	idx.maybeScheduleSealLocked(time.Now(), false, nil)
	idx.maybeScheduleRewriteLocked()
	idx.stateMu.Unlock()
	if old != nil {
		_ = old.Release()
	}
	idx.wakeMaintenanceLoop()
}

func measureDeltaCheckpointArtifactBytes(
	directory string,
	checkpoint *DeltaCheckpoint,
) (uint64, error) {
	if checkpoint == nil || checkpoint.Generation() == 0 {
		return 0, fmt.Errorf("%w: missing built delta checkpoint", ErrCheckpointResourceAccounting)
	}
	paths := makeDeltaCheckpointPaths(directory, checkpoint.Generation())
	var total uint64
	for _, path := range []string{paths.index, paths.records, paths.descriptor} {
		info, err := os.Lstat(path)
		if err != nil {
			return 0, fmt.Errorf("accountsdb: stat built delta checkpoint artifact %s: %w", path, err)
		}
		if !info.Mode().IsRegular() || info.Size() <= 0 {
			return 0, fmt.Errorf(
				"%w: built delta checkpoint artifact %s has mode %s and size %d",
				ErrCheckpointResourceAccounting, path, info.Mode(), info.Size(),
			)
		}
		size := uint64(info.Size())
		if size > ^uint64(0)-total {
			return 0, fmt.Errorf("%w: built delta checkpoint artifact sizes overflow", ErrCheckpointResourceAccounting)
		}
		total += size
	}
	return total, nil
}

func (idx *ShardedMutableAccountIndex) finishFailedSeal(
	shardID uint32,
	coveredSequence uint64,
	old *ShardedDeltaCheckpointHandle,
	err error,
) {
	idx.stateMu.Lock()
	shard := &idx.shards[shardID]
	idx.sealsInProgress--
	if !shard.sealing || shard.frozenSeq != coveredSequence || shard.checkpoint != old {
		shard.sealing = false
		idx.poison = fmt.Errorf(
			"accountsdb: shard %d failed checkpoint no longer matches frozen epoch %d",
			shardID,
			coveredSequence,
		)
		idx.maintenanceErrors++
	} else {
		shard.sealing = false
		switch {
		case idx.closing && errors.Is(err, context.Canceled):
			// Close owns cancellation. Frozen RAM needs no retry once the file and
			// process locks are being released.
		case idx.closing:
			shard.sealErr = err
			idx.maintenanceErrors++
		case isFatalShardedMutableSealError(err):
			shard.sealErr = err
			idx.poison = fmt.Errorf("accountsdb: shard %d checkpoint failed permanently: %w", shardID, err)
			idx.maintenanceErrors++
			idx.resetShardSealBackoffLocked(shard)
		default:
			// The exact frozen epoch, its old checkpoint, and proposed logical
			// coverage stay unchanged until the same cut publishes successfully.
			shard.sealErr = err
			idx.maintenanceErrors++
			idx.recordShardSealFailureLocked(shard, time.Now())
		}
	}
	idx.signalProgressLocked()
	idx.maybeScheduleRebasesLocked()
	idx.maybeScheduleSealLocked(time.Now(), false, nil)
	idx.stateMu.Unlock()
	idx.wakeMaintenanceLoop()
}

type deltaCheckpointBuildEntries map[string]struct{}

// snapshotDeltaCheckpointBuildEntries records only names this builder owns or
// may leave behind. A retry may clean names absent from this snapshot; it must
// never enumerate generations and retain merely the current checkpoint because
// older immutable generations can still be pinned by readers.
func snapshotDeltaCheckpointBuildEntries(directory string) (deltaCheckpointBuildEntries, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	result := make(deltaCheckpointBuildEntries)
	for _, entry := range entries {
		_, _, _, checkpointArtifact := parseDeltaCheckpointArtifactName(entry.Name())
		checkpointBuildDir := entry.IsDir() && strings.HasPrefix(entry.Name(), deltaCheckpointBuildDirPrefix)
		if checkpointArtifact || checkpointBuildDir {
			result[entry.Name()] = struct{}{}
		}
	}
	return result, nil
}

// cleanupRetryableFailedSealArtifacts removes only files/directories created by
// one failed build. The caller has already released and drained the candidate
// checkpoint handle, so no mmap owned by this attempt remains live. Final
// artifacts are identity-bound before removal; partials and private spill
// directories are safe by their unique, pre-build-absent names.
func cleanupRetryableFailedSealArtifacts(
	root string,
	directory string,
	before deltaCheckpointBuildEntries,
) error {
	entries, err := os.ReadDir(directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("accountsdb: list failed checkpoint build artifacts: %w", err)
	}
	var (
		finalArtifacts []IndexCatalogArtifact
		privatePaths   []string
	)
	for _, entry := range entries {
		if _, existed := before[entry.Name()]; existed {
			continue
		}
		path := filepath.Join(directory, entry.Name())
		if entry.IsDir() {
			if strings.HasPrefix(entry.Name(), deltaCheckpointBuildDirPrefix) {
				privatePaths = append(privatePaths, path)
			}
			continue
		}
		_, _, partial, recognized := parseDeltaCheckpointArtifactName(entry.Name())
		if !recognized {
			continue
		}
		if partial {
			info, statErr := os.Lstat(path)
			if statErr != nil {
				return fmt.Errorf("accountsdb: inspect failed checkpoint partial %s: %w", path, statErr)
			}
			if !info.Mode().IsRegular() {
				return fmt.Errorf("accountsdb: failed checkpoint partial is not regular: %s", path)
			}
			privatePaths = append(privatePaths, path)
			continue
		}
		artifact, identifyErr := computeImmutableArtifactAtPath(root, path)
		if identifyErr != nil {
			return fmt.Errorf("accountsdb: identify failed checkpoint artifact %s: %w", path, identifyErr)
		}
		finalArtifacts = append(finalArtifacts, artifact)
	}

	if err := removeImmutableArtifacts(root, finalArtifacts); err != nil {
		return fmt.Errorf("accountsdb: remove failed checkpoint generation: %w", err)
	}
	removedPrivate := false
	for _, path := range privatePaths {
		info, statErr := os.Lstat(path)
		if errors.Is(statErr, os.ErrNotExist) {
			continue
		}
		if statErr != nil {
			return fmt.Errorf("accountsdb: inspect failed checkpoint private path %s: %w", path, statErr)
		}
		if info.IsDir() {
			if err := os.RemoveAll(path); err != nil {
				return fmt.Errorf("accountsdb: remove failed checkpoint spill directory %s: %w", path, err)
			}
		} else {
			if !info.Mode().IsRegular() {
				return fmt.Errorf("accountsdb: failed checkpoint private path is not regular: %s", path)
			}
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("accountsdb: remove failed checkpoint partial %s: %w", path, err)
			}
		}
		removedPrivate = true
	}
	if removedPrivate {
		if err := fsyncDir(directory); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("accountsdb: sync failed checkpoint cleanup: %w", err)
		}
	}
	return nil
}

// fatalShardedMutableCandidateCleanup makes reclamation failure independent of
// the original build/publication error's retry classification. In particular,
// ENOSPC and EIO are safe reasons to rebuild only after the prior candidate was
// proved gone; otherwise every retry could strand another complete generation.
func fatalShardedMutableCandidateCleanup(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %v", errShardedMutableCandidateCleanup, err)
}

func (idx *ShardedMutableAccountIndex) maybeScheduleRebasesLocked() bool {
	if idx.closing || idx.poison != nil || idx.config.Callbacks.RequestRebase == nil || idx.config.RebaseKeys == 0 {
		return false
	}
	now := time.Now()
	scheduled := false
	baseFrontier := idx.minimumBaseCoveredSequenceLocked()
	retirementNeedsProgress := len(idx.retired) != 0
	for idx.rebasesInProgress < idx.config.MaxConcurrentRebases {
		selected := -1
		selectedKeys := uint64(0)
		for i := range idx.shards {
			shard := &idx.shards[i]
			if shard.rebasing || shard.sealing || len(shard.frozen) != 0 || shard.checkpoint == nil {
				continue
			}
			if shard.rebaseErr != nil &&
				(shard.rebaseRetryAt.IsZero() || now.Before(shard.rebaseRetryAt)) {
				continue
			}
			checkpoint := shard.checkpoint.Checkpoint()
			if checkpoint == nil {
				continue
			}
			thresholdEligible := checkpoint.Len() >= idx.config.RebaseKeys
			pressureEligible := idx.checkpointPressure
			frontierEligible := retirementNeedsProgress &&
				shard.baseCoveredSeq == baseFrontier &&
				shard.coveredSeq > shard.baseCoveredSeq
			if !thresholdEligible && !pressureEligible && !frontierEligible {
				continue
			}
			if selected < 0 || checkpoint.Len() > selectedKeys {
				selected, selectedKeys = i, checkpoint.Len()
			}
		}
		if selected < 0 {
			break
		}
		shard := &idx.shards[selected]
		checkpoint := shard.checkpoint
		covered := make([]uint64, len(idx.shards))
		for i := range idx.shards {
			covered[i] = idx.shards[i].coveredSeq
		}
		shard.rebasing = true
		if idx.checkpointPressure && selectedKeys < idx.config.RebaseKeys {
			idx.checkpointPressureRebases++
		}
		shard.rebaseRetryAt = time.Time{}
		idx.rebasesInProgress++
		idx.background.Add(1)
		go idx.runRebase(uint32(selected), checkpoint, covered)
		scheduled = true
	}
	if scheduled {
		idx.signalProgressLocked()
	}
	return scheduled
}

func (idx *ShardedMutableAccountIndex) minimumBaseCoveredSequenceLocked() uint64 {
	minimum := ^uint64(0)
	for shardID := range idx.shards {
		minimum = min(minimum, idx.shards[shardID].baseCoveredSeq)
	}
	if minimum == ^uint64(0) {
		return 0
	}
	return minimum
}

func (idx *ShardedMutableAccountIndex) runRebase(
	shardID uint32,
	checkpoint *ShardedDeltaCheckpointHandle,
	covered []uint64,
) {
	defer idx.background.Done()
	err := idx.config.Callbacks.RequestRebase(idx.ctx, ShardedMutableRebaseRequest{
		ShardID:          shardID,
		Checkpoint:       checkpoint,
		CoveredSequence:  checkpoint.Checkpoint().CoveredSeq(),
		CoveredSequences: covered,
	})
	idx.stateMu.Lock()
	shard := &idx.shards[shardID]
	idx.rebasesInProgress--
	if shard.checkpoint != checkpoint || !shard.rebasing {
		idx.poison = fmt.Errorf("accountsdb: shard %d rebase completed against a superseded checkpoint", shardID)
		idx.maintenanceErrors++
		shard.rebasing = false
		idx.signalProgressLocked()
		idx.stateMu.Unlock()
		idx.wakeMaintenanceLoop()
		return
	}
	if err != nil {
		shard.rebasing = false
		if idx.closing {
			if !errors.Is(err, context.Canceled) {
				shard.rebaseErr = err
				idx.maintenanceErrors++
			}
		} else if isFatalShardedMutableRebaseError(err) {
			shard.rebaseErr = err
			idx.poison = fmt.Errorf("accountsdb: shard %d rebase failed permanently: %w", shardID, err)
			idx.maintenanceErrors++
			idx.resetShardRebaseBackoffLocked(shard)
		} else {
			shard.rebaseErr = err
			idx.maintenanceErrors++
			idx.recordShardRebaseFailureLocked(shard, time.Now())
		}
		idx.signalProgressLocked()
		idx.maybeScheduleRebasesLocked()
		idx.maybeScheduleSealLocked(time.Now(), false, nil)
		idx.stateMu.Unlock()
		idx.wakeMaintenanceLoop()
		return
	}
	// The callback's nil return is the root publication commit point.
	shard.checkpoint = nil
	shard.baseCoveredSeq = max(shard.baseCoveredSeq, covered[shardID])
	shard.rebasing = false
	shard.rebaseErr = nil
	idx.resetShardRebaseBackoffLocked(shard)
	idx.rebaseCount++
	idx.signalProgressLocked()
	idx.maybeScheduleRebasesLocked()
	idx.maybeScheduleSealLocked(time.Now(), false, nil)
	idx.maybeScheduleRewriteLocked()
	idx.stateMu.Unlock()
	_ = checkpoint.Release()
	idx.wakeMaintenanceLoop()
}

func isFatalShardedMutableRebaseError(err error) bool {
	if err == nil {
		return false
	}
	// Publication ambiguity and durable format/integrity failures are never
	// healed by rebuilding the same shard. Keep this list aligned with the seal
	// path so newly introduced corruption classes fail closed everywhere.
	if errors.Is(err, ErrProductionAccountIndexPoisoned) ||
		errors.Is(err, errShardedMutableCandidateDrain) ||
		errors.Is(err, errShardedMutableCandidateCleanup) ||
		errors.Is(err, ErrShardedStreamBaseBuildCleanup) ||
		errors.Is(err, ErrInvalidRootIndexCatalog) ||
		errors.Is(err, ErrInvalidCatalogArtifact) ||
		errors.Is(err, ErrInvalidExtentCatalog) ||
		errors.Is(err, ErrInvalidBaseLocator) ||
		errors.Is(err, ErrInvalidShardedStreamBase) ||
		errors.Is(err, ErrInvalidStreamIndexManifest) ||
		errors.Is(err, ErrInvalidDeltaCheckpoint) ||
		errors.Is(err, ErrDeltaCheckpointCapacity) ||
		errors.Is(err, ErrDeltaCheckpointCommitDecided) ||
		errors.Is(err, ErrCheckpointResourceAccounting) ||
		errors.Is(err, ErrIndexResourceReleased) {
		return true
	}
	// Only explicitly understood, lossless retry conditions are retryable.
	// Unknown logic/library errors fail closed instead of spinning forever.
	if errors.Is(err, ErrIndexGenerationRejected) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, os.ErrDeadlineExceeded) ||
		isRetryableShardedMutableIOError(err) {
		return false
	}
	return true
}

func isFatalShardedMutableSealError(err error) bool {
	if err == nil {
		return false
	}
	// Root publication ambiguity and durable format/integrity failures are not
	// healed by rebuilding the same frozen epoch. Retrying them could instead
	// hide split on-disk/in-process state behind an unrelated later generation.
	if errors.Is(err, ErrProductionAccountIndexPoisoned) ||
		errors.Is(err, errShardedMutableCandidateDrain) ||
		errors.Is(err, errShardedMutableCandidateCleanup) ||
		errors.Is(err, ErrInvalidRootIndexCatalog) ||
		errors.Is(err, ErrInvalidCatalogArtifact) ||
		errors.Is(err, ErrInvalidExtentCatalog) ||
		errors.Is(err, ErrInvalidBaseLocator) ||
		errors.Is(err, ErrInvalidShardedStreamBase) ||
		errors.Is(err, ErrInvalidStreamIndexManifest) ||
		errors.Is(err, ErrInvalidDeltaCheckpoint) ||
		errors.Is(err, ErrDeltaCheckpointCapacity) ||
		errors.Is(err, ErrDeltaCheckpointCommitDecided) ||
		errors.Is(err, ErrCheckpointResourceAccounting) ||
		errors.Is(err, ErrIndexResourceReleased) {
		return true
	}
	// Losing a root CAS is expected when independent shards publish out of
	// order. Cancellation/deadline errors and a bounded set of local resource
	// failures are also safe to retry because frozen RAM remains authoritative.
	if errors.Is(err, ErrIndexGenerationRejected) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, os.ErrDeadlineExceeded) ||
		isRetryableShardedMutableIOError(err) {
		return false
	}
	// Unknown logic/library errors fail closed. New retryable failure classes
	// must be made explicit rather than silently spinning forever in production.
	return true
}

func isRetryableShardedMutableIOError(err error) bool {
	return errors.Is(err, unix.ENOSPC) ||
		errors.Is(err, unix.EDQUOT) ||
		errors.Is(err, unix.EIO) ||
		errors.Is(err, unix.EINTR) ||
		errors.Is(err, unix.EAGAIN) ||
		errors.Is(err, unix.EBUSY) ||
		errors.Is(err, unix.ETIMEDOUT) ||
		errors.Is(err, unix.ESTALE) ||
		errors.Is(err, unix.EMFILE) ||
		errors.Is(err, unix.ENFILE) ||
		errors.Is(err, unix.ENOMEM)
}

func (idx *ShardedMutableAccountIndex) resetShardSealBackoffLocked(shard *shardedMutableShard) {
	shard.sealFailures = 0
	shard.sealRetryAt = time.Time{}
}

func (idx *ShardedMutableAccountIndex) recordShardSealFailureLocked(
	shard *shardedMutableShard,
	now time.Time,
) {
	if shard.sealFailures != ^uint32(0) {
		shard.sealFailures++
	}
	shard.sealRetryAt = shardedMutableRetryDeadline(
		now,
		shard.sealFailures,
		idx.sealRetryInitial,
		idx.sealRetryMaximum,
		shardedMutableSealRetryInitial,
	)
}

func (idx *ShardedMutableAccountIndex) resetShardRebaseBackoffLocked(shard *shardedMutableShard) {
	shard.rebaseFailures = 0
	shard.rebaseRetryAt = time.Time{}
}

func (idx *ShardedMutableAccountIndex) recordShardRebaseFailureLocked(
	shard *shardedMutableShard,
	now time.Time,
) {
	if shard.rebaseFailures != ^uint32(0) {
		shard.rebaseFailures++
	}
	shard.rebaseRetryAt = shardedMutableRetryDeadline(
		now,
		shard.rebaseFailures,
		idx.rebaseRetryInitial,
		idx.rebaseRetryMaximum,
		shardedMutableRebaseRetryInitial,
	)
}

func shardedMutableRetryDeadline(
	now time.Time,
	failures uint32,
	initial time.Duration,
	maximum time.Duration,
	defaultInitial time.Duration,
) time.Time {
	if initial <= 0 {
		initial = defaultInitial
	}
	if maximum < initial {
		maximum = initial
	}
	delay := initial
	for shift := uint32(1); shift < failures && delay < maximum; shift++ {
		if delay > maximum/2 {
			delay = maximum
			break
		}
		delay *= 2
	}
	return now.Add(min(delay, maximum))
}

// ForceSeal durably publishes checkpoints for every currently hot shard. New
// concurrent writes may create a later active epoch; callers that require a
// quiescent cut must stop their writers first.
func (idx *ShardedMutableAccountIndex) ForceSeal(ctx context.Context) error {
	return idx.forceSeal(ctx, nil)
}

func (idx *ShardedMutableAccountIndex) ForceSealShard(ctx context.Context, shardID uint32) error {
	if idx == nil {
		return nil
	}
	if int(shardID) >= idx.router.Count() {
		return fmt.Errorf("accountsdb: sharded mutable shard %d out of range", shardID)
	}
	return idx.forceSeal(ctx, &shardID)
}

func (idx *ShardedMutableAccountIndex) forceSeal(ctx context.Context, only *uint32) error {
	if ctx == nil {
		return errors.New("accountsdb: nil sharded mutable force-seal context")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		idx.stateMu.Lock()
		if idx.closed || idx.closing {
			idx.stateMu.Unlock()
			return ErrShardedMutableClosed
		}
		if idx.poison != nil {
			err := idx.poison
			idx.stateMu.Unlock()
			return fmt.Errorf("accountsdb: sharded mutable index is poisoned: %w", err)
		}
		idx.maybeScheduleSealLocked(time.Now(), true, only)
		pending := false
		for i := range idx.shards {
			if only != nil && uint32(i) != *only {
				continue
			}
			shard := &idx.shards[i]
			pending = pending || len(shard.active) != 0 || len(shard.frozen) != 0 || shard.sealing || shard.rebasing
		}
		wait := idx.progress
		budgetWait := idx.config.checkpointBudget.changedChannel()
		idx.stateMu.Unlock()
		if !pending {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wait:
		case <-budgetWait:
		}
	}
}

func (idx *ShardedMutableAccountIndex) maybeScheduleRewriteLocked() bool {
	if idx.closing || idx.rewriting || idx.config.JournalRewriteBytes == 0 {
		return false
	}
	current := uint64(idx.offset)
	if current < idx.rewriteBaseline || current-idx.rewriteBaseline < idx.config.JournalRewriteBytes {
		return false
	}
	idx.rewriting = true
	idx.signalProgressLocked()
	idx.background.Add(1)
	go func() {
		defer idx.background.Done()
		idx.writeMu.Lock()
		err := idx.rewriteJournalLocked(idx.ctx, nil)
		idx.writeMu.Unlock()
		idx.stateMu.Lock()
		idx.rewriting = false
		if err == nil {
			idx.rewriteCount++
		} else if !idx.closing || !errors.Is(err, context.Canceled) {
			idx.poison = fmt.Errorf("accountsdb: rewrite sharded mutable journal: %w", err)
			idx.maintenanceErrors++
		}
		idx.signalProgressLocked()
		idx.stateMu.Unlock()
	}()
	return true
}

// CompactJournal replaces append-only history with an exact hot-state frame
// set. It is crash atomic (new file fsync, rename, directory fsync) and keeps
// the global sequence monotonic.
func (idx *ShardedMutableAccountIndex) CompactJournal(ctx context.Context) error {
	return idx.compactJournal(ctx, nil)
}

// PruneRetiredThrough durably removes retirement markers at or before the
// caller-supplied global minimum *base*-covered WAL sequence. The caller must
// also have drained all root generations below that watermark; delta coverage
// alone is insufficient because one appendvec can contain keys from many
// shards.
func (idx *ShardedMutableAccountIndex) PruneRetiredThrough(ctx context.Context, globalMinBaseCovered uint64) error {
	return idx.compactJournal(ctx, &globalMinBaseCovered)
}

func (idx *ShardedMutableAccountIndex) compactJournal(ctx context.Context, pruneThrough *uint64) error {
	if idx == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("accountsdb: nil sharded mutable journal compaction context")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		idx.writeMu.Lock()
		idx.stateMu.Lock()
		if idx.closed || idx.closing {
			idx.stateMu.Unlock()
			idx.writeMu.Unlock()
			return ErrShardedMutableClosed
		}
		if idx.poison != nil {
			err := idx.poison
			idx.stateMu.Unlock()
			idx.writeMu.Unlock()
			return fmt.Errorf("accountsdb: sharded mutable index is poisoned: %w", err)
		}
		if pruneThrough != nil && *pruneThrough > idx.seq {
			idx.stateMu.Unlock()
			idx.writeMu.Unlock()
			return fmt.Errorf(
				"accountsdb: retirement prune sequence %d exceeds journal tail %d",
				*pruneThrough, idx.seq,
			)
		}
		if idx.rewriting {
			wait := idx.progress
			idx.stateMu.Unlock()
			idx.writeMu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-wait:
			}
			continue
		}
		idx.rewriting = true
		idx.signalProgressLocked()
		idx.stateMu.Unlock()
		err := idx.rewriteJournalLocked(ctx, pruneThrough)
		idx.writeMu.Unlock()
		idx.stateMu.Lock()
		idx.rewriting = false
		if err == nil {
			idx.rewriteCount++
		} else if !errors.Is(err, context.Canceled) {
			idx.poison = fmt.Errorf("accountsdb: rewrite sharded mutable journal: %w", err)
			idx.maintenanceErrors++
		}
		idx.signalProgressLocked()
		idx.stateMu.Unlock()
		return err
	}
}

// rewriteJournalLocked requires writeMu and performs no callback. It copies
// bounded hot state under stateMu, then releases stateMu for all disk I/O.
func (idx *ShardedMutableAccountIndex) rewriteJournalLocked(ctx context.Context, pruneThrough *uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	idx.stateMu.RLock()
	baseSequence := idx.seq
	latest := make(map[solana.PublicKey]deltaIndexValue, idx.hotKeys)
	for i := range idx.shards {
		shard := &idx.shards[i]
		for key, value := range shard.frozen {
			latest[key] = value
		}
		for key, value := range shard.active {
			latest[key] = value
		}
	}
	mutations := make([]deltaIndexMutation, 0, len(latest)+len(idx.retired))
	for key, value := range latest {
		if value.Tombstone {
			mutations = append(mutations, tombstoneDeltaMutation(key))
		} else {
			mutations = append(mutations, liveDeltaMutation(key, value.Entry))
		}
	}
	for marker, sequence := range idx.retired {
		if pruneThrough != nil && sequence <= *pruneThrough {
			continue
		}
		mutations = append(mutations, retireDeltaMutationAtSequence(marker, sequence))
	}
	var meta *foldMeta
	if idx.hasMeta {
		copy := idx.meta
		meta = &copy
	}
	idx.stateMu.RUnlock()

	sort.Slice(mutations, func(i, j int) bool {
		left, right := mutations[i], mutations[j]
		if left.Kind == deltaMutationRetire || right.Kind == deltaMutationRetire {
			if left.Kind != right.Kind {
				return left.Kind != deltaMutationRetire
			}
			if left.Value.Entry.Slot != right.Value.Entry.Slot {
				return left.Value.Entry.Slot < right.Value.Entry.Slot
			}
			return left.Value.Entry.FileId < right.Value.Entry.FileId
		}
		return bytes.Compare(left.Key[:], right.Key[:]) < 0
	})
	maxStateFrameMutations := int(idx.maxFrameMutations())
	if maxStateFrameMutations == 0 {
		return fmt.Errorf("%w: configured journal frame cannot hold one mutation", ErrShardedMutableCapacity)
	}
	stateFrames := 0
	if len(mutations) != 0 {
		stateFrames = (len(mutations) + maxStateFrameMutations - 1) / maxStateFrameMutations
	} else if meta != nil {
		stateFrames = 1
	}
	if uint64(stateFrames) > uint64(^uint32(0)) || uint64(stateFrames) > ^uint64(0)-baseSequence {
		return fmt.Errorf("accountsdb: sharded compact state needs too many frames: %d", stateFrames)
	}

	tmpPath := filepath.Join(idx.config.Directory, ShardedDeltaIndexJournalRewriteFileName)
	if err := os.Remove(tmpPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("accountsdb: remove stale sharded journal rewrite: %w", err)
	}
	replacement, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("accountsdb: create sharded journal rewrite: %w", err)
	}
	keep := false
	defer func() {
		if !keep {
			_ = replacement.Close()
			_ = os.Remove(tmpPath)
		}
	}()
	if err := unix.Flock(int(replacement.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return fmt.Errorf("accountsdb: lock sharded journal rewrite: %w", err)
	}
	if err := writeFullAt(replacement, encodeDeltaJournalHeader(baseSequence, uint32(stateFrames)), 0); err != nil {
		return fmt.Errorf("accountsdb: write compact sharded journal header: %w", err)
	}
	sequence := baseSequence
	writeOffset := int64(deltaJournalHeaderSize)
	for ordinal := 0; ordinal < stateFrames; ordinal++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		start := min(ordinal*maxStateFrameMutations, len(mutations))
		end := min(start+maxStateFrameMutations, len(mutations))
		var frameMeta *foldMeta
		if ordinal == stateFrames-1 {
			frameMeta = meta
		}
		sequence++
		frame, err := encodeDeltaFrame(sequence, mutations[start:end], frameMeta, true)
		if err != nil {
			return fmt.Errorf("accountsdb: encode sharded compact state frame %d: %w", ordinal+1, err)
		}
		if err := writeFullAt(replacement, frame, writeOffset); err != nil {
			return fmt.Errorf("accountsdb: write sharded compact state frame %d: %w", ordinal+1, err)
		}
		writeOffset += int64(len(frame))
	}
	if err := replacement.Sync(); err != nil {
		return fmt.Errorf("accountsdb: sync compact sharded journal: %w", err)
	}
	if err := os.Rename(tmpPath, idx.path); err != nil {
		return fmt.Errorf("accountsdb: publish compact sharded journal: %w", err)
	}
	keep = true
	old := idx.file
	idx.file = replacement
	idx.stateMu.Lock()
	idx.seq = sequence
	idx.offset = writeOffset
	idx.rewriteBaseline = uint64(writeOffset)
	if pruneThrough != nil {
		for marker, markerSequence := range idx.retired {
			if markerSequence <= *pruneThrough {
				delete(idx.retired, marker)
				idx.hotBytes -= idx.config.BytesPerRetired
			}
		}
	}
	idx.signalProgressLocked()
	idx.stateMu.Unlock()
	dirErr := fsyncDir(idx.config.Directory)
	closeErr := old.Close()
	return errors.Join(dirErr, closeErr)
}

func journalHasLaterFrameMagic(file *os.File, start, end int64) (bool, error) {
	if start >= end {
		return false, nil
	}
	const chunkSize = 64 << 10
	buffer := make([]byte, chunkSize+len(deltaFrameMagic)-1)
	carry := 0
	for offset := start; offset < end; {
		want := min(int64(chunkSize), end-offset)
		n, err := file.ReadAt(buffer[carry:carry+int(want)], offset)
		if err != nil && !errors.Is(err, io.EOF) {
			return false, fmt.Errorf("accountsdb: scan sharded journal after corrupt frame: %w", err)
		}
		if bytes.Contains(buffer[:carry+n], deltaFrameMagic[:]) {
			return true, nil
		}
		if n == 0 {
			break
		}
		keep := min(len(deltaFrameMagic)-1, carry+n)
		copy(buffer[:keep], buffer[carry+n-keep:carry+n])
		carry = keep
		offset += int64(n)
	}
	return false, nil
}

// beginClose fences new WAL work and cancels maintenance without waiting for
// background tasks or releasing any resource. ProductionAccountIndex invokes
// it before waiting for its outer apply serialization lock: an Apply already
// holding that lock may itself be backpressured on cancellable maintenance.
func (idx *ShardedMutableAccountIndex) beginClose() {
	if idx == nil {
		return
	}
	// Cancellation must precede writeMu. The internal journal rewrite holds that
	// mutex across cancellable disk I/O; waiting for the mutex before cancelling
	// would force shutdown to finish the entire rewrite it is trying to stop.
	// context.CancelFunc is idempotent and safe under concurrent Close calls.
	if idx.cancel != nil {
		idx.cancel()
	}
	idx.writeMu.Lock()
	idx.stateMu.Lock()
	if !idx.closed && !idx.closing {
		idx.closing = true
		idx.signalProgressLocked()
	}
	idx.stateMu.Unlock()
	idx.writeMu.Unlock()
	idx.wakeMaintenanceLoop()
}

func (idx *ShardedMutableAccountIndex) Close() error {
	if idx == nil {
		return nil
	}
	idx.beginClose()

	idx.background.Wait()

	idx.writeMu.Lock()
	idx.stateMu.Lock()
	if idx.closed {
		idx.stateMu.Unlock()
		idx.writeMu.Unlock()
		return nil
	}
	idx.closed = true
	checkpoints := make([]*ShardedDeltaCheckpointHandle, 0, len(idx.shards))
	reservations := make([]*checkpointBuildReservation, 0, len(idx.shards))
	for i := range idx.shards {
		if idx.shards[i].checkpoint != nil {
			checkpoints = append(checkpoints, idx.shards[i].checkpoint)
			idx.shards[i].checkpoint = nil
		}
		if idx.shards[i].sealReservation != nil {
			reservations = append(reservations, idx.shards[i].sealReservation)
			idx.shards[i].sealReservation = nil
		}
	}
	file := idx.file
	idx.file = nil
	idx.signalProgressLocked()
	idx.stateMu.Unlock()
	var fileErr error
	if file != nil {
		fileErr = errors.Join(file.Sync(), file.Close())
	}
	idx.writeMu.Unlock()
	for _, checkpoint := range checkpoints {
		_ = checkpoint.Release()
	}
	for _, reservation := range reservations {
		idx.config.checkpointBudget.releaseReservation(reservation)
	}
	return fileErr
}
