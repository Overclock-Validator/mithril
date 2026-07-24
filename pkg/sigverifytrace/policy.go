package sigverifytrace

import (
	"container/list"
	"errors"
	"fmt"
	"sort"
)

type ReplayConfig struct {
	// MaxKeyEntries and MaxTableBytes independently bound retained tables.
	// Zero means no limit for that dimension; both zero disable the key cache.
	MaxKeyEntries     int
	MaxTableBytes     uint64
	TableBytesPerKey  uint64
	AdmitAfterValid   uint64
	DuplicateCapacity int
}

type AttemptDecision struct {
	Sequence     uint64 `json:"sequence"`
	KeyHit       bool   `json:"key_hit"`
	DuplicateHit bool   `json:"duplicate_hit"`
	Admitted     bool   `json:"admitted"`
	Evictions    uint64 `json:"evictions"`
}

type KeyCacheStats struct {
	Lookups              uint64 `json:"lookups"`
	Hits                 uint64 `json:"hits"`
	Misses               uint64 `json:"misses"`
	MissPreparations     uint64 `json:"estimated_miss_preparations"`
	ValidMissCompletions uint64 `json:"valid_miss_completions"`
	InvalidCompletions   uint64 `json:"invalid_completions"`
	Admissions           uint64 `json:"admissions"`
	RetainedBuilds       uint64 `json:"retained_build_attempts"`
	RejectedAdmissions   uint64 `json:"rejected_admissions"`
	Evictions            uint64 `json:"evictions"`
	ResidentEntries      uint64 `json:"resident_entries"`
	PeakResidentEntries  uint64 `json:"peak_resident_entries"`
	ResidentTableBytes   uint64 `json:"resident_table_bytes"`
	PeakTableBytes       uint64 `json:"peak_table_bytes"`
}

type DuplicateStats struct {
	Lookups         uint64 `json:"lookups"`
	Hits            uint64 `json:"hits"`
	Misses          uint64 `json:"misses"`
	Insertions      uint64 `json:"insertions"`
	Evictions       uint64 `json:"evictions"`
	ResidentEntries uint64 `json:"resident_entries"`
	PeakEntries     uint64 `json:"peak_entries"`
}

type ReplayReport struct {
	TruncatedPrefix    bool
	FirstSequence      uint64
	LastSequence       uint64
	ValidCompletions   uint64
	InvalidCompletions uint64
	UnknownCompletions uint64
	Keys               KeyCacheStats
	Duplicates         DuplicateStats
	Attempts           []AttemptDecision
	decisionBySequence map[uint64]AttemptDecision
}

func (config ReplayConfig) validate() error {
	if config.MaxKeyEntries < 0 || config.DuplicateCapacity < 0 {
		return errors.New("sigverifytrace: cache capacities must not be negative")
	}
	keyEnabled := config.MaxKeyEntries > 0 || config.MaxTableBytes > 0
	if keyEnabled && (config.TableBytesPerKey == 0 || config.AdmitAfterValid == 0) {
		return errors.New("sigverifytrace: enabled key cache requires table bytes and a positive admission threshold")
	}
	return nil
}

type replayEventKind uint8

const (
	replayBegin replayEventKind = iota
	replayComplete
)

type replayEvent struct {
	event uint64
	kind  replayEventKind
	index int
}

type pendingAttempt struct {
	keyHit bool
}

// Replay applies an offline cache policy to the retained event suffix. Exact
// duplicate LRU entries are observations inserted at attempt begin, matching
// telemetry's recurrence question; they are not claimed as reusable results
// for an in-flight earlier verification. Key tables are looked up at begin
// and admitted only after valid completions.
func Replay(trace *Trace, config ReplayConfig) (ReplayReport, error) {
	var report ReplayReport
	if err := config.validate(); err != nil {
		return report, err
	}
	if err := trace.Validate(); err != nil {
		return report, err
	}
	if !trace.Summary.Enabled {
		return report, errors.New("sigverifytrace: cannot replay a disabled trace")
	}
	if len(trace.Verifications) == 0 {
		return report, nil
	}
	report.FirstSequence = trace.Verifications[0].Sequence
	report.LastSequence = trace.Verifications[len(trace.Verifications)-1].Sequence
	report.TruncatedPrefix = report.FirstSequence != 1 || uint64(len(trace.Verifications)) != trace.Summary.VerificationAttempts
	report.Attempts = make([]AttemptDecision, len(trace.Verifications))
	report.decisionBySequence = make(map[uint64]AttemptDecision, len(trace.Verifications))

	events := make([]replayEvent, 0, 2*len(trace.Verifications))
	for index, verification := range trace.Verifications {
		events = append(events, replayEvent{event: verification.BeginEventSequence, kind: replayBegin, index: index})
		if verification.CompletionEventSequence != 0 {
			events = append(events, replayEvent{event: verification.CompletionEventSequence, kind: replayComplete, index: index})
		}
	}
	sort.Slice(events, func(i, j int) bool { return events[i].event < events[j].event })

	keys := newKeyLRU(config)
	duplicates := newStringLRU(config.DuplicateCapacity)
	pending := make(map[uint64]pendingAttempt, len(trace.Verifications))
	for _, event := range events {
		verification := &trace.Verifications[event.index]
		decision := report.Attempts[event.index]
		decision.Sequence = verification.Sequence
		switch event.kind {
		case replayBegin:
			if duplicates.enabled() {
				report.Duplicates.Lookups++
				exact := exactKey(verification)
				if duplicates.get(exact) {
					decision.DuplicateHit = true
					report.Duplicates.Hits++
				} else {
					report.Duplicates.Misses++
					report.Duplicates.Insertions++
					if duplicates.put(exact) {
						report.Duplicates.Evictions++
					}
				}
				report.Duplicates.ResidentEntries = uint64(duplicates.len())
				if report.Duplicates.ResidentEntries > report.Duplicates.PeakEntries {
					report.Duplicates.PeakEntries = report.Duplicates.ResidentEntries
				}
			}
			if keys.enabled() {
				report.Keys.Lookups++
				if keys.get(verification.PublicKey) {
					decision.KeyHit = true
					report.Keys.Hits++
				} else {
					report.Keys.Misses++
					report.Keys.MissPreparations++
				}
			}
			pending[verification.Sequence] = pendingAttempt{keyHit: decision.KeyHit}
		case replayComplete:
			started, ok := pending[verification.Sequence]
			if !ok {
				return report, fmt.Errorf("sigverifytrace: completion for verification %d has no begin", verification.Sequence)
			}
			delete(pending, verification.Sequence)
			switch verification.Outcome {
			case OutcomeValid:
				report.ValidCompletions++
				if keys.enabled() && !started.keyHit && !keys.contains(verification.PublicKey) {
					report.Keys.ValidMissCompletions++
					if keys.recordValidMiss(verification.PublicKey) {
						report.Keys.RetainedBuilds++
						evicted, admitted := keys.admit(verification.PublicKey)
						decision.Evictions += evicted
						report.Keys.Evictions += evicted
						if admitted {
							decision.Admitted = true
							report.Keys.Admissions++
						} else {
							report.Keys.RejectedAdmissions++
						}
					}
				}
			case OutcomeInvalid:
				report.InvalidCompletions++
				if keys.enabled() {
					report.Keys.InvalidCompletions++
				}
			default:
				return report, fmt.Errorf("sigverifytrace: completed verification %d has unknown outcome", verification.Sequence)
			}
		}
		report.Attempts[event.index] = decision
	}
	for index, verification := range trace.Verifications {
		if verification.CompletionEventSequence == 0 {
			report.UnknownCompletions++
		}
		decision := report.Attempts[index]
		report.decisionBySequence[verification.Sequence] = decision
	}
	report.Keys.ResidentEntries = uint64(keys.len())
	report.Keys.ResidentTableBytes = keys.bytes
	report.Keys.PeakResidentEntries = keys.peakEntries
	report.Keys.PeakTableBytes = keys.peakBytes
	return report, nil
}

func exactKey(verification *Verification) string {
	buffer := make([]byte, 0, len(verification.PublicKey)+len(verification.Signature)+len(verification.Message))
	buffer = append(buffer, verification.PublicKey[:]...)
	buffer = append(buffer, verification.Signature[:]...)
	buffer = append(buffer, verification.Message...)
	return string(buffer)
}

type keyLRU struct {
	config      ReplayConfig
	order       *list.List
	entries     map[[32]byte]*list.Element
	validMisses map[[32]byte]uint64
	bytes       uint64
	peakBytes   uint64
	peakEntries uint64
}

func newKeyLRU(config ReplayConfig) *keyLRU {
	return &keyLRU{
		config:      config,
		order:       list.New(),
		entries:     make(map[[32]byte]*list.Element),
		validMisses: make(map[[32]byte]uint64),
	}
}

func (cache *keyLRU) enabled() bool {
	return cache.config.MaxKeyEntries > 0 || cache.config.MaxTableBytes > 0
}

func (cache *keyLRU) len() int { return len(cache.entries) }

func (cache *keyLRU) contains(key [32]byte) bool {
	_, ok := cache.entries[key]
	return ok
}

func (cache *keyLRU) get(key [32]byte) bool {
	element := cache.entries[key]
	if element == nil {
		return false
	}
	cache.order.MoveToFront(element)
	return true
}

func (cache *keyLRU) recordValidMiss(key [32]byte) bool {
	next := cache.validMisses[key] + 1
	cache.validMisses[key] = next
	return next >= cache.config.AdmitAfterValid
}

func (cache *keyLRU) admit(key [32]byte) (evictions uint64, admitted bool) {
	if cache.contains(key) {
		delete(cache.validMisses, key)
		return 0, false
	}
	if cache.config.MaxTableBytes > 0 && cache.config.TableBytesPerKey > cache.config.MaxTableBytes {
		return 0, false
	}
	for cache.exceedsAfterInsert() {
		oldest := cache.order.Back()
		if oldest == nil {
			return evictions, false
		}
		oldKey := oldest.Value.([32]byte)
		delete(cache.entries, oldKey)
		delete(cache.validMisses, oldKey)
		cache.order.Remove(oldest)
		cache.bytes -= cache.config.TableBytesPerKey
		evictions++
	}
	element := cache.order.PushFront(key)
	cache.entries[key] = element
	cache.bytes += cache.config.TableBytesPerKey
	delete(cache.validMisses, key)
	if uint64(cache.len()) > cache.peakEntries {
		cache.peakEntries = uint64(cache.len())
	}
	if cache.bytes > cache.peakBytes {
		cache.peakBytes = cache.bytes
	}
	return evictions, true
}

func (cache *keyLRU) exceedsAfterInsert() bool {
	if cache.config.MaxKeyEntries > 0 && cache.len()+1 > cache.config.MaxKeyEntries {
		return true
	}
	return cache.config.MaxTableBytes > 0 && cache.bytes+cache.config.TableBytesPerKey > cache.config.MaxTableBytes
}

type stringLRU struct {
	capacity int
	order    *list.List
	entries  map[string]*list.Element
}

func newStringLRU(capacity int) *stringLRU {
	return &stringLRU{capacity: capacity, order: list.New(), entries: make(map[string]*list.Element)}
}

func (cache *stringLRU) enabled() bool { return cache.capacity > 0 }
func (cache *stringLRU) len() int      { return len(cache.entries) }

func (cache *stringLRU) get(key string) bool {
	element := cache.entries[key]
	if element == nil {
		return false
	}
	cache.order.MoveToFront(element)
	return true
}

func (cache *stringLRU) put(key string) (evicted bool) {
	if element := cache.entries[key]; element != nil {
		cache.order.MoveToFront(element)
		return false
	}
	cache.entries[key] = cache.order.PushFront(key)
	if cache.len() <= cache.capacity {
		return false
	}
	oldest := cache.order.Back()
	delete(cache.entries, oldest.Value.(string))
	cache.order.Remove(oldest)
	return true
}
