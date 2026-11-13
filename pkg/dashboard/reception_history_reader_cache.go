package dashboard

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"
)

const (
	receptionReaderMetadataTTL   = 30 * time.Second
	receptionReaderMetadataLimit = 3 << 20
	receptionReaderScanBudget    = 25 * time.Millisecond
	receptionReaderPageRows      = 128
)

// ReceptionHistoryChunkInput is one untrusted descriptor/body pair. Imports
// validate all bytes before locking or installing any member of the batch.
type ReceptionHistoryChunkInput struct {
	Descriptor ReceptionChunkDescriptor
	Body       string
}

// ReceptionHistoryRow is the exact original ten-column occurrence tuple. It
// contains only scalar values and its original UTC date, never a source handle.
type ReceptionHistoryRow = receptionChunkRow

// ReceptionHistoryRead is a bounded detached result, not a cache generation.
// Count is at most128; the fixed129-row capacity also bounds future encoding
// lookahead. ScannedThrough advances over nonmatching dates as well as matches.
// More means unscanned cached sequences exist, not that they match the filter.
// On failure no rows/cursor advancement are returned; NextAvailable identifies
// a gap the caller must explicitly acknowledge before continuing.
type ReceptionHistoryRead struct {
	RecordingID                       string
	After, ScannedThrough             uint64
	CacheBounds                       ReceptionSequenceBounds
	NextAvailable                     *uint64
	Rows                              [129]ReceptionHistoryRow
	Count, ScannedRows, ScannedChunks int
	More                              bool
}

type receptionReaderMetadata struct {
	bundle      *PreparedReceptionMetadata
	installedAt time.Time
}

// ReceptionHistoryReaderCache owns one mutable encoded ring and its parallel
// fixed parsed rows. The encoded cache is private: callers can obtain one body,
// eight descriptors or one fixed row result, never the cache or a generation.
// One collector imports batches; bounded gates reject overlapping import/scan
// work. No method performs I/O, source callbacks, polling or encoding on reads.
type ReceptionHistoryReaderCache struct {
	mu               sync.RWMutex
	importMu         sync.Mutex
	scanMu           sync.Mutex
	encoded          *ReceptionHistoryCache
	rows             [maxReceptionChunks]receptionChunkRows
	metadata         [6]receptionReaderMetadata
	metadataBytes    int
	metadataRevision uint64
	metadataDigest   string
	metadataStartup  ReceptionStartup
	metadataClock    time.Time
}

// NewReceptionHistoryReaderCache allocates nothing when disabled. A valid
// recording binding is required before any retained row can be installed.
func NewReceptionHistoryReaderCache(enabled bool, binding ReceptionRecordingBinding) (*ReceptionHistoryReaderCache, error) {
	if !enabled {
		return nil, nil
	}
	encoded, err := NewReceptionHistoryCache(true, binding)
	if err != nil {
		return nil, err
	}
	return &ReceptionHistoryReaderCache{encoded: encoded}, nil
}

// Reset adopts a validated replacement in place. The collector must authorize
// a real node-context change; unavailable captures do not call Reset. The
// encoded cache preserves its same-node generation high-water across failures.
func (c *ReceptionHistoryReaderCache) Reset(binding ReceptionRecordingBinding) error {
	if c == nil {
		return errors.New("source_unavailable")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	old := c.encoded.recordingID
	if err := c.encoded.Reset(binding); err != nil {
		return err
	}
	if old == c.encoded.recordingID {
		return nil
	}
	clear(c.rows[:])
	clear(c.metadata[:])
	c.metadataBytes, c.metadataRevision, c.metadataDigest = 0, 0, ""
	c.metadataStartup, c.metadataClock = ReceptionStartup{}, time.Time{}
	return nil
}

// ImportChunks parses at most eight chunks outside the cache lock, then commits
// encoded bodies and their rows atomically after the recording fence. An exact
// repeated body is reused; changed/overlapping/evicted intervals cannot revive.
func (c *ReceptionHistoryReaderCache) ImportChunks(recordingID string, inputs []ReceptionHistoryChunkInput) error {
	if c == nil {
		return errors.New("source_unavailable")
	}
	if len(inputs) > 8 {
		return errors.New("response_limit")
	}
	if !c.importMu.TryLock() {
		return errors.New("source_busy")
	}
	defer c.importMu.Unlock()
	var prepared [8]PreparedReceptionChunk
	var rows [8]receptionChunkRows
	for i, input := range inputs {
		var err error
		prepared[i], rows[i], err = decodeReceptionHistoryChunk(input.Descriptor, input.Body)
		if err != nil {
			return err
		}
	}
	return c.installPreparedChunks(recordingID, prepared[:len(inputs)], rows[:len(inputs)])
}

// installPreparedChunks is shared by ImportChunks and the single collector.
// Both supply detached pairs from decodeReceptionHistoryChunk; no JSON decode,
// hash or encoding occurs under the outer status gate or this cache lock.
func (c *ReceptionHistoryReaderCache) installPreparedChunks(recordingID string, prepared []PreparedReceptionChunk, rows []receptionChunkRows) error {
	if c == nil {
		return errors.New("source_unavailable")
	}
	if len(prepared) > 8 || len(prepared) != len(rows) {
		return errors.New("response_limit")
	}
	for i, p := range prepared {
		if rows[i].count != int(p.descriptor.Count) {
			return errors.New("source_invalid")
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.encoded.InstallChunks(recordingID, prepared); err != nil {
		return err
	}
	// The encoded ring's indices are stable while mu is held; it is not exposed
	// to any other writer. Reuse its exact atomic overlap/retention decisions.
	for i, p := range prepared {
		for j := 0; j < c.encoded.count; j++ {
			index := (c.encoded.first + j) % maxReceptionChunks
			if c.encoded.chunks[index].descriptor.Digest == p.descriptor.Digest {
				c.rows[index] = rows[i]
				break
			}
		}
	}
	return nil
}

// ChunkBody returns one immutable string and cannot pin the encoded dictionary.
func (c *ReceptionHistoryReaderCache) ChunkBody(recordingID, digest string) (string, bool) {
	if c == nil {
		return "", false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.encoded.ChunkBody(recordingID, digest)
}

// Index copies at most eight descriptors and the exact local availability gap.
func (c *ReceptionHistoryReaderCache) Index(recordingID string, after uint64) (ReceptionHistoryIndex, bool, bool) {
	if c == nil {
		return ReceptionHistoryIndex{}, false, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.encoded.Index(recordingID, after)
}

// Cursor copies the next at most128 occurrences, in sequence rather than date
// order. History does not acquire a live source lease by being read.
func (c *ReceptionHistoryReaderCache) Cursor(ctx context.Context, recordingID string, after uint64) (ReceptionHistoryRead, error) {
	return c.scan(ctx, recordingID, after, time.Time{}, time.Time{}, time.Now().Add(receptionReaderScanBudget))
}

// Query applies an exact original UTC [start,end) filter over at most sixty
// seconds. It still scans nonmatching future/reversed dates to advance through
// every sequence. A parser at the route boundary owns canonical URL spelling.
func (c *ReceptionHistoryReaderCache) Query(ctx context.Context, recordingID string, start, end time.Time, after uint64) (ReceptionHistoryRead, error) {
	if !validReceptionQueryTime(start) || !validReceptionQueryTime(end) {
		return ReceptionHistoryRead{}, errors.New("invalid_query")
	}
	start, end = start.UTC(), end.UTC()
	if !start.Before(end) || end.Sub(start) > time.Minute {
		return ReceptionHistoryRead{}, errors.New("invalid_query")
	}
	return c.scan(ctx, recordingID, after, start, end, time.Now().Add(receptionReaderScanBudget))
}

func validReceptionQueryTime(at time.Time) bool {
	_, offset := at.Zone()
	return at.Year() >= 1 && at.Year() <= 9999 && offset == 0
}

func (c *ReceptionHistoryReaderCache) scan(ctx context.Context, recordingID string, after uint64, start, end, deadline time.Time) (ReceptionHistoryRead, error) {
	out := ReceptionHistoryRead{RecordingID: recordingID, After: after, ScannedThrough: after}
	if c == nil {
		return out, errors.New("source_unavailable")
	}
	if !c.scanMu.TryLock() {
		return out, errors.New("query_budget")
	}
	defer c.scanMu.Unlock()
	if !c.mu.TryRLock() {
		return out, errors.New("query_budget")
	}
	defer c.mu.RUnlock()
	if recordingID != c.encoded.recordingID {
		return out, errors.New("binding_changed")
	}
	if ctx.Err() != nil {
		return out, ctx.Err()
	}
	if !time.Now().Before(deadline) {
		return out, errors.New("query_budget")
	}
	if c.encoded.count == 0 {
		return out, nil
	}
	first := c.encoded.chunks[c.encoded.first].descriptor.FirstSequence
	last := c.encoded.chunks[(c.encoded.first+c.encoded.count-1)%maxReceptionChunks].descriptor.LastSequence
	out.CacheBounds = ReceptionSequenceBounds{First: &first, Last: &last}
	filtered := !end.IsZero()
	for i := 0; i < c.encoded.count; i++ {
		index := (c.encoded.first + i) % maxReceptionChunks
		d := c.encoded.chunks[index].descriptor
		if d.LastSequence <= after {
			continue
		}
		out.ScannedChunks++
		if d.FirstSequence > out.ScannedThrough && d.FirstSequence-out.ScannedThrough > 1 {
			next := d.FirstSequence
			out.NextAvailable = &next
			return receptionReadFailure(out, errors.New("cache_gap"))
		}
		for j := 0; j < c.rows[index].count; j++ {
			row := c.rows[index].values[j]
			if row.Sequence <= after {
				continue
			}
			// ponytail: one cooperative check per32 of at most32,768 rows.
			// No blocking lock wait or callback can extend this scan's budget.
			if out.ScannedRows%32 == 0 {
				if err := ctx.Err(); err != nil {
					return receptionReadFailure(out, err)
				}
				if !time.Now().Before(deadline) {
					return receptionReadFailure(out, errors.New("query_budget"))
				}
			}
			out.ScannedRows++
			out.ScannedThrough = row.Sequence
			if !filtered || !row.At.Before(start) && row.At.Before(end) {
				out.Rows[out.Count] = row
				out.Count++
				if out.Count == receptionReaderPageRows {
					if err := ctx.Err(); err != nil {
						return receptionReadFailure(out, err)
					}
					if !time.Now().Before(deadline) {
						return receptionReadFailure(out, errors.New("query_budget"))
					}
					out.More = row.Sequence < last
					return out, nil
				}
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return receptionReadFailure(out, err)
	}
	if !time.Now().Before(deadline) {
		return receptionReadFailure(out, errors.New("query_budget"))
	}
	return out, nil
}

func receptionReadFailure(out ReceptionHistoryRead, err error) (ReceptionHistoryRead, error) {
	clear(out.Rows[:])
	out.Count, out.ScannedThrough, out.More = 0, out.After, false
	return out, err
}

// InstallMetadata retains up to six completed body-free bundles. Capacity can
// retire the oldest token; thirty seconds is a maximum age, not a renewal lease.
// Revision/startup high-waters survive expiry so an old token cannot resurrect.
func (c *ReceptionHistoryReaderCache) InstallMetadata(recordingID string, p *PreparedReceptionMetadata, now time.Time) error {
	if c == nil {
		return errors.New("source_unavailable")
	}
	if p == nil || p.body == "" || p.bytes < 1 || p.bytes > MaxReceptionMetadataBytes ||
		now.IsZero() || now == now.Round(0) {
		return errors.New("source_invalid")
	}
	id, err := ReceptionRecordingID(p.manifest.Binding)
	if err != nil || id != recordingID {
		return errors.New("binding_changed")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if id != c.encoded.recordingID {
		return errors.New("binding_changed")
	}
	installedAt := now
	if now.Before(c.metadataClock) {
		now = c.metadataClock
	}
	c.metadataClock = now
	c.expireMetadata(now)
	for _, old := range c.metadata {
		if old.bundle != nil && old.bundle.manifest.Digest == p.manifest.Digest {
			if old.bundle.body != p.body {
				return errors.New("source_invalid")
			}
			return nil // first-install time is never renewed
		}
	}
	if p.manifest.Revision <= c.metadataRevision {
		return errors.New("metadata_expired")
	}
	if now.Sub(installedAt) >= receptionReaderMetadataTTL {
		return errors.New("metadata_expired")
	}
	if c.metadataRevision != 0 && !receptionStartupCompatible(c.metadataStartup, p.manifest.StartupAtCapture) {
		return errors.New("source_invalid")
	}
	slot := -1
	for i := range c.metadata {
		if c.metadata[i].bundle == nil {
			slot = i
			break
		}
		if slot < 0 || c.metadata[i].installedAt.Before(c.metadata[slot].installedAt) {
			slot = i
		}
	}
	oldBytes := 0
	if old := c.metadata[slot].bundle; old != nil {
		oldBytes = old.bytes
	}
	if c.metadataBytes-oldBytes+p.bytes > receptionReaderMetadataLimit {
		return errors.New("response_limit")
	}
	installed := *p
	installed.manifest = p.Manifest()
	installed.pages = slices.Clone(p.pages)
	c.metadata[slot] = receptionReaderMetadata{bundle: &installed, installedAt: installedAt}
	c.metadataBytes += p.bytes - oldBytes
	c.metadataRevision, c.metadataDigest = p.manifest.Revision, p.manifest.Digest
	c.metadataStartup = cloneReceptionStartup(p.manifest.StartupAtCapture)
	return nil
}

// MetadataManifest returns a detached historical token while its original
// monotonic TTL is valid. The empty token selects the latest installed bundle;
// an expired explicit token never falls back to that bundle.
func (c *ReceptionHistoryReaderCache) MetadataManifest(recordingID, digest string, now time.Time) (ReceptionMetadataManifest, bool) {
	if c == nil {
		return ReceptionMetadataManifest{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.metadataAt(recordingID, digest, now)
	if p == nil {
		return ReceptionMetadataManifest{}, false
	}
	return p.Manifest(), true
}

// MetadataPage copies one page string and its fixed descriptor under the same
// lock. Expiry/context failure returns no page, never current page zero.
func (c *ReceptionHistoryReaderCache) MetadataPage(recordingID, digest, kind string, page int, now time.Time) (string, ReceptionMetadataPageDescriptor, bool) {
	if c == nil || digest == "" {
		return "", ReceptionMetadataPageDescriptor{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.metadataAt(recordingID, digest, now)
	if p != nil {
		for i, d := range p.manifest.Pages {
			if d.Kind == kind && d.Page == page {
				return p.pages[i], d, true
			}
		}
	}
	return "", ReceptionMetadataPageDescriptor{}, false
}

func (c *ReceptionHistoryReaderCache) metadataAt(recordingID, digest string, now time.Time) *PreparedReceptionMetadata {
	if recordingID != c.encoded.recordingID || now.IsZero() || now == now.Round(0) {
		return nil
	}
	// Concurrent viewers may acquire the lock in a different order from their
	// clock reads. Use the latest witnessed monotonic instant conservatively;
	// an earlier caller cannot revive an expired token or renew its deadline.
	if now.Before(c.metadataClock) {
		now = c.metadataClock
	}
	c.metadataClock = now
	c.expireMetadata(now)
	if digest == "" {
		digest = c.metadataDigest
	}
	for _, entry := range c.metadata {
		if entry.bundle != nil && entry.bundle.manifest.Digest == digest {
			return entry.bundle
		}
	}
	return nil
}

func (c *ReceptionHistoryReaderCache) expireMetadata(now time.Time) {
	for i, entry := range c.metadata {
		if entry.bundle != nil && now.Sub(entry.installedAt) >= receptionReaderMetadataTTL {
			c.metadataBytes -= entry.bundle.bytes
			c.metadata[i] = receptionReaderMetadata{}
		}
	}
}
