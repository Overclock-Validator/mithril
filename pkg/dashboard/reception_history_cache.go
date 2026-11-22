package dashboard

import (
	"errors"
	"sync"
	"time"
)

const receptionMetadataHold = 6 * time.Second

// ReceptionHistoryCache is the node adapter's encoded-only mutable cache. One
// serial preparation owner installs <=8 bodies at a time and builds at most
// one metadata candidate. Readers obtain one body or <=8 copied descriptors,
// never a cache generation. It has no source, clock, worker or liveness lease.
type ReceptionHistoryCache struct {
	mu                  sync.RWMutex
	binding             ReceptionRecordingBinding
	recordingID         string
	chunks              [maxReceptionChunks]PreparedReceptionChunk
	first               int
	count               int
	last                uint64
	currentMetadata     *PreparedReceptionMetadata
	previousMetadata    *PreparedReceptionMetadata
	metadataInstalledAt time.Time
}

// NewReceptionHistoryCache does no work when disabled. Construction only
// validates a fixed recording binding; it never samples the source.
func NewReceptionHistoryCache(enabled bool, binding ReceptionRecordingBinding) (*ReceptionHistoryCache, error) {
	if !enabled {
		return nil, nil
	}
	id, err := ReceptionRecordingID(binding)
	if err != nil {
		return nil, err
	}
	return &ReceptionHistoryCache{binding: binding, recordingID: id}, nil
}

// Reset clears incompatible bodies and metadata in place. Same-recording
// repetition is a no-op. The caller must fence accepted node-context changes;
// a failed source read is not a replacement. Same-node generations never fall.
func (c *ReceptionHistoryCache) Reset(binding ReceptionRecordingBinding) error {
	if c == nil {
		return errors.New("source_unavailable")
	}
	id, err := ReceptionRecordingID(binding)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if id == c.recordingID {
		return nil
	}
	if binding.RunID == c.binding.RunID && binding.NodeStartedAt.Equal(c.binding.NodeStartedAt) &&
		binding.GenesisHash == c.binding.GenesisHash && binding.RecordingGeneration <= c.binding.RecordingGeneration {
		return errors.New("binding_changed")
	}
	c.binding, c.recordingID = binding, id
	c.chunks = [maxReceptionChunks]PreparedReceptionChunk{}
	c.first, c.count, c.last = 0, 0, 0
	c.currentMetadata, c.previousMetadata = nil, nil
	c.metadataInstalledAt = time.Time{}
	return nil
}

// InstallChunks atomically validates a bounded import batch against the
// current recording and retained sequence intervals. Repeated exact chunks
// are reusable; overlapping, changed or already-evicted intervals cannot revive.
func (c *ReceptionHistoryCache) InstallChunks(recordingID string, chunks []PreparedReceptionChunk) error {
	if c == nil {
		return errors.New("source_unavailable")
	}
	if len(chunks) > 8 {
		return errors.New("response_limit")
	}
	for i, p := range chunks {
		if p.body == "" || !validReceptionChunk(p.descriptor) || uint64(len(p.body)) != p.descriptor.Bytes ||
			len(p.body) > MaxReceptionCanonicalChunkBytes || i > 0 && p.descriptor.FirstSequence <= chunks[i-1].descriptor.LastSequence {
			return errors.New("source_invalid")
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if recordingID != c.recordingID {
		return errors.New("binding_changed")
	}
	last := c.last
	var reuse [8]bool
	for i, p := range chunks {
		for j := 0; j < c.count; j++ {
			old := c.chunks[(c.first+j)%len(c.chunks)]
			if p.descriptor.FirstSequence <= old.descriptor.LastSequence && p.descriptor.LastSequence >= old.descriptor.FirstSequence {
				if !sameReceptionChunk(p.descriptor, old.descriptor) || p.body != old.body {
					return errors.New("source_invalid")
				}
				reuse[i] = true
				break
			}
		}
		if !reuse[i] {
			if p.descriptor.FirstSequence <= last {
				return errors.New("cache_gap")
			}
			last = p.descriptor.LastSequence
		}
	}
	for i, p := range chunks {
		if reuse[i] {
			continue
		}
		if c.count == len(c.chunks) {
			c.chunks[c.first] = PreparedReceptionChunk{}
			c.first = (c.first + 1) % len(c.chunks)
			c.count--
		}
		c.chunks[(c.first+c.count)%len(c.chunks)] = p
		c.count++
		c.last = p.descriptor.LastSequence
	}
	return nil
}

// ChunkBody pins only one immutable string after releasing the short read
// lock. A matching digest from another recording is never sufficient.
func (c *ReceptionHistoryCache) ChunkBody(recordingID, digest string) (string, bool) {
	if c == nil {
		return "", false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if recordingID != c.recordingID {
		return "", false
	}
	for i := 0; i < c.count; i++ {
		p := c.chunks[(c.first+i)%len(c.chunks)]
		if p.descriptor.Digest == digest {
			return p.body, true
		}
	}
	return "", false
}

// Index returns at most eight detached descriptors and an explicit availability
// gap before the first returned chunk. A future cursor yields empty, not loss.
// The gap is cache coverage, never a claim that network packets were dropped.
func (c *ReceptionHistoryCache) Index(recordingID string, after uint64) (ReceptionHistoryIndex, bool, bool) {
	if c == nil {
		return ReceptionHistoryIndex{}, false, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if recordingID != c.recordingID {
		return ReceptionHistoryIndex{}, false, false
	}
	out := ReceptionHistoryIndex{RecordingID: recordingID, After: after, Chunks: make([]ReceptionChunkDescriptor, 0, 8)}
	if c.count == 0 {
		return out, false, true
	}
	first := c.chunks[c.first].descriptor.FirstSequence
	last := c.chunks[(c.first+c.count-1)%len(c.chunks)].descriptor.LastSequence
	out.CacheBounds = ReceptionSequenceBounds{First: &first, Last: &last}
	for i := 0; i < c.count && len(out.Chunks) < 8; i++ {
		d := c.chunks[(c.first+i)%len(c.chunks)].descriptor
		if d.LastSequence > after {
			out.Chunks = append(out.Chunks, d)
		}
	}
	gap := len(out.Chunks) != 0 && out.Chunks[0].FirstSequence > after && out.Chunks[0].FirstSequence-after > 1
	return out, gap, true
}

// InstallMetadata retains current and previous bundles, never their source
// metadata inputs. A changed token can replace current only after six real
// monotonic seconds. Repeated delivery cannot renew that original hold; dates
// inside a bundle remain original and may move backwards with the wall clock.
func (c *ReceptionHistoryCache) InstallMetadata(recordingID string, p *PreparedReceptionMetadata, now time.Time) error {
	if c == nil {
		return errors.New("source_unavailable")
	}
	if p == nil || p.body == "" || p.bytes > MaxReceptionMetadataBytes || now.IsZero() || now == now.Round(0) {
		return errors.New("source_invalid")
	}
	id, err := ReceptionRecordingID(p.manifest.Binding)
	if err != nil || id != recordingID {
		return errors.New("binding_changed")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if id != c.recordingID {
		return errors.New("binding_changed")
	}
	if old := c.currentMetadata; old != nil {
		if now.Before(c.metadataInstalledAt) {
			return errors.New("source_invalid")
		}
		if p.manifest.Digest == old.manifest.Digest {
			if p.body != old.body {
				return errors.New("source_invalid")
			}
			return nil
		}
		if p.manifest.Revision <= old.manifest.Revision || !receptionStartupCompatible(old.manifest.StartupAtCapture, p.manifest.StartupAtCapture) {
			return errors.New("source_invalid")
		}
		if now.Sub(c.metadataInstalledAt) < receptionMetadataHold {
			return errors.New("metadata_pending")
		}
	}
	// The caller can replace its prepared value after this call. Retain our own
	// header; its private page arrays and immutable strings need no deep copy.
	installed := *p
	c.previousMetadata, c.currentMetadata = c.currentMetadata, &installed
	c.metadataInstalledAt = now
	return nil
}

// MetadataManifest copies only the current body-free manifest. A historical
// bundle's availability does not assert recorder liveness.
func (c *ReceptionHistoryCache) MetadataManifest(recordingID string) (ReceptionMetadataManifest, bool) {
	if c == nil {
		return ReceptionMetadataManifest{}, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if recordingID != c.recordingID || c.currentMetadata == nil {
		return ReceptionMetadataManifest{}, false
	}
	return c.currentMetadata.Manifest(), true
}

// MetadataPage returns one string from either retained bundle. A retired token
// cannot silently resolve to the current page of the same kind and number.
func (c *ReceptionHistoryCache) MetadataPage(recordingID, digest, kind string, page int) (string, bool) {
	if c == nil {
		return "", false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if recordingID != c.recordingID {
		return "", false
	}
	for _, p := range [2]*PreparedReceptionMetadata{c.currentMetadata, c.previousMetadata} {
		if p != nil && p.manifest.Digest == digest {
			return p.Page(kind, page)
		}
	}
	return "", false
}

func sameReceptionChunk(a, b ReceptionChunkDescriptor) bool {
	return a.FirstSequence == b.FirstSequence && a.LastSequence == b.LastSequence && a.Count == b.Count &&
		a.MinAt.Equal(b.MinAt) && a.MaxAt.Equal(b.MaxAt) && a.Digest == b.Digest && a.Bytes == b.Bytes
}
