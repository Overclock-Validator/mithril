package node

import (
	"encoding/hex"
	"errors"
	"strings"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/blockstream"
	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
)

func validateDashboardShredHistory(enabled bool, socket string, replayDetails, networkDetails bool, source string) error {
	if !enabled {
		return nil
	}
	if strings.TrimSpace(socket) == "" {
		return errors.New("dashboard.shred_history requires dashboard.socket")
	}
	if !replayDetails {
		return errors.New("dashboard.shred_history requires dashboard.replay_details")
	}
	if !networkDetails {
		return errors.New("dashboard.shred_history requires dashboard.network_details")
	}
	if source != "turbine" {
		return errors.New("dashboard.shred_history requires block.source=turbine")
	}
	return nil
}

// nodeReceptionHistory belongs to one node run. The binding and owner never
// change; stopping leaves their terminal history readable but cannot restart it.
type nodeReceptionHistory struct {
	binding dashboard.ReceptionRecordingBinding
	owner   *turbine.ShredHistory
}

// nodePrewarmLifetime keeps the existing manifest callbacks serialized, including
// StartTurbinePrewarm's existing readiness wait (up to 15 seconds). Closing waits
// for that critical section; receiver and recorder joins happen after unlocking.
type nodePrewarmLifetime struct {
	mu         sync.Mutex
	collector  *blockstream.TurbinePrewarm
	closing    bool
	finishOnce sync.Once
	drained    bool
}

// forReplay closes optional history admission before handing the current
// prewarm to replay. The lifetime retains it for incomplete-bootstrap cleanup.
func (p *nodePrewarmLifetime) forReplay(closeAdmission bool) *blockstream.TurbinePrewarm {
	p.mu.Lock()
	defer p.mu.Unlock()
	if closeAdmission {
		p.closing = true
	}
	return p.collector
}

// finishReceptionHistory stops and joins prewarm before finishing the recorder.
// producersJoined is true only after the replay wrapper has returned and joined
// every BlockSource attempt. The fallback interrupts instead of promising drain.
// Nil/default-off history does not change prewarm's existing shutdown behavior.
func (d *nodeDashboard) finishReceptionHistory(p *nodePrewarmLifetime, producersJoined bool) bool {
	if d == nil {
		return true
	}
	d.mu.Lock()
	owned := d.reception != nil
	d.mu.Unlock()
	if !owned {
		return true
	}
	if p == nil {
		if producersJoined {
			return d.closeReceptionHistoryAfterProducers()
		}
		d.interruptReceptionHistory()
		return false
	}
	// A second caller must not drain while the first caller still joins prewarm.
	p.finishOnce.Do(func() {
		p.mu.Lock()
		p.closing = true
		collector := p.collector
		p.collector = nil
		p.mu.Unlock()
		if collector != nil {
			// Opted-in Stop joins both the drainer and receiver Run retirement.
			collector.Stop()
		}
		if producersJoined {
			p.drained = d.closeReceptionHistoryAfterProducers()
		} else {
			d.interruptReceptionHistory()
		}
	})
	return p.drained
}

// enableReceptionHistory is startup-only. It exposes callbacks before genesis
// is known without creating a recorder, reading a clock or starting a worker.
func (d *nodeDashboard) enableReceptionHistory() *dashboard.ReceptionHistorySampler {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.receptionStopped || d.state.Phase == "stopping" {
		return nil
	}
	d.receptionEnabled = true
	return &dashboard.ReceptionHistorySampler{Metadata: d.sampleReceptionHistory, ChunkBody: d.receptionHistoryChunk}
}

// startReceptionHistory is called explicitly after genesis/ledger validation,
// before any prewarm or live receiver. It never replaces an existing recorder.
func (d *nodeDashboard) startReceptionHistory() (*turbine.ShredHistory, error) {
	if d == nil {
		return nil, nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.receptionEnabled {
		return nil, nil
	}
	if d.receptionStopped || d.state.Phase == "stopping" {
		return nil, errors.New("source_unavailable")
	}
	if d.reception != nil {
		if !receptionHistoryMatchesNode(d.state, d.reception.binding) {
			return nil, errors.New("binding_changed")
		}
		return d.reception.owner, nil
	}
	if d.state.GenesisHash == nil {
		return nil, errors.New("waiting_source")
	}
	b := dashboard.ReceptionRecordingBinding{RunID: d.state.RunID, NodeStartedAt: d.state.StartedAt,
		GenesisHash: *d.state.GenesisHash, RecordingGeneration: 1}
	if _, err := dashboard.ReceptionRecordingID(b); err != nil || !d.runStartedAt.Equal(b.NodeStartedAt) {
		return nil, errors.New("source_invalid")
	}
	genesis, err := solana.HashFromBase58(b.GenesisHash)
	if err != nil {
		return nil, errors.New("source_invalid")
	}
	// Startup allocation is serialized with stop. Samplers never construct an
	// owner, and a concurrent close cannot miss a newly started consumer.
	h, err := turbine.NewShredHistory(turbine.ShredHistoryBinding{
		RunID: b.RunID, NodeStartedAt: d.runStartedAt, GenesisHash: genesis,
	})
	if err != nil {
		return nil, errors.New("source_invalid")
	}
	d.reception = &nodeReceptionHistory{binding: b, owner: h}
	return h, nil
}

// closeReceptionHistoryAfterProducers requires every receiver/submitter to
// have joined. False means an earlier interruption won; it is not full drain.
func (d *nodeDashboard) closeReceptionHistoryAfterProducers() bool {
	if d == nil {
		return true
	}
	d.mu.Lock()
	d.receptionStopped = true
	m := d.reception
	d.mu.Unlock()
	if m == nil {
		return true
	}
	return m.owner.CloseAfterProducers()
}

// interruptReceptionHistory joins the recorder without claiming producer join
// or complete drain. It is also the fallback used by dashboard close.
func (d *nodeDashboard) interruptReceptionHistory() {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.receptionStopped = true
	m := d.reception
	d.mu.Unlock()
	if m != nil {
		m.owner.Interrupt()
	}
}

func receptionHistoryMatchesNode(s dashboard.Snapshot, b dashboard.ReceptionRecordingBinding) bool {
	return b.RecordingGeneration == 1 && s.RunID == b.RunID && s.StartedAt.Equal(b.NodeStartedAt) &&
		s.GenesisHash != nil && *s.GenesisHash == b.GenesisHash
}

func (d *nodeDashboard) receptionHistoryOwner() (*nodeReceptionHistory, error) {
	if d == nil {
		return nil, errors.New("source_unavailable")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.receptionEnabled {
		return nil, errors.New("source_unavailable")
	}
	if d.reception == nil {
		return nil, errors.New("waiting_source")
	}
	if !receptionHistoryMatchesNode(d.state, d.reception.binding) {
		return nil, errors.New("binding_changed")
	}
	return d.reception, nil
}

func (d *nodeDashboard) receptionHistoryCurrent(m *nodeReceptionHistory) bool {
	if d == nil || m == nil {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.receptionEnabled && d.reception == m && receptionHistoryMatchesNode(d.state, m.binding)
}

// receptionHistoryCaptureCurrent checks final metadata admission separately
// from historical body availability. The capture retains its original flags.
func (d *nodeDashboard) receptionHistoryCaptureCurrent(m *nodeReceptionHistory, c dashboard.ReceptionHistoryCapture) bool {
	if d == nil || m == nil {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.receptionEnabled && d.reception == m && receptionHistoryMatchesNode(d.state, m.binding) &&
		(!d.receptionStopped || c.Closed || c.Interrupted || c.Exhausted)
}

func (d *nodeDashboard) sampleReceptionHistory() (dashboard.ReceptionHistoryCapture, error) {
	m, err := d.receptionHistoryOwner()
	if err != nil {
		return dashboard.ReceptionHistoryCapture{}, err
	}
	source, ok := m.owner.Metadata()
	if !ok {
		return dashboard.ReceptionHistoryCapture{}, errors.New("waiting_source")
	}
	c, err := receptionHistoryCapture(source, m.binding)
	if !d.receptionHistoryCaptureCurrent(m, c) {
		return dashboard.ReceptionHistoryCapture{}, errors.New("binding_changed")
	}
	return c, err
}

func (d *nodeDashboard) receptionHistoryChunk(b dashboard.ReceptionRecordingBinding, c dashboard.ReceptionChunkDescriptor) (string, bool) {
	m, err := d.receptionHistoryOwner()
	if err != nil || b != m.binding || len(c.Digest) != 64 {
		return "", false
	}
	var digest [32]byte
	if n, err := hex.Decode(digest[:], []byte(c.Digest)); err != nil || n != len(digest) || hex.EncodeToString(digest[:]) != c.Digest {
		return "", false
	}
	body, ok := m.owner.ChunkBody(c.FirstSequence, c.LastSequence, digest)
	if !d.receptionHistoryCurrent(m) || !ok || uint64(len(body)) != c.Bytes {
		return "", false
	}
	return body, true
}

// receptionHistoryCapture copies only the bounded body-free publication. Wire
// dates are UTC; PublishedAt separately retains the original monotonic clock.
func receptionHistoryCapture(s turbine.ShredHistoryMetadata, b dashboard.ReceptionRecordingBinding) (dashboard.ReceptionHistoryCapture, error) {
	if s.Binding.RunID != b.RunID || !s.Binding.NodeStartedAt.Equal(b.NodeStartedAt) ||
		s.Binding.GenesisHash.String() != b.GenesisHash || b.RecordingGeneration != 1 ||
		len(s.Sources) > 256 || len(s.Chunks) > 256 {
		return dashboard.ReceptionHistoryCapture{}, errors.New("source_invalid")
	}
	for _, ranges := range s.StartupRanges {
		if len(ranges) > 2048 {
			return dashboard.ReceptionHistoryCapture{}, errors.New("source_invalid")
		}
	}
	var markerState string
	switch s.MarkerState {
	case turbine.ShredHistoryMarkerOpen:
		markerState = "open"
	case turbine.ShredHistoryMarkerQueued:
		markerState = "queued"
	case turbine.ShredHistoryMarkerClosed:
		markerState = "closed"
	case turbine.ShredHistoryMarkerUnknown:
		markerState = "unknown"
	default:
		return dashboard.ReceptionHistoryCapture{}, errors.New("source_invalid")
	}
	c := dashboard.ReceptionHistoryCapture{
		Metadata: dashboard.ReceptionMetadata{Binding: b, PublishedAt: s.PublishedAt.UTC(),
			StartupAtCapture: dashboard.ReceptionStartup{State: markerState, PrefixCutoff: s.PrefixCutoff, Limited: s.StartupLimited,
				Losses: dashboard.ReceptionLosses{
					QueueRefusals: receptionHistoryCounter(s.QueueRefusals), InvalidInputs: receptionHistoryCounter(s.InvalidInputs),
					DescriptorEvictions: receptionHistoryCounter(s.DescriptorEvictions), EvictedChunks: receptionHistoryCounter(s.EvictedChunks),
					EvictedRows: receptionHistoryCounter(s.EvictedRows), ChunkRefusals: receptionHistoryCounter(s.ChunkRefusals),
				}}, Sources: make([]dashboard.ReceptionSource, len(s.Sources))},
		Chunks: make([]dashboard.ReceptionChunkDescriptor, len(s.Chunks)), PublishedAt: s.PublishedAt,
		ConsumedThrough: s.LastSequence, Closed: s.Closed, Interrupted: s.Interrupted, Exhausted: s.Exhausted,
	}
	if m := s.Marker; m != nil {
		c.Metadata.StartupAtCapture.Marker = &dashboard.ReceptionMarker{At: m.At.UTC(), SourceGeneration: m.SourceGeneration,
			Slot: m.Slot, Bankhash: m.Bankhash.String(), ReplayAttempt: m.ReplayAttempt, Rewinds: m.Rewinds, CompletedIndex: m.CompletedIndex}
	}
	for i, source := range s.Sources {
		var kind string
		switch source.Kind {
		case turbine.ShredHistoryPrewarm:
			kind = "prewarm"
		case turbine.ShredHistoryLive:
			kind = "live"
		default:
			return dashboard.ReceptionHistoryCapture{}, errors.New("source_invalid")
		}
		c.Metadata.Sources[i] = dashboard.ReceptionSource{Generation: source.Generation, StartedAt: source.StartedAt.UTC(), Kind: kind, Retired: source.Retired}
	}
	for i, ranges := range s.StartupRanges {
		c.Metadata.StartupAtCapture.Losses.RangeRefusals[i] = receptionHistoryCounter(s.RangeRefusals[i])
		c.Metadata.Ranges[i] = make([]dashboard.ReceptionRange, len(ranges))
		for j, r := range ranges {
			c.Metadata.Ranges[i][j] = dashboard.ReceptionRange{First: r.First, Last: r.Last}
		}
	}
	for i, chunk := range s.Chunks {
		c.Chunks[i] = dashboard.ReceptionChunkDescriptor{FirstSequence: chunk.FirstSequence, LastSequence: chunk.LastSequence,
			Count: chunk.Count, MinAt: chunk.MinAt.UTC(), MaxAt: chunk.MaxAt.UTC(), Digest: hex.EncodeToString(chunk.Digest[:]), Bytes: chunk.Bytes}
	}
	return c, nil
}

func receptionHistoryCounter(c turbine.ShredHistoryCounter) dashboard.ReceptionCounter {
	return dashboard.ReceptionCounter{Value: c.Value, Saturated: c.Saturated}
}
