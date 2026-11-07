package replay

import (
	"crypto/ed25519"
	"encoding/json"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/blockstream"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/rpcclient"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
)

func nearReceptionInput() (CompletedBlock, time.Time, blockstream.ReceptionObservation, time.Time) {
	now := time.Now()
	b := CompletedBlock{Slot: 42, Epoch: 2, Bankhash: solana.Hash{1}, At: now.Add(-30 * time.Second)}
	r := blockstream.ReceptionObservation{StartedAt: now.Add(-time.Minute), Session: 3,
		WindowMilliseconds: uint64(turbine.ReceptionWindow / time.Millisecond), Progress: turbine.ReceptionSnapshot{
			SampledAt: now.Add(-500 * time.Millisecond).UTC(), ObservedAt: now.Add(-time.Second).UTC(),
			Slot: 46, HasSlot: true, RetainedSlots: 12, ValidUntil: now.Add(2 * time.Second)}}
	return b, now.Add(-2 * time.Minute), r, now
}

func TestReplayNearReceptionActualSourceHook(t *testing.T) {
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var call struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, req.Body, 4096)).Decode(&call); err != nil || call.Method != "getSlot" {
			t.Errorf("unexpected loopback control-plane request: method=%q error=%v", call.Method, err)
			http.Error(w, "refused", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": call.ID, "result": 41}); err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(rpc.Close)
	reserved, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	address := reserved.LocalAddr().String()
	if err := reserved.Close(); err != nil {
		t.Fatal(err)
	}
	// Disposable deterministic fixture identity; no operator key or chain state.
	key := solana.PrivateKey(ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize)))
	source := blockstream.NewBlockSource(&blockstream.BlockSourceOpts{SourceType: blockstream.BlockSourceTurbine,
		RpcClient: rpcclient.NewRpcClient(rpc.URL), StartSlot: 42, EndSlot: 43, MaxInflight: 1, TipPollMs: 500,
		DisableRPCBlockFetch: true, TurbineBindAddr: address, TurbineShredVersion: 7,
		LeaderForSlot: func(uint64) (solana.PublicKey, bool) { return key.PublicKey(), true }})
	p := diagnosticProgress(t)
	p.sourceStarted(source)
	done := make(chan struct{})
	go func() { defer close(done); source.Start() }()
	t.Cleanup(func() {
		source.Stop()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("owned block source did not stop")
		}
	})
	deadline := time.Now().Add(5 * time.Second)
	for {
		if s, ok := source.NetworkSnapshot(); ok && s.LiveConnected {
			if _, _, cached := source.CachedReceptionObservation(); cached {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("owned TVU receiver did not connect")
		}
		time.Sleep(10 * time.Millisecond)
	}
	row := metrics.BlockReplay{Slot: 42}
	p.completed(CompletedBlock{At: time.Now(), Slot: 42, Bankhash: solana.Hash{1}}, 0, false)
	p.recordReplayDiagnostic(time.Now(), &row, nil)
	if s, _ := p.ReplayDiagnosticsSnapshot(); !s.NearReception.ObservedAt.IsZero() {
		t.Fatal("no receipt became known proximity")
	}
	generator := turbine.ShredGenerator{Slot: 42, ParentSlot: 41, Version: 7}
	packets, _, _, _, err := generator.MakeShredsFromData(key, nil, false, solana.Hash{1}, 0, 0)
	if err != nil || len(packets) == 0 {
		t.Fatalf("synthetic shred generation: %v", err)
	}
	conn, err := net.Dial("udp", address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := conn.Write(packets[0]); err != nil {
		t.Fatal(err)
	}
	for {
		if _, r, ok := source.CachedReceptionObservation(); ok && r.Progress.HasSlot && r.Progress.Slot == 42 && time.Now().Before(r.Progress.ValidUntil) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("actual authenticated reception did not reach its source cache")
		}
		time.Sleep(10 * time.Millisecond)
	}
	p.recordReplayDiagnostic(time.Now(), &row, nil)
	p.skipped(0, false)
	p.recordReplayDiagnostic(time.Now(), &row, nil)
	if s, _ := p.ReplayDiagnosticsSnapshot(); !s.NearReception.ObservedAt.IsZero() {
		t.Fatal("duplicate completion or skip retroactively latched proximity")
	}
	completedAt := time.Now().Add(-time.Second)
	p.completed(CompletedBlock{At: completedAt, Slot: 42, Bankhash: solana.Hash{2}}, 0, false)
	p.recordReplayDiagnostic(time.Now(), &row, nil)
	s, _ := p.ReplayDiagnosticsSnapshot()
	if s.NearReception.ObservedAt.IsZero() || s.NearReception.Bankhash != (solana.Hash{2}) || !s.NearReception.CompletedAt.Equal(completedAt) {
		t.Fatal("actual cache to new completed-row hook did not capture the original bank")
	}
	want := s.NearReception
	p.recordReplayDiagnostic(time.Now(), &row, nil)
	if again, _ := p.ReplayDiagnosticsSnapshot(); again.NearReception != want {
		t.Fatal("duplicate row redated historical proximity")
	}
}

func TestReplayNearReceptionObservation(t *testing.T) {
	b, started, r, now := nearReceptionInput()
	d := &replayDiagnostics{}
	d.recordNearReceptionLocked(b, started, r, now)
	want := d.nearReception
	if want.Slot != 42 || want.ReceptionSlot != 46 || want.Epoch != 2 || want.Bankhash != b.Bankhash ||
		want.ReceiverSession != 3 || want.RetainedSlots != 12 || !want.CompletedAt.Equal(b.At) || !want.ObservedAt.Equal(now) ||
		!want.ReceptionSampledAt.Equal(r.Progress.SampledAt) || !want.SourceStartedAt.Equal(started) {
		t.Fatalf("historical input proximity lost original context: %+v", want)
	}
	r.Session++
	r.Progress.Slot = 42
	d.recordNearReceptionLocked(b, started, r, now.Add(time.Second))
	if d.nearReception != want {
		t.Fatal("later sample or receiver replacement rewrote the first observation")
	}
	for _, slots := range [][2]uint64{{0, 4}, {4, 0}, {math.MaxUint64, math.MaxUint64 - 4}, {math.MaxUint64 - 4, math.MaxUint64}} {
		b, started, r, now := nearReceptionInput()
		b.Slot, r.Progress.Slot = slots[0], slots[1]
		d := &replayDiagnostics{}
		d.recordNearReceptionLocked(b, started, r, now)
		if d.nearReception.ObservedAt.IsZero() {
			t.Fatalf("valid four-slot boundary rejected: %v", slots)
		}
	}
	for _, mutate := range []func(*CompletedBlock, *time.Time, *blockstream.ReceptionObservation, *time.Time){
		func(b *CompletedBlock, s *time.Time, r *blockstream.ReceptionObservation, n *time.Time) {
			b.Bankhash = solana.Hash{}
		},
		func(b *CompletedBlock, s *time.Time, r *blockstream.ReceptionObservation, n *time.Time) {
			b.At = time.Time{}
		},
		func(b *CompletedBlock, s *time.Time, r *blockstream.ReceptionObservation, n *time.Time) {
			*s = time.Time{}
		},
		func(b *CompletedBlock, s *time.Time, r *blockstream.ReceptionObservation, n *time.Time) {
			r.StartedAt = time.Time{}
		},
		func(b *CompletedBlock, s *time.Time, r *blockstream.ReceptionObservation, n *time.Time) {
			r.Session = 0
		},
		func(b *CompletedBlock, s *time.Time, r *blockstream.ReceptionObservation, n *time.Time) {
			r.WindowMilliseconds = 0
		},
		func(b *CompletedBlock, s *time.Time, r *blockstream.ReceptionObservation, n *time.Time) {
			r.Progress.HasSlot = false
		},
		func(b *CompletedBlock, s *time.Time, r *blockstream.ReceptionObservation, n *time.Time) {
			r.Progress.CoverageLimited = true
		},
		func(b *CompletedBlock, s *time.Time, r *blockstream.ReceptionObservation, n *time.Time) {
			r.Progress.RetainedSlots = 0
		},
		func(b *CompletedBlock, s *time.Time, r *blockstream.ReceptionObservation, n *time.Time) {
			r.Progress.RetainedSlots = 65
		},
		func(b *CompletedBlock, s *time.Time, r *blockstream.ReceptionObservation, n *time.Time) {
			r.Progress.SampledAt = time.Time{}
		},
		func(b *CompletedBlock, s *time.Time, r *blockstream.ReceptionObservation, n *time.Time) {
			r.Progress.ObservedAt = time.Time{}
		},
		func(b *CompletedBlock, s *time.Time, r *blockstream.ReceptionObservation, n *time.Time) {
			r.Progress.ValidUntil = *n
		},
		func(b *CompletedBlock, s *time.Time, r *blockstream.ReceptionObservation, n *time.Time) {
			r.Progress.ValidUntil = n.Add(-time.Nanosecond)
		},
		func(b *CompletedBlock, s *time.Time, r *blockstream.ReceptionObservation, n *time.Time) {
			r.Progress.ValidUntil = r.Progress.ValidUntil.UTC()
		},
		func(b *CompletedBlock, s *time.Time, r *blockstream.ReceptionObservation, n *time.Time) { *n = n.UTC() },
		func(b *CompletedBlock, s *time.Time, r *blockstream.ReceptionObservation, n *time.Time) {
			r.Progress.Slot = 47
		},
		func(b *CompletedBlock, s *time.Time, r *blockstream.ReceptionObservation, n *time.Time) {
			b.Slot = math.MaxUint64
			r.Progress.Slot = 0
		},
	} {
		b, started, r, now := nearReceptionInput()
		mutate(&b, &started, &r, &now)
		d := &replayDiagnostics{}
		d.recordNearReceptionLocked(b, started, r, now)
		if !d.nearReception.ObservedAt.IsZero() {
			t.Fatal("invalid or stale reception recorded a milestone")
		}
	}
}

func TestReplayNearReceptionResetAndDisabled(t *testing.T) {
	p := diagnosticProgress(t)
	before, _ := p.Snapshot()
	d := p.diagnostics.Load()
	b, started, r, now := nearReceptionInput()
	d.mu.Lock()
	d.recordNearReceptionLocked(b, started, r, now)
	d.mu.Unlock()
	s, _ := p.ReplayDiagnosticsSnapshot()
	if s.NearReception.ObservedAt.IsZero() {
		t.Fatal("detached diagnostics omitted the observation")
	}
	s.NearReception.Slot++
	if next, _ := p.ReplayDiagnosticsSnapshot(); next.NearReception.Slot != 42 {
		t.Fatal("returned observation aliases diagnostics")
	}
	if after, _ := p.Snapshot(); after.ReadyAt != before.ReadyAt || after.Phase != before.Phase || after.ExecutedBlocks != before.ExecutedBlocks {
		t.Fatal("telemetry changed replay readiness or progress")
	}
	p.rewound()
	if next, _ := p.ReplayDiagnosticsSnapshot(); !next.NearReception.ObservedAt.IsZero() {
		t.Fatal("rewind retained a prior context's observation")
	}
	var absent *Progress
	var disabled Progress
	var row metrics.BlockReplay
	if n := testing.AllocsPerRun(1000, func() {
		absent.recordReplayDiagnostic(now, &row, nil)
		disabled.recordReplayDiagnostic(now, &row, nil)
	}); n != 0 {
		t.Fatalf("disabled diagnostics allocated: %v", n)
	}
}
