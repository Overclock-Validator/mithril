package node

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/blockprod"
	"github.com/Overclock-Validator/mithril/pkg/blockprod/scheduler"
	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/identity"
	"github.com/Overclock-Validator/mithril/pkg/tpu"
	"github.com/Overclock-Validator/mithril/pkg/tpu/dedup"
	"github.com/Overclock-Validator/mithril/pkg/tpu/pipeline"
	"github.com/Overclock-Validator/mithril/pkg/tpu/quicserver"
	"github.com/Overclock-Validator/mithril/pkg/tpu/quicserver/testutils"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
)

func TestDashboardPipelineRealQUICTrafficPublicProjection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	holder, err := identity.NewEphemeralHolder()
	if err != nil {
		t.Fatal(err)
	}
	sched := scheduler.NewWithConfig(blockprod.NewController(), scheduler.Config{MaxBufferedTransactions: 4})
	sched.Start(ctx)
	defer sched.Stop()
	service, err := tpu.Start(ctx, tpu.Config{
		Name: "dashboard-traffic-test", ListenAddr: "127.0.0.1:0", Identity: holder,
		QUIC: quicserver.ServerConfig{MaxConnections: 2, MaxConnectionsPerIP: 2, MaxStreamsPerConnection: 2},
		Pipeline: pipeline.Config{SigverifyWorkers: 1, DedupCacheCapacity: 16, IngressCap: 4,
			DedupOutCap: 4, VerifiedCap: 4, Sink: sched},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		shutdown, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		if err := service.Stop(shutdown); err != nil {
			t.Errorf("stop disposable TPU: %v", err)
		}
	}()
	addr, err := service.AdvertisedQUICAddr()
	if err != nil {
		t.Fatal(err)
	}
	client, err := testutils.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	request, stop := context.WithTimeout(ctx, 3*time.Second)
	defer stop()
	conn, err := client.Dial(request, addr.String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseWithError(0, "done")
	wire := txfixture.MustSignedTransferWire(0)
	if err := client.Send(request, conn, wire); err != nil {
		t.Fatal(err)
	}
	// No working bank: a verified transaction is buffered, never admitted.
	deadline := time.Now().Add(3 * time.Second)
	for sched.Stats().Buffered != 1 || sched.Buffered() != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("transaction not buffered: TPU=%+v scheduler=%+v", service.Stats(), sched.Stats())
		}
		time.Sleep(5 * time.Millisecond)
	}
	d := &nodeDashboard{state: dashboard.Snapshot{RunID: "pipeline-traffic", StartedAt: time.Now().UTC(), Mode: "validator", Phase: "starting"}, identity: holder}
	d.pipelineStarted(service, sched)
	private := d.snapshot().Pipeline
	if private == nil || private.QUIC == nil || private.Ingress.EnqueuedPackets != 1 || private.Ingress.EnqueuedBytes != uint64(len(wire)) ||
		private.Dedup.InPackets != 1 || private.Dedup.OutPackets != 1 || private.Dedup.OutBytes != uint64(len(wire)) ||
		private.Sigverify.VerifiedPackets != 1 || private.Sigverify.VerifiedBytes != uint64(len(wire)) ||
		private.Scheduler.InPackets != 1 || private.Scheduler.InBytes != uint64(len(wire)) || private.Scheduler.BufferedTotal != 1 || private.Scheduler.Accepted != 0 ||
		private.Scheduler.DroppedExpired == nil || *private.Scheduler.DroppedExpired != 0 || private.Scheduler.EvictedCapacity == nil || *private.Scheduler.EvictedCapacity != 0 ||
		private.QUIC.AcceptedConnections != 1 || private.QUIC.AcceptedStreams != 1 {
		t.Fatalf("real traffic mapping: %+v", private)
	}
	// Keep the Unix path short enough for Darwin's sockaddr_un limit.
	dir, err := os.MkdirTemp("/tmp", "dashboard-pipeline-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "node.sock")
	feed, err := dashboard.ListenFeed(ctx, socket, d.snapshot)
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close()
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := reserved.Addr().String()
	if err := reserved.Close(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- dashboard.ServePublic(ctx, socket, address, "https://pipeline.invalid") }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("stop public reader: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("public reader did not stop")
		}
	}()
	httpClient := &http.Client{Timeout: time.Second}
	defer httpClient.CloseIdleConnections()
	deadline = time.Now().Add(4 * time.Second)
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+"/api/v1/status", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Host = "pipeline.invalid"
		response, err := httpClient.Do(req)
		var public dashboard.PublicSnapshot
		if err == nil {
			err = json.NewDecoder(io.LimitReader(response.Body, dashboard.MaxSnapshotBytes+1)).Decode(&public)
			response.Body.Close()
		}
		if err == nil && public.SourceState == "fresh" && public.Pipeline != nil {
			p := public.Pipeline
			if public.Node == nil || public.Node.RunID != "pipeline-traffic" || public.Node.Identity == nil || *public.Node.Identity != holder.PublicKey().String() ||
				p.State != "fresh" || p.SampledAt.IsZero() || p.IngressPackets != 1 || p.IngressBytes != uint64(len(wire)) ||
				p.PassedDedupPackets != 1 || p.VerifiedPackets != 1 || p.DuplicateDrops != 0 ||
				p.InvalidWireDrops != 0 || p.InvalidSignatureDrops != 0 || p.SchedulerBufferedTotal != 1 ||
				p.SchedulerQueueDepth == nil || *p.SchedulerQueueDepth != 1 || p.BankAdmissions != 0 ||
				p.SchedulerExpiredRemovals == nil || *p.SchedulerExpiredRemovals != 0 || p.SchedulerReceiveEvictions == nil || *p.SchedulerReceiveEvictions != 0 ||
				p.QUICOpenConnections == nil || *p.QUICOpenConnections != 1 || p.QUICActiveStreams == nil ||
				p.QUICStreamBufferDrops == nil || *p.QUICStreamBufferDrops != 0 || !p.StartedAt.Equal(private.StartedAt) {
				t.Fatalf("real traffic public projection: %+v", p)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("public pipeline unavailable: state=%s err=%v", public.SourceState, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPipelineMeasurementCopiesExistingCounters(t *testing.T) {
	now := time.Date(2026, 10, 30, 12, 0, 0, 123, time.FixedZone("offset", 19800))
	started := now.Add(-time.Minute)
	q := &quicserver.ServerStats{}
	q.TotalNewConnections.Store(1<<53 + 1)
	q.TotalConnections.Store(999) // Different producer: do not substitute it.
	q.OpenConnections.Store(2)
	q.TotalNewStreams.Store(3)
	q.ActiveStreams.Store(4)
	q.InvalidStreamSize.Store(5)
	q.TotalStreamReadErrors.Store(6)
	q.TotalStreamReadTimeout.Store(7)
	q.ConnectionSetupError.Store(8)
	q.RefusedTooManyOpen.Store(9)
	q.RefusedPerIP.Store(10)
	q.ConnectionAddFailed.Store(11)
	stats := tpu.Stats{QUIC: q, Pipeline: pipeline.Stats{
		Ingress: pipeline.IngressStats{EnqueuedPackets: 1<<53 + 2, EnqueuedBytes: 13, DroppedFull: 14},
		Dedup: dedup.Stats{InPackets: 15, InBytes: 16, OutPackets: 17, OutBytes: 18,
			DroppedSanitize: 19, DroppedUnsupportedVersion: 20, DroppedDedup: 21},
		Sigverify: pipeline.SigverifyStats{InPackets: 22, InBytes: 23, VerifiedPackets: 24, VerifiedBytes: 25, DroppedSigverify: 26},
	}}
	s := scheduler.Stats{InPackets: 27, InBytes: 28, Buffered: 1<<53 + 3, Accepted: 30,
		DroppedParse: 31, DroppedVote: 32, DroppedDuplicate: 33, DroppedCapacity: 34, EvictedCapacity: 35,
		DroppedExpired: 36, DroppedAlreadyProcessed: 37, DroppedAccountLocks: 38, DroppedLookup: 39,
		DroppedNoBank: 40, DroppedCost: 41, DroppedExecution: 42}
	depth := uint64(43)
	evicted, expired := uint64(35), uint64(36)
	want := &dashboard.Pipeline{
		SampledAt: now.UTC(), StartedAt: started.UTC(),
		Ingress:   dashboard.PipelineIngress{EnqueuedPackets: 1<<53 + 2, EnqueuedBytes: 13, DroppedFull: 14},
		Dedup:     dashboard.PipelineDedup{InPackets: 15, InBytes: 16, OutPackets: 17, OutBytes: 18, DroppedSanitize: 19, DroppedUnsupportedVersion: 20, DroppedDedup: 21},
		Sigverify: dashboard.PipelineSigverify{InPackets: 22, InBytes: 23, VerifiedPackets: 24, VerifiedBytes: 25, DroppedSigverify: 26},
		QUIC: &dashboard.PipelineQUIC{AcceptedConnections: 1<<53 + 1, OpenConnections: 2, AcceptedStreams: 3, ActiveStreams: 4,
			InvalidStreamSize: 5, ReadErrors: 6, ReadTimeouts: 7, AcceptErrors: 8, RefusedTooManyOpen: 9, RefusedPerIP: 10, StreamBufferDrops: 11},
		Scheduler: dashboard.PipelineScheduler{InPackets: 27, InBytes: 28, BufferedTotal: 1<<53 + 3, QueueDepth: &depth, Accepted: 30,
			DroppedParse: 31, DroppedVote: 32, DroppedDuplicate: 33, DroppedCapacity: 34, EvictedCapacity: &evicted,
			DroppedExpired: &expired, DroppedAlreadyProcessed: 37, DroppedAccountLocks: 38, DroppedLookup: 39,
			DroppedNoBank: 40, DroppedCost: 41, DroppedExecution: 42},
	}
	m := pipelineMeasurement(now, started, stats, s, int(depth))
	if !reflect.DeepEqual(m, want) {
		t.Fatalf("pipeline = %+v, want %+v", m, want)
	}
	q.TotalNewConnections.Store(0)
	stats.Pipeline.Ingress.EnqueuedPackets = 0
	s.Buffered = 0
	s.EvictedCapacity, s.DroppedExpired = 0, 0
	if !reflect.DeepEqual(m, want) {
		t.Fatal("source mutation changed a published observation")
	}
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	for _, exact := range []string{`"accepted_connections":"9007199254740993"`, `"enqueued_packets":"9007199254740994"`, `"buffered_total":"9007199254740995"`, `"queue_depth":"43"`} {
		if !strings.Contains(string(data), exact) {
			t.Fatalf("integer precision or queue semantics lost: %s", data)
		}
	}
}

func TestPipelineMeasurementPreservesUnavailableAndZero(t *testing.T) {
	now := time.Now()
	started := now.Add(-time.Minute)
	m := pipelineMeasurement(now, started, tpu.Stats{}, scheduler.Stats{}, -1)
	if m.QUIC != nil || m.Scheduler.QueueDepth != nil || m.Scheduler.DroppedExpired == nil || *m.Scheduler.DroppedExpired != 0 ||
		m.Scheduler.EvictedCapacity == nil || *m.Scheduler.EvictedCapacity != 0 {
		t.Fatalf("unavailable values fabricated: %+v", m)
	}
	m = pipelineMeasurement(now, started.Add(time.Second), tpu.Stats{}, scheduler.Stats{Buffered: 100}, 0)
	if m.Scheduler.QueueDepth == nil || *m.Scheduler.QueueDepth != 0 || m.Scheduler.BufferedTotal != 100 || m.StartedAt.Equal(started) {
		t.Fatalf("empty queue confused with cumulative buffering or prior instance: %+v", m)
	}
	data, err := json.Marshal(m)
	if err != nil || !strings.Contains(string(data), `"quic":null`) || !strings.Contains(string(data), `"queue_depth":"0"`) {
		t.Fatalf("zero/unavailable JSON semantics: %s, %v", data, err)
	}
}

func TestPipelineMeasurementReadsQUICAtomics(t *testing.T) {
	q := &quicserver.ServerStats{}
	var writer sync.WaitGroup
	writer.Go(func() {
		for i := range uint64(1000) {
			q.TotalNewConnections.Store(i)
			q.OpenConnections.Store(i)
			q.TotalNewStreams.Store(i)
			q.ActiveStreams.Store(i)
		}
	})
	for range 1000 {
		pipelineMeasurement(time.Now(), time.Now(), tpu.Stats{QUIC: q}, scheduler.Stats{}, 0)
	}
	writer.Wait()
}
