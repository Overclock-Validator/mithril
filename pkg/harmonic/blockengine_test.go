package harmonic

import (
	"context"
	"sync"
	"testing"
	"time"

	authpb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/auth"
	blockpb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/block"
	blockenginepb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/blockengine"
	packetpb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/packet"
	tpupacket "github.com/Overclock-Validator/mithril/pkg/tpu/packet"
	"github.com/Overclock-Validator/mithril/pkg/version"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type blockRecorder struct {
	mu       sync.Mutex
	slots    []uint64
	arrivals []time.Time
	lost     []uint64
}

func (r *blockRecorder) HandleBlock(block *blockpb.Block, arrival time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.slots = append(r.slots, block.GetSlot())
	r.arrivals = append(r.arrivals, arrival)
}

func (r *blockRecorder) StreamLost(slot uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lost = append(r.lost, slot)
}

func (r *blockRecorder) received() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uint64(nil), r.slots...)
}

func (r *blockRecorder) lostSlots() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uint64(nil), r.lost...)
}

type healthRecorder struct {
	mu      sync.Mutex
	changes []bool
}

func (h *healthRecorder) record(healthy bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.changes = append(h.changes, healthy)
}

func (h *healthRecorder) all() []bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]bool(nil), h.changes...)
}

type blockEngineHarness struct {
	be      *mockBlockEngine
	session *BlockEngineSession
	packets *packetRecorder
	blocks  *blockRecorder
	health  *healthRecorder
	start   func()
}

// newBlockEngineHarness builds a session against a mock block engine; call
// start to run it, after configuring the mock or queueing notices.
func newBlockEngineHarness(t *testing.T, feeInfoInterval time.Duration) *blockEngineHarness {
	t.Helper()
	h := &blockEngineHarness{be: newMockBlockEngine(), packets: &packetRecorder{}, blocks: &blockRecorder{}, health: &healthRecorder{}}
	dial := serveBufconn(t, func(s *grpc.Server) {
		authpb.RegisterAuthServiceServer(s, newMockAuth(time.Now))
		blockenginepb.RegisterBlockEngineValidatorServer(s, h.be)
	})
	h.session = NewBlockEngineSession(BlockEngineConfig{
		Endpoint:        dial.Endpoint,
		Dialer:          dial.Dialer,
		Identity:        rfc8032Key(t),
		Packets:         h.packets,
		Blocks:          h.blocks,
		OnHealth:        h.health.record,
		FeeInfoInterval: feeInfoInterval,
		MaxBackoff:      20 * time.Millisecond,
	})
	h.start = func() {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			h.session.Run(ctx)
			close(done)
		}()
		t.Cleanup(func() {
			cancel()
			<-done
		})
	}
	return h
}

func eventually(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	require.Eventually(t, cond, 2*time.Second, 5*time.Millisecond, msg)
}

func TestParseFeeInfo(t *testing.T) {
	got, err := ParseFeeInfo(&blockenginepb.BlockBuilderFeeInfoResponse{Pubkey: "11111111111111111111111111111112", Commission: 100})
	require.NoError(t, err)
	assert.Equal(t, solana.MustPublicKeyFromBase58("11111111111111111111111111111112"), got.Pubkey)
	assert.Equal(t, uint64(100), got.Commission, "commission is a percentage, up to 100")
	for name, resp := range map[string]*blockenginepb.BlockBuilderFeeInfoResponse{
		"not base58":    {Pubkey: "0OIl", Commission: 5},
		"wrong length":  {Pubkey: "1111", Commission: 5},
		"all zero":      {Pubkey: "11111111111111111111111111111111", Commission: 5},
		"over 100":      {Pubkey: "11111111111111111111111111111112", Commission: 101},
		"basis points?": {Pubkey: "11111111111111111111111111111112", Commission: 500},
	} {
		_, err := ParseFeeInfo(resp)
		assert.Errorf(t, err, "%s", name)
	}
}

func TestBlockEngineSessionEstablishesAndStreams(t *testing.T) {
	h := newBlockEngineHarness(t, time.Hour)
	h.start()
	eventually(t, h.session.Healthy, "the session becomes healthy")
	assert.Equal(t, []bool{true}, h.health.all())
	fee, ok := h.session.FeeInfo()
	require.True(t, ok)
	assert.Equal(t, uint64(5), fee.Commission)

	h.be.mu.Lock()
	assert.Equal(t, []blockenginepb.SchedulingStrategy{blockenginepb.SchedulingStrategy_SCHEDULING_STRATEGY_FBA}, h.be.strategies)
	require.Len(t, h.be.blockRequests, 1)
	assert.Equal(t, version.Semver(), h.be.blockRequests[0].GetVersion())
	assert.Equal(t, version.Commit(), h.be.blockRequests[0].GetCommitHash())
	h.be.mu.Unlock()

	h.be.packets <- &blockenginepb.SubscribePacketsResponse{Batch: &packetpb.PacketBatch{Packets: []*packetpb.Packet{
		{Data: []byte{7}}, {Data: nil}, {Data: make([]byte, tpupacket.DataSize+1)},
	}}}
	eventually(t, func() bool { return h.packets.count() == 1 }, "the valid packet is submitted")
	assert.Equal(t, []tpupacket.Source{tpupacket.SourceBlockEngine}, h.packets.sources)

	h.be.blocks <- &blockpb.Block{Slot: 10}
	h.be.blocks <- &blockpb.Block{Slot: 11}
	eventually(t, func() bool { return len(h.blocks.received()) == 2 }, "blocks are delivered")
	assert.Equal(t, []uint64{10, 11}, h.blocks.received(), "in stream order")
	assert.False(t, h.blocks.arrivals[0].IsZero())

	stats := h.session.Stats()
	assert.Equal(t, uint64(1), stats.Sessions)
	assert.Zero(t, stats.Resets)
	assert.Equal(t, uint64(2), stats.DroppedSize)
}

func TestBlockEngineSessionToleratesSetStrategyErrorsButNotAuthErrors(t *testing.T) {
	h := newBlockEngineHarness(t, time.Hour)
	h.be.strategyErrs = []error{status.Error(codes.Unimplemented, "old block engine")}
	h.start()
	eventually(t, h.session.Healthy, "an unsupported SetStrategy does not block the session")
	assert.Equal(t, uint64(1), h.session.Stats().StrategyErrors)
	assert.Zero(t, h.session.Stats().Resets)

	auth := newBlockEngineHarness(t, time.Hour)
	auth.be.strategyErrs = []error{status.Error(codes.Unauthenticated, "token rejected")}
	auth.start()
	eventually(t, auth.session.Healthy, "an auth error re-authenticates on a new session")
	assert.Equal(t, uint64(1), auth.session.Stats().Resets)
	assert.Equal(t, uint64(2), auth.session.Stats().Sessions)
}

func TestBlockEngineFeeInfoGatesHealthAndKeepsTheLastGoodValue(t *testing.T) {
	h := newBlockEngineHarness(t, 30*time.Millisecond)
	h.be.feeInfo = &blockenginepb.BlockBuilderFeeInfoResponse{Pubkey: "11111111111111111111111111111111", Commission: 5}
	h.start()
	eventually(t, func() bool { return h.session.Stats().InvalidFeeInfo >= 2 }, "fee info keeps being refreshed")
	assert.False(t, h.session.Healthy(), "no valid fee info, not healthy")
	_, ok := h.session.FeeInfo()
	assert.False(t, ok)

	h.be.mu.Lock()
	h.be.feeInfo = &blockenginepb.BlockBuilderFeeInfoResponse{Pubkey: "11111111111111111111111111111112", Commission: 7}
	h.be.mu.Unlock()
	eventually(t, h.session.Healthy, "valid fee info makes the session healthy")

	h.be.mu.Lock()
	h.be.feeInfo = &blockenginepb.BlockBuilderFeeInfoResponse{Pubkey: "11111111111111111111111111111112", Commission: 250}
	h.be.mu.Unlock()
	invalid := h.session.Stats().InvalidFeeInfo
	eventually(t, func() bool { return h.session.Stats().InvalidFeeInfo > invalid }, "the bad value is fetched")
	fee, ok := h.session.FeeInfo()
	require.True(t, ok)
	assert.Equal(t, uint64(7), fee.Commission, "a bad refresh keeps the previous value")
	assert.True(t, h.session.Healthy())
	assert.Zero(t, h.session.Stats().Resets)
}

func TestBlockEngineLeaderNotices(t *testing.T) {
	t.Run("queued before the block stream is live, latest wins, ended ones are dropped", func(t *testing.T) {
		h := newBlockEngineHarness(t, time.Hour)
		h.session.NotifyLeaderSlot(99, time.Now().Add(-time.Second))
		h.session.NotifyLeaderSlot(100, time.Now().Add(time.Hour))
		h.session.NotifyLeaderSlot(101, time.Now().Add(time.Hour))
		h.start()
		eventually(t, func() bool { return h.session.Stats().NoticesSent == 1 }, "the latest notice is sent")
		assert.Equal(t, []uint64{101}, h.be.noticeSlots())
		assert.Equal(t, uint64(2), h.session.Stats().NoticesExpired, "the replaced notices count as expired")

		h.be.mu.Lock()
		notice := h.be.notices[0]
		h.be.mu.Unlock()
		assert.WithinDuration(t, time.Now(), notice.GetStartTimestamp().AsTime(), time.Second)
		assert.True(t, notice.GetEndTimestamp().AsTime().After(time.Now()))

		h.session.NotifyLeaderSlot(102, time.Now().Add(-time.Millisecond))
		eventually(t, func() bool { return h.session.Stats().NoticesExpired == 3 }, "a notice past its end is dropped")
		assert.Equal(t, []uint64{101}, h.be.noticeSlots(), "and never sent")
	})
	t.Run("other errors are counted, not retried", func(t *testing.T) {
		h := newBlockEngineHarness(t, time.Hour)
		h.be.noticeErr = status.Error(codes.Unavailable, "busy")
		h.start()
		eventually(t, h.session.Healthy, "healthy")
		h.session.NotifyLeaderSlot(200, time.Now().Add(time.Hour))
		eventually(t, func() bool { return h.session.Stats().NoticeErrors == 1 }, "the error is counted")
		time.Sleep(50 * time.Millisecond)
		assert.Equal(t, []uint64{200}, h.be.noticeSlots())
		assert.Zero(t, h.session.Stats().Resets)
	})
	t.Run("FAILED_PRECONDITION resets the session and loses the open slot", func(t *testing.T) {
		h := newBlockEngineHarness(t, time.Hour)
		h.be.noticeErr = status.Error(codes.FailedPrecondition, "block subscription lost")
		h.start()
		eventually(t, h.session.Healthy, "healthy")
		h.session.NotifyLeaderSlot(300, time.Now().Add(time.Hour))
		eventually(t, func() bool { return h.session.Stats().Resets == 1 }, "the session resets")
		assert.Equal(t, []uint64{300}, h.blocks.lostSlots())
		eventually(t, func() bool { return h.session.Stats().Sessions == 2 }, "and reconnects")
	})
}

func TestBlockEngineStreamLossMarksOnlyAnOpenAnnouncedSlotLost(t *testing.T) {
	h := newBlockEngineHarness(t, time.Hour)
	h.start()
	eventually(t, h.session.Healthy, "healthy")

	h.session.NotifyLeaderSlot(400, time.Now().Add(time.Hour))
	eventually(t, func() bool { return h.session.Stats().NoticesSent == 1 }, "notice sent")
	h.be.endBlocks <- status.Error(codes.Unavailable, "block stream reset")
	eventually(t, func() bool { return h.session.Stats().Resets == 1 }, "the session resets")
	assert.Equal(t, []uint64{400}, h.blocks.lostSlots())
	eventually(t, h.session.Healthy, "and recovers")
	assert.Equal(t, []bool{true, false, true}, h.health.all())

	// No notice on the new session: a reset loses nothing.
	h.be.endBlocks <- status.Error(codes.Unavailable, "again")
	eventually(t, func() bool { return h.session.Stats().Resets == 2 }, "second reset")
	eventually(t, h.session.Healthy, "recovers again")

	// A notice whose slot has ended is not lost by a later reset.
	h.session.NotifyLeaderSlot(401, time.Now().Add(100*time.Millisecond))
	eventually(t, func() bool { return h.session.Stats().NoticesSent == 2 }, "notice sent")
	time.Sleep(150 * time.Millisecond)
	h.be.endBlocks <- status.Error(codes.Unavailable, "after the slot")
	eventually(t, func() bool { return h.session.Stats().Resets == 3 }, "third reset")
	assert.Equal(t, []uint64{400}, h.blocks.lostSlots())
	assert.Equal(t, uint64(1), h.session.Stats().StreamsLost)
}

func TestBlockEngineStrategyChangeReconnectsWithTheNewStrategy(t *testing.T) {
	h := newBlockEngineHarness(t, time.Hour)
	h.start()
	eventually(t, h.session.Healthy, "healthy")
	h.session.SetStrategy(blockenginepb.SchedulingStrategy_SCHEDULING_STRATEGY_MREV)
	eventually(t, func() bool { return h.session.Stats().Sessions == 2 }, "a new session")
	h.be.mu.Lock()
	defer h.be.mu.Unlock()
	assert.Equal(t, []blockenginepb.SchedulingStrategy{
		blockenginepb.SchedulingStrategy_SCHEDULING_STRATEGY_FBA,
		blockenginepb.SchedulingStrategy_SCHEDULING_STRATEGY_MREV,
	}, h.be.strategies)
}
