package harmonic

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	authpb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/auth"
	blockpb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/block"
	blockenginepb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/blockengine"
	"github.com/Overclock-Validator/mithril/pkg/identity"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/statsd"
	tpupacket "github.com/Overclock-Validator/mithril/pkg/tpu/packet"
	"github.com/Overclock-Validator/mithril/pkg/version"
	"github.com/gagliardetto/solana-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultFeeInfoInterval = 5 * time.Minute
	leaderNoticeTimeout    = 8 * time.Second
	maxVersionBytes        = 32
	maxCommitBytes         = 40
	maxCommissionPercent   = 100
)

var errStrategyChanged = errors.New("scheduling strategy changed")

// BlockHandler receives the Harmonic block stream.
type BlockHandler interface {
	// HandleBlock receives each block message in stream order, stamped with
	// its arrival time. It runs on the block-stream goroutine and may block,
	// which holds back the stream.
	HandleBlock(block *blockpb.Block, arrival time.Time)
	// StreamLost reports that the session reset after slot's leader notice
	// was sent and before the slot ended, so the rest of its block is lost.
	StreamLost(slot uint64)
}

// FeeInfo is the block builder's validated tip fee configuration.
type FeeInfo struct {
	Pubkey solana.PublicKey
	// Commission is a percentage, 0 to 100.
	Commission uint64
}

// ParseFeeInfo validates a GetBlockBuilderFeeInfo response.
func ParseFeeInfo(resp *blockenginepb.BlockBuilderFeeInfoResponse) (FeeInfo, error) {
	pubkey, err := solana.PublicKeyFromBase58(resp.GetPubkey())
	if err != nil {
		return FeeInfo{}, fmt.Errorf("block builder pubkey %q: %w", resp.GetPubkey(), err)
	}
	if pubkey.IsZero() {
		return FeeInfo{}, errors.New("block builder pubkey is all zero")
	}
	if resp.GetCommission() > maxCommissionPercent {
		return FeeInfo{}, fmt.Errorf("block builder commission %d%% is over 100%%", resp.GetCommission())
	}
	return FeeInfo{Pubkey: pubkey, Commission: resp.GetCommission()}, nil
}

// BlockEngineConfig configures a block engine session.
type BlockEngineConfig struct {
	Endpoint           Endpoint
	InsecureSkipVerify bool
	Identity           *identity.Holder
	// Strategy defaults to FBA.
	Strategy blockenginepb.SchedulingStrategy
	Packets  PacketSink
	Blocks   BlockHandler
	// OnHealth is called from the session goroutine whenever the session's
	// health changes.
	OnHealth func(bool)
	// Zero values use the defaults: 5m, 5s.
	FeeInfoInterval time.Duration
	MaxBackoff      time.Duration
	// Dialer replaces the TCP dialer, for tests.
	Dialer func(ctx context.Context, addr string) (net.Conn, error)
}

// BlockEngineStats counts block engine session activity.
type BlockEngineStats struct {
	Sessions       uint64
	Resets         uint64
	Packets        uint64
	DroppedSize    uint64
	DroppedFull    uint64
	Blocks         uint64
	NoticesSent    uint64
	NoticesExpired uint64
	NoticeErrors   uint64
	StreamsLost    uint64
	StrategyErrors uint64
	InvalidFeeInfo uint64
}

type leaderNotice struct {
	slot uint64
	end  time.Time
}

// BlockEngineSession keeps an authenticated session with a Harmonic block
// engine: packets into the TPU, the block stream into a BlockHandler, and
// leader notices out.
type BlockEngineSession struct {
	cfg      BlockEngineConfig
	now      func() time.Time
	strategy atomic.Int32
	restart  chan struct{}
	notices  chan struct{}
	stats    struct {
		sessions, resets, packets, droppedSize, droppedFull, blocks,
		noticesSent, noticesExpired, noticeErrors, streamsLost, strategyErrors, invalidFeeInfo atomic.Uint64
	}

	mu sync.Mutex
	// fee is the last valid fee info; it outlives sessions.
	fee *FeeInfo
	// pending is the queued leader notice; sent is the one most recently
	// sent, or in flight, on the current session.
	pending, sent *leaderNotice
	live          sessionLiveness
	healthy       bool
}

// sessionLiveness is what the current session has established.
type sessionLiveness struct {
	authenticated, packets, blocks bool
}

func NewBlockEngineSession(cfg BlockEngineConfig) *BlockEngineSession {
	if cfg.Strategy == blockenginepb.SchedulingStrategy_SCHEDULING_STRATEGY_UNSPECIFIED {
		cfg.Strategy = blockenginepb.SchedulingStrategy_SCHEDULING_STRATEGY_FBA
	}
	if cfg.FeeInfoInterval <= 0 {
		cfg.FeeInfoInterval = defaultFeeInfoInterval
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = defaultMaxBackoff
	}
	b := &BlockEngineSession{
		cfg:     cfg,
		now:     time.Now,
		restart: make(chan struct{}, 1),
		notices: make(chan struct{}, 1),
	}
	b.strategy.Store(int32(cfg.Strategy))
	return b
}

func (b *BlockEngineSession) Stats() BlockEngineStats {
	return BlockEngineStats{
		Sessions:       b.stats.sessions.Load(),
		Resets:         b.stats.resets.Load(),
		Packets:        b.stats.packets.Load(),
		DroppedSize:    b.stats.droppedSize.Load(),
		DroppedFull:    b.stats.droppedFull.Load(),
		Blocks:         b.stats.blocks.Load(),
		NoticesSent:    b.stats.noticesSent.Load(),
		NoticesExpired: b.stats.noticesExpired.Load(),
		NoticeErrors:   b.stats.noticeErrors.Load(),
		StreamsLost:    b.stats.streamsLost.Load(),
		StrategyErrors: b.stats.strategyErrors.Load(),
		InvalidFeeInfo: b.stats.invalidFeeInfo.Load(),
	}
}

// FeeInfo returns the last valid fee info.
func (b *BlockEngineSession) FeeInfo() (FeeInfo, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.fee == nil {
		return FeeInfo{}, false
	}
	return *b.fee, true
}

// Healthy reports whether the session is authenticated, has valid fee info,
// and has both its packet and block subscriptions live.
func (b *BlockEngineSession) Healthy() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.healthy
}

// Strategy is the scheduling strategy the session applies.
func (b *BlockEngineSession) Strategy() blockenginepb.SchedulingStrategy {
	return blockenginepb.SchedulingStrategy(b.strategy.Load())
}

// SetStrategy changes the scheduling strategy. The session reconnects so the
// block engine applies it.
func (b *BlockEngineSession) SetStrategy(strategy blockenginepb.SchedulingStrategy) {
	if strategy == blockenginepb.SchedulingStrategy_SCHEDULING_STRATEGY_UNSPECIFIED ||
		b.strategy.Swap(int32(strategy)) == int32(strategy) {
		return
	}
	select {
	case b.restart <- struct{}{}:
	default:
	}
}

// NotifyLeaderSlot queues the leader notice for slot, which the validator
// stops recording at end. The notice is sent once the block subscription is
// live, never after end. A newer notice replaces a queued one.
func (b *BlockEngineSession) NotifyLeaderSlot(slot uint64, end time.Time) {
	b.mu.Lock()
	if b.pending != nil {
		b.stats.noticesExpired.Add(1)
	}
	b.pending = &leaderNotice{slot: slot, end: end}
	b.mu.Unlock()
	b.signalNotice()
}

func (b *BlockEngineSession) signalNotice() {
	select {
	case b.notices <- struct{}{}:
	default:
	}
}

// Run keeps a session open until ctx ends, reconnecting after jittered
// backoff whenever it fails.
func (b *BlockEngineSession) Run(ctx context.Context) {
	for ctx.Err() == nil {
		err := b.runSession(ctx)
		lost := b.endSession()
		if ctx.Err() != nil {
			return
		}
		b.stats.resets.Add(1)
		if lost != nil {
			b.stats.streamsLost.Add(1)
			b.cfg.Blocks.StreamLost(lost.slot)
		}
		mlog.Log.Infof("Harmonic block engine %s session reset: %v", b.cfg.Endpoint.Target, err)
		if errors.Is(err, errStrategyChanged) {
			continue
		}
		select {
		case <-ctx.Done():
		case <-time.After(jitteredBackoff(b.cfg.MaxBackoff)):
		}
	}
}

// endSession clears the session's liveness and returns the notice whose slot
// the reset interrupted, if any.
func (b *BlockEngineSession) endSession() *leaderNotice {
	b.mu.Lock()
	sent := b.sent
	b.sent = nil
	b.live = sessionLiveness{}
	b.mu.Unlock()
	b.updateHealth()
	if sent != nil && b.now().Before(sent.end) {
		return sent
	}
	return nil
}

func (b *BlockEngineSession) setLive(update func(*sessionLiveness)) {
	b.mu.Lock()
	update(&b.live)
	b.mu.Unlock()
	b.updateHealth()
}

func (b *BlockEngineSession) updateHealth() {
	b.mu.Lock()
	healthy := b.live.authenticated && b.live.packets && b.live.blocks && b.fee != nil
	changed := healthy != b.healthy
	b.healthy = healthy
	b.mu.Unlock()
	if changed && b.cfg.OnHealth != nil {
		b.cfg.OnHealth(healthy)
	}
}

func (b *BlockEngineSession) runSession(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	tokens := NewTokenSource(nil, b.cfg.Identity)
	dial := DialConfig{
		Endpoint:           b.cfg.Endpoint,
		InsecureSkipVerify: b.cfg.InsecureSkipVerify,
		Credentials:        tokens.Bearer(),
		Dialer:             b.cfg.Dialer,
	}
	conn, err := Dial(ctx, dial)
	if err != nil {
		return err
	}
	defer conn.Close()
	tokens.client = authpb.NewAuthServiceClient(conn)
	if err := tokens.Authenticate(ctx); err != nil {
		return err
	}
	b.stats.sessions.Add(1)
	b.setLive(func(l *sessionLiveness) { l.authenticated = true })
	errs := make(chan error, 4)
	go func() { errs <- tokens.Run(ctx) }()

	client := blockenginepb.NewBlockEngineValidatorClient(conn)
	if err := b.setStrategy(ctx, client); err != nil {
		return err
	}
	if err := b.refreshFeeInfo(ctx, client); err != nil {
		return err
	}
	packets, err := client.SubscribePackets(ctx, &blockenginepb.SubscribePacketsRequest{})
	if err == nil {
		err = awaitHeaders(packets)
	}
	if err != nil {
		return fmt.Errorf("subscribe packets: %w", err)
	}
	b.setLive(func(l *sessionLiveness) { l.packets = true })
	go func() { errs <- b.readPackets(packets) }()

	// The block stream gets its own connection, so it never shares an HTTP/2
	// flow-control window with packets.
	blockConn, err := Dial(ctx, dial)
	if err != nil {
		return fmt.Errorf("block connection: %w", err)
	}
	defer blockConn.Close()
	blocks, err := blockenginepb.NewBlockEngineValidatorClient(blockConn).SubscribeBlocks2(ctx, &blockenginepb.SubscribeBlocksRequest{
		Version:    truncate(version.Semver(), maxVersionBytes),
		CommitHash: truncate(version.Commit(), maxCommitBytes),
	})
	if err == nil {
		err = awaitHeaders(blocks)
	}
	if err != nil {
		return fmt.Errorf("subscribe blocks: %w", err)
	}
	b.setLive(func(l *sessionLiveness) { l.blocks = true })
	go func() { errs <- b.readBlocks(blocks) }()
	go b.sendNotices(ctx, client, errs)
	b.signalNotice()

	feeInfo := time.NewTicker(b.cfg.FeeInfoInterval)
	defer feeInfo.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errs:
			return err
		case <-b.restart:
			return errStrategyChanged
		case <-feeInfo.C:
			if err := b.refreshFeeInfo(ctx, client); err != nil {
				return err
			}
		}
	}
}

// setStrategy applies the scheduling strategy. Only an auth error fails the
// session; a block engine without SetStrategy is used without it.
func (b *BlockEngineSession) setStrategy(ctx context.Context, client blockenginepb.BlockEngineValidatorClient) error {
	ctx, cancel := context.WithTimeout(ctx, unaryRPCTimeout)
	defer cancel()
	strategy := blockenginepb.SchedulingStrategy(b.strategy.Load())
	_, err := client.SetStrategy(ctx, &blockenginepb.SetStrategyRequest{Strategy: strategy})
	if err == nil {
		return nil
	}
	if IsAuthError(err) {
		return fmt.Errorf("set strategy: %w", err)
	}
	b.stats.strategyErrors.Add(1)
	mlog.Log.Infof("WARNING: Harmonic block engine %s: set strategy %s: %v; continuing without it", b.cfg.Endpoint.Target, strategy, err)
	return nil
}

// refreshFeeInfo fetches the fee info. An invalid response or a failed call
// keeps the previous value; only an auth error fails the session.
func (b *BlockEngineSession) refreshFeeInfo(ctx context.Context, client blockenginepb.BlockEngineValidatorClient) error {
	ctx, cancel := context.WithTimeout(ctx, unaryRPCTimeout)
	defer cancel()
	resp, err := client.GetBlockBuilderFeeInfo(ctx, &blockenginepb.BlockBuilderFeeInfoRequest{})
	if IsAuthError(err) {
		return fmt.Errorf("get block builder fee info: %w", err)
	}
	var fee FeeInfo
	if err == nil {
		fee, err = ParseFeeInfo(resp)
	}
	if err != nil {
		b.stats.invalidFeeInfo.Add(1)
		mlog.Log.Infof("Harmonic block engine %s: fee info: %v; keeping the previous value", b.cfg.Endpoint.Target, err)
		return nil
	}
	b.mu.Lock()
	b.fee = &fee
	b.mu.Unlock()
	b.updateHealth()
	return nil
}

func (b *BlockEngineSession) readPackets(stream blockenginepb.BlockEngineValidator_SubscribePacketsClient) error {
	for {
		msg, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("packet stream: %w", err)
		}
		for _, pkt := range msg.GetBatch().GetPackets() {
			data := pkt.GetData()
			if len(data) == 0 || len(data) > tpupacket.DataSize {
				b.stats.droppedSize.Add(1)
				continue
			}
			b.stats.packets.Add(1)
			if !b.cfg.Packets.Submit(data, tpupacket.SourceBlockEngine) {
				b.stats.droppedFull.Add(1)
			}
		}
	}
}

// readBlocks hands each block message to the handler. A message that fails
// to decode ends the stream, and with it the session.
func (b *BlockEngineSession) readBlocks(stream blockenginepb.BlockEngineValidator_SubscribeBlocks2Client) error {
	for {
		block, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("block stream: %w", err)
		}
		b.stats.blocks.Add(1)
		b.cfg.Blocks.HandleBlock(block, b.now())
	}
}

// sendNotices sends queued leader notices while the block subscription is
// live, which is only while this goroutine runs.
func (b *BlockEngineSession) sendNotices(ctx context.Context, client blockenginepb.BlockEngineValidatorClient, errs chan<- error) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-b.notices:
		}
		if ctx.Err() != nil {
			// Leave the notice queued for the next session.
			b.signalNotice()
			return
		}
		notice := b.takeNotice()
		if notice == nil {
			continue
		}
		err := b.sendNotice(ctx, client, notice)
		switch {
		case err == nil:
			b.stats.noticesSent.Add(1)
		case status.Code(err) == codes.FailedPrecondition, IsAuthError(err):
			// FAILED_PRECONDITION with a live subscription means the block
			// engine lost it; every later notice would fail the same way.
			errs <- fmt.Errorf("leader notice for slot %d: %w", notice.slot, err)
			return
		default:
			// Not retried: the slot may be over before a retry lands.
			b.stats.noticeErrors.Add(1)
			mlog.Log.Infof("Harmonic block engine %s: leader notice for slot %d: %v", b.cfg.Endpoint.Target, notice.slot, err)
		}
	}
}

// takeNotice dequeues the pending notice and records it as sent, unless its
// slot has already ended.
func (b *BlockEngineSession) takeNotice() *leaderNotice {
	b.mu.Lock()
	defer b.mu.Unlock()
	notice := b.pending
	b.pending = nil
	if notice == nil {
		return nil
	}
	if !b.now().Before(notice.end) {
		b.stats.noticesExpired.Add(1)
		return nil
	}
	b.sent = notice
	return notice
}

func (b *BlockEngineSession) sendNotice(ctx context.Context, client blockenginepb.BlockEngineValidatorClient, notice *leaderNotice) error {
	ctx, cancel := context.WithTimeout(ctx, leaderNoticeTimeout)
	defer cancel()
	sent := time.Now()
	_, err := client.SubmitLeaderWindowInfo(ctx, &blockenginepb.SubmitLeaderWindowInfoRequest{
		Slot:           notice.slot,
		StartTimestamp: timestamppb.New(b.now()),
		EndTimestamp:   timestamppb.New(notice.end),
	})
	if err == nil {
		_ = statsd.Duration(statsd.HarmonicLeaderNoticeRTT, time.Since(sent), nil)
	}
	return err
}

func truncate(s string, maxBytes int) string {
	if len(s) > maxBytes {
		return s[:maxBytes]
	}
	return s
}
