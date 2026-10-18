package harmonic

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/blockprod"
	"github.com/Overclock-Validator/mithril/pkg/gossip"
	blockenginepb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/blockengine"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
)

const (
	// blockEventBuffer lets the block stream run ahead of execution.
	blockEventBuffer     = 256
	clientIDPollInterval = 50 * time.Millisecond
	// The Harmonic client ID is advertised once the block engine session has
	// stayed healthy this long, and withdrawn once it has stayed unhealthy
	// for baseClientIDAfter, so a flapping session does not churn gossip.
	harmonicClientIDAfter = 5 * time.Second
	baseClientIDAfter     = 2 * time.Second
)

// ParseStrategy parses a scheduling strategy name: fifo, fba or mrev.
func ParseStrategy(name string) (blockenginepb.SchedulingStrategy, error) {
	switch strings.ToLower(name) {
	case "fifo":
		return blockenginepb.SchedulingStrategy_SCHEDULING_STRATEGY_FIFO, nil
	case "fba":
		return blockenginepb.SchedulingStrategy_SCHEDULING_STRATEGY_FBA, nil
	case "mrev":
		return blockenginepb.SchedulingStrategy_SCHEDULING_STRATEGY_MREV, nil
	default:
		return 0, fmt.Errorf("scheduling strategy %q: want fifo, fba or mrev", name)
	}
}

// Gossip is the part of the gossip client the Harmonic service changes.
type Gossip interface {
	TPUAdvertiser
	SetClientID(id uint16) error
}

// ServiceConfig configures the Harmonic service.
type ServiceConfig struct {
	BlockEngine Endpoint
	// Relayer is optional; with no Target, no relayer session runs.
	Relayer            Endpoint
	InsecureSkipVerify bool
	// Strategy defaults to FBA.
	Strategy blockenginepb.SchedulingStrategy
	// Tips configures the tip crank. Its Identity also authenticates the
	// sessions.
	Tips TipConfig
	// TPU takes the block engine's and the relayer's packets.
	TPU PacketSink
	// Gossip advertises the TPU sockets and the client ID. LocalTPU is this
	// validator's own TPU, advertised whenever the relayer's is not.
	Gossip   Gossip
	LocalTPU gossip.TPUSockets
	// TxV1Enabled reports whether the current bank accepts v1 transactions.
	TxV1Enabled func() bool
	// Dialer replaces the TCP dialer, for tests.
	Dialer func(ctx context.Context, addr string) (net.Conn, error)
}

// Service runs a validator's Harmonic integration: the block engine and
// relayer sessions, the block pipeline, the executor, and the gossip
// advertisement of the relayer's TPU and the Harmonic client ID. Harmonic
// failures never stop the validator; its leader slots fall back to local
// scheduling.
type Service struct {
	cfg         ServiceConfig
	pipeline    *BlockPipeline
	blockEngine *BlockEngineSession
	relayer     *RelayerSession
	executor    *Executor
	tpu         *TPUAdvertisement

	harmonicClientIDAfter, baseClientIDAfter time.Duration
}

func NewService(cfg ServiceConfig) (*Service, error) {
	if cfg.TPU == nil || cfg.Gossip == nil {
		return nil, errors.New("harmonic service needs the TPU and gossip")
	}
	crank, err := NewTipCrank(cfg.Tips)
	if err != nil {
		return nil, err
	}
	events := make(chan BlockEvent, blockEventBuffer)
	s := &Service{
		cfg:                   cfg,
		pipeline:              NewBlockPipeline(events, cfg.TxV1Enabled),
		tpu:                   NewTPUAdvertisement(cfg.Gossip, cfg.LocalTPU),
		harmonicClientIDAfter: harmonicClientIDAfter,
		baseClientIDAfter:     baseClientIDAfter,
	}
	identity := ed25519.PrivateKey(cfg.Tips.Identity)
	s.blockEngine = NewBlockEngineSession(BlockEngineConfig{
		Endpoint:           cfg.BlockEngine,
		InsecureSkipVerify: cfg.InsecureSkipVerify,
		Identity:           identity,
		Strategy:           cfg.Strategy,
		Packets:            cfg.TPU,
		Blocks:             s.pipeline,
		OnHealth:           s.blockEngineHealthChanged,
		Dialer:             cfg.Dialer,
	})
	s.executor = NewExecutor(ExecutorConfig{
		BlockEngine: s.blockEngine,
		Pipeline:    s.pipeline,
		Events:      events,
		Crank:       crank.Builder(s.blockEngine),
	})
	if cfg.Relayer.Target != "" {
		s.relayer = NewRelayerSession(RelayerConfig{
			Endpoint:           cfg.Relayer,
			InsecureSkipVerify: cfg.InsecureSkipVerify,
			Identity:           identity,
			Packets:            cfg.TPU,
			OnStatus:           s.relayerStatusChanged,
			Dialer:             cfg.Dialer,
		})
	}
	return s, nil
}

// Hooks is the leader loop's LeaderHooks. The leader loop must gate the TPU,
// which the executor opens whenever a slot falls back.
func (s *Service) Hooks() blockprod.LeaderHooks {
	return s.executor
}

func (s *Service) blockEngineHealthChanged(healthy bool) {
	mlog.Log.Infof("Harmonic block engine %s healthy=%t", s.cfg.BlockEngine.Target, healthy)
	s.executor.BlockEngineHealthChanged(healthy)
	if err := s.tpu.SetBlockEngineHealthy(healthy); err != nil {
		mlog.Log.Warnf("Harmonic gossip TPU advertisement: %v", err)
	}
}

func (s *Service) relayerStatusChanged(cfg *TPUConfig) {
	if err := s.tpu.SetRelayer(cfg); err != nil {
		mlog.Log.Warnf("Harmonic gossip TPU advertisement: %v", err)
	}
}

// Run runs the service until ctx ends. The sessions restore the local TPU in
// gossip as they stop; Run then restores mithril's own client ID.
func (s *Service) Run(ctx context.Context) {
	var wg sync.WaitGroup
	run := func(f func(context.Context)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f(ctx)
		}()
	}
	run(s.executor.Run)
	run(s.blockEngine.Run)
	if s.relayer != nil {
		run(s.relayer.Run)
	}
	run(s.advertiseClientID)
	<-ctx.Done()
	// The block stream may be blocked on the stopped executor.
	s.pipeline.Close()
	wg.Wait()
	if err := s.cfg.Gossip.SetClientID(gossip.ClientIDMithril); err != nil {
		mlog.Log.Warnf("Harmonic gossip client ID: %v", err)
	}
}

// advertiseClientID advertises the Harmonic client ID while the block engine
// session is healthy, with hysteresis.
func (s *Service) advertiseClientID(ctx context.Context) {
	tick := time.NewTicker(clientIDPollInterval)
	defer tick.Stop()
	advertised := gossip.ClientIDMithril
	healthy, since := false, time.Now()
	for {
		var now time.Time
		select {
		case <-ctx.Done():
			return
		case now = <-tick.C:
		}
		if h := s.blockEngine.Healthy(); h != healthy {
			healthy, since = h, now
		}
		want := advertised
		switch {
		case healthy && now.Sub(since) >= s.harmonicClientIDAfter:
			want = gossip.ClientIDMithrilHarmonic
		case !healthy && now.Sub(since) >= s.baseClientIDAfter:
			want = gossip.ClientIDMithril
		}
		if want == advertised {
			continue
		}
		if err := s.cfg.Gossip.SetClientID(want); err != nil {
			mlog.Log.Warnf("Harmonic gossip client ID: %v", err)
			continue
		}
		advertised = want
		mlog.Log.Infof("Harmonic: advertising gossip client ID %d", want)
	}
}
