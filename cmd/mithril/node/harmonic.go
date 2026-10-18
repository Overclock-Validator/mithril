package node

import (
	"context"
	"crypto/ed25519"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/blockprod"
	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/Overclock-Validator/mithril/pkg/harmonic"
	blockenginepb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/blockengine"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/tpu"
	solana "github.com/gagliardetto/solana-go"
)

// harmonicSettings is the validated [harmonic] section.
type harmonicSettings struct {
	enabled            bool
	blockEngine        harmonic.Endpoint
	relayer            harmonic.Endpoint
	insecureSkipVerify bool
	strategy           blockenginepb.SchedulingStrategy
	// tips lacks the vote account and identity, which come from [validator].
	tips harmonic.TipConfig
}

// loadHarmonicSettings reads [harmonic]. Harmonic is off unless enabled, and
// then every key it needs must be valid.
func loadHarmonicSettings() (harmonicSettings, error) {
	s := harmonicSettings{enabled: config.GetBool("harmonic.enabled")}
	if !s.enabled {
		return s, nil
	}
	var err error
	if s.blockEngine, err = harmonic.ParseEndpoint(config.GetString("harmonic.block_engine_url"), config.GetString("harmonic.block_engine_tls_domain")); err != nil {
		return s, fmt.Errorf("harmonic.block_engine_url: %w", err)
	}
	if url := config.GetString("harmonic.relayer_url"); url != "" {
		if s.relayer, err = harmonic.ParseEndpoint(url, config.GetString("harmonic.relayer_tls_domain")); err != nil {
			return s, fmt.Errorf("harmonic.relayer_url: %w", err)
		}
	}
	s.insecureSkipVerify = !config.GetBool("harmonic.tls_cert_verify")
	if s.strategy, err = harmonic.ParseStrategy(config.GetString("harmonic.strategy")); err != nil {
		return s, fmt.Errorf("harmonic.strategy: %w", err)
	}
	for _, key := range []struct {
		name string
		dst  *solana.PublicKey
	}{
		{"harmonic.tip_payment_program", &s.tips.TipPaymentProgram},
		{"harmonic.tip_distribution_program", &s.tips.TipDistributionProgram},
		{"harmonic.merkle_root_upload_authority", &s.tips.MerkleRootUploadAuthority},
	} {
		value := config.GetString(key.name)
		if value == "" {
			return s, fmt.Errorf("%s is required when harmonic.enabled=true", key.name)
		}
		if *key.dst, err = solana.PublicKeyFromBase58(value); err != nil {
			return s, fmt.Errorf("%s: %w", key.name, err)
		}
	}
	bps := config.GetInt64("harmonic.commission_bps")
	if bps < 0 || bps > 10_000 {
		return s, fmt.Errorf("harmonic.commission_bps must be between 0 and 10000, got %d", bps)
	}
	s.tips.CommissionBps = uint16(bps)
	return s, nil
}

// startHarmonic runs the Harmonic service until the returned stop function,
// which waits for it to finish. The service's hooks must be given to the
// leader loop with GateTPU set.
func startHarmonic(ctx context.Context, s harmonicSettings, identity ed25519.PrivateKey, voteAccount solana.PublicKey,
	tpuService *tpu.TPU, gossipClient *gossip.Client, localTPU gossip.TPUSockets, txV1Enabled func() bool,
) (blockprod.LeaderHooks, func(), error) {
	tips := s.tips
	tips.VoteAccount = voteAccount
	tips.Identity = solana.PrivateKey(identity)
	service, err := harmonic.NewService(harmonic.ServiceConfig{
		BlockEngine:        s.blockEngine,
		Relayer:            s.relayer,
		InsecureSkipVerify: s.insecureSkipVerify,
		Strategy:           s.strategy,
		Tips:               tips,
		TPU:                tpuService,
		Gossip:             gossipClient,
		LocalTPU:           localTPU,
		TxV1Enabled:        txV1Enabled,
	})
	if err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		service.Run(ctx)
	}()
	relayer := "none"
	if s.relayer.Target != "" {
		relayer = s.relayer.Target
	}
	mlog.Log.Infof("Harmonic enabled: block engine %s, relayer %s, strategy %s", s.blockEngine.Target, relayer, s.strategy)
	return service.Hooks(), func() {
		cancel()
		<-done
	}, nil
}
