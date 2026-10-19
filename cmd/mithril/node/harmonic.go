package node

import (
	"context"
	"crypto/ed25519"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/blockprod"
	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/Overclock-Validator/mithril/pkg/harmonic"
	blockenginepb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/blockengine"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/tpu"
	solana "github.com/gagliardetto/solana-go"
	"github.com/spf13/pflag"
)

// harmonicSettings is the validated [harmonic] section.
type harmonicSettings struct {
	enabled            bool
	blockEngine        harmonic.Endpoint
	relayer            harmonic.Endpoint
	insecureSkipVerify bool
	strategy           blockenginepb.SchedulingStrategy
	workers            int
	// tips lacks the vote account and identity, which come from [validator].
	tips harmonic.TipConfig
}

// registerHarmonicFlags adds a flag for each [harmonic] key.
func registerHarmonicFlags(flags *pflag.FlagSet) {
	flags.Bool("harmonic-enabled", false, "Run leader slots from a Harmonic block engine (validator mode)")
	flags.String("harmonic-block-engine-url", "", "Harmonic block engine URL (http:// or https://)")
	flags.String("harmonic-block-engine-tls-domain", "", "TLS server name override for the block engine")
	flags.String("harmonic-relayer-url", "", "Optional Harmonic relayer URL")
	flags.String("harmonic-relayer-tls-domain", "", "TLS server name override for the relayer")
	flags.Bool("harmonic-tls-cert-verify", true, "Verify Harmonic TLS certificates (false for development only)")
	flags.String("harmonic-strategy", "fba", "Block engine scheduling strategy: fba, fifo or mrev")
	flags.String("harmonic-tip-payment-program", "", "Tip payment program")
	flags.String("harmonic-tip-distribution-program", "", "Tip distribution program")
	flags.String("harmonic-merkle-root-upload-authority", "", "Merkle root upload authority for tip distribution accounts")
	flags.Int64("harmonic-commission-bps", 0, "Validator commission on tips, in basis points (0-10000)")
	flags.Int64("harmonic-num-workers", 8, "Threads executing block bundles that do not conflict in parallel")
}

// optionGetters resolve an option from its CLI flag when set, else from the
// config file.
type optionGetters struct {
	getString func(cliKey, tomlKey string) string
	getInt64  func(cliKey, tomlKey string) int64
	getBool   func(cliKey, tomlKey string) bool
}

// loadHarmonicSettings reads [harmonic]. Harmonic is off unless enabled, and
// then every key it needs must be valid.
func loadHarmonicSettings(get optionGetters) (harmonicSettings, error) {
	s := harmonicSettings{enabled: get.getBool("harmonic-enabled", "harmonic.enabled")}
	if !s.enabled {
		return s, nil
	}
	var err error
	if s.blockEngine, err = harmonic.ParseEndpoint(get.getString("harmonic-block-engine-url", "harmonic.block_engine_url"),
		get.getString("harmonic-block-engine-tls-domain", "harmonic.block_engine_tls_domain")); err != nil {
		return s, fmt.Errorf("harmonic.block_engine_url: %w", err)
	}
	if url := get.getString("harmonic-relayer-url", "harmonic.relayer_url"); url != "" {
		if s.relayer, err = harmonic.ParseEndpoint(url, get.getString("harmonic-relayer-tls-domain", "harmonic.relayer_tls_domain")); err != nil {
			return s, fmt.Errorf("harmonic.relayer_url: %w", err)
		}
	}
	s.insecureSkipVerify = !get.getBool("harmonic-tls-cert-verify", "harmonic.tls_cert_verify")
	if s.strategy, err = harmonic.ParseStrategy(get.getString("harmonic-strategy", "harmonic.strategy")); err != nil {
		return s, fmt.Errorf("harmonic.strategy: %w", err)
	}
	for _, key := range []struct {
		flag, name string
		dst        *solana.PublicKey
	}{
		{"harmonic-tip-payment-program", "harmonic.tip_payment_program", &s.tips.TipPaymentProgram},
		{"harmonic-tip-distribution-program", "harmonic.tip_distribution_program", &s.tips.TipDistributionProgram},
		{"harmonic-merkle-root-upload-authority", "harmonic.merkle_root_upload_authority", &s.tips.MerkleRootUploadAuthority},
	} {
		value := get.getString(key.flag, key.name)
		if value == "" {
			return s, fmt.Errorf("%s is required when harmonic.enabled=true", key.name)
		}
		if *key.dst, err = solana.PublicKeyFromBase58(value); err != nil {
			return s, fmt.Errorf("%s: %w", key.name, err)
		}
	}
	bps := get.getInt64("harmonic-commission-bps", "harmonic.commission_bps")
	if bps < 0 || bps > 10_000 {
		return s, fmt.Errorf("harmonic.commission_bps must be between 0 and 10000, got %d", bps)
	}
	s.tips.CommissionBps = uint16(bps)
	workers := get.getInt64("harmonic-num-workers", "harmonic.num_workers")
	if workers < 1 || workers > 64 {
		return s, fmt.Errorf("harmonic.num_workers must be between 1 and 64, got %d", workers)
	}
	s.workers = int(workers)
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
		Workers:            s.workers,
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
	mlog.Log.Infof("Harmonic enabled: block engine %s, relayer %s, strategy %s, workers %d", s.blockEngine.Target, relayer, s.strategy, s.workers)
	return service.Hooks(), func() {
		cancel()
		<-done
	}, nil
}
