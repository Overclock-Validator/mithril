package alpenglowcmd

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"github.com/spf13/cobra"
)

var (
	probeDuration          time.Duration
	probeStatsInterval     time.Duration
	probeTurbineBind       string
	probeGossipEntry       string
	probeGossipBind        string
	probeAdvertisedIP      string
	probeShredVersion      uint16
	probeAlpenglowBind     string
	probeMaxMessageBytes   int64
	probeIdentityKeypair   string
	probeVoteKeypair       string
	probeWithdrawerKeypair string
)

var AlpenglowCmd = cobra.Command{
	Use:   "alpenglow",
	Short: "Alpenglow diagnostics and tools",
}

var probeCmd = cobra.Command{
	Use:   "probe",
	Short: "Probe Turbine and Alpenglow Votor ingress without replaying",
	Long: `Join gossip, advertise Mithril's TVU and Alpenglow sockets, receive
Turbine shreds with repair enabled, and listen for Alpenglow Votor QUIC
messages. This is a passive diagnostic command; it does not replay banks or
sign votes.`,
	RunE: runProbe,
}

func init() {
	probeCmd.Flags().DurationVar(&probeDuration, "duration", 2*time.Minute, "How long to run the probe (0 = until interrupted)")
	probeCmd.Flags().DurationVar(&probeStatsInterval, "stats-interval", 10*time.Second, "How often to print probe stats")
	probeCmd.Flags().StringVar(&probeTurbineBind, "turbine-bind-addr", "", "UDP address for native turbine shreds (default: config turbine.bind_addr or 0.0.0.0:8001)")
	probeCmd.Flags().StringVar(&probeGossipEntry, "gossip-entrypoint", "", "Solana gossip entrypoint for joining the cluster")
	probeCmd.Flags().StringVar(&probeGossipBind, "gossip-bind-addr", "", "UDP address for gossip traffic (default: config turbine.gossip_bind_addr or 0.0.0.0:0)")
	probeCmd.Flags().StringVar(&probeAdvertisedIP, "advertised-ip", "", "Public IP advertised in gossip (optional; otherwise discovered from entrypoint)")
	probeCmd.Flags().Uint16Var(&probeShredVersion, "shred-version", 0, "Shred version override (0 = discover from entrypoint)")
	probeCmd.Flags().StringVar(&probeAlpenglowBind, "alpenglow-bind-addr", "", "QUIC address for passive Alpenglow Votor messages (default: config consensus.alpenglow_observer_bind_addr or 0.0.0.0:8002)")
	probeCmd.Flags().Int64Var(&probeMaxMessageBytes, "alpenglow-max-message-bytes", 0, "Maximum Votor QUIC datagram payload size (0 = default)")
	probeCmd.Flags().StringVar(&probeIdentityKeypair, "identity-keypair", "", "Validator identity keypair to advertise in gossip (Solana keygen JSON)")
	probeCmd.Flags().StringVar(&probeVoteKeypair, "vote-account-keypair", "", "Vote account keypair for diagnostics (Solana keygen JSON; not used for signing)")
	probeCmd.Flags().StringVar(&probeWithdrawerKeypair, "authorized-withdrawer-keypair", "", "Authorized withdrawer keypair for diagnostics (Solana keygen JSON; not used for signing)")
	AlpenglowCmd.AddCommand(&probeCmd)
}

func runProbe(cmd *cobra.Command, _ []string) error {
	if err := config.InitConfig(); err != nil {
		return err
	}

	opts := probeOptionsFromConfig(cmd)
	if opts.gossipEntrypoint == "" {
		return fmt.Errorf("alpenglow probe requires --gossip-entrypoint or turbine.gossip_entrypoint")
	}
	if opts.shredVersion == 0 {
		entrypoint, err := net.ResolveUDPAddr("udp", opts.gossipEntrypoint)
		if err != nil {
			return fmt.Errorf("resolve gossip entrypoint %q: %w", opts.gossipEntrypoint, err)
		}
		response, err := gossip.QueryEntrypoint(entrypoint, 5*time.Second)
		if err != nil {
			return fmt.Errorf("discover shred version from gossip entrypoint %s: %w", entrypoint, err)
		}
		opts.shredVersion = response.ShredVersion
	}
	if opts.statsInterval <= 0 {
		opts.statsInterval = 10 * time.Second
	}

	ctx := cmd.Context()
	if opts.duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.duration)
		defer cancel()
	}
	identity, identityPubkey, err := loadProbeIdentity(opts.identityKeypair)
	if err != nil {
		return err
	}
	if len(identity) == 0 {
		_, identity, err = ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return fmt.Errorf("generate ephemeral probe identity: %w", err)
		}
		identityPubkey = solana.PublicKey(identity.Public().(ed25519.PublicKey)).String()
	}

	observer := alpenglow.NewObserver()
	var turbineReceiver *turbine.UDPReceiver
	votorReceiver, err := alpenglow.NewReceiver(alpenglow.ReceiverConfig{
		BindAddr:        opts.alpenglowBind,
		MaxMessageBytes: opts.maxMessageBytes,
		ShredVersion:    opts.shredVersion,
		Identity:        identity,
		LogInterval:     0,
		OnMessage: func(msg alpenglow.Message) {
			seedTurbineBlockIDFromVotor(turbineReceiver, msg)
		},
	}, observer)
	if err != nil {
		return err
	}
	defer votorReceiver.Close()

	advertisedAlpenglowAddr := advertisedAddrForListener(opts.alpenglowBind, votorReceiver.Addr())
	votePubkey, err := loadOptionalKeypairPubkey("vote account", opts.voteKeypair)
	if err != nil {
		return err
	}
	withdrawerPubkey, err := loadOptionalKeypairPubkey("authorized withdrawer", opts.withdrawerKeypair)
	if err != nil {
		return err
	}

	gossipClient, err := gossip.NewClient(gossip.Config{
		Entrypoint:    opts.gossipEntrypoint,
		BindAddr:      opts.gossipBind,
		TVUAddr:       opts.turbineBind,
		AlpenglowAddr: advertisedAlpenglowAddr,
		AdvertisedIP:  opts.advertisedIP,
		ShredVersion:  opts.shredVersion,
		Identity:      identity,
		Name:          gossip.ClientName,
	})
	if err != nil {
		return err
	}
	localIdentity := solana.PublicKey(identity.Public().(ed25519.PublicKey))
	// A passive probe still exercises the outbound half of the protocol by
	// establishing authenticated datagram connections. It never enqueues a
	// message, so no vote or certificate can be emitted.
	votorBroadcaster, err := alpenglow.NewVotorBroadcaster(alpenglow.VotorBroadcasterConfig{
		Identity:     identity,
		ShredVersion: opts.shredVersion,
		Peers: func() []alpenglow.VotorPeer {
			contacts := gossipClient.AlpenglowPeers()
			peers := make([]alpenglow.VotorPeer, 0, len(contacts))
			for _, contact := range contacts {
				peerIdentity := solana.PublicKey(contact.Pubkey)
				if peerIdentity == localIdentity {
					continue
				}
				peers = append(peers, alpenglow.VotorPeer{Identity: peerIdentity, Addr: contact.AlpenglowAddr})
			}
			return peers
		},
	})
	if err != nil {
		return fmt.Errorf("start passive outbound Votor handshake probe: %w", err)
	}
	defer votorBroadcaster.Close()
	turbineReceiver = turbine.NewUDPReceiver(opts.turbineBind)
	turbineReceiver.SetShredVersion(opts.shredVersion)
	if err := turbineReceiver.SetRepairPeerSource(gossipClient.Identity(), gossipClient.RepairPeers); err != nil {
		return err
	}

	fmt.Printf("Alpenglow probe starting\n")
	fmt.Printf("  gossip entrypoint: %s\n", opts.gossipEntrypoint)
	fmt.Printf("  gossip bind:       %s\n", nonEmpty(opts.gossipBind, gossip.DefaultBindAddr))
	fmt.Printf("  turbine bind:      %s\n", opts.turbineBind)
	fmt.Printf("  alpenglow bind:    %s\n", votorReceiver.Addr())
	fmt.Printf("  alpenglow gossip:  %s\n", advertisedAlpenglowAddr)
	fmt.Printf("  identity:          %s%s\n", identityPubkey, keypairLabel(opts.identityKeypair))
	fmt.Printf("  shred version:     %d\n", opts.shredVersion)
	if votePubkey != "" {
		fmt.Printf("  vote account:      %s%s\n", votePubkey, keypairLabel(opts.voteKeypair))
	}
	if withdrawerPubkey != "" {
		fmt.Printf("  withdrawer:        %s%s\n", withdrawerPubkey, keypairLabel(opts.withdrawerKeypair))
	}
	fmt.Printf("  duration:          %s\n", durationLabel(opts.duration))
	fmt.Printf("\n")

	errCh := make(chan error, 3)
	go func() { errCh <- votorReceiver.Run(ctx) }()
	go func() { errCh <- turbineReceiver.Run(ctx) }()

	select {
	case err := <-turbineReceiver.Ready():
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return nil
	}

	go func() { errCh <- gossipClient.Run(ctx) }()

	ticker := time.NewTicker(opts.statsInterval)
	defer ticker.Stop()

	var (
		localBlocks      uint64
		localTurbineErrs uint64
		latestBlock      alpenglow.BlockID
		blocksCh         = turbineReceiver.Blocks()
		turbineErrsCh    = turbineReceiver.Errors()
	)

	printProbeStats(gossipClient, turbineReceiver, votorReceiver, votorBroadcaster, observer, localBlocks, localTurbineErrs, latestBlock)
	for {
		select {
		case <-ctx.Done():
			printProbeStats(gossipClient, turbineReceiver, votorReceiver, votorBroadcaster, observer, localBlocks, localTurbineErrs, latestBlock)
			return nil
		case err := <-errCh:
			if err == nil && ctx.Err() != nil {
				return nil
			}
			return err
		case blk, ok := <-blocksCh:
			if !ok {
				blocksCh = nil
				continue
			}
			localBlocks++
			latestBlock = alpenglow.BlockID{Slot: blk.Slot}
			if blk.HasAlpenglowBlockID {
				latestBlock.Hash = solana.Hash(blk.AlpenglowBlockID)
			}
			observer.ObserveReplayBlock(alpenglow.ReplayBlockObservation{
				Block:  latestBlock,
				Source: "turbine-probe",
				At:     time.Now(),
			})
		case err, ok := <-turbineErrsCh:
			if !ok {
				turbineErrsCh = nil
			} else if err != nil {
				localTurbineErrs++
			}
		case <-ticker.C:
			printProbeStats(gossipClient, turbineReceiver, votorReceiver, votorBroadcaster, observer, localBlocks, localTurbineErrs, latestBlock)
		}
	}
}

type probeOptions struct {
	duration          time.Duration
	statsInterval     time.Duration
	turbineBind       string
	gossipEntrypoint  string
	gossipBind        string
	advertisedIP      string
	shredVersion      uint16
	alpenglowBind     string
	maxMessageBytes   int64
	identityKeypair   string
	voteKeypair       string
	withdrawerKeypair string
}

func probeOptionsFromConfig(cmd *cobra.Command) probeOptions {
	turbineBind := stringFlag(cmd, "turbine-bind-addr", "block.turbine_bind_addr")
	if turbineBind == "" {
		turbineBind = stringFlag(cmd, "turbine-bind-addr", "turbine.bind_addr")
	}
	if turbineBind == "" {
		turbineBind = "0.0.0.0:8001"
	}

	alpenglowBind := stringFlag(cmd, "alpenglow-bind-addr", "consensus.alpenglow_observer_bind_addr")
	if alpenglowBind == "" {
		alpenglowBind = "0.0.0.0:8002"
	}

	maxMessageBytes := int64Flag(cmd, "alpenglow-max-message-bytes", "consensus.alpenglow_max_message_bytes")

	return probeOptions{
		duration:          probeDuration,
		statsInterval:     probeStatsInterval,
		turbineBind:       turbineBind,
		gossipEntrypoint:  stringFlag(cmd, "gossip-entrypoint", "turbine.gossip_entrypoint"),
		gossipBind:        stringFlag(cmd, "gossip-bind-addr", "turbine.gossip_bind_addr"),
		advertisedIP:      stringFlag(cmd, "advertised-ip", "turbine.advertised_ip"),
		shredVersion:      uint16Flag(cmd, "shred-version", "turbine.shred_version"),
		alpenglowBind:     alpenglowBind,
		maxMessageBytes:   maxMessageBytes,
		identityKeypair:   stringFlag(cmd, "identity-keypair", "validator.identity_keypair"),
		voteKeypair:       stringFlag(cmd, "vote-account-keypair", "validator.vote_account_keypair"),
		withdrawerKeypair: stringFlag(cmd, "authorized-withdrawer-keypair", "validator.authorized_withdrawer_keypair"),
	}
}

func loadProbeIdentity(path string) (ed25519.PrivateKey, string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, "", nil
	}
	key, err := solana.PrivateKeyFromSolanaKeygenFile(path)
	if err != nil {
		return nil, "", fmt.Errorf("load identity keypair %s: %w", path, err)
	}
	if len(key) != ed25519.PrivateKeySize {
		return nil, "", fmt.Errorf("identity keypair %s has invalid private key size %d", path, len(key))
	}
	return ed25519.PrivateKey(append([]byte(nil), key...)), key.PublicKey().String(), nil
}

func loadOptionalKeypairPubkey(label, path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", nil
	}
	key, err := solana.PrivateKeyFromSolanaKeygenFile(path)
	if err != nil {
		return "", fmt.Errorf("load %s keypair %s: %w", label, path, err)
	}
	return key.PublicKey().String(), nil
}

func keypairLabel(path string) string {
	if strings.TrimSpace(path) == "" {
		return " (ephemeral)"
	}
	return fmt.Sprintf(" (%s)", path)
}

func stringFlag(cmd *cobra.Command, flagName, configKey string) string {
	if cmd.Flags().Changed(flagName) {
		value, _ := cmd.Flags().GetString(flagName)
		return strings.TrimSpace(value)
	}
	return strings.TrimSpace(config.GetString(configKey))
}

func int64Flag(cmd *cobra.Command, flagName, configKey string) int64 {
	if cmd.Flags().Changed(flagName) {
		value, _ := cmd.Flags().GetInt64(flagName)
		return value
	}
	return config.GetInt64(configKey)
}

func uint16Flag(cmd *cobra.Command, flagName, configKey string) uint16 {
	if cmd.Flags().Changed(flagName) {
		value, _ := cmd.Flags().GetUint16(flagName)
		return value
	}
	value := config.GetInt(configKey)
	if value < 0 || value > 0xffff {
		return 0
	}
	return uint16(value)
}

func advertisedAddrForListener(configured string, actual net.Addr) string {
	host, port, err := net.SplitHostPort(configured)
	if err != nil {
		return configured
	}
	if host == "" {
		host = "0.0.0.0"
	}
	if actual != nil {
		if actualHost, actualPort, err := net.SplitHostPort(actual.String()); err == nil {
			_ = actualHost
			port = actualPort
		}
	}
	return net.JoinHostPort(host, port)
}

func printProbeStats(
	gossipClient *gossip.Client,
	turbineReceiver *turbine.UDPReceiver,
	votorReceiver *alpenglow.Receiver,
	votorBroadcaster *alpenglow.VotorBroadcaster,
	observer *alpenglow.Observer,
	localBlocks uint64,
	localTurbineErrs uint64,
	latestBlock alpenglow.BlockID,
) {
	gossipStats := gossipClient.Stats()
	turbineStats := turbineReceiver.Stats()
	votorStats := votorReceiver.Stats()
	votorOutStats := votorBroadcaster.Stats()
	observerStats := observer.Snapshot()

	fmt.Printf(
		"probe stats: turbine packets=%d data=%d coding=%d recovered=%d blocks=%d local_blocks=%d active_slots=%d repair=%d/%d timeouts=%d peers=%d errs=%d parse=%d sig=%d last_packet=%s last_data_slot=%d last_block=%s | gossip rx=%d tx=%d peers=%d repair_peers=%d contacts=%d | votor_in conn=%d datagrams=%d msgs=%d votes=%d certs=%d decode_errors=%d shred_version_mismatches=%d last_msg=%s latest_vote=%d latest_cert=%d | votor_out desired=%d conn=%d pending=%d attempts=%d errors=%d | cert_replay match/miss/pending=%d/%d/%d mature=%d pre_window=%d pending_range=%s mature_oldest=%s\n",
		turbineStats.Packets, turbineStats.DataShreds, turbineStats.CodingShreds, turbineStats.RecoveredData,
		turbineStats.BlocksEmitted, localBlocks, turbineStats.ActiveSlots, turbineStats.Repair.Responses, turbineStats.Repair.Requests,
		turbineStats.Repair.Timeouts, turbineStats.Repair.Peers, localTurbineErrs, turbineStats.ParseErrors, turbineStats.SignatureErrors,
		ageLabel(turbineStats.LastPacketUnix), turbineStats.LastDataSlot, blockLabel(latestBlock),
		gossipStats.RxPackets, gossipStats.TxPackets, gossipStats.Peers, gossipStats.RepairPeers, gossipStats.AcceptedContacts,
		votorStats.ConnectionsAccepted, votorStats.DatagramsReceived, votorStats.MessagesDecoded, votorStats.VotesDecoded, votorStats.CertificatesDecoded,
		votorStats.DecodeErrors, votorStats.ShredVersionMismatches, timeAgeLabel(votorStats.LastMessageAt), votorStats.LatestVoteSlot, votorStats.LatestCertSlot,
		votorOutStats.DesiredPeers, votorOutStats.Connections, votorOutStats.PendingConnections, votorOutStats.ConnectionAttempts, votorOutStats.ConnectionErrors,
		observerStats.CertificateReplayMatches, observerStats.CertificateReplayMismatches, observerStats.CertificateReplayPending,
		observerStats.CertificateReplayMaturePending, observerStats.CertificateReplayPreWindowPending,
		slotRangeLabel(observerStats.CertificateReplayPendingOldestSlot, observerStats.CertificateReplayPendingNewestSlot),
		slotLabel(observerStats.CertificateReplayMatureOldestSlot),
	)
}

func blockLabel(block alpenglow.BlockID) string {
	if block.Slot == 0 {
		return "none"
	}
	if !block.HasHash() {
		return fmt.Sprintf("%d:no-alpenglow-id", block.Slot)
	}
	return block.String()
}

func slotRangeLabel(oldest, newest uint64) string {
	if oldest == 0 {
		return "none"
	}
	if newest == 0 || newest == oldest {
		return fmt.Sprintf("%d", oldest)
	}
	return fmt.Sprintf("%d-%d", oldest, newest)
}

func slotLabel(slot uint64) string {
	if slot == 0 {
		return "none"
	}
	return fmt.Sprintf("%d", slot)
}

func seedTurbineBlockIDFromVotor(receiver *turbine.UDPReceiver, msg alpenglow.Message) {
	if receiver == nil {
		return
	}
	var (
		blockID alpenglow.BlockID
		ok      bool
	)
	if msg.Certificate != nil {
		blockID, ok = msg.Certificate.Block()
	} else if msg.Vote != nil {
		blockID, ok = msg.Vote.Vote.Block()
	}
	if !ok || !blockID.HasHash() {
		return
	}
	receiver.SetKnownAlpenglowBlockID(blockID.Slot, blockID.Hash)
}

func ageLabel(unix int64) string {
	if unix == 0 {
		return "never"
	}
	return time.Since(time.Unix(unix, 0)).Round(time.Second).String()
}

func timeAgeLabel(t time.Time) string {
	if t.IsZero() {
		return "never"
	}
	return time.Since(t).Round(time.Second).String()
}

func durationLabel(d time.Duration) string {
	if d <= 0 {
		return "until interrupted"
	}
	return d.String()
}

func nonEmpty(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}
