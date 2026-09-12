// turbine_stream: Native turbine receiver integration: stream lifecycle, alpenglow block-id hints, and cert-driven repair steering.
package blockstream

import (
	"context"
	"fmt"
	"time"

	gossipclient "github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
)

func (bs *BlockSource) resetTurbineSlotForAlpenglowBlock(slot uint64, blockID solana.Hash) {
	if !bs.turbineAlpenglowBlockIDHints || slot == 0 || blockID == (solana.Hash{}) {
		return
	}
	bs.SetKnownAlpenglowBlockID(slot, blockID)

	bs.alpenglowMu.Lock()
	receiver := bs.activeTurbineReceiver
	bs.alpenglowMu.Unlock()
	if receiver != nil {
		// A certificate supplies an exact block identity. Any already-held or
		// spooled variant for this slot is unsafe to rehydrate after the reset.
		receiver.ResetSlotAndDiscardSpool(slot)
		receiver.PrioritizeRepairSlot(slot)
	}
}

// TurbineShredEdges reports the monotonic shred frontier (latest shred slot,
// highest full slot) from the active turbine receiver. ok is false when no
// receiver is active (RPC-only / pre-handoff) — callers must not fabricate
// shred stats then.
func (bs *BlockSource) TurbineShredEdges() (latestShredSlot, highestFullSlot uint64, ok bool) {
	bs.alpenglowMu.Lock()
	receiver := bs.activeTurbineReceiver
	bs.alpenglowMu.Unlock()
	if receiver == nil {
		return 0, 0, false
	}
	latest, full := receiver.ShredEdges()
	return latest, full, true
}

// TurbineShredObservation reports partial shred arrivals for a slot that never
// became full — "the leader sent SOMETHING" skip observability. ok is false
// when no receiver is active or no shred was ever accepted for the slot.
func (bs *BlockSource) TurbineShredObservation(slot uint64) (dataShreds, repairedShreds int, firstNanos int64, ok bool) {
	bs.alpenglowMu.Lock()
	receiver := bs.activeTurbineReceiver
	bs.alpenglowMu.Unlock()
	if receiver == nil {
		return 0, 0, 0, false
	}
	obs, found := receiver.ShredObservation(slot)
	if !found {
		return 0, 0, 0, false
	}
	return obs.DataShreds, obs.RepairedShreds, obs.FirstNanos, true
}

func (bs *BlockSource) prioritizeTurbineRepairRange(start, end uint64) {
	if bs.sourceType != BlockSourceTurbine || !bs.turbineAlpenglowBlockIDHints || start == 0 {
		return
	}
	if end < start {
		end = start
	}

	bs.alpenglowMu.Lock()
	receiver := bs.activeTurbineReceiver
	bs.alpenglowMu.Unlock()
	if receiver != nil {
		receiver.PrioritizeRepairRange(start, end)
	}
}

func (bs *BlockSource) prioritizeTurbineRepairForLiveGap(waitingSlot, firstBufferedParentSlot uint64) {
	end := waitingSlot
	if firstBufferedParentSlot > waitingSlot {
		end = firstBufferedParentSlot
	}
	bs.prioritizeTurbineRepairRange(waitingSlot, end)
}

// Cert-driven repair. Certificates prove which block data the cluster voted
// real BEFORE turbine finishes delivering it here: a certified-but-unobserved
// block means we are missing (or mis-assembled) data the chain has already
// settled on. The repair loop closes that gap continuously instead of waiting
// for the emission frontier to stall on it:
//
//   - certified block not yet observed  -> pin the assembler to the certified
//     block id and pull repair for the slot
//   - buffered pre-emission candidate carrying a DIFFERENT id -> provably
//     non-canonical; discard it and re-arm the slot for the certified version
//     (the post-emission case is the replay switch sweep's job)
//   - certificate-skipped slot -> drop its partial shred state so the
//     receiver stops assembling and repairing data the chain discarded
//
// This also services the switch sweep: after a wrong-sibling unwind the
// certified sibling stays "wanted" until observed, so the hints re-fire every
// nudge interval until the data lands.
const (
	alpenglowRepairTick       = 250 * time.Millisecond
	alpenglowRepairMaxWanted  = 32
	alpenglowRepairNudgePause = time.Second // at most one nudge per slot per second
)

func (bs *BlockSource) alpenglowRepairLoop() {
	ticker := time.NewTicker(alpenglowRepairTick)
	defer ticker.Stop()
	nudged := make(map[uint64]time.Time) // loop-local: single-goroutine rate limiter
	for {
		select {
		case <-bs.stopChan:
			return
		case <-ticker.C:
			bs.serviceAlpenglowWantedBlocks(nudged, time.Now())
		}
	}
}

// serviceAlpenglowWantedBlocks runs one repair pass. nudged is the per-slot
// rate limiter (owned by the calling goroutine); entries at or below the
// emission frontier are pruned as it advances.
func (bs *BlockSource) serviceAlpenglowWantedBlocks(nudged map[uint64]time.Time, now time.Time) {
	if bs.alpenglowWantedBlocksFn == nil || bs.sourceType != BlockSourceTurbine || !bs.turbineAlpenglowBlockIDHints {
		return
	}
	if !bs.isNearTip.Load() && !bs.repairCatchupActive() {
		return // RPC catch-up fills gaps via ordinary backfill; hints would be noise
	}

	bs.reorderMu.Lock()
	waiting := bs.nextSlotToSend
	bs.reorderMu.Unlock()
	if waiting == 0 {
		return
	}
	after := waiting - 1

	for slot := range nudged {
		if slot <= after {
			delete(nudged, slot)
		}
	}

	for _, w := range bs.alpenglowWantedBlocksFn(after, alpenglowRepairMaxWanted) {
		slot := w.Block.Slot
		if bs.isInvalidAlpenglowBlockID(slot, w.Block.Hash) {
			// Fallback certificates are repair leads, not authority to revive
			// deterministic-invalid contents or evict a valid sibling.
			continue
		}
		if last, ok := nudged[slot]; ok && now.Sub(last) < alpenglowRepairNudgePause {
			continue
		}

		bs.reorderMu.Lock()
		blk := bs.reorderBuffer[slot]
		haveCertified := blk != nil && blk.HasAlpenglowBlockID && solana.Hash(blk.AlpenglowBlockID) == w.Block.Hash
		mismatch := blk != nil && blk.HasAlpenglowBlockID && !haveCertified
		if mismatch {
			delete(bs.reorderBuffer, slot)
			bs.slotStateMu.Lock()
			delete(bs.slotState, slot)
			delete(bs.inflightStart, slot)
			bs.slotStateMu.Unlock()
		}
		bs.reorderMu.Unlock()

		if haveCertified {
			continue // assembled and waiting its emission turn; nothing to repair
		}
		nudged[slot] = now
		if mismatch {
			bs.clearSlotErrors(slot)
			bs.resetTurbineSlotForAlpenglowBlock(slot, w.Block.Hash)
			mlog.Log.Warnf("ALPENGLOW repair: discarded buffered non-certified candidate at slot %d; repairing toward certified block %s",
				slot, w.Block.Hash)
			continue
		}
		bs.SetKnownAlpenglowBlockID(slot, w.Block.Hash)
		bs.prioritizeTurbineRepairRange(slot, slot)
	}

	// Skip-cancel: certificate-skipped slots ahead of the frontier stop
	// accumulating shred state and stop generating repair requests. Slots with
	// a finalized block over a skip never reach here — the wanted-block nudge
	// above refreshes their limiter entry first (finality outranks a skip).
	if bs.alpenglowSkipCertifiedFn == nil {
		return
	}
	for slot := after + 1; slot <= after+alpenglowRepairMaxWanted; slot++ {
		if last, ok := nudged[slot]; ok && now.Sub(last) < alpenglowRepairNudgePause {
			continue
		}
		if !bs.alpenglowSkipCertifiedFn(slot) {
			continue
		}
		nudged[slot] = now
		bs.alpenglowMu.Lock()
		receiver := bs.activeTurbineReceiver
		bs.alpenglowMu.Unlock()
		if receiver != nil {
			receiver.ResetSlot(slot)
		}
	}
}

func (bs *BlockSource) attachAlpenglowBlockIDHintsToReceiver(receiver *turbine.UDPReceiver) {
	if receiver == nil {
		return
	}
	// The active-receiver pointer must be set regardless of the hints flag:
	// staging-eviction marker resets, repair prioritization, and the catchup
	// diagnostics all reach the receiver through it, and with hints off they
	// would silently no-op — an evicted staged block's completed marker
	// would then make its slot permanently unfetchable.
	bs.alpenglowMu.Lock()
	bs.activeTurbineReceiver = receiver
	if !bs.turbineAlpenglowBlockIDHints {
		bs.alpenglowMu.Unlock()
		return
	}
	known := make([]struct {
		slot    uint64
		blockID solana.Hash
	}, 0, len(bs.knownAlpenglowBlockIDs))
	for slot, blockID := range bs.knownAlpenglowBlockIDs {
		known = append(known, struct {
			slot    uint64
			blockID solana.Hash
		}{slot: slot, blockID: blockID})
	}
	bs.alpenglowMu.Unlock()

	invalid := make([]struct {
		slot    uint64
		blockID solana.Hash
	}, 0)
	bs.rejectedAlpenglowMu.RLock()
	for slot, ids := range bs.invalidAlpenglowBlockIDs {
		for blockID := range ids {
			invalid = append(invalid, struct {
				slot    uint64
				blockID solana.Hash
			}{slot: slot, blockID: blockID})
		}
	}
	bs.rejectedAlpenglowMu.RUnlock()
	for _, entry := range invalid {
		receiver.RejectAlpenglowBlockIDAndDiscardSlot(entry.slot, entry.blockID)
	}
	for _, entry := range known {
		if bs.isInvalidAlpenglowBlockID(entry.slot, entry.blockID) {
			continue
		}
		receiver.SetKnownAlpenglowBlockID(entry.slot, entry.blockID)
	}
}

// resetTurbineSlotState clears the assembler's shred state AND completed-slot
// marker for slot, making it repair-fetchable again.
func (bs *BlockSource) resetTurbineSlotState(slot uint64) {
	bs.alpenglowMu.Lock()
	receiver := bs.activeTurbineReceiver
	bs.alpenglowMu.Unlock()
	if receiver != nil {
		receiver.ResetSlot(slot)
	}
}

// discardTurbineSlotState is the destructive counterpart used only when the
// held packets are known to be poisoned or to belong to a rejected branch.
// Staging evictions use resetTurbineSlotState instead so good spool data stays
// available for cheap rehydration.
func (bs *BlockSource) discardTurbineSlotState(slot uint64) {
	bs.alpenglowMu.Lock()
	receiver := bs.activeTurbineReceiver
	bs.alpenglowMu.Unlock()
	if receiver != nil {
		receiver.ResetSlotAndDiscardSpool(slot)
	}
}

func (bs *BlockSource) rejectInvalidTurbineBlockState(slot uint64, blockID solana.Hash) {
	bs.alpenglowMu.Lock()
	receiver := bs.activeTurbineReceiver
	bs.alpenglowMu.Unlock()
	if receiver != nil {
		receiver.RejectAlpenglowBlockIDAndDiscardSlot(slot, blockID)
	}
}

func (bs *BlockSource) runTurbineStream() {
	defer bs.liveStreamWg.Done()

	backoff := liveRetryBackoff
	for {
		if bs.stopped.Load() {
			return
		}

		bs.liveStreamConnected.Store(false)
		streamCtx, cancelStream := context.WithCancel(context.Background())
		bs.setLiveStreamCancel(cancelStream)

		var gossipClient *gossipclient.Client
		var gossipDone <-chan error
		ownsGossipClient := false
		if bs.gossipClient != nil {
			gossipClient = bs.gossipClient
		} else if bs.turbineGossipEntrypoint != "" {
			gossipCfg := gossipclient.Config{
				Entrypoint:    bs.turbineGossipEntrypoint,
				BindAddr:      bs.turbineGossipBindAddr,
				TVUAddr:       bs.turbineBindAddr,
				AlpenglowAddr: bs.turbineAlpenglowAddr,
				AdvertisedIP:  bs.turbineAdvertisedIP,
				ShredVersion:  bs.turbineShredVersion,
				Identity:      bs.turbineIdentity,
				Name:          gossipclient.ClientName,
			}
			client, err := gossipclient.NewClient(gossipCfg)
			if err != nil {
				cancelStream()
				bs.handleLiveShredStreamClosed(fmt.Sprintf("native turbine gossip client setup failed: %v", err))
				if bs.waitForStopOrTimeout(backoff) {
					return
				}
				backoff *= 2
				if backoff > liveMaxRetryBackoff {
					backoff = liveMaxRetryBackoff
				}
				continue
			}
			gossipClient = client
			ownsGossipClient = true
		}

		receiver := turbine.NewUDPReceiver(bs.turbineBindAddr)
		receiver.SetLeaderForSlot(bs.leaderForSlot)
		receiver.SetShredVersion(bs.turbineShredVersion)
		receiver.SetFirstShredSink(bs.alpenglowFirstShredSink)
		if bs.shredSpoolDir != "" {
			if spool, serr := turbine.OpenShredSpool(bs.shredSpoolDir, shredSpoolMaxBytes); serr != nil {
				mlog.Log.Warnf("shred spool disabled (%s): %v", bs.shredSpoolDir, serr)
			} else {
				receiver.SetShredSpool(spool)
				if slots, bytes := spool.Stats(); slots > 0 {
					mlog.Log.Infof("shred spool: adopted %d spooled slots (%d proven complete, %.1f MiB) from a previous run — complete slots need zero network", slots, spool.CompleteSlots(), float64(bytes)/(1<<20))
				}
			}
		}
		// Apply hard invalid identities only after the spool is attached, so
		// RejectAlpenglowBlockIDAndDiscardSlot removes adopted poisoned files
		// before hydration can reconstruct them.
		bs.attachAlpenglowBlockIDHintsToReceiver(receiver)
		if gossipClient != nil {
			if bs.turbineStakesForSlot != nil {
				if err := receiver.SetRetransmit(turbine.RetransmitConfig{
					Identity:     gossipClient.Identity(),
					Peers:        gossipClient,
					Stakes:       bs.turbineStakesForSlot,
					EpochForSlot: bs.turbineEpochForSlot,
					RootSlot:     bs.turbineRootSlot,
					UseChaCha8:   bs.turbineUseChaCha8,
					DedupAddrs:   bs.turbineDedupAddrs,
				}); err != nil {
					cancelStream()
					bs.handleLiveShredStreamClosed(fmt.Sprintf("native turbine retransmit setup failed: %v", err))
					if bs.waitForStopOrTimeout(backoff) {
						return
					}
					continue
				}
			}
			if err := receiver.SetRepairPeerSource(gossipClient.Identity(), gossipClient.RepairPeers); err == nil {
				receiver.SetRepairRequestRate(bs.repairMaxRequestsPerSecond)
			} else {
				cancelStream()
				bs.handleLiveShredStreamClosed(fmt.Sprintf("native turbine repair setup failed: %v", err))
				if bs.waitForStopOrTimeout(backoff) {
					return
				}
				backoff *= 2
				if backoff > liveMaxRetryBackoff {
					backoff = liveMaxRetryBackoff
				}
				continue
			}
		}
		streamDone := make(chan error, 1)
		go func() {
			streamDone <- receiver.Run(streamCtx)
		}()
		go func() {
			select {
			case <-bs.stopChan:
				cancelStream()
			case <-streamCtx.Done():
			}
		}()

		select {
		case err := <-receiver.Ready():
			if err != nil {
				cancelStream()
				<-streamDone
				bs.handleLiveShredStreamClosed(err.Error())
				if bs.waitForStopOrTimeout(backoff) {
					return
				}
				backoff *= 2
				if backoff > liveMaxRetryBackoff {
					backoff = liveMaxRetryBackoff
				}
				continue
			}
		case <-bs.stopChan:
			cancelStream()
			<-streamDone
			return
		}

		mlog.Log.Infof("Native turbine receiver listening on %s", bs.turbineBindAddr)
		bs.liveStreamConnected.Store(true)
		bs.liveLastRecvUnix.Store(time.Now().Unix())
		backoff = liveRetryBackoff

		// (Re)start the repair-catchup monitor with EVERY turbine stream,
		// not just the construction-time one: a stream restart mid-run left
		// the replacement without a monitor (pending is construction-only),
		// so a post-restart gap had no repair drive to fill it.
		if bs.sourceType == BlockSourceTurbine && (bs.repairCatchupMaxGapSlots > 0 || !bs.rpcFallbackEnabled) {
			bs.repairCatchupPending.Store(true)
			go bs.runRepairCatchup(streamCtx, receiver)
		}

		if gossipClient != nil {
			if !ownsGossipClient {
				mlog.Log.FileOnlyf("Native turbine receiver using shared gossip client for repair, retransmit, and peer discovery")
			} else {
				done := make(chan error, 1)
				go func() {
					done <- gossipClient.Run(streamCtx)
				}()
				gossipDone = done
				bindAddr := bs.turbineGossipBindAddr
				if bindAddr == "" {
					bindAddr = gossipclient.DefaultBindAddr
				}
				alpenglowAddr := "disabled"
				if bs.turbineAlpenglowAddr != "" {
					alpenglowAddr = bs.turbineAlpenglowAddr
				}
				mlog.Log.FileOnlyf("Native turbine gossip client starting: entrypoint=%s bind=%s client=%s repair=enabled retransmit=%t alpenglow=%s", bs.turbineGossipEntrypoint, bindAddr, gossipclient.ClientName, bs.turbineStakesForSlot != nil, alpenglowAddr)
			}
		} else {
			mlog.Log.Warnf("Native turbine gossip entrypoint is not configured; receiver is running UDP-only on %s with repair disabled", bs.turbineBindAddr)
		}

		var streamErr error
		streamDoneConsumed := false
		var ignoredPacketCount uint64
		var ignoredPacketLastLog time.Time
		statsTicker := time.NewTicker(10 * time.Second)
	streamLoop:
		for {
			select {
			case blk, ok := <-receiver.Blocks():
				if !ok {
					streamErr = <-streamDone
					streamDoneConsumed = true
					break streamLoop
				}
				keepRunning := bs.ingestLiveShredBlock(blk)
				receiver.AcknowledgeBlockDelivery(blk.Slot)
				if !keepRunning {
					statsTicker.Stop()
					cancelStream()
					<-streamDone
					return
				}
			case err, ok := <-receiver.Errors():
				if ok && err != nil {
					ignoredPacketCount++
					now := time.Now()
					if ignoredPacketLastLog.IsZero() || now.Sub(ignoredPacketLastLog) >= 10*time.Second {
						mlog.Log.FileOnlyf("native turbine packets ignored: count=%d latest=%v", ignoredPacketCount, err)
						ignoredPacketCount = 0
						ignoredPacketLastLog = now
					} else {
						mlog.Log.Debugf("native turbine packet ignored: %v", err)
					}
				}
			case <-statsTicker.C:
				stats := receiver.Stats()
				lastPacketAge := "never"
				if stats.LastPacketUnix != 0 {
					lastPacketAge = time.Since(time.Unix(stats.LastPacketUnix, 0)).Round(time.Second).String()
				}
				nonCanonicalDesc := "none"
				if stats.NonCanonicalBlockIDs != 0 {
					nonCanonicalDesc = fmt.Sprintf("%d:%s!=%s", stats.LastNonCanonicalSlot, stats.LastNonCanonicalGot, stats.LastNonCanonicalWant)
				}
				sendDiagnostic := stats.Retransmit.LastSendDiagnostic
				if sendDiagnostic == "" {
					sendDiagnostic = "none"
				}
				parentDiagnostic := stats.Retransmit.LastParentDiagnostic
				if parentDiagnostic == "" {
					parentDiagnostic = "none"
				}
				mlog.Log.FileOnlyf("native turbine receiver stats: packets=%d data=%d coding=%d recovered=%d blocks=%d active_slots=%d evicted_slots=%d ignored_old_shreds=%d priority_repair_slots=%d noncanonical_block_ids=%d last_noncanonical=%s repair_requests=%d repair_responses=%d repair_timeouts=%d repair_outstanding=%d repair_peers=%d repair_pings=%d/%d repair_errors=%d repair_socket_packets=%d repair_socket_unmatched=%d retransmit_shreds=%d retransmit_packets=%d retransmit_target_packets=%d retransmit_packet_attempts=%d retransmit_unsent_packets=%d retransmit_duplicates=%d retransmit_queue_drops=%d retransmit_no_peers=%d retransmit_parent_sigs=%d/%d retransmit_sig_errors=%d retransmit_sig_direct_leader=%d retransmit_sig_repair_socket=%d retransmit_sig_unexplained=%d retransmit_parent_sig_samples=%d retransmit_actual_signer_found=%d retransmit_actual_signer_unknown=%d retransmit_send_errors=%d retransmit_peer_selection_errors=%d retransmit_send_buffer_bytes=%d retransmit_send_syscall_errors=%d retransmit_short_batches=%d retransmit_retry_batches=%d retransmit_retry_packets=%d retransmit_retry_sent=%d retransmit_exhausted_batches=%d retransmit_send_samples=%d retransmit_last_send=%q retransmit_last_parent_sig=%q shred_version_mismatches=%d parse_errors=%d sig_errors=%d missing_leaders=%d assembly_errors=%d last_packet=%s last_data_slot=%d last_block_slot=%d",
					stats.Packets, stats.DataShreds, stats.CodingShreds, stats.RecoveredData, stats.BlocksEmitted, stats.ActiveSlots,
					stats.EvictedSlots, stats.IgnoredOldShreds, stats.PriorityRepairSlots, stats.NonCanonicalBlockIDs, nonCanonicalDesc, stats.Repair.Requests, stats.Repair.Responses, stats.Repair.Timeouts, stats.Repair.Outstanding, stats.Repair.Peers,
					stats.Repair.Pings, stats.Repair.Pongs, stats.Repair.Errors, stats.RepairSocketPackets, stats.RepairSocketUnmatched, stats.Retransmit.SentShreds, stats.Retransmit.SentPackets, stats.Retransmit.TargetPackets, stats.Retransmit.PacketAttempts, stats.Retransmit.UnsentPackets,
					stats.Retransmit.DuplicateShreds, stats.Retransmit.QueueDrops, stats.Retransmit.NoPeers, stats.Retransmit.ParentSignaturesVerified, stats.Retransmit.ParentSignaturesSkipped, stats.Retransmit.InvalidParentSignatures,
					stats.Retransmit.ParentSourceDirectLeader, stats.Retransmit.ParentSourceRepairSocket, stats.Retransmit.ParentSourceUnexplained, stats.Retransmit.ParentDiagnosticSamples, stats.Retransmit.ParentSignerFound, stats.Retransmit.ParentSignerNotFound, stats.Retransmit.SendErrors, stats.Retransmit.PeerSelectionErrors, stats.Retransmit.SendBufferBytes,
					stats.Retransmit.SendSyscallErrors, stats.Retransmit.ShortSendBatches, stats.Retransmit.RetryBatches, stats.Retransmit.RetryPackets, stats.Retransmit.RetrySentPackets,
					stats.Retransmit.ExhaustedSendBatches, stats.Retransmit.SendDiagnosticSamples, sendDiagnostic, parentDiagnostic, stats.ShredVersionMismatch,
					stats.ParseErrors, stats.SignatureErrors, stats.MissingLeaders, stats.AssemblyErrors,
					lastPacketAge, stats.LastDataSlot, stats.LastBlockSlot)
			case err := <-streamDone:
				streamErr = err
				streamDoneConsumed = true
				break streamLoop
			case err := <-gossipDone:
				if err != nil {
					streamErr = fmt.Errorf("native turbine gossip client stopped: %w", err)
				} else {
					streamErr = fmt.Errorf("native turbine gossip client stopped")
				}
				break streamLoop
			case <-bs.stopChan:
				statsTicker.Stop()
				cancelStream()
				<-streamDone
				return
			}
		}

		statsTicker.Stop()
		cancelStream()
		if !streamDoneConsumed {
			if err := <-streamDone; streamErr == nil {
				streamErr = err
			}
		}
		if streamErr == nil && bs.stopped.Load() {
			return
		}
		reason := ""
		if streamErr != nil {
			reason = streamErr.Error()
		}
		bs.handleLiveShredStreamClosed(reason)
		if bs.waitForStopOrTimeout(backoff) {
			return
		}
		backoff *= 2
		if backoff > liveMaxRetryBackoff {
			backoff = liveMaxRetryBackoff
		}
	}
}
