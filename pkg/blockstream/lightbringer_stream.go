// lightbringer_stream: The Lightbringer sidecar stream (the genuinely lightbringer-specific path).
package blockstream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/overcast"
	"google.golang.org/grpc"
)

func (bs *BlockSource) maybeStartLightbringerStream() {
	if !bs.usesLiveShredStream() {
		return
	}
	if bs.liveStreamStarted.CompareAndSwap(false, true) {
		bs.liveStreamWg.Add(1)
		if bs.sourceType == BlockSourceTurbine {
			go bs.runTurbineStream()
		} else {
			go bs.runLightbringerStream()
		}
	}
}

func (bs *BlockSource) runLightbringerStream() {
	defer bs.liveStreamWg.Done()

	backoff := liveRetryBackoff

	for {
		if bs.stopped.Load() {
			return
		}

		bs.liveStreamConnected.Store(false)

		conn, err := grpc.NewClient(bs.lightbringerEndpoint, grpc.WithInsecure())
		if err != nil {
			mlog.Log.Warnf("Lightbringer dial failed for %s: %v", bs.lightbringerEndpoint, err)
			if bs.waitForStopOrTimeout(backoff) {
				return
			}
			backoff *= 2
			if backoff > liveMaxRetryBackoff {
				backoff = liveMaxRetryBackoff
			}
			continue
		}

		streamCtx, cancelStream := context.WithCancel(context.Background())
		bs.setLiveStreamCancel(cancelStream)
		streamDone := make(chan struct{})
		go func() {
			defer close(streamDone)
			select {
			case <-bs.stopChan:
				cancelStream()
			case <-streamCtx.Done():
			}
		}()

		client := overcast.NewSlotStreamClient(conn)
		stream, err := client.StreamSlots(streamCtx, &overcast.SlotStreamRequest{})
		if err != nil {
			cancelStream()
			<-streamDone
			bs.clearLiveStreamCancel()
			_ = conn.Close()
			mlog.Log.Warnf("Lightbringer stream setup failed for %s: %v", bs.lightbringerEndpoint, err)
			if bs.waitForStopOrTimeout(backoff) {
				return
			}
			backoff *= 2
			if backoff > liveMaxRetryBackoff {
				backoff = liveMaxRetryBackoff
			}
			continue
		}

		mlog.Log.Infof("Lightbringer stream connected to %s", bs.lightbringerEndpoint)
		bs.liveStreamConnected.Store(true)
		bs.liveLastRecvUnix.Store(time.Now().Unix())
		backoff = liveRetryBackoff
		firstSlotReceived := make(chan struct{})
		firstSlotOnce := sync.Once{}
		connectionClosed := make(chan struct{})
		connectionClosedOnce := sync.Once{}
		go func(endpoint string) {
			ticker := time.NewTicker(lightbringerFirstSlotWarn)
			defer ticker.Stop()

			connectedAt := time.Now()
			for {
				select {
				case <-bs.stopChan:
					return
				case <-connectionClosed:
					return
				case <-firstSlotReceived:
					return
				case <-ticker.C:
					bs.reorderMu.Lock()
					waitingSlot := bs.nextSlotToSend
					bs.reorderMu.Unlock()
					mlog.Log.Warnf("Lightbringer stream connected to %s but has not delivered its first slot after %s | mode=%s | waiting_slot=%d",
						endpoint, time.Since(connectedAt).Round(time.Second), bs.currentModeString(), waitingSlot)
				}
			}
		}(bs.lightbringerEndpoint)
		go func(endpoint string) {
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-bs.stopChan:
					return
				case <-connectionClosed:
					return
				case <-ticker.C:
					if !bs.liveStreamActive.Load() || !bs.isNearTip.Load() {
						continue
					}
					lastRecvUnix := bs.liveLastRecvUnix.Load()
					if lastRecvUnix == 0 {
						continue
					}
					idleFor := time.Since(time.Unix(lastRecvUnix, 0))
					if idleFor < lightbringerIdleReconnect {
						// The stream may still be delivering some traffic while failing to
						// make useful forward progress on the next slot Mithril needs. If
						// we've emitted nothing for a while and the next slot is still not
						// available to replay, reconnect the Lightbringer stream anyway.
						lastProgressUnix := bs.lastProgress.Load()
						if lastProgressUnix == 0 {
							continue
						}
						noEmitFor := time.Since(time.Unix(lastProgressUnix, 0))
						if noEmitFor < liveNoEmitReconnect {
							continue
						}

						bs.reorderMu.Lock()
						waitingSlot := bs.nextSlotToSend
						waitingReady := bs.reorderBuffer[waitingSlot] != nil || bs.skippedSlots[waitingSlot]
						bs.reorderMu.Unlock()
						if waitingReady || len(bs.streamChan) > 0 {
							continue
						}

						bs.requestLiveStreamReconnect(fmt.Sprintf("no block emitted for %s while Lightbringer is active and replay is waiting on slot %d",
							noEmitFor.Round(time.Second), waitingSlot))
						continue
					}
					bs.requestLiveStreamReconnect(fmt.Sprintf("live stream idle for %s while near-tip replay is active",
						idleFor.Round(time.Second)))
				}
			}
		}(bs.lightbringerEndpoint)

		for {
			resp, err := stream.Recv()
			if err != nil {
				connectionClosedOnce.Do(func() {
					close(connectionClosed)
				})
				bs.handleLiveShredStreamClosed("")
				cancelStream()
				<-streamDone
				_ = conn.Close()

				reconnectRequested := bs.liveReconnectRequested.Swap(false)
				canceledByReconnect := reconnectRequested && isLiveStreamReconnectCancel(err)

				if bs.stopped.Load() || isLiveStreamReconnectCancel(err) || errors.Is(err, io.EOF) {
					if bs.stopped.Load() {
						return
					}
					if reconnectRequested {
						if canceledByReconnect {
							mlog.Log.Warnf("Lightbringer stream reconnecting after watchdog request")
						} else {
							mlog.Log.Warnf("Lightbringer stream reconnecting after watchdog request (recv err: %v)", err)
						}
					} else if isLiveStreamReconnectCancel(err) {
						return
					}
					if !reconnectRequested {
						mlog.Log.Warnf("Lightbringer stream closed, retrying: %v", err)
					}
				} else {
					if reconnectRequested {
						mlog.Log.Warnf("Lightbringer stream reconnecting after watchdog request (recv err: %v)", err)
					} else {
						mlog.Log.Warnf("Lightbringer stream receive failed, retrying: %v", err)
					}
				}

				if bs.waitForStopOrTimeout(backoff) {
					return
				}
				backoff *= 2
				if backoff > liveMaxRetryBackoff {
					backoff = liveMaxRetryBackoff
				}
				break
			}

			if resp == nil {
				continue
			}

			firstSlotOnce.Do(func() {
				close(firstSlotReceived)
			})
			bs.liveLastStreamSlot.Store(resp.Slot)
			bs.liveLastRecvUnix.Store(time.Now().Unix())

			if len(resp.Entries) == 0 {
				mlog.Log.Warnf("Lightbringer delivered slot %d with no entries; ignoring", resp.Slot)
				continue
			}

			if !bs.shouldDecodeLiveSlot(resp.Slot) {
				continue
			}

			blk, err := block.FromLightbringerStreamMsg(resp)
			if err != nil {
				connectionClosedOnce.Do(func() {
					close(connectionClosed)
				})
				reason := fmt.Sprintf("cannot decode Lightbringer slot %d: %v; the Lightbringer/Overcast schema supports only legacy and V0 transactions, so native Turbine is required for TxV1", resp.Slot, err)
				bs.handleLiveShredStreamClosed(reason)
				cancelStream()
				<-streamDone
				_ = conn.Close()
				bs.liveReconnectRequested.Store(false)
				mlog.Log.Warnf("Lightbringer stream decode failed closed; reconnecting")

				if bs.waitForStopOrTimeout(backoff) {
					return
				}
				backoff *= 2
				if backoff > liveMaxRetryBackoff {
					backoff = liveMaxRetryBackoff
				}
				break
			}
			if !bs.ingestLiveShredBlock(blk) {
				cancelStream()
				<-streamDone
				_ = conn.Close()
				return
			}
		}
	}
}
