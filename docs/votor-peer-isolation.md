# Votor outbound peer isolation

A stalled QUIC peer could previously occupy all 32 shared send/connect workers.
quic-go's `SendDatagram` blocks when its 32-frame connection queue is full.
Repeated jobs for that peer could therefore prevent votes reaching healthy
peers, even while the broadcaster reported zero queue drops.

Each authenticated connection now owns one sender and a FIFO queue of at most
256 encoded messages. The encoded payload is immutable and shared across peer
queues. The existing bounded worker pool handles connection attempts only;
`VotorBroadcasterConfig.Workers` controls that pool. A blocked connection cannot
consume another peer's sender or a connection worker.

A watchdog checks every 100 ms whether the active datagram's peer-queue wait
plus its current `SendDatagram` duration has reached one second. Occasional QUIC
PTO probes can free queue entries without proving delivery; they no longer
restart this budget for an old backlog. Dequeue also retires a connection before
feeding an already-one-second-old entry into QUIC, even if sends keep completing
between watchdog ticks. The effective active-send bound is one second from
fanout enqueue, plus up to one watchdog interval and runtime scheduling delay.

Retirement closes that connection, wakes a blocked sender, discards its queued
copies and requests a bounded reconnect. This is an operational limit on local
queueing, not a consensus validity deadline or a remote-delivery guarantee. It
adds no per-send timer or goroutine. An idle connection is not expired merely
because its previous send had a long queue delay.

## Failure and ordering semantics

- The global `Enqueue` contract is unchanged: rejecting a newly signed message
  from the global queue returns an error to the voting engine.
- Per-peer fanout remains best effort. A full peer queue rejects that peer's
  newest copy, increments both `PeerQueueDrops` and the existing
  `MessagesDropped` counter, and continues sending to other peers.
- Messages queued on a failed, removed or replaced connection are discarded and
  counted in `PeerQueueDiscarded`. They are not transferred to a new connection
  or address. A datagram expired at dequeue is counted in `PeerQueueDiscarded`;
  a failed `SendDatagram` call is counted in `PeerSendErrors`. The watchdog and
  dequeue path claim retirement under the sender mutex, counting one timeout.
- Every sender exit requests a bounded, deduplicated reconnect. This covers
  both a remote-close notification and closure detected while dequeuing. Peer
  departure, shutdown and a healthy replacement suppress obsolete requests;
  normal remote closure no longer relies on the periodic reconciliation tick.
- Messages for disconnected peers increment `PeerSendsSkipped`, as before.
  This change adds no automatic application-level retransmission.
- A single sender preserves local enqueue order within its connection. QUIC
  datagrams themselves still do not guarantee arrival or ordering.
- Shutdown cancels connection attempts, closes the connections outside the
  broadcaster mutex, drains their queues, and waits for sender exit.

Signing decisions, persisted vote history, durable slot reservations, and
certificate validation are unchanged.

## Observability

The voting log adds `peer_queue_drops`, `peer_queue_discarded`,
`peer_send_timeouts`, and `peer_queue_max_delay`. These counters/high-water marks
survive reconnects. `PeerQueueMaxDelay` measures time from fanout enqueue to
sender dequeue, including entries retired before reaching QUIC; it does not
include time blocked inside `SendDatagram`.

Voting snapshots also expose `broadcast_peer_queues` with each current peer's
identity, address, queue depth, time in the active send, last/max queue delay,
and queue drops. Durations in the JSON snapshot use nanoseconds. Per-connection
statistics disappear when that connection is replaced; totals remain available.
Successful `PeerSends` only means quic-go accepted the datagram, not that a
remote validator received it.

## Regression coverage

`TestVotorBroadcasterIsolatesBlockedPeer` establishes two real loopback QUIC
connections, then blackholes one UDP path. It observes the blocked
`datagramQueue.Add` stack and requires a healthy-peer vote to arrive within
250 ms while the failed connection is still open. It also covers peer queue
overflow, watchdog reconnection, fresh traffic after reconnect, shutdown with a
blocked sender, peer departure, and replacement of a blocked peer's address.
Its connection-retirement deadline is measured from the blackhole, with explicit
scheduler slack; the healthy-peer latency assertion remains 250 ms.

Deterministic regressions reproduce a fresh send following an old queue wait,
remote closure while idle and while dequeuing, and an already-aged queue entry.
The reconnect fixture has no reconciliation loop, so a timer cannot hide a missed
reconnect trigger. The pre-fix failures and fresh validation are retained under
[historical evidence](https://github.com/Overclock-Validator/mithril/blob/54b233ff0e27fb929644f7d53bf4a699cb590cd8/docs/results/review-fixes/2026-09-15).

The signature-verification config template and its tests now belong to the
streaming branch, which reads those settings. This standalone voting branch
retains its base branch's supported `tuning.sigverify_backend` template.

```sh
go test -race ./pkg/alpenglow ./pkg/consensus -count=1 -timeout=180s
go vet ./pkg/alpenglow ./pkg/consensus
go build ./cmd/mithril
```

This is a transport regression, not a prediction of FAST score improvement.
The live validator needs a separately validated integrated build and matching
probe addresses before deployment; the PR checkout does not include every
change in the currently enrolled validator binary.
