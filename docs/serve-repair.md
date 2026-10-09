# Solana UDP repair serving

The turbine receiver can advertise and bind a Solana serve-repair socket backed
by its verified raw-shred spool. It requires an identity key, a configured spool,
and leader signature verification. Modern signed `WindowIndex` requests return
the exact data shred; `HighestWindowIndex` returns the highest retained data
shred at or above the requested index. Responses append the original request's
four-byte nonce to the canonical shred payload.

Requests must name this server, have an acceptable timestamp, and pass strict
Ed25519 verification. Before reading or serving shreds, the server sends the
standard signed repair ping and requires a matching signed pong from the same
requester identity and UDP source address. The client retries its request after
the handshake. This prevents a valid signed request with a spoofed source from
reflecting shred data. Verified sources expire after 20 minutes; outstanding
challenges expire after one minute and are retried at most every two seconds.
Challenge state and per-address rate limits each retain at most 65,536 entries.
The request queue is bounded and verification uses at most four workers.

An incoming repair ping is also answered with the serving identity. This is a
separate challenge initiated by the peer; answering it does not authorize that
peer to request shreds. Forged, unmatched, expired or replayed pongs cannot
extend authorization.

Repair lookups reconstruct an index on first access after restart and verify
stored checksums on each read. A damaged tail is truncated only after the ordered
completion-journal invalidation finishes, so an older queued completion cannot
restore a stale hint. The spool remains a disposable cache under its existing
byte cap and advancing retention floor, rather than a durable historical ledger.
Only retained canonical raw data shreds are served; this change does not add
orphan/ancestor-hash repair, FEC-reconstructed wire payloads, snapshots or
independent live-cluster startup.

Review validation on macOS arm64, Go 1.26.4:

```sh
go test -race ./pkg/repair ./pkg/gossip ./pkg/turbine ./pkg/blockstream \
  ./cmd/mithril/node ./cmd/mithril/configcmd ./pkg/config
go vet ./pkg/repair ./pkg/gossip ./pkg/turbine ./pkg/blockstream \
  ./cmd/mithril/node ./cmd/mithril/configcmd ./pkg/config
go build ./cmd/mithril
```

These checks passed, including real UDP request/challenge/pong/retry exchanges,
forged-pong rejection, identity/address matching, expiry, cache bounds and a
blocked completion-journal writer during a repair read. External Agave/live
cluster repair interoperability was not exercised during this review.
