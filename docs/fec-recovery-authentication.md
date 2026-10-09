# Authenticating recovered FEC data

Received shreds must pass leader-signature verification before entering the slot
assembler. Reed–Solomon reconstruction alone does not authenticate missing data:
a leader can sign a Merkle tree containing inconsistent data and coding shards.
Structural validation of recovered headers does not reject that case.

Both the specialized one-missing-data decoder and the general decoder now require
`authenticateRecoveredFEC` to succeed before returning any recovered data. It
reconstructs missing coding shards too, builds the complete data/coding Merkle
tree, and compares its root with a received coding shred's signed root. Checking
only recovered-data proofs would not detect an inconsistent commitment to a
missing coding shard. Existing received packets are read-only throughout recovery.

On success, recovered data receives complete Merkle proofs and the coding
template's chained root and, where applicable, retransmitter signature. The latter
is a hop signature copied from a received packet, not a reconstruction of a lost
relay's signature. On failure, no recovered data is published. The all-data-present
path does not reconstruct or authenticate another tree; it relies on ingress
verification of the received data.

This follows [Agave's recovery algorithm](https://github.com/anza-xyz/alpenglow/blob/9f284c913f3c78b36179ae2461fa91286a616fb9/ledger/src/shred/merkle.rs#L670):
reconstruct all missing shards, validate recovered headers, compare the full root,
and populate proofs. Signature verification is not repeated after the root match.

## Validation

`TestRecoveredFECAuthentication` exercises one, three and all 32 missing data
shreds, chained and resigned packets, altered recovered bytes, and leader-signed
inconsistent parity both received and absent. Invalid sets return no recovered
shreds. Valid recovered packets verify with the leader's public key.

`TestRecoveredFECAgaveSignedCapture` uses four FEC sets from the existing captured
Agave slot 1,752,420. It regenerates parity without signing a new root, checks the
original committed roots, and compares recovered authenticated bytes and proofs
with the capture. Transport repair nonces and relay-specific signatures are
handled separately. This is a captured-wire compatibility test, not a run of a
Rust recovery oracle. The older localnet recovery test also covers an unchained
1+17 layout, with its 2022 proofs replaced by current signed proofs.

## Cost and reproduction

Apple M4 Pro, `GOMAXPROCS=2`, five 200 ms samples, warmed encoder cache. The baseline
is `039ebd67`; times are medians for one FEC recovery, excluding ingress signature
verification and block execution.

| Received / recovered shape | Before | Authenticated recovery |
|---|---:|---:|
| 31 data + 32 coding; recover one data | 2.47 µs | 30.17 µs |
| 31 data + 1 coding; recover one data and missing parity | — | 104.52 µs |
| 29 data + 3 coding; recover three data and missing parity | — | 117.93 µs |
| 32 coding; recover all data | — | 118.01 µs |

The first case increases from 4,264 bytes / 5 allocations to 8,360 bytes / 6
allocations per operation. Threshold-arrival cases also pay to reconstruct missing
parity and assemble its leaf bytes. These measurements establish the cost of the
correctness check, not a live FAST-score or whole-block performance result.

```
GOMAXPROCS=2 go test ./pkg/turbine -run '^$' \
  -bench 'Benchmark(RecoverFECOneMissingBoundary|AuthenticatedFECRecovery)$' \
  -benchmem -benchtime=200ms -count=5
```
