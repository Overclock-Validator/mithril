# Mithril performance review branches

The changes from PR #279 and subsequent improvements are published in six focused review branches under **7layermagik**. GitHub still rejects PR creation with HTTP 403 (`Resource not accessible by personal access token`), confirmed September 15. These are published branches and ready PR descriptions, not six open PRs. PR #279 remains the older combined implementation.

The verified development base is `33dde4050d9250557583395810799aaac2f54017`. Voting is stacked on certificate processing; leader packing is stacked on streaming preparation. The other four branches target `alpenglow-dev`.

| Review | Current head | Base | Description |
|---|---|---|---|
| [turbine: prepare complete transaction batches during shred arrival](https://github.com/Overclock-Validator/mithril/compare/alpenglow-dev...7layer%2Freview-streaming-preparation) | `311f83ea` | `alpenglow-dev` | [Scope, tests and benchmarks](review-prs/streaming-preparation.md) |
| [alpenglow: reduce certificate verification lock contention](https://github.com/Overclock-Validator/mithril/compare/alpenglow-dev...7layer%2Freview-certificate-processing) | `72514abc` | `alpenglow-dev` | [Scope, tests and benchmarks](review-prs/certificate-processing.md) |
| [replay: prepare status publication and defer checkpoint work](https://github.com/Overclock-Validator/mithril/compare/alpenglow-dev...7layer%2Freview-status-checkpoint-expiry) | `c78e35cd` | `alpenglow-dev` | [Scope, tests and benchmarks](review-prs/status-checkpoint-expiry.md) |
| [runtime: enable VM pooling and preserve owned vote state](https://github.com/Overclock-Validator/mithril/compare/alpenglow-dev...7layer%2Freview-runtime-allocation) | `752ef973` | `alpenglow-dev` | [Scope, tests and benchmarks](review-prs/runtime-allocation.md) |
| [leader: improve packing and add near-limit block benchmarks](https://github.com/Overclock-Validator/mithril/compare/7layer%2Freview-streaming-preparation...7layer%2Freview-leader-packing) | `cc1e3dfc` | `7layer/review-streaming-preparation` | [Scope, tests and benchmarks](review-prs/leader-packing.md) |
| [alpenglow: isolate vote delivery and reserve durable signing bounds](https://github.com/Overclock-Validator/mithril/compare/7layer%2Freview-certificate-processing...7layer%2Freview-vote-delivery-persistence) | `54b233ff` | `7layer/review-certificate-processing` | [Scope, tests and benchmarks](review-prs/vote-delivery-persistence.md) |

## Newest validator changes

- Certificate processing includes bounded MultiExp with unchanged full-strength random coefficients and pending-only observer reconciliation (`72514abc`).
- Status checkpoints include immutable node encoding reuse, allocation-free rejection of incomplete fold batches (`d55c7962`), and retirement of completed rewards bookkeeping after durable promotion (`c78e35cd`). The latter avoids an unnecessary fork-recovery guard after rewards are safely rooted; native tests passed and it was deployed at 12:20 UTC. Live latency benefit awaits an applicable fork switch.
- Streaming includes relay-buffer reuse (`311f83ea`); voting and leader branches include their latest respective parent branches.

These changes are included in the combined Zen 5 testnet deployment. The September 15 empty-block comparison measured replay-admission p99 of **4.486 → 0.483 ms** (565 before, 401 after), after deploying both observer reconciliation and fold preflight. This short observational comparison does not isolate either change or establish sustained overall FAST improvement. The descriptions retain component benchmarks, their actual baselines, validation and limitations.

## Non-empty block follow-up

The subsequent investigation changed the synthetic sender’s pacing and added operational diagnostics; it did not change or restart the validator binary. The first matched pacing comparison included only ten baseline and four candidate blocks. It is not p99 evidence. The leader description records the settings, results, variable block fill and exclusions. Machine-specific services, funding/runtime state and trace collectors remain outside these validator review branches.

A roughly 51 ms completion-to-ingestion outlier remains unexplained and did not recur in the bounded trace. Omission from one FAST certificate does not prove absence from all FAST certificates. No fix or sustained FAST improvement is claimed for that investigation.

## Review and validation

Start with runtime allocation, certificate processing and status checkpoints. Review voting after certificates, and leader packing after streaming. Voting should remain draft: reserved persistence is opt-in and its explicit crash-recovery contract sacrifices availability when previous signing decisions are uncertain. Native race tests, vet and combined builds passed as documented in each description; testnet deployment is not mainnet power-loss qualification.

The split was initially recombined and checked against the preserved implementation; later follow-ups were checked in separate combined audit builds. Full sealevel-suite success is not claimed: unrelated BPF-loader failures were reproduced on the unchanged development base.

This integration branch is a review index and historical combined snapshot. Its source is not the latest combined deployed source; use the six focused branch heads for code review. No validator restart or runtime configuration change was performed while preparing this index.
