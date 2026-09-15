# Mithril performance review branches

The accumulated changes from PR #279 and the later working tree are organized into six focused branches. All branches have been pushed as 7layermagik. PR creation is pending because GitHub returned HTTP403 for the token’s pull-request write permission; the existing PR #279 is unchanged.

The current alpenglow-dev base was checked immediately before publication: `33dde4050d9250557583395810799aaac2f54017`. Four branches are based directly on it. Voting is stacked on certificate processing; leader packing is stacked on streaming preparation.

| Review | Base | Code diff | Description |
|---|---|---|---|

| turbine: prepare complete transaction batches during shred arrival | `alpenglow-dev` | [Review diff](https://github.com/Overclock-Validator/mithril/compare/alpenglow-dev...7layer%2Freview-streaming-preparation) | [Scope, tests and benchmarks](review-prs/streaming-preparation.md) |

| alpenglow: reduce certificate verification lock contention | `alpenglow-dev` | [Review diff](https://github.com/Overclock-Validator/mithril/compare/alpenglow-dev...7layer%2Freview-certificate-processing) | [Scope, tests and benchmarks](review-prs/certificate-processing.md) |

| replay: defer status checkpoint encoding and batch expiry | `alpenglow-dev` | [Review diff](https://github.com/Overclock-Validator/mithril/compare/alpenglow-dev...7layer%2Freview-status-checkpoint-expiry) | [Scope, tests and benchmarks](review-prs/status-checkpoint-expiry.md) |

| runtime: enable VM pooling and preserve owned vote state | `alpenglow-dev` | [Review diff](https://github.com/Overclock-Validator/mithril/compare/alpenglow-dev...7layer%2Freview-runtime-allocation) | [Scope, tests and benchmarks](review-prs/runtime-allocation.md) |

| leader: improve packing and add near-limit block benchmarks | `7layer/review-streaming-preparation` | [Review diff](https://github.com/Overclock-Validator/mithril/compare/7layer%2Freview-streaming-preparation...7layer%2Freview-leader-packing) | [Scope, tests and benchmarks](review-prs/leader-packing.md) |

| alpenglow: isolate vote delivery and reserve durable signing bounds | `7layer/review-certificate-processing` | [Review diff](https://github.com/Overclock-Validator/mithril/compare/7layer%2Freview-certificate-processing...7layer%2Freview-vote-delivery-persistence) | [Scope, tests and benchmarks](review-prs/vote-delivery-persistence.md) |


Start with runtime allocation, certificate processing and the status cache for the smaller independent reviews. Streaming preparation contains the newest large-block fix. Review voting after certificate processing, and leader packing after streaming. Each diff includes its own tests and applicable benchmark evidence; generated evidence is collapsed using .gitattributes.

## Verification

The split branches were recombined in an isolated audit checkout. After reconciling shared CLI/configuration additions in three files, Go sources, module files, TOML configuration and CI matched the preserved full implementation exactly. That snapshot includes the original PR’s review-fix commit d1172b9a and the later performance work. Formatting-only cleanup followed. The original dirty working tree was left unchanged and its17file hashes verified.

Individual branch checks passed as documented in docs/results/pr-split-2026-09-15. The combined validator build passed. The combined race run hit the known intermittent `TestVotorBroadcasterIsolatesBlockedPeer/reconnect` timeout; its other suites passed. The intended voting PR is draft pending review of this qualification issue. Full sealevel-suite success is not claimed because the original PR recorded unrelated BPF-loader failures on the unchanged base.

## Scope and runtime

No new transaction-status publication optimization was started. Deferred scheduler retry experiments remain separate. No validator deployment, restart, load-policy change or key/ledger operation was performed during the split. The previously deployed validator, continuous own-leader large blocks and monitoring remain under the existing server services.

This integration branch preserves the full combined implementation and historical combined evidence for reference. Review the six focused diffs above; it is not an additional monolithic PR.
