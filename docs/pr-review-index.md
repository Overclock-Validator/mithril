# Mithril performance reviews

Seven review branches are published. Runtime allocation and certificate processing target `alpenglow-dev` directly; Turbine/FEC and checkpoint/publication include PR #278 (`e1204b32`). The table below links each branch and its proposed PR description. For testing all changes together, use [7layer/review-performance-combined](https://github.com/Overclock-Validator/mithril/tree/7layer/review-performance-combined).

#278 was still open when checked before publication. Turbine/FEC and checkpoint/publication target its branch so their diffs exclude its changes; after it merges, retarget them to `alpenglow-dev`. Runtime allocation and certificate processing are independent and can merge first. Voting is stacked on certificates; leader packing and spool completion are stacked on Turbine/FEC.

| Review / create link | Published head | Base | Ready description |
|---|---|---|---|
| [runtime: enable VM pooling and own retained vote state](https://github.com/Overclock-Validator/mithril/compare/alpenglow-dev...7layer%2Freview-runtime-allocation?expand=1) | `7e8d497e` | `alpenglow-dev` | [Description](review-prs/runtime-allocation.md) |
| [alpenglow: reduce certificate verification contention](https://github.com/Overclock-Validator/mithril/compare/alpenglow-dev...7layer%2Freview-certificate-processing?expand=1) | `3f074edd` | `alpenglow-dev` | [Description](review-prs/certificate-processing.md) |
| [turbine: accelerate FEC and prepare transactions during shred arrival](https://github.com/Overclock-Validator/mithril/compare/smcio%2Ffix-skipped-slot-cert-handling...7layer%2Freview-streaming-preparation?expand=1) | `039ebd67` | `smcio/fix-skipped-slot-cert-handling` | [Description](review-prs/streaming-preparation.md) |
| [replay: prepare status publication and defer checkpoint work](https://github.com/Overclock-Validator/mithril/compare/smcio%2Ffix-skipped-slot-cert-handling...7layer%2Freview-status-checkpoint-expiry?expand=1) | `1f91ebb2` | `smcio/fix-skipped-slot-cert-handling` | [Description](review-prs/status-checkpoint-expiry.md) |
| [leader: reduce packing overhead and add near-limit block tests](https://github.com/Overclock-Validator/mithril/compare/7layer%2Freview-streaming-preparation...7layer%2Freview-leader-packing?expand=1) | `8e0409f8` | `7layer/review-streaming-preparation` | [Description](review-prs/leader-packing.md) |
| [turbine: publish spool completion without waiting for journal writes](https://github.com/Overclock-Validator/mithril/compare/7layer%2Freview-streaming-preparation...7layer%2Fspool-completion-journal?expand=1) | `23a78a92` | `7layer/review-streaming-preparation` | [Description](review-prs/spool-completion-journal.md) |
| [alpenglow: isolate vote delivery and bound crash recovery](https://github.com/Overclock-Validator/mithril/compare/7layer%2Freview-certificate-processing...7layer%2Freview-vote-delivery-persistence?expand=1) | `fc7d5d99` | `7layer/review-certificate-processing` | [Description](review-prs/vote-delivery-persistence.md) |

The runtime review is now two commits, with redundant Viper defaults and the evidence-only document removed. The combined testing branch includes this cleanup and the certificate follow-ups at `9db7dcba`; the September 16 validation manifest below preserves the earlier tested heads.

The FEC producer target and component-completion correction now land in one atomic commit. The combined testing branch is a single integration commit to avoid retaining the superseded unsafe intermediate history.

## Scope and order

Start with runtime ownership/defaults, certificate processing, and checkpoint/publication work. The combined Turbine review includes the FEC producer/recovery changes formerly reviewed in #259 plus streaming preparation. Review leader packing and spool completion after it. Keep vote delivery/persistence **draft** until its transport and crash-recovery contract receive dedicated review.

The later partitioned status-map experiment is excluded: it added preparation work without establishing an overall p99 gain. Its original source and measurements remain in the status evidence tag. No new execution optimization or live-validator deployment was performed here.

## Clean review policy

The proposed diffs retain subsystem contracts, reusable tests/benchmarks, concise methodology and material limitations. Raw output, session notes, deployment diaries and machine-specific paths were removed. Historical evidence is preserved in the `review-evidence-20260916-*` tags. Descriptions state the actual benchmark baselines and do not treat historical component results as fresh #278 comparisons or combine their speedups.

[Fresh validation and exact source manifest](results/review-preparation/2026-09-16/README.md) records standalone and combined checks, the corrected test-only integration failure, and the remaining whole-suite limitations. No broad sealevel or mainnet power-loss qualification is claimed.

## Existing PRs

- [#259](https://github.com/Overclock-Validator/mithril/pull/259): older FEC-only draft. Its implementation is incorporated in the new combined Turbine branch; retain it until the replacement PR can be opened and linked.
- [#279](https://github.com/Overclock-Validator/mithril/pull/279): older combined performance PR. Its passing CI is not validation of these newer split heads. Replace it with the focused reviews once their PRs exist.

This branch is a review index and historical combined source snapshot, not a merge candidate or deployable checkout of the final integration. Use the individual published heads for review; the validation manifest identifies the recombined source. Proposed PR descriptions are maintained alongside this index.
