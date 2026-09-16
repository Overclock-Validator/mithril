One stalled QUIC peer could occupy shared send workers and delay votes to healthy validators. Give each connection a bounded ordered sender, enforce total queue-plus-send age, and reconnect after remote closure. Preserve queue limits, shutdown joining and obsolete-connection suppression. Correct the Votor certificate layout for Agave/Firedancer interoperability.

Also add **opt-in** durable signing reservations, an ordered background history writer, and `--wait-to-vote-slot`. Default history persistence remains synchronous. Keep this review **draft** for transport and consensus-recovery review.

### Recovery contract

- Default mode syncs retained history before local publication/network enqueue.
- Reserved mode durably acknowledges a future slot allowance before signing. Detailed history is queued before publication; queue submission is not a per-vote durability acknowledgement.
- After unclean restart with saved bound H, all signing through H is forbidden. Voting above H additionally requires verified finality/checkpoint state to reach H, a new durable allowance, and ordinary protocol checks. RPC tips, elapsed time and operator cutoffs cannot release this barrier.
- Clean vote recovery requires joined signers/writers, a synced exact-history digest and durable consumption of the clean marker at startup. Leader production still skips the previous reservation because vote history is not a leader-block journal.

This deliberately sacrifices availability when decisions are uncertain. It assumes sync-honoring storage, one fenced identity owner and preserved safety files. It does not detect rollback of an old valid file pair or qualify storage for mainnet power loss.

Historical real-QUIC blackhole tests on Zen 5 measured healthy-peer marker latency **0.180 ms median / 8.361 ms maximum** while retiring the stalled connection in **1.038–1.098 s**. These are loopback fault tests, not remote receipt or FAST measurements.

[Design, method and limitations](https://github.com/Overclock-Validator/mithril/blob/00125ab1618dc87be3aaeb8d4149556454bd761b/docs/reserved-vote-history.md).
[Design, method and limitations](https://github.com/Overclock-Validator/mithril/blob/00125ab1618dc87be3aaeb8d4149556454bd761b/docs/votor-peer-isolation.md).
Fresh local Alpenglow, consensus, block-production and node race suites passed, covering QUIC blackholes, history recovery, signing gates and startup. Combined integration validation also covers #278 and the other performance reviews. Historical native benchmarks retain their original baselines; this preparation pass ran locally, without touching the live validator.

[Historical benchmark evidence](https://github.com/Overclock-Validator/mithril/tree/54b233ff0e27fb929644f7d53bf4a699cb590cd8/docs/results) is preserved outside the proposed merge; reusable benchmarks and maintained contracts remain in source.

Stacked on `7layer/review-certificate-processing`, which now targets `alpenglow-dev` directly. Review this diff against that parent.

[Rebased validation and exact source heads](https://github.com/Overclock-Validator/mithril/blob/7layer/review-integration-20260915/docs/results/review-preparation/2026-09-16/README.md).
