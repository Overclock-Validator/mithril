A stalled QUIC peer could occupy all shared vote-send workers. Give each authenticated connection one bounded ordered sender while retaining a separate bounded connection pool. Timeout the active datagram by total peer-queue wait plus QUIC enqueue time, retire already-aged entries at dequeue, and request reconnect from both remote-close exit paths. Sporadic PTO queue progress cannot restart an old backlog's budget. Shutdown, departure and healthy replacements suppress obsolete reconnect requests; timeout/discard accounting remains bounded and connection-specific.

Correct the Votor certificate layout for Agave/Firedancer interoperability. Separately, opt-in durable signing reservations remove explicit per-vote sync from the live path while bounding crash uncertainty. Use one ordered background history writer, retain detailed decisions, gate restart voting/leadership conservatively, preserve clean-shutdown markers, and expose `--wait-to-vote-slot`. Order vote admission and pruning behind replay. Bind and validate local transports before consuming the clean marker.

Stacked on certificate processing for the replay-progress API. Leader-packing optimizations are separate; blockprod changes here enforce signing safety only. Signature-verification template changes belong to streaming, which reads those settings; this branch retains its base template. Default history persistence remains synchronous. Reserved mode is experimental and requires preserving both safety files; these tests do not establish mainnet power-loss qualification.

### Review and validation

Start with `docs/reserved-vote-history.md` and `docs/votor-peer-isolation.md` for queue bounds, ordering and recovery assumptions. Keep this branch draft pending review of its consensus-sensitive behavior.

The earlier reconnect timeout exposed a real queue-age weakness. New deterministic regressions fail before the fix and cover progress with an old backlog, expiry before QUIC enqueue, and remote closure while idle or dequeuing. The reconnect fixture has no periodic reconciliation that could mask a missed request.

Ten real QUIC blackhole race runs passed on both M4 Pro and Zen 5, covering reconnect, shutdown, departure and address replacement. Native retirement was 1.038–1.098 seconds from the blackhole; healthy marker latency across 40 subcases was 0.180 ms median and 8.361 ms maximum. The 250 ms healthy-peer assertion is unchanged; retirement allowance is measured from the fault with explicit scheduler slack. These are loopback tests, not live delivery or FAST measurements.

Full local standalone race suites, vet and build passed. Fresh combined native race suites, vet and build also passed, including the latest status-publication and streaming follow-ups. Raw before/after logs and tested source hashes: `docs/results/review-fixes/2026-09-15`. The earlier combined failure remains preserved in `docs/results/pr-split-2026-09-15/voting/combined-tests.log`.

Extracted from #279 with subsequent fixes. These follow-ups are not deployed; the enrolled validator continues running its existing binary.
