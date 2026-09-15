# Review follow-up validation, September 15

Fixes the stalled-peer deadline, remote-close reconnect paths, config-template ownership and entry-identity recovery from the external review. The two-worker Turbine default remains unchanged.

The sender deadline now covers queue wait plus the active QUIC enqueue, with an age check at dequeue as well. Every sender exit requests a bounded/deduplicated reconnect, subject to desired-peer, shutdown and replacement checks. Peer queues remain bounded FIFO with existing best-effort failure/discard behavior; no voting decision, persistence or certificate rule changed.

Streaming handles missing/oversized ranges and partial/mismatched identities by joining old readers and fully re-verifying final transactions. Valid blocks recover from optimization faults; invalid signatures, failed recovery, closed verifiers and cancellation remain errors. The template and test live with the config reader in streaming. VM pooling remains owned by the runtime branch: the standalone streaming template does not enable it implicitly.

Before-fix logs demonstrate missing reconnect requests in both idle/dequeue paths, missing queue-age expiry, stale enqueueing and valid-block rejection on inconsistent identity metadata. After-fix targeted race tests pass. Full local standalone voting and streaming race suites, vet and builds pass. Final template narrowing also has separate config-test evidence.

The native combined checkout includes all six branches, the later transaction-status publication work and these fixes. Zen 5 Ryzen 7 9700X, Go 1.26.4, GOMAXPROCS=2, package parallelism=2; isolated staging process with CPU quota 200%, Nice=15, memory cap 6 GB. Full native race suites passed for Alpenglow, consensus, Turbine, block, txverify, txstatus, sigverify, replay, configcmd and node. Native vet and validator build passed. After narrowing the template, config/default/node race tests and the final native build passed again.

The real QUIC blackhole regression passed ten race runs on M4 Pro and ten on Zen 5. Each run covers reconnect, shutdown, peer departure and address replacement. Native watchdog retirement was 1.038–1.098 seconds after the fault; healthy marker latency across 40 subcases was 0.180 ms median, 8.361 ms maximum. These are loopback regression measurements under race detection, not live network delivery or FAST results. The existing 250 ms healthy-marker assertion was retained. Retirement allowance is measured from the fault with explicit scheduler slack. Deterministic reconnect tests disable reconciliation entirely, so a later tick cannot hide the bug.

Source hashes, test failures before changes, passing logs and native timing summary are retained here. Native test/build transcripts have native prefixes. The staging executable was not deployed. The enrolled validator remained PID 568418, voting at lag one with all four server services active at the final health check.
