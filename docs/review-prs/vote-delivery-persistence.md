A stalled QUIC peer could occupy all shared vote-send workers. Give each authenticated connection one bounded ordered sender, retain bounded connection workers, account for drops/discards, and close/reconnect stalled sends. Correct the Votor certificate layout for Agave/Firedancer interoperability.

Separately, opt-in durable signing reservations remove explicit per-vote sync from the live path while bounding crash uncertainty. Use one ordered background history writer, retain detailed vote decisions, gate restart voting/leadership conservatively, preserve clean-shutdown markers, and expose `--wait-to-vote-slot`. Order vote admission and pruning behind replay. Bind/validate local transports before consuming the clean marker, and expose configuration options in the template.

This is stacked on the certificate-processing PR for the replay-progress API. Leader-packing optimizations are separate; changes under blockprod here enforce signing safety only. Default history persistence remains synchronous. Reserved mode is experimental, requires preserving both safety files, and is not qualified for mainnet power-loss safety by these tests.

### Review and validation

Start with `docs/reserved-vote-history.md` and `docs/votor-peer-isolation.md`. Queue bounds, failure behavior, recovery assumptions and tests are documented there. The healthy-peer isolation regression measures continued delivery during a blocked send; local serialization is not proof of remote receipt.

Standalone voting/consensus, Alpenglow, leader safety, config, node and config-template race suites passed; vet and the full build passed. The recombined audit build passed, and its other race suites passed, but `TestVotorBroadcasterIsolatesBlockedPeer/reconnect` hit the previously observed intermittent timeout at peer_sender_test.go:209. The healthy peer received its marker in4.27ms in that run. The failure is preserved in `docs/results/pr-split-2026-09-15/voting/combined-tests.log`; tests were not weakened or rerun to conceal it. **Keep this PR draft pending review of that qualification issue.**

This extracts the transport/persistence part of #279 and includes subsequent peer isolation. Reorganization did not restart or modify the running validator.

Split from #279; base development commit: `33dde4050d9250557583395810799aaac2f54017`. Historical native measurements retain their original tested source; these reorganized heads have fresh local validation.
