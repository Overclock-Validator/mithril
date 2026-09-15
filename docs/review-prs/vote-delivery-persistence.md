A stalled QUIC peer could occupy all shared vote-send workers. Give each authenticated connection one bounded ordered sender while retaining a separate bounded connection pool. Timeout the active datagram by total peer-queue wait plus QUIC enqueue time, retire already-aged entries at dequeue, and request reconnect from both remote-close exit paths. Sporadic PTO queue progress cannot restart an old backlog's budget. Shutdown, departure and healthy replacements suppress obsolete reconnect requests; timeout/discard accounting remains bounded and connection-specific.

Correct the Votor certificate layout for Agave/Firedancer interoperability. Separately, opt-in durable signing reservations remove explicit per-vote sync from the live path while bounding crash uncertainty. Use one ordered background history writer, retain detailed decisions, gate restart voting/leadership conservatively, preserve clean-shutdown markers, and expose `--wait-to-vote-slot`. Order vote admission and pruning behind replay. Bind and validate local transports before consuming the clean marker.

Stacked on certificate processing for the replay-progress API. Leader-packing optimizations are separate; blockprod changes here enforce signing safety only. Signature-verification template changes belong to streaming, which reads those settings; this branch retains its base template. Default history persistence remains synchronous. Reserved mode is experimental and requires preserving both safety files; these tests do not establish mainnet power-loss qualification.

### Crash-recovery contract

The intended guarantee is that loss of recent detailed history must not authorize conflicting votes or reserved-mode leader actions after restart. Availability is deliberately sacrificed when prior decisions are uncertain. This does not promise that every vote survives on disk or that voting resumes immediately.

- **Default synchronous mode:** write, file-sync, rename and directory-sync exact retained history before local pool admission or network enqueue. BLS bytes may already have been computed privately; the guarantee is persist-before-publication, not before BLS computation.
- **Reserved mode:** acknowledge the durable slot allowance before signing. A complete detailed history snapshot is prepared/queued before publication, but its submission, background rename and `written` counter are not durable per-vote acknowledgements.
- **Unclean restart:** let `H` be the bound loaded at startup. Every vote type and restored local vote at `S <= H` remains forbidden. Voting at `S > H` additionally requires verified finality/checkpoint state `F >= H`, a current acknowledged reservation covering `S`, and all ordinary protocol checks. New blocks, elapsed time, RPC tips and `--wait-to-vote-slot` cannot release this barrier.
- **Clean vote recovery:** stop/join signers and writers, sync exact history, then sync its digest in the reservation. Startup must validate the history/digest and durably consume the clean marker before new signing or history mutation. A normal exit or attempted save alone is insufficient. Leader production still skips the previous reservation even after a clean vote-history seal, because vote history is not a leader-block journal.

Example: votes escaped through 1,015, surviving history ends at 1,012, and `H=1,032`. Slots through 1,032 remain forbidden after an unclean restart; 1,033 can pass the recovery gate only once verified finality reaches 1,032 and its new grant is durable. This can mean indefinite abstention if the cluster halts below the bound.

The guarantee assumes sync-honoring storage, a single fenced identity owner and preserved current safety files independent of AccountsDB checkpoints. Signed files/generation numbers do not detect rollback of an old valid pair. Missing/corrupt enrolled state must not be automatically reset; deleting both files and re-enrolling an old identity is outside the contract. The status checkpoint encoding cache does not replace either safety file.

`docs/reserved-vote-history.md` defines the failure model, startup matrix, clean-seal ordering and named tests. Existing tests cover lost valid history suffixes, H/H+1 for all vote types/restoration, clean-marker consumption, leader gates, uncertain writes and process death with pending snapshots. They do not power-cycle storage or constitute a formal consensus/mainnet durability qualification.

### Review and validation

Start with `docs/reserved-vote-history.md` and `docs/votor-peer-isolation.md` for queue bounds, ordering and recovery assumptions. Keep this branch draft pending review of its consensus-sensitive behavior.

The earlier reconnect timeout exposed a real queue-age weakness. New deterministic regressions fail before the fix and cover progress with an old backlog, expiry before QUIC enqueue, and remote closure while idle or dequeuing. The reconnect fixture has no periodic reconciliation that could mask a missed request.

Ten real QUIC blackhole race runs passed on both M4 Pro and Zen 5, covering reconnect, shutdown, departure and address replacement. Native retirement was 1.038–1.098 seconds from the blackhole; healthy marker latency across 40 subcases was 0.180 ms median and 8.361 ms maximum. The 250 ms healthy-peer assertion is unchanged; retirement allowance is measured from the fault with explicit scheduler slack. These are loopback tests, not live delivery or FAST measurements.

Full local standalone race suites, vet and build passed. Fresh combined native race suites, vet and build also passed, including the latest status-publication and streaming follow-ups. Raw before/after logs and tested source hashes: `docs/results/review-fixes/2026-09-15`. The earlier combined failure remains preserved in `docs/results/pr-split-2026-09-15/voting/combined-tests.log`.

Extracted from #279 with subsequent fixes. These follow-ups are not deployed; the enrolled validator continues running its existing binary.
