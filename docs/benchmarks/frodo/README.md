# FRODO benchmark figures

These figures reuse recorded mainnet data measured at336a5249. They are not measurements of the later preset/CLI commit. OFF uses two original OFF passes; ON uses the two UNCHANGED-enabled passes. Each timed pass followed a full15,000-block OFF warm-up; first30excluded,14,970unique measured slots469508707–469523680,29,940observations/arm. The heavy group is1,577distinct blocks consuming at least25M CUs (3,154observations/arm,34.206914MaverageCUs).

OFF and ON were run on different local calendar days, not interleaved. Storage/cache scheduling differences limit causal attribution. All runs matched the recorded bank hashes, counts and exact consumed CUs. Ratios use sums over identical blocks and repetitions. Old measurements were not rerun or overwritten.

- `block-interval-25m-plus.svg`: completion-to-completion intervals,0.5msbins,all>=100ms in overflow. Both repetitions pooled as observations. Median/p99/peak are independent distribution statistics, not necessarily the same slot in each arm.
- `txloop-time-remaining-25m-plus.svg`: total ON TxLoop ns / total OFF TxLoop ns in10-percentage-point failed-CU-share bands. The common failed-CU metadata comes from a separate untimed pass. Each dot summarizes matched blocks. Bands with at least30blocks are connected; sparse bands are hollow. Shading is a conditional95%interval from3,000circular128-slotblock-bootstrap draws, not independent run/day uncertainty. Axes0–100%; sparse-band estimates outside the display are shown at the boundary.

Both plots retain measured hardware, Go version, build and process-RSS provenance. Peak RSS excludes OS page cache and does not establish a RAM requirement. These are offline saved-shred results, not live final-shred latency. Raw per-block and machine evidence is retained separately; no server names, paths or credentials are embedded in these figures.

The approximately 65% offset callout is the aggregate TxLoop reduction (34.624%) divided by failed-CU share (53.366%). It assumes proportional time per CU and does not directly measure the fraction of failed-transaction execution removed.
