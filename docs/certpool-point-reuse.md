# Reuse verified BLS signature points

Incoming verification returns parsed, verified members and reuses their signature
points when folding the tally. Failed aggregate checks subdivide parsed members
instead of reparsing each subset. Individual verification returns the same member
representation. Installed validator sets retain their parsed public keys.

The unused tally public-key sum is removed. Randomized verification still builds
its required weighted public-key sum with independent full-field coefficients.
Subgroup/infinity checks, invalid-share rejection, duplicate/equivocation checks,
stake accounting and paired-vote disjointness remain enforced.

Point ownership and message association are covered by differential tests against
individual verification, including malformed signatures, invalid ranks and wrong
payloads. See [pool concurrency and aggregation](certpool-offlock.md) for the
current lock contract, combined implementation and benchmark method.
The isolated point-reuse experiment is preserved in the
[historical evidence](certificate-processing-evidence.md).
