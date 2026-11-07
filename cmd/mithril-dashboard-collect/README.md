# Optional external-cluster collector

This one-shot command reads fixed public Alpenglow Community Cluster providers
and atomically writes a typed cache. It does not start a server or install a
service. It does not read keys, node configuration, or a local validator feed.

```sh
go run ./cmd/mithril-dashboard-collect --profile alpenglow --output /existing/private-directory/cluster.json
```

Add `--performance-sample` for one recent RPC sample, or
`--performance-history` for up to 120 samples.

Provision the directory and reader group separately. The cache is written with
mode `0640`; a setgid directory can supply the intended reader group. A scheduler
may invoke the command periodically (for example once per minute). Do not run it
per viewer request or give the network-denied reader outbound access. There is
no arbitrary endpoint flag. An existing destination symlink is refused.

Exit 0 means every supported provider was fresh. Exit 1 may mean a valid degraded
snapshot was written; inspect provider states/reasons and timestamps. An invalid
profile, output failure, or internal validation failure does not replace the old
file. Readers must mark that old file stale rather than treating its presence as
freshness.

The profile is fixed to genesis
`9mi4SxvrMwaErcAN9r8wAAsX8YTKedGNogzQCuhTZ7Nz`, not mainnet or a private preview.
RPC genesis and epoch are checked before and after capture. Vote identities come
from that RPC's current vote-account response. Provider names and versions are
untrusted public metadata, not operator identity verification.

SolStat supplies directory/credit-efficiency observations; Titan supplies
certificate inclusion over its recorded observation windows. Inclusion is not
FAST, payment, stake-weighted performance, or complete chain coverage. The rank
is a derived competition rank: exact inclusion ratio, then opportunities,
descending; exact ties share rank. Zero opportunities have no score or rank.

Country estimates require matching Lantern's peer identity and gossip IP to the
same identity's current address at the checked RPC. Unknown/mismatched/old
contacts stay unlocated. Only country codes/source survive; IPs are never placed
in the cache or logs. `source_time_kind=receipt` on Lantern is the collection
receipt time, not an independently attested GeoIP update time. Country estimates
are not exact locations, facilities, liveness, or remote-host health.

Pages displaying countries must credit [DB-IP](https://db-ip.com/) (CC BY 4.0)
and [Lantern](https://alpenglow.lantern.one/). The inspected country derivation is
[pinned here](https://github.com/bola-sol/agave/blob/ef95987724d87162fc9d86a09714a1f2a99b7d50/dashboard/src/geoip.rs);
this is not an attestation of the current live binary. No per-IP GeoIP service
is queried. Provider availability and licensing may change.

Bounds: one capture at most 55 seconds; HTTP bodies at most 2 MiB; Lantern
capture at most 18 seconds/4 MiB/2,500 frames; at most 2,048 rows per source;
persisted UTF-8 JSON at most 4 MiB. Redirects are denied. Counters, slots, epochs,
and ranks are decimal strings. `stake_sol` is a bounded provider decimal SOL
estimate, never an asserted exact lamport balance.

Operational questions are: did a bound capture complete, which provider failed
or became stale, and how long did its bounded operation take? JSON logs carry
stable event names, fixed provider/operation labels, and a run ID. Per-operation
duration samples can be aggregated into percentiles externally; this one-shot
job intentionally has no metrics listener, retry loop, alert service, or tracing
daemon. Response bodies and raw errors/peer addresses are not logged.

```sh
go test -race ./pkg/dashboard/cluster ./cmd/mithril-dashboard-collect
go vet ./pkg/dashboard/cluster ./cmd/mithril-dashboard-collect
```
