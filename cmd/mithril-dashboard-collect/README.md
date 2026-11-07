# Optional external-cluster collector

This one-shot command reads fixed public Alpenglow Community Cluster providers
and atomically writes a typed cache. It does not start a server or install a
service. It does not read keys, node configuration, or a local validator feed.

```sh
go run ./cmd/mithril-dashboard-collect --profile alpenglow --output /existing/private-directory/cluster.json
```

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

Country estimates use the bundled [DB-IP Country Lite](https://db-ip.com/db/lite.php)
database and public gossip addresses. The NOTICE records the database version.
Estimates may be inaccurate or unavailable.
GeoIP timestamps are RPC response times; the RPC does not provide contact age.
Raw addresses are not written to the cache or logs. Pages displaying these
estimates must link to [DB-IP](https://db-ip.com/) under its CC BY 4.0 license.
For releases, refresh `pkg/dashboard/cluster/dbip-country.mmdb.gz` from DB-IP,
update its NOTICE, and run the collector tests below.

Bounds: one capture at most 55 seconds; HTTP bodies at most 2 MiB; at most
2,048 rows per source; persisted UTF-8 JSON at most 4 MiB. Redirects are denied.
Counters, slots, epochs,
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
