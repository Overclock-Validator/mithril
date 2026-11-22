# Optional external-cluster collector

This one-shot command reads public cluster data and atomically writes a typed
cache. It does not start a server, install a service, or read node keys or
configuration.

```sh
go run ./cmd/mithril-dashboard-collect --profile alpenglow --output /existing/private-directory/cluster.json
```

For Solana mainnet-beta, use `--profile mainnet` for the Solana public RPC or
`--profile mainnet-publicnode` for the PublicNode RPC. Both profiles check the
mainnet genesis and epoch before and after capture. Mainnet directory rows come
from vote accounts; version and country information comes from matching gossip
contacts when available. This profile reports directory and RPC observations.

Provision the directory and reader group separately. The cache is written with
mode `0640`; a setgid directory can supply the intended reader group. A scheduler
may invoke the command periodically (for example once per minute). Do not run it
per viewer request or give the network-denied reader outbound access. There is
no arbitrary endpoint flag. An existing destination symlink is refused.

Updated captures may include the optional `rpc_nodes` aggregate. Upgrade every
strict reader sharing this cache before running the updated collector; older
readers reject the added field. For rollback, stop the new writer and obtain a
fresh legacy-shaped snapshot before reverting readers. An expired older cache
must not be relabelled fresh.

For the community profile, exit 0 means every supported provider was fresh. For
mainnet, it means the RPC capture was fresh; inspect the GeoIP state separately.
Exit 1 may mean a valid degraded snapshot was written; inspect provider states,
reasons and timestamps. An invalid profile, output failure, or internal validation
failure does not replace the old file. Readers must mark that old file stale
rather than treating its presence as freshness.

The community profile uses a fixed cluster genesis. RPC genesis and epoch are
checked before and after capture. Provider names and versions are untrusted
public metadata, not operator identity verification.

For the community profile, SolStat supplies directory and credit-efficiency
observations. Titan supplies certificate inclusion for bounded observation
windows. The collector ranks rows by exact inclusion ratio, then by opportunity
count, both descending. Exact ties share a rank. Zero opportunities have no
score or rank. These observations cover the provider's recorded cohort.

Country estimates use the bundled [DB-IP Country Lite](https://db-ip.com/db/lite.php)
database and public gossip addresses. The NOTICE records the database version.
Estimates may be inaccurate or unavailable.
GeoIP timestamps are RPC response times; the RPC does not provide contact age.
Raw addresses are not written to the cache or logs. Pages displaying these
estimates must link to [DB-IP](https://db-ip.com/) under its CC BY 4.0 license.
For releases, refresh `pkg/dashboard/cluster/dbip-country.mmdb.gz` from DB-IP,
update its NOTICE, and run the collector tests below.

Bounds: one capture at most 55 seconds; HTTP bodies at most 2 MiB; persisted
UTF-8 JSON at most 4 MiB. Community sources and validator-directory rows are
limited to 2,048; mainnet `getClusterNodes` contacts are limited to 8,192.
Redirects are denied.
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
