# Dashboard API

For installation, see [Private setup](#private-setup). For an externally
accessible deployment, also read [Public profile](#public-profile) and its
reverse-proxy and isolation requirements.

`mithril-dashboard` reads a private status feed from one Mithril process and
serves a read-only API. It does not start, restart or configure the node. The
feed is disabled by default. Enable it with `dashboard.enabled` or
`--dashboard-enabled`, or configure a socket explicitly with
`dashboard.socket` or `--dashboard-socket`.
Even with the feed disabled, this checkout records bank-level transaction
classification and compute-unit counters during replay and block production.
Their production overhead has not been established; disabling the feed is not
a guarantee of zero instrumentation cost. When account observations are
enabled, the replay owner samples up to two accounts at completed-bank
boundaries, normally at most once per second; changed epoch-cache facts can
trigger an earlier sample. Those reads can reach AccountsDB. Measure their
latency and replay impact on the target workload before production use.

It covers node identity and lifecycle, replay, Go runtime and process resources,
Alpenglow consensus and voting observations, RPC method metrics and the selected
startup recovery state, local replay throughput, gossip and TPU pipeline
observations. The reader does not scan accounts or query external dashboards.
Non-voting nodes use the same API; their voting group stays unavailable. Bounded
selected-and-folded certificate and leader outcome counts are available in
bounded windows. A percentage over known outcomes in those windows is not a
full-epoch rate or a FAST score, and it may differ from external dashboards.

## Connecting a dashboard

After the [public profile](#public-profile) and its HTTPS proxy are configured,
a dashboard can read one node from that origin:

- `GET /api/v1/status` returns the current public status object.
- `GET /api/v1/features` lists the optional route families enabled on the reader.
- `/websocket` sends status updates with topic `mithril` and key `public`;
  the `value` has the same shape as the status response for the same profile.

For example, replace the hostname below with the configured HTTPS origin:

```sh
curl --silent --show-error --max-time 5 -i https://dashboard.example.com/api/v1/status
```

The response can be unavailable or stale even when the reader is reachable.
Check `source_state` and each child observation's state. For HTTP, respect
the remaining status lease from before the request. WebSocket frames have no
lease and can be buffered; age their source timestamps and treat a disconnect as
loss of current updates. Treat missing values as unknown, not zero. Slots and
64-bit counters are decimal strings when present; use an exact integer type
when comparing or subtracting them.
See [Public fields and freshness](dashboard-api-reference.md#public-fields-and-freshness) for the contract.
The public profile is read-only, not authenticated; its disclosure and proxy
boundary require an operator decision before exposure.

The private `/snapshot` response includes `Mithril-Status-Remaining-Milliseconds`,
the cached sample's remaining lifetime, capped at five seconds. The reader
counts that lifetime from before its request and does not renew an unchanged
sample. A zero, expired or invalid lifetime cannot make the sample fresh. A
reader still accepts a legacy feed that has never sent this header; once a run
has sent it, removing it cannot silently restore legacy freshness for that run.
Within one run, a repeated sample timestamp must have the same decoded fields;
changed data under that timestamp is rejected, while unknown private fields
remain ignored. This check survives a projection failure. An identical retry
can recover if projection succeeds, without extending its original lease.
The reader also provides a bounded freshness header on public HTTP status, as
described below. Neither header establishes a trustworthy whole-host clock;
status remains stale when its timestamp or clock checks fail.

For field definitions, optional routes, and the WebSocket contract, see the
[API reference](dashboard-api-reference.md).

## Private setup

### Optional configuration-driven Linux service

The example Linux service can start the separate reader automatically when the
node creates its feed socket. This requires one-time operator provisioning:

1. Create an unprivileged reader account and dedicated
   `mithril-dashboard-feed` group. Give the reader only the feed group, and
   verify it cannot access the node's keys, databases, processes or other IPC.
2. Provision `/run/mithril-dashboard-feed` owned by the node's effective
   user and the feed group, with exact mode `2710`. Arrange for the directory
   to be recreated on reboot. Review trusted ancestors, ACLs and labels.
3. Install the example reader service and
   [path unit](../examples/dashboard/mithril-dashboard.path), adapting the
   reader's account, executable and inaccessible paths. Enable the path unit
   with `systemctl enable --now mithril-dashboard.path`. Verify the service
   isolation as described below before using it on a validator.

After provisioning, enable the feed in the node configuration:

```toml
[dashboard]
enabled = true
```

The equivalent flag is `--dashboard-enabled`. This Linux-only profile uses
`/run/mithril-dashboard-feed/dashboard.sock` and group
`mithril-dashboard-feed`; it cannot be combined with a nonempty socket or
socket-group setting. The node needs a planned restart to apply the change.
This example uses one fixed socket and reader port per host. For multiple
nodes, configure separate sockets, reader units and ports manually.
The path unit starts the reader when the socket appears. The reader defaults to
loopback and the private profile; other local processes may still reach it.
Stopping the node leaves the reader running with unavailable or stale data.
An unclean node exit can leave a socket that must be inspected before removal.
If repeated starts hit systemd's start limit, the path unit can fail and must be
inspected and restarted by the operator. This does not install a service or
publish a URL.

The following manual setup remains available on supported platforms.

Build both executables from the same checkout:

```sh
go build -o mithril ./cmd/mithril
go build -o mithril-dashboard ./cmd/mithril-dashboard
```

Without release build flags, the dashboard feed reports `0.0.0-dev`; the CLI
may show `dev`. The displayed commit may identify only the base revision when
the checkout has uncommitted changes; build metadata may also be absent. Record
the binary hash and, when available, `go version -m ./mithril` for additional
build information.

Create a private directory owned by the account that runs both processes:

```sh
install -d -m 700 "$HOME/.local/run/mithril-dashboard"
```

Add this flag to the node's existing run command, without changing its other
arguments. A running node needs an operator-planned restart to load the new
binary or enable the feed; starting the reader alone does not change the node:

```sh
--dashboard-socket "$HOME/.local/run/mithril-dashboard/dashboard.sock"
```

The equivalent configuration uses an absolute path (replace the placeholder):

```toml
[dashboard]
socket = "/absolute/private/directory/dashboard.sock"
```

Start the reader on the node host, using the same absolute socket path:

```sh
./mithril-dashboard \
  --node-socket "$HOME/.local/run/mithril-dashboard/dashboard.sock" \
  --listen 127.0.0.1:8080
```

To check the private feed from an account with socket access:

```sh
curl --fail --silent --show-error --max-time 5 -i \
  --unix-socket "$HOME/.local/run/mithril-dashboard/dashboard.sock" \
  http://node/snapshot
```

Inspect `Mithril-Status-Remaining-Milliseconds`: a zero lease means the
sample is not fresh, even if the response is HTTP 200. For a positive lease,
consumers must subtract elapsed monotonic time from the start of the request;
the response must not renew the sample's lifetime. This checks the private
feed, not the reader's WebSocket; do not publish the private response.

For remote viewing, forward the reader's loopback endpoint over SSH. Keep the
reader beside the node's Unix feed; do not tunnel the raw feed over a WAN.

The node also has a separate Prometheus listener that defaults to `:9090` on
all interfaces. If it is not needed, add `--no-metrics-server` to the node
command or set `no_metrics_server = true` in `[reporting]`; otherwise restrict
it with host or network controls. The dashboard reader's loopback binding and
reverse proxy do not secure that listener.

The socket is mode `0600`; its immediate parent must be a private directory
owned by the node's user. Use a trusted parent path, not a directory writable by
other accounts. Graceful shutdown removes its socket. After an unclean node exit
(for example SIGKILL or an out-of-memory kill), the next start replaces the old
socket only if it is owned by the node's user and nothing accepts connections on
it; any other existing path is left in place and the feed stays off. A feed setup
failure is logged and does not stop the node.

The feed serves one request per connection and accepts at most four connections
per second, so a feed client cannot make the node spend more than that serving
it. The reader polls once per second.

Unix socket paths have platform-specific length limits. If a long path causes a
bind error, choose a shorter trusted directory and use the same socket path for
the node and reader.

On Linux, a systemd-managed node can use a dedicated runtime directory for the
socket, so systemd removes the old path when the node's service stops or
restarts. For a system service running the node directly as the same user as
the reader, the relevant service settings are (choose a name unique to this
node service):

```ini
RuntimeDirectory=mithril-dashboard-feed
RuntimeDirectoryMode=0700
RuntimeDirectoryPreserve=no
```

Use `/run/mithril-dashboard-feed/dashboard.sock` as the socket path. This
directory must contain only disposable runtime files, never ledger, account or
signing data: systemd removes it when the service stops. This does not itself
configure automatic restart, and does not clean the path when a wrapper keeps
the service running while restarting only its child. Check ownership, permissions
and reader reconnection for the actual service configuration. See the
[systemd runtime-directory documentation](https://www.freedesktop.org/software/systemd/man/latest/systemd.exec.html#RuntimeDirectory=).

Keep the executable name `mithril-dashboard`: the node's existing startup process
cleanup matches executables named `mithril`.

The node and reader in this setup run as the same user. A separate process
isolates browser parsing and slow writes from replay, but it is not a key-isolation
sandbox: code running as that user may have the same file access as the node.

### Optional Linux feed group

To let a separately configured reader user connect on Linux, provision a
dedicated group and a trusted feed-only directory owned by the node's effective
UID and that group, with exact mode `2710` (including setgid). Then supply both
node flags:

```text
--dashboard-socket /run/mithril-dashboard-feed/dashboard.sock
--dashboard-socket-group mithril-dashboard-feed
```

The equivalent configuration is:

```toml
[dashboard]
socket = "/run/mithril-dashboard-feed/dashboard.sock"
socket_group = "mithril-dashboard-feed"
```

The node resolves the named group, checks the immediate directory's ownership,
group and mode, and creates a socket owned by the node and that group with mode
`0620`. The reader needs directory search and socket write permission through
the dedicated group; it does not need directory write access. The operator must
verify trusted ancestor directories, ACLs, group membership and security labels:
the feed does not audit those. A bad group, directory or socket setup, or use of
this mode on a non-Linux system, leaves the feed unavailable and is logged; it
does not fall back to a less restrictive mode or stop the node. Without the
group setting, the private `0600` default above is unchanged.

The reader receives the whole private feed before publishing its public
projection. Restricting its access to node keys, storage, processes and other
IPC requires a separately reviewed and tested service policy.

The viewer listener accepts only a literal loopback IP. Local access is not
authentication: other local processes may read it. Remote private access can use
an SSH tunnel, with the same local and remote port. If a request supplies an
Origin, it must use HTTP or HTTPS and match the request Host and port. Requests
without an Origin are also accepted; these checks do not authenticate clients.
The reader does not serve HTML or accept a separate UI origin. Forwarded headers
do not bypass Host or Origin checks. This private profile refuses any request
carrying a proxy header (`Forwarded`, `Via`, `X-Forwarded-*` or `X-Real-IP`):
a proxy's default upstream Host is the loopback listener itself, so the Host
check alone would admit remote clients that send no Origin. Put a reverse proxy
only in front of the public profile below.

## Public profile

The reader has an explicit public-only profile. It still listens on loopback and
requires an HTTPS reverse proxy. Set the exact external origin:

```sh
./mithril-dashboard \
  --node-socket "$HOME/.local/run/mithril-dashboard/dashboard.sock" \
  --listen 127.0.0.1:8080 \
  --public-origin https://dashboard.example.com
```

For a public deployment, use the Linux feed-group setup above and run this
command as a distinct reader user whose only node access is that feed group:

```sh
./mithril-dashboard \
  --node-socket /run/mithril-dashboard-feed/dashboard.sock \
  --listen 127.0.0.1:8080 \
  --public-origin https://dashboard.example.com
```

The reader must not inherit access to node keys, databases, process control or
other private sockets. A separate UID and feed group are only part of that
boundary; review filesystem permissions, supplementary groups and service
confinement on the target host. The reader receives the whole private feed, not
just its public fields.

### Linux reader service example

For a new fixed-user deployment, copy the complete
[reader unit](../examples/dashboard/mithril-dashboard.service) only after
adapting its paths and account names. It defaults to the private profile; add
`--public-origin` only after configuring the HTTPS proxy and reviewing the
public disclosure below. It does not create accounts, enable
the node feed, install a service or restart the validator.
It assumes an existing unprivileged `mithril-dashboard` account whose only
additional group is `mithril-dashboard-feed`, an executable at the shown path,
and the separately provisioned feed directory above. The illustrative node
private paths must be replaced with the actual key, database, state and admin
socket paths; they must exist and must not contain the feed directory.

The unit runs as a distinct fixed reader account with only the feed group,
allows at most three starts within 60 seconds, and never restarts the node.
The process can start before the feed; use the dated API state, not systemd's
`active` state, to verify that the intended node is being observed.

The resource values are examples to measure and adjust, not production limits.
`ProtectSystem=strict` makes most filesystem paths read-only; it does not hide
readable files or stop connections to Unix sockets. Path masks alone do not
cover other mounts or IPC.
The IP filter requires supported cgroup filtering and still permits connections
to other loopback services; restrict and test those services separately.
Verify the reader's full group list, DAC/ACLs, labels, inherited descriptors and
all actual node paths. Confirm that the reader cannot read or write dummy key
and database canaries, inspect or signal the node process, or connect to its
admin sockets. Also test the feed, reader restart, proxy, and resource limits in
the actual service context. Do not use `PrivateNetwork=yes` with this same-host
proxy arrangement: it separates the reader's loopback from the proxy.

Check the target systemd and kernel support for each directive, including the
syscall filter, before use. This fragment does not establish the complete
unit's runtime policy.
See the [execution policy](https://www.freedesktop.org/software/systemd/man/latest/systemd.exec.html)
and [resource controls](https://www.freedesktop.org/software/systemd/man/latest/systemd.resource-control.html)
references. An unsupported IP filter can be ineffective; verify its enforcement,
not just whether the service starts.
Validate the policy against the actual validator paths and the complete proxy
configuration. Existing dynamic-user node units require their own directory
ownership and lifecycle design; do not change an installed node identity to fit
this example. Shared-kernel and host resource risks remain even after a policy
passes its tests.

The proxy must preserve the configured Host and browser Origin. Use a lowercase
HTTPS origin without credentials, a path, trailing slash, query or fragment.
Host and Origin are compared exactly, including an explicitly configured port;
omit the default `:443` so browser normalization does not cause a mismatch. The public
profile always serves `/api/v1/features`, `/api/v1/status` and `/websocket`.
The status endpoint returns the cached public JSON object and allows a request without an Origin;
when an Origin is present it must match exactly. The WebSocket requires exactly
one matching Origin. Features rejects query strings; status and WebSocket accept
the exact optional queries `include=snapshot_manifests`,
`include=snapshot_manifests_v2` and `include=snapshot_manifests_v3`. All three
routes reject request bodies. The default and earlier opt-in profile schemas
remain compatible with existing clients. Opt-in profiles may include historical
`startup.snapshot_manifests`; v2 and v3 also include its optional ready marker.
V3 can additionally expose `consensus.safety_fault` independently of lifecycle
details. Optional routes enforce their documented query limits. Forwarded
headers are not trusted. Public responses use `Cache-Control: no-store` and
`X-Content-Type-Options: nosniff`. The proxy must provide TLS and its own
appropriate traffic limits. These checks are not authentication: anyone who can
reach the public route may read the approved fields. Review the field disclosure,
proxy and host security before exposure. This example does not install a service
or publish a URL.

If the v3 projection cannot fit the public response limit, that profile reports
the source as unavailable rather than silently returning an older profile that
could omit the safety flag. This does not change the default or v1/v2 profiles.

### Reverse proxy boundary

Configure the HTTPS proxy for the exact hostname and origin configured in the
reader, with an approved certificate. Allow only the exact API routes intended
for exposure, preserve Host and Origin, and explicitly forward the WebSocket
upgrade. Disabled optional routes return `404`. Never route an unmatched path
to the node, its RPC/admin interface or the private feed.

The public profile uses `/api/v1/features`, `/api/v1/status` and `/websocket`.
Do not forward an unrestricted prefix. Allow only
documented query shapes and reject unsupported queries instead of silently
stripping or rewriting them.

Disable upstream caching, apply host-appropriate connection and request limits,
and verify the certificate and WebSocket upgrade. The reader's own limits still
apply. nginx's [WebSocket proxying guide](https://nginx.org/en/docs/http/websocket.html)
explains Upgrade forwarding. Validate the deployed host, certificate and routing.

Before public exposure, test both a valid client connection and rejection of
wrong Host/Origin, extra paths, unsupported queries and application messages. Stop the
reader and confirm node replay continues; stop the source in a disposable test
and confirm the viewer marks its last data stale. Set and test reader CPU/memory
limits on the intended host. Do not infer a key-isolation sandbox or acceptable
validator overhead from these examples alone.

The public WebSocket sends a single `mithril.public` envelope with the same
object as the status endpoint, then sends changed objects. It accepts no
application data messages, including the private application ping or queries;
the connection closes if one is sent. WebSocket control ping/pong remains in
use. The private profile does not serve `/api/v1/status` and retains the private
WebSocket contract below. Neither profile serves UI files.

The public model is a typed allowlist. It excludes private runtime diagnostics,
recovery hashes, raw logs and errors, paths, private keys, request contents and peer
endpoints. It exposes node identity and the fields described below; public access
therefore needs an explicit operator disclosure decision. Process and fixed-role
filesystem aggregates are excluded unless `--public-resources` is enabled.

`--public-peers` separately enables a `peers` group containing at most eight
recent signed contact adverts: identity, advertised base version and client ID,
shred version, local receipt time, and repair/TVU/Alpenglow service flags. The
version contains `major.minor.patch`; prerelease labels are omitted. No
endpoints, geographic guesses, stake, rank or health claims are included. The
group is omitted by default, unavailable before an observation, and an empty
array only after an observed empty sample. Both disclosure flags require
`--public-origin`; neither changes the node's routing or network configuration.

The gossip client keeps only the eight most recently updated matching-shred
contacts after its existing signature checks. Repeated or older `(outset,
wallclock)` adverts do not refresh the retained receipt time. Contacts expire
15 minutes after receipt; a relayed signed advert is not proof that its author
is currently reachable. Versions and services are self-reported. The cached
ten-second sample is independent of viewers and becomes stale if publication
stops. An old sample may still display historical contacts with their dated age.
This is a bounded sample, not a full gossip directory or a cluster health map.

Public status JSON has a 32 KiB cap; the private node-to-reader status snapshot
has a separate 33 KiB cap. Oversized input still fails closed, and status collection remains once per second.

### Releases and rollback

Record the reader binary's SHA-256 and `go version -m` output, proxy
configuration hash, enabled reader options and exact public origin. Do not
include certificate private keys or other credentials in that receipt. Retain
the previous compatible reader and proxy configuration for rollback.

Check features/status responses and WebSocket binding through the intended
HTTPS proxy, and verify any separately deployed client accepts the response
shapes. If validation fails, restore the previous compatible reader and proxy
configuration, then repeat the checks. Keep transport, client and browser test
results separate; an offline parsing test does not prove browser connectivity.

## Checks

```sh
go test -race -p 2 ./pkg/dashboard ./cmd/mithril/node ./pkg/rpcserver ./pkg/consensus ./pkg/blockstream ./pkg/gossip ./pkg/block ./pkg/blockprod ./pkg/global ./pkg/rewardcerts ./pkg/replay ./pkg/sealevel ./pkg/metrics
go vet ./...
```

The transport tests use real local Unix sockets, HTTP and WebSocket connections.
They cover source loss and restart, stale samples, identity/run replacement,
large integer precision, private paths, unsupported requests, client limits,
slow writes and shutdown. They do not prove live-cluster metric accuracy or
performance. Live accuracy needs same-run comparisons with each source. Cost
checks need bounded capture-off/on runs with their workload and instrumentation
recorded; whole-process timings do not isolate each measurement's cost.
