# Set up an Alpenglow validator

Validator mode is experimental and currently supports the **Alpenglow community
cluster only**. Standard Solana testnet, devnet and mainnet-beta remain non-voting
on this branch; follow the [verifying node guide](setup-verifying.md) for those.

## Prepare and configure

Follow the [server preparation and build steps](setup-verifying.md#prepare-the-server)
for Ubuntu 26.04 LTS and the Go version in `go.mod`. Then generate the validator
profile:

```bash
./mithril config init --validator
```

Set these values in the generated `config.toml`:

| Section | Required settings |
| --- | --- |
| `[network]` | `cluster = "alpenglow"`; the RPC endpoint for your Alpenglow cluster |
| `[storage]` | Dedicated writable accounts, shredstore, snapshot and log paths |
| `[block]` | `source = "turbine"`; your turbine UDP bind address |
| `[turbine]` | Cluster `gossip_entrypoint`, gossip UDP bind address and public advertised IP |
| `[consensus]` | `mode = "validator"`; `alpenglow_observer_bind_addr` for Votor QUIC |
| `[validator]` | Identity keypair, vote-account public key or keypair path, public `advertised_ip`, TPU QUIC bind address |

`vote_account_keypair` accepts the **public address** of the on-chain vote account;
the vote-account private key is not required for runtime voting. The authorized
voter defaults to the identity. Set `authorized_voter_keypair` if the vote account
uses a separate signer, and ensure its Alpenglow BLS registration matches the cluster.
Coordinate vote-account creation, stake and BLS registration with the cluster
operators before enabling voting. Keep the authorized withdrawer key offline.

Protect identity and voter key files with mode `0600`, owned by the runtime user.
Allow your configured turbine, gossip, Votor QUIC and TPU QUIC **UDP** ports through
both host and provider firewalls. Do not copy example advertised IPs or assume
the community cluster shares standard testnet entrypoints or genesis.

## Start and validate

```bash
./mithril doctor --config config.toml
./mithril run --config config.toml
```

Use the [systemd instructions](setup-verifying.md#run-under-systemd) after reviewing
the service user, storage and private-key access. Stop the node cleanly before
switching modes. Keep its identity-bound vote history across restarts; startup
fails closed on corrupt history.

Inspect the journal for advancing rooted slots and `alpenglow voting stats`.
`vote landed source=votor-quic proof=verified-aggregate` means a network-received,
BLS-verified aggregate certificate includes this validator's rank for a vote in
its durable history. A successful broadcast alone is not a vote landing proof.

Validator mode verifies local execution against the certified footer bank hash;
it does not use the verifying node's RPC trailing execution oracle. Current leader
limitations include skipping epoch-transition slots and active partitioned epoch
rewards, and accepting only legacy TPU transactions. Repair serving and Rotor relay
duty remain incomplete. Exercise this mode on the community cluster before relying
on it for production.
