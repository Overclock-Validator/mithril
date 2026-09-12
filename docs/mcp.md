# Mithril MCP

Mithril MCP works with Codex, Claude Code, Cursor, VS Code, and other clients
that support stdio MCP. It lets them inspect a Mithril node through its RPC,
metrics, logs, state, and replay data. The default `monitor` profile is
read-only.

The client launches Mithril as a local stdio process and stops it when the
session ends. A remote connection uses the same stdio transport over SSH.

## Quick start

Build Mithril:

```bash
make build
```

### Codex

```bash
./mithril mcp setup codex
codex mcp list
```

The setup command records the binary's absolute path, so Codex can launch it
from any directory.

### Claude Code

```bash
./mithril mcp setup claude
claude mcp get mithril
```

The Codex and Claude Code setup commands register Mithril for the current user
and record the binary's absolute path.

### Cursor

Run `./mithril mcp config`, then put its output under `mcpServers.mithril` in
`~/.cursor/mcp.json` or a project `.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "mithril": {
      "type": "stdio",
      "command": "/absolute/path/to/mithril",
      "args": ["mcp"]
    }
  }
}
```

### VS Code

```bash
./mithril mcp setup vscode
```

For a workspace-specific setup, run `./mithril mcp config`, then put its output
under `servers.mithril` in `.vscode/mcp.json`:

```json
{
  "servers": {
    "mithril": {
      "type": "stdio",
      "command": "/absolute/path/to/mithril",
      "args": ["mcp"]
    }
  }
}
```

### Other MCP clients

`mithril mcp config` prints a standard stdio server entry:

```json
{
  "type": "stdio",
  "command": "/absolute/path/to/mithril",
  "args": ["mcp"]
}
```

Add the entry as a server named `mithril` using your client's MCP settings.
MCP clients use different names for the outer configuration object, but the
server entry is the same.

## Remote node

First confirm that SSH works without a password or host-key prompt:

```bash
ssh NODE true
```

Codex can add the remote server directly:

```bash
./mithril mcp setup codex \
  --ssh NODE \
  --remote-binary /absolute/path/to/mithril
```

For another client, generate its stdio entry:

```bash
./mithril mcp config \
  --ssh NODE \
  --remote-binary /absolute/path/to/mithril
```

SSH uses your existing host alias, known-hosts entry, and authentication
configuration.

## Check the connection

Ask the client to call `mithril_mcp_info`, then `mithril_diagnose`. Both calls
should return structured node information. The remaining monitor tools cover
slots, metrics, logs, replay state, rewards, bank hashes, and shutdown state.

## Agent instructions

Mithril sends usage guidance during MCP initialization, so a project rule is
not required. To pin the same workflow in a repository, add this to
`AGENTS.md`, `CLAUDE.md`, or your client's project instructions:

> For Mithril node questions, use the Mithril MCP tools. Call
> `mithril_mcp_info` first, then `mithril_diagnose`; treat `unknown` or
> `evidence_complete=false` as incomplete evidence.

## Profiles

- `monitor` is the default read-only profile.
- `diagnostic` adds account reads, transaction simulation, and profiling. Use
  `./mithril mcp config --profile diagnostic` only when those tools are needed.

Access is controlled by the local user account or the SSH connection.
