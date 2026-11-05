# Mithril Codebase Context (Quick Orientation)

This file is a lightweight entry point to the repo and how to approach changes.

---

## ⚠️ CRITICAL INVARIANT - NEVER VIOLATE ⚠️

**Mithril MUST produce EXACTLY the same state changes as mainnet for every single block.**

This is non-negotiable. Mithril is a verifying full node - its entire purpose is to independently verify that blocks are correct by reproducing the exact same state transitions.

**Divergence = Broken.** There is no such thing as "acceptable" divergence or "close enough":
- Each block must produce byte-for-byte identical account state changes
- Each block's bank hash must match mainnet exactly
- If Block N diverges, every subsequent block will also fail

**Before ANY change, verify:** Does this produce identical state to Agave/Firedancer at the same slot?

**High-risk areas:** epoch boundaries, feature gates, fees and rent, account storage and hashing, the VM and
loaders, sysvars, nonces and transaction age, Alpenglow consensus, parallel and speculative replay, snapshots and
restarts. Changes there need the extra checks in [docs/HIGH-RISK-CHANGES.md](docs/HIGH-RISK-CHANGES.md); name the
applicable entry in the PR.

---

## AI Guidelines

1. **Validate behavior against Agave and Firedancer** - cite `file:line` references
2. **If unsure, say so explicitly** and propose a concrete verification step
3. **Always include slot/epoch context** for boundary-sensitive values
4. **Proactively note** (when confidence is medium-high or higher):
   - Areas where performance could be improved
   - Potential bugs or edge cases that could be avoided
   - Comments or code that appears outdated and could be removed
5. **Help maintain readable code**:
   - Add clear commentary (Firedancer-style) to well-validated code that gets tested and merged
   - Ask if more context/comments would help when exploring areas that lack documentation
   - Goal: make the codebase self-documenting and easy to understand
6. **Flag dependency changes clearly**:
   - If a change adds a new library that overlaps with an existing dependency, call it out explicitly
   - State whether the intent is to replace the existing library or if this creates redundancy
   - Example: "This adds a new chacha library - are we replacing the existing one or keeping both?"

### Review Request Template

```
You are reviewing Mithril for Agave/Firedancer parity.
Goal: <exact target behavior>.
Context: snapshot slot S, boundary slot B, first reward slot F, epoch E.
Branch/commit: <hash>. Logs: <paste 5–15 lines>.
Code pointers: <file:line>, <file:line>.

Please:
1) List likely causes in priority order (with confidence).
2) For each cause, point to FD/Agave code paths and our code paths.
3) Propose the minimal fix + how to verify it.
4) If unsure, say so and give a concrete test/diagnostic.
```

---

## Change Approach (Alignment First)

- When in doubt, validate behavior against Agave and Firedancer code.
- If unsure, say so explicitly and propose a concrete verification step.
- Always note which slot/epoch a value comes from. Most mismatches trace back to slot confusion.
- Note if a change may cause performance regression (slower block replay) or increased resource usage (RAM, disk I/O, etc.).
- Keep logs + RPC comparisons as sanity checks.
- Add new design notes to `architecture/` as features stabilize.

---

## Repo Map (High Level)

| Directory | Purpose |
|-----------|---------|
| `cmd/mithril/` | CLI entrypoints (`mithril run`, `mithril state`, etc.) |
| `pkg/replay/` | Replay loop, epoch boundary logic, schedule handling |
| `pkg/rewards/` | Staking rewards, partitions, points computation |
| `pkg/leaderschedule/` | Leader schedule generation + validation |
| `pkg/accountsdb/` | AccountsDB, appendvec parsing, index lookups |
| `pkg/snapshot/` | Snapshot manifest decode, AccountsDB build |
| `pkg/rpcclient/` | RPC fetchers (blocks, schedules, sysvars) |
| `pkg/sealevel/` | Solana VM implementation, native programs |
| `pkg/gossip/` | Gossip protocol implementation |
| `architecture/` | Evolving design docs |

---

## Reference Implementations

Mithril behavior should match these canonical Solana implementations:

- **Firedancer** (C): https://github.com/firedancer-io/firedancer
- **Agave** (Rust): https://github.com/anza-xyz/agave
