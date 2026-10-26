# Mithril Codebase Context (Quick Orientation)

This file is a lightweight entry point to the repo and how to approach changes.
It is the shared context for AI coding agents: tools that read `AGENTS.md` load
it directly, and `CLAUDE.md` imports it for Claude Code. Keep it free of private
infrastructure details.

---

## ⚠️ CRITICAL INVARIANT - NEVER VIOLATE ⚠️

**Mithril MUST produce EXACTLY the same state changes as mainnet for every single block.**

This is non-negotiable. Mithril replays every block itself and must reproduce the exact same state transitions. That holds in both node modes: `verifying` (non-voting, the default) and `validator` (Alpenglow voting and block production), where a divergent bank also means voting for or producing the wrong state.

**Divergence = Broken.** There is no such thing as "acceptable" divergence or "close enough":
- Each block must produce byte-for-byte identical account state changes
- Each block's bank hash must match mainnet exactly
- If Block N diverges, every subsequent block will also fail

**Before ANY change, verify:** Does this produce identical state to Agave/Firedancer at the same slot?

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

## When to Use a Long-Reasoning Model

Some questions are better sent to a long-reasoning model (for example ChatGPT
Pro) as one self-contained prompt than worked through interactively. When a
question fits, suggest writing such a prompt.

Good fits:
- Design research with many candidate answers (interpreter dispatch, index
  layout, signature or BLS batching).
- Whole-repo audits against one stated invariant (for example "durable
  AccountsDB state changes only through folds").
- Cross-checks against Agave, Firedancer and the SIMDs.
- Correctness arguments: crash safety of fold, recovery and rewind; concurrency
  between replay, folds and compaction; fork-choice safety.
- An independent second-opinion review of a PR or a tricky algorithm.

Poor fits: anything that must be measured (benchmarks, profiles, replay runs),
fast debugging loops, and anything that needs private infrastructure details.

Prompt shape: one focused question, the invariants stated up front, a pointer
to the public branch and files, and a requested output of ranked findings with
`file:line`, confidence, and how to confirm each one. Treat the answers as
leads and verify each against the code before acting.

---

## Change Approach (Alignment First)

- When in doubt, validate behavior against Agave and Firedancer code.
- If unsure, say so explicitly and propose a concrete verification step.
- Always note which slot/epoch a value comes from. Most mismatches trace back to slot confusion.
- Note if a change may cause performance regression (slower block replay) or increased resource usage (RAM, disk I/O, etc.).
- Keep logs and bank-hash comparisons as sanity checks. Take block and transaction data from local shreds (the shred spool), not from public RPC.
- Add new design notes to `docs/` as features stabilize.

---

## Repo Map (High Level)

| Directory | Purpose |
|-----------|---------|
| `cmd/mithril/` | CLI entrypoints (`mithril run`, `setup`, `doctor`, `config`, `state`, `status`, `dashboard`, `set-identity`) |
| `cmd/mithril/node/` | `mithril run`: config parsing, bootstrap, startup checks, replay wiring |
| `pkg/replay/` | Replay loop, unrooted tail and fold promotion, streaming execution, epoch boundary logic |
| `pkg/accounts/` | In-RAM account working set for unrooted slots |
| `pkg/accountsdb/` | Durable AccountsDB: appendvecs, Pebble index, batch folds, compaction, rewind |
| `pkg/state/` | `mithril_state.json` resume state and ledger/genesis binding |
| `pkg/blockstream/` | Block source (turbine, RPC catch-up), ordering, fork parent switches |
| `pkg/turbine/`, `pkg/repair/` | Shred receive, block assembly and retransmit; shred repair |
| `pkg/consensus/`, `pkg/alpenglow/` | Alpenglow certificate engine and Votor message model |
| `pkg/blockprod/`, `pkg/tpu/` | Leader block production; transaction ingress |
| `pkg/sealevel/`, `pkg/sbpf/` | Solana runtime and native programs; SBF interpreter |
| `pkg/rewards/` | Staking rewards, partitions, points computation |
| `pkg/leaderschedule/` | Leader schedule generation + validation |
| `pkg/snapshot/`, `pkg/snapshotdl/` | Snapshot manifest decode and AccountsDB build; snapshot discovery and download |
| `pkg/gossip/` | Gossip protocol implementation |
| `pkg/rpcclient/`, `pkg/rpcserver/` | RPC client for control-plane data; Mithril's own RPC server |
| `conformance/` | Conformance test harness |
| `docs/` | Design docs and troubleshooting |

---

## Reference Implementations

Mithril behavior should match these canonical Solana implementations:

- **Firedancer** (C): https://github.com/firedancer-io/firedancer
- **Agave** (Rust): https://github.com/anza-xyz/agave
