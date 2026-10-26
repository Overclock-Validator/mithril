# Account and program caches

These settings use the normal CLI-over-TOML precedence and apply when
AccountsDB opens; a running cache is not resized.

| CLI | TOML (`[tuning]`) | Default |
|---|---|---|
| `--index-cache-max-mb` | `index_cache_max_mb` | 8 MiB |
| `--common-account-cache-max-mb` | `common_account_cache_max_mb` | 256 MiB |
| `--common-account-cache-layout` | `common_account_cache_layout` | `flat` |
| `--program-cache-max-mb` | `program_cache_max_mb` | 1024 MiB |
| `--program-cache-accounting` | `program_cache_accounting` | `mib` |

- `common_account_cache_layout`: `flat` uses one weighted cache for every
  account size. `tiered` reserves 1/16 of the budget for empty accounts and 1/8
  for accounts of 1-200 data bytes; `expanded` reserves 1/6 and 1/2. The small
  tiers store fixed records without per-entry pointers. Reservations divide the
  configured total; they do not add to it.
- `program_cache_accounting`: a program's cost is its parsed size plus its
  source bytes, rounded to MiB (`mib`) or KiB (`kib`) within the same budget.
- `index_cache_max_mb` is Pebble's block cache for the account index, not for
  account data. The 8 MiB default holds almost nothing once memtables are
  reserved.

Budgets are approximate and are not a process memory cap.

## A faster configuration

On a 64 GB machine this replayed noticeably faster than the defaults, at the
cost of a few GiB more resident memory:

```toml
[tuning]
common_account_cache_max_mb = 768
common_account_cache_layout = "expanded"
program_cache_max_mb = 1024
program_cache_accounting = "kib"
index_cache_max_mb = 512
```

with `MITHRIL_ACCOUNT_PREFETCH=1`, which reads the accounts of the next queued
block and preloads its programs while the current block executes. On machines
with little memory for the page cache, `MITHRIL_TIP_WARM=1` additionally warms
each block's transaction batches as they arrive at the tip.

## Upgrading

The account index uses Pebble v2, which cannot open indexes written by older
Mithril versions. Rebuild the AccountsDB from a snapshot when upgrading.
