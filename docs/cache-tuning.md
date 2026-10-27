# Account and program caches

These settings use the normal CLI-over-TOML precedence and apply when
AccountsDB opens; a running cache is not resized.

| CLI | TOML (`[tuning]`) | Default |
|---|---|---|
| `--index-cache-max-mb` | `index_cache_max_mb` | 256 MiB |
| `--common-account-cache-max-mb` | `common_account_cache_max_mb` | 768 MiB |
| `--common-account-cache-layout` | `common_account_cache_layout` | `fixed` |
| `--program-cache-max-mb` | `program_cache_max_mb` | 1024 MiB |

- `common_account_cache_layout`: `fixed` (default) gives empty accounts up to
  16 MiB and accounts of 1-200 data bytes up to 64 MiB (at most an eighth and a
  quarter of the budget), and every larger account the rest.
  On mainnet catch-up empty-account reads fit in about 6 MiB and small-account
  reuse stops improving near 32-64 MiB, while larger accounts keep gaining up to
  about 1 GiB. `flat` uses one weighted cache for every account size. The small
  tiers store fixed records without per-entry pointers. Reservations divide the
  configured total; they do not add to it.
- A program's cost in the program cache is its parsed size plus its source
  bytes, rounded up to MiB.
- `index_cache_max_mb` is Pebble's block cache for the account index, not for
  account data. With account prefetch, 128, 256 and 512 MiB replayed the same;
  8 MiB (the earlier default) was markedly slower.

Budgets are approximate and are not a process memory cap.

## Prefetch

Account prefetch is off by default. `MITHRIL_ACCOUNT_PREFETCH=1` reads the
accounts of the next queued block and preloads its programs while the current block executes. On machines
with little memory for the page cache, `MITHRIL_TIP_WARM=1` additionally warms
each block's transaction batches as they arrive at the tip.

## Upgrading

The account index uses Pebble v2. An index written by an older Mithril version
is upgraded in place when the AccountsDB opens (Pebble may compact it for a
while after the first start); no snapshot rebuild is needed.
