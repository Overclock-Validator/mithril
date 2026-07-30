# Erasure recovery experiments

Status: experimental; no production `SlotAssembler` dispatch uses these paths.

This document separates two repair regimes that have different objectives. It
also records the fixed 32 data + 32 coding Reed-Solomon contract used by the
synthetic implementation in `pkg/turbine/internal/rsrecover`.

## Regimes

Near-tip repair minimizes the time until replay receives a particular blocking
data shred. Its primary candidate is direct recovery when exactly one data
shred is absent and at least one coding shred is present.

Catch-up repair minimizes useful recovered-data time across many incomplete FEC
sets. Its candidate constructs only the reduced system induced by the missing
data columns and produces only missing data outputs.

Slot age alone should not select the regime. A future policy experiment should
consume observed Turbine progress:

- number and fraction of incomplete FEC sets;
- missing data shreds per FEC set;
- whether new shreds are still arriving;
- time or scheduling intervals since the last useful arrival;
- number of FEC sets that have crossed the recovery threshold;
- data-heavy versus coding-heavy availability.

A progressing slot with one or two holes remains a near-tip workload even if it
is not the newest slot. A stalled slot with many incomplete FEC sets is a
catch-up workload even if wall-clock age is modest. Any eventual selector needs
hysteresis so bursty arrivals cannot oscillate the algorithm on every packet.

## Matrix contract

The experiment uses the systematic generator over `GF(256)/0x11d`:

```text
V[x,j] = x^j
A      = V[0:32,0:32]
G      = V * A^-1
G      = [I_32; C]
```

For the fixed 32+32 shape, exhaustive scalar tests confirm all 1,024 entries:

```text
C[r,c] = 0xa5 / (0x20 xor r xor c)
```

They also confirm `C*C=I`. Recovery tests remain differential against
`github.com/klauspost/reedsolomon`; the closed form is not the sole oracle.

## Candidates

### Near tip: direct one-data recovery

For missing data position `m` and available coding position `r`:

```text
D_m = C[r,m]^-1 * P_r
    + sum(i != m, C[r,m]^-1 * C[r,i] * D_i)
```

This prepares one 32-source coefficient row and writes one destination. It does
not construct or invert a general 32x32 matrix.

### Catch up: reduced missing-data system

For missing data columns `M` and selected coding rows `R`, substitute every
known data shard and solve:

```text
B[i,j] = C[R[i],M[j]]
B * D_M = adjusted_coding_rows
```

The experiment uses the Cauchy closed form to construct `B^-1` in `O(m^2)`,
expands direct rows over 32 selected sources, and proves each row satisfies the
requested systematic generator row before byte processing. An independently
implemented Gauss-Jordan inverse is the setup fallback.

The byte kernel is intentionally portable and uses `reedsolomon.LowLevel`.
This isolates algorithm and plan costs; it is not evidence that a portable
kernel will beat the dependency's generated AVX2/GFNI kernels on amd64.

### Catch up edge: all coding rows

When all data rows are missing and every coding row is present, `C*C=I` means
the existing optimized encoder can apply `C` to the coding rows and recover the
data directly. This is kept as a separate synthetic arm. It is simpler than a
general decoder, but a cached reduced-system plan may still have a faster byte
kernel; hardware decides between them.

## Synthetic coverage

The tests cover:

- every missing-data position with every coding-row choice for the direct path;
- every pair of missing data positions;
- deterministic mixed patterns at 2, 4, 8, 16, 24, and 32 missing data shreds;
- exactly-threshold and one-below-threshold availability;
- changed availability between setup and execution;
- destination failure atomicity;
- coefficient mutation detection;
- Cauchy inverses against independent Gauss-Jordan inversion;
- recovered bytes against the existing general decoder.

Run:

```bash
go test ./pkg/turbine/internal/rsrecover
go test -run '^$' \
  -bench '^(BenchmarkRecoverOneData|BenchmarkRecoverDataSubset)$' \
  -benchmem -benchtime=2s -count=6 \
  ./pkg/turbine/internal/rsrecover
```

Benchmark result interpretation must keep these cases separate:

- `prepare`: cold or changing erasure pattern;
- `execute`: prepared/repeated pattern;
- `prepare-and-execute`: first useful output for a new pattern;
- general cache off: existing decoder with changing pattern cost;
- general cache on: existing decoder after the inversion is cached.

No production dispatch threshold should be chosen from an Apple benchmark.
Final crossover decisions require the pinned amd64 target and synthetic arrival
traces for progressing, stalled, and bursty slots.

## Preliminary Apple M4 Pro diagnostic

These single-sample medians use 987-byte shards and exist only to reject or
retain candidates before the amd64 gate. Times are microseconds per FEC set.

| Missing data | Specialized first use | Specialized prepared | General uncached | General cached |
|---:|---:|---:|---:|---:|
| 1 | 1.97 | 1.07 | 8.36 | 2.31 |
| 2 | 4.19 | 2.10 | 10.78 | 3.39 |
| 4 | 8.84 | 4.19 | 17.00 | 5.91 |
| 8 | 18.94 | 8.24 | 25.54 | 10.06 |
| 16 | 39.90 | 16.44 | 44.93 | 19.78 |
| 24 | 62.14 | 24.71 | 63.43 | 28.55 |
| 32 | 79.50 | 32.76 | 89.53 | 37.53 |

The all-coding involution arm measured approximately 36.5 microseconds,
compared with 82.7 microseconds for an uncached general decode and 37.5
microseconds for its cached form. Its main possible value is avoiding plan
setup; the prepared reduced-system byte path was faster on this machine.

The current interpretation is deliberately conditional:

- direct one-data recovery is strong enough to require an amd64 prototype;
- reduced-system first use wins through most of the tested range, but the
  24-missing crossover is within noise on this machine;
- prepared reduced-system execution wins at every tested width;
- none of these figures establishes a production policy or Zen 5 result.
