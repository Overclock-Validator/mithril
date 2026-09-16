# Fixed-shape FEC recovery

Production uses direct recovery only for exactly one missing data shard in a
32-data/32-coding FEC set with an available coding shard. All other availability
patterns keep the general decoder. Recovery still passes ordinary packet/root
validation before assembler admission.

The deterministic production-path harness is described in [repair_sim.md](repair_sim.md).
Reduced-subset and all-coding plans remain reference benchmarks, not runtime
policies. Historical investigation notes are in the [evidence archive](fec-producer-evidence.md).

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

Production uses a process-wide table containing every missing-data and
coding-row combination. This removes per-call plan construction while keeping
the same equation. The table is exhaustively differential-tested against the
general decoder across all 32 x 32 combinations.

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

## Zen 5 production gate

The direct one-data path was measured on a Ryzen 7 9700X with Go 1.26.4,
`GOMAXPROCS=1`, and one pinned physical core. Medians below are from seven
sequential one-second samples unless otherwise noted.

| Benchmark | General path | Direct one-data path | Change |
| --- | ---: | ---: | ---: |
| one-missing `SlotAssembler` boundary | 10.73 us/FEC | 2.82 us/FEC | -73.7% (3.8x) |
| near-tip repair simulation | 3.0295 ms/op | 2.8659 ms/op | -5.40% |
| deep-mixed repair simulation | 4.0343 ms/op | 4.0240 ms/op | -0.26% |
| deep-sparse repair simulation | 10.8489 ms/op | 10.8327 ms/op | -0.15% |

The production dispatch is intentionally narrow. The deep scenarios do not
enter it and remain effectively neutral, while the near-tip workload benefits
from repeated exactly-one-missing recoveries. The one-missing boundary also
dropped from 144 to 5 allocations per operation.

