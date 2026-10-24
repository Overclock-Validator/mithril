# Zen 5 interpreter placement

The optional Zen 5 build places `sbpf.(*Interpreter).Run` at an address that is
32 modulo 64 bytes. On the tested Ryzen 9700X, moving only this function reduced
arithmetic-program execution time from approximately 90–92 us to 79–81 us across
four binary layouts. Token and CPI effects were smaller. The ordinary build was
already near the fast end; these results show avoidance of a placement regression,
not an additional 12% gain over every ordinary build or improved voting latency.

Fixed entry placement does not freeze the instruction layout inside `Run`. With
both source versions at the preferred offset, the gap-translation change improved
memory loops by 12–14%, but pure ALU loops were 3.5–5.7% slower. Arithmetic program
fixtures improved approximately 1–2%; token and CPI differences were below 1%.
These were six alternating single-core runs with unchanged allocation counts.
Matched captured-block replay preserved bank hashes but showed no overall gain;
that corpus contained only 34 SBPF executions among 657,711 transactions.

## Build

Requires Go **1.27.2**, a Linux/amd64 host and target, Python 3, and `patch`.
Use the default Go experiments or `GOEXPERIMENT=arenas`, and start with `GOAMD64=v1`.
The mainnet comparison uses arenas consistently for every build.

```sh
make build-zen5 GO=/path/to/go1.27.2/bin/go
```

The build copies only `cmd/link` into a private source tree, applies the pinned
patch, and caches the resulting linker under `.build/zen5-linker`. The compiler,
runtime, and installed GOROOT remain unchanged. The patch is limited to placement
of `Run`. It pads the end consistently so the offset-0 comparison can keep
surrounding functions fixed. The ordinary `make build` remains available.

The build verifies the final ELF symbol address after any external C linking,
then publishes the executable and `mithril.layout.json`. The sidecar records the
Go version, patch and linker hashes, interpreter source hashes, final address,
binary hash, and Go build information. Ordinary builds remove a stale placement
sidecar. An ignored placement setting, missing/duplicate symbol,
unsupported artifact, changed Go linker source, or conflicting alignment fails
the build. Symbol stripping before verification is unsupported.

## Validation and comparisons

The helper can also compile test executables. It verifies them before they run:

```sh
python3 scripts/zen5-build.py --go /path/to/go1.27.2/bin/go \
  --output /tmp/sbpf.test -- test -c ./pkg/sbpf
(cd pkg/sbpf && SBPF_DIFF_OUT=/tmp/candidate-diff.txt SBPF_CHECK_POOL_ZERO=1 \
  /tmp/sbpf.test -test.run '^TestDifferentialDump$')
python3 scripts/zen5-build.py --go /path/to/go1.27.2/bin/go \
  --output /tmp/sealevel.test -- test -c ./pkg/sealevel
```

`--offset 0` produces a control build; `--offset 32` is the measured profile.
PGO is explicitly disabled. Compare generated-program dumps with the ordinary
reference, including return values, errors, CU, memory and pool isolation. Run
arithmetic, memory and call loops as well as real token/CPI workloads. Alternate
order and compare identical captured blocks/state before claiming replay gains.
See [execution validation](sbpf-interpreter-benchmarks.md) for the fixture hashes
and differential harness.

The preferred offset is specific to this interpreter/compiler/CPU combination.
Recheck actual placement and benchmark both offsets after a Go or interpreter
change. A Go upgrade requires updating the pinned source hashes and reviewing the
patch; the build deliberately rejects an unknown toolchain.
