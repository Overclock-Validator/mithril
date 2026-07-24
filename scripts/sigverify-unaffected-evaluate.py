#!/usr/bin/env python3
"""Evaluate the frozen-baseline disabled-telemetry public verifier A/B."""

from __future__ import annotations

from dataclasses import dataclass
import argparse
import contextlib
import io
import re
import statistics
import sys
import tempfile
from pathlib import Path
from typing import Sequence


BASELINE_REVISION = "9f43c94203a5f705c8bff09033fa5995448132cc"
EXPECTED_SAMPLES = 10
REGRESSION_LIMIT = 0.01
EPSILON = 1e-12
NUMBER = r"[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?"
ROW = re.compile(rf"^(Benchmark\S+)\s+\d+\s+({NUMBER})\s+ns/op(?P<metrics>.*)$")
BYTES = re.compile(rf"(?:^|\s)({NUMBER})\s+B/op(?:\s|$)")
ALLOCS = re.compile(rf"(?:^|\s)({NUMBER})\s+allocs/op(?:\s|$)")
CPU_SUFFIX = re.compile(r"-\d+$")
HEX40 = re.compile(r"[0-9a-f]{40}")
HEX64 = re.compile(r"[0-9a-f]{64}")

EXPECTED_ROWS = (
    "BenchmarkDisabledTelemetryPublicVerification/path=tpu-root-transaction",
    "BenchmarkDisabledTelemetryPublicVerification/path=tpu-root-packet",
    "BenchmarkDisabledTelemetryPublicVerification/path=tpu-sigverify-transaction",
    "BenchmarkDisabledTelemetryPublicVerification/path=tpu-sigverify-packet",
    "BenchmarkDisabledTelemetryPublicVerification/path=txverify-transaction",
)


class GateError(RuntimeError):
    pass


@dataclass(frozen=True)
class Sample:
    nanoseconds: float
    bytes_allocated: float
    allocations: float


@dataclass(frozen=True)
class Stats:
    median_nanoseconds: float
    maximum_bytes: float
    maximum_allocations: float


Rows = dict[str, list[Sample]]


def parse_lines(lines: Sequence[str]) -> Rows:
    rows: Rows = {}
    for line in lines:
        match = ROW.match(line.strip())
        if match is None:
            continue
        bytes_match = BYTES.search(match.group("metrics"))
        allocs_match = ALLOCS.search(match.group("metrics"))
        if bytes_match is None or allocs_match is None:
            raise GateError(f"benchmark row lacks -benchmem metrics: {line.strip()}")
        name = CPU_SUFFIX.sub("", match.group(1))
        rows.setdefault(name, []).append(
            Sample(
                nanoseconds=float(match.group(2)),
                bytes_allocated=float(bytes_match.group(1)),
                allocations=float(allocs_match.group(1)),
            )
        )
    return rows


def load_rows(result_dir: Path, filename: str) -> Rows:
    path = result_dir / filename
    if not path.is_file():
        raise GateError(f"missing benchmark output: {path}")
    rows = parse_lines(path.read_text(encoding="utf-8").splitlines())
    actual, expected = set(rows), set(EXPECTED_ROWS)
    if actual != expected:
        missing = sorted(expected - actual)
        unexpected = sorted(actual - expected)
        raise GateError(
            f"{filename} benchmark matrix mismatch: "
            f"missing={missing}, unexpected={unexpected}"
        )
    for name in EXPECTED_ROWS:
        if len(rows[name]) != EXPECTED_SAMPLES:
            raise GateError(
                f"{filename} row {name} has {len(rows[name])} samples; "
                f"expected exactly {EXPECTED_SAMPLES}"
            )
    return rows


def stats(samples: list[Sample]) -> Stats:
    return Stats(
        median_nanoseconds=statistics.median(s.nanoseconds for s in samples),
        maximum_bytes=max(s.bytes_allocated for s in samples),
        maximum_allocations=max(s.allocations for s in samples),
    )


def read_key_values(path: Path) -> dict[str, str]:
    if not path.is_file():
        raise GateError(f"missing provenance: {path}")
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        key, separator, value = line.partition("=")
        if separator:
            values[key] = value
    return values


def validate_provenance(result_dir: Path) -> None:
    values = read_key_values(result_dir / "provenance.txt")
    if values.get("baseline_revision") != BASELINE_REVISION:
        raise GateError("provenance baseline revision is missing or incorrect")
    if values.get("release_cpu_match") != "true":
        raise GateError("result is not from the Ryzen 7 PRO 8700GE release CPU")
    if "AMD Ryzen 7 PRO 8700GE" not in values.get("cpu_model", ""):
        raise GateError("provenance lacks the exact release CPU model")
    if re.fullmatch(r"[0-9]+", values.get("pinned_core", "")) is None:
        raise GateError("provenance lacks a valid pinned core")
    if values.get("telemetry_environment") != "forced-unset":
        raise GateError("telemetry environment was not recorded as forced-unset")
    for key in ("current_head", "harness_git_blob"):
        if HEX40.fullmatch(values.get(key, "")) is None:
            raise GateError(f"provenance lacks valid {key}")
    for key in (
        "source_tree_sha256",
        "harness_sha256",
        "gate_script_sha256",
        "evaluator_sha256",
        "manifest_script_sha256",
        "current_binary_sha256",
        "baseline_binary_sha256",
        "baseline_archive_sha256",
        "git_status_sha256",
        "git_diff_sha256",
    ):
        if HEX64.fullmatch(values.get(key, "")) is None:
            raise GateError(f"provenance lacks valid {key}")
    manifest_values = read_key_values(result_dir / "source-manifest-start.tsv")
    if manifest_values.get("source_tree_sha256") != values.get(
        "source_tree_sha256"
    ):
        raise GateError("provenance and source manifest digests differ")

    order_path = result_dir / "run-order.txt"
    if not order_path.is_file():
        raise GateError(f"missing run order: {order_path}")
    expected_order = [
        f"round={round_number} order="
        + ("current,baseline" if round_number % 2 else "baseline,current")
        for round_number in range(1, EXPECTED_SAMPLES + 1)
    ]
    actual_order = order_path.read_text(encoding="utf-8").splitlines()
    if actual_order != expected_order:
        raise GateError("run order is missing, incomplete, or not alternating")


def regression(baseline: Stats, current: Stats) -> float:
    if baseline.median_nanoseconds <= 0:
        raise GateError("non-positive benchmark baseline")
    return current.median_nanoseconds / baseline.median_nanoseconds - 1.0


def row_passes(baseline: Stats, current: Stats) -> bool:
    return (
        regression(baseline, current) <= REGRESSION_LIMIT + EPSILON
        and current.maximum_bytes <= baseline.maximum_bytes + EPSILON
        and current.maximum_allocations <= baseline.maximum_allocations + EPSILON
    )


def evaluate(result_dir: Path) -> bool:
    validate_provenance(result_dir)
    baseline_rows = load_rows(result_dir, "baseline.txt")
    current_rows = load_rows(result_dir, "current.txt")

    passed = True
    print("Mithril disabled-telemetry public verifier A/B")
    print("================================================")
    print(f"baseline={BASELINE_REVISION}")
    print("limit=median regression <=1%; no B/op or allocs/op increase")
    for name in EXPECTED_ROWS:
        baseline = stats(baseline_rows[name])
        current = stats(current_rows[name])
        change = regression(baseline, current)
        row_ok = row_passes(baseline, current)
        passed = passed and row_ok
        short_name = name.removeprefix(
            "BenchmarkDisabledTelemetryPublicVerification/path="
        )
        print(
            f"  {short_name:27s} {change:8.2%}  "
            f"B/op={current.maximum_bytes:g} "
            f"allocs/op={current.maximum_allocations:g} "
            f"{'PASS' if row_ok else 'FAIL'}"
        )
    print(f"DISABLED_TELEMETRY_PUBLIC_GATE={'pass' if passed else 'fail'}")
    print("REPLAY_WALL_TIME_GATE=pending_end_to_end_replay")
    return passed


def assert_gate_error(function, fragment: str) -> None:
    try:
        function()
    except GateError as error:
        assert fragment in str(error), error
    else:
        raise AssertionError(f"expected GateError containing {fragment!r}")


def self_test() -> None:
    lines = [
        "BenchmarkDisabledTelemetryPublicVerification/path=tpu-root-transaction-16 "
        "100 100.0 ns/op 64 B/op 2 allocs/op",
        "BenchmarkDisabledTelemetryPublicVerification/path=tpu-root-transaction-16 "
        "100 101.0 ns/op 64 B/op 2 allocs/op",
    ]
    parsed = parse_lines(lines)
    row = parsed[EXPECTED_ROWS[0]]
    assert stats(row) == Stats(100.5, 64.0, 2.0)
    baseline = Stats(100.0, 64.0, 2.0)
    assert row_passes(baseline, Stats(101.0, 64.0, 2.0))
    assert not row_passes(baseline, Stats(101.01, 64.0, 2.0))
    assert not row_passes(baseline, Stats(90.0, 65.0, 2.0))
    assert not row_passes(baseline, Stats(90.0, 64.0, 3.0))
    assert_gate_error(
        lambda: parse_lines(
            ["BenchmarkX-16 100 10 ns/op 0 allocs/op"]
        ),
        "lacks -benchmem",
    )
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "provenance.txt"
        path.write_text(
            f"baseline_revision={BASELINE_REVISION}\n"
            "release_cpu_match=false\n",
            encoding="utf-8",
        )
        assert_gate_error(
            lambda: validate_provenance(Path(directory)), "release CPU"
        )

    def benchmark_text(multiplier: float) -> str:
        result: list[str] = []
        for name in EXPECTED_ROWS:
            for _ in range(EXPECTED_SAMPLES):
                result.append(
                    f"{name}-16 100 {100.0 * multiplier:.6f} ns/op "
                    "64 B/op 2 allocs/op\n"
                )
        return "".join(result)

    with tempfile.TemporaryDirectory() as directory:
        result_dir = Path(directory)
        hashes = {
            key: "b" * 64
            for key in (
                "source_tree_sha256",
                "harness_sha256",
                "gate_script_sha256",
                "evaluator_sha256",
                "manifest_script_sha256",
                "current_binary_sha256",
                "baseline_binary_sha256",
                "baseline_archive_sha256",
                "git_status_sha256",
                "git_diff_sha256",
            )
        }
        provenance = {
            "baseline_revision": BASELINE_REVISION,
            "release_cpu_match": "true",
            "cpu_model": "AMD Ryzen 7 PRO 8700GE w/ Radeon 780M Graphics",
            "pinned_core": "2",
            "telemetry_environment": "forced-unset",
            "current_head": "a" * 40,
            "harness_git_blob": "c" * 40,
            **hashes,
        }
        (result_dir / "provenance.txt").write_text(
            "".join(f"{key}={value}\n" for key, value in provenance.items()),
            encoding="utf-8",
        )
        (result_dir / "source-manifest-start.tsv").write_text(
            f"source_tree_sha256={hashes['source_tree_sha256']}\n",
            encoding="utf-8",
        )
        (result_dir / "run-order.txt").write_text(
            "".join(
                f"round={round_number} order="
                + (
                    "current,baseline\n"
                    if round_number % 2
                    else "baseline,current\n"
                )
                for round_number in range(1, EXPECTED_SAMPLES + 1)
            ),
            encoding="utf-8",
        )
        (result_dir / "baseline.txt").write_text(
            benchmark_text(1.0), encoding="utf-8"
        )
        (result_dir / "current.txt").write_text(
            benchmark_text(1.01), encoding="utf-8"
        )
        with contextlib.redirect_stdout(io.StringIO()):
            assert evaluate(result_dir)
        (result_dir / "current.txt").write_text(
            benchmark_text(1.0101), encoding="utf-8"
        )
        with contextlib.redirect_stdout(io.StringIO()):
            assert not evaluate(result_dir)
        with (result_dir / "current.txt").open("a", encoding="utf-8") as output:
            output.write("BenchmarkUnexpected-16 100 1 ns/op 0 B/op 0 allocs/op\n")
        assert_gate_error(
            lambda: load_rows(result_dir, "current.txt"), "matrix mismatch"
        )
    print("sigverify-unaffected-evaluate: self-test passed")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("result_dir", nargs="?")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    try:
        if args.self_test:
            self_test()
            return 0
        if args.result_dir is None:
            parser.error("result_dir is required")
        return 0 if evaluate(Path(args.result_dir)) else 1
    except GateError as error:
        print(f"sigverify-unaffected-evaluate: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
