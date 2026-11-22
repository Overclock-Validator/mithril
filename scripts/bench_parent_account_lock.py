#!/usr/bin/env python3
"""Compare parent-account locking with matched synthetic ProcessBlock replay.

The live checkout is never edited. Both test binaries come from one detached
worktree; the unlocked baseline is a Go overlay that changes only the accessor.
Only the opt-in measurement test is run, never a validator process.
"""

import argparse
import datetime
import difflib
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import statistics
import subprocess
import tempfile


def command_output(command, cwd):
    return subprocess.check_output(command, cwd=cwd, text=True).strip()


def distribution(values):
    values = sorted(values)
    return {
        "mean_ms": statistics.mean(values) / 1_000_000,
        "median_ms": statistics.median(values) / 1_000_000,
        "p95_ms": values[math.ceil(len(values) * 0.95) - 1] / 1_000_000,
        "p99_ms": values[math.ceil(len(values) * 0.99) - 1] / 1_000_000,
        "max_ms": max(values) / 1_000_000,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, type=Path, help="new output directory")
    parser.add_argument("--revision", default="HEAD")
    parser.add_argument("--blocks", type=int, default=100, help="measured repetitions per workload and trial")
    parser.add_argument("--trials", type=int, default=4, help="paired trials; order alternates")
    args = parser.parse_args()
    if args.blocks < 1 or args.trials < 1:
        parser.error("blocks and trials must be positive")
    repo = Path(command_output(["git", "rev-parse", "--show-toplevel"], Path(__file__).resolve().parent))
    commit = command_output(["git", "rev-parse", args.revision + "^{commit}"], repo)
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    fixture_path = Path("pkg/replay/parent_account_lock_benchmark_test.go")
    fixture = (repo / fixture_path).read_bytes()
    env = os.environ.copy()
    env["GOMAXPROCS"] = "2"
    env["MITHRIL_PARENT_ACCOUNT_LOCK_BENCH_BLOCKS"] = str(args.blocks)
    report = {
        "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "source_commit": commit,
        "fixture_sha256": hashlib.sha256(fixture).hexdigest(),
        "go_version": command_output(["go", "version"], repo),
        "platform": platform.platform(),
        "gomaxprocs": 2,
        "tx_parallelism": 4,
        "paired_trials": args.trials,
        "measured_blocks_per_workload_per_trial": args.blocks,
        "scope": "Synthetic whole-ProcessBlock replay over fixed in-memory parent state",
        "limitations": [
            "This is not mainnet replay and does not measure the complete production SlotReplay interval.",
            "Transactions are independent system transfers, not a representative mainnet program mix.",
            "Account reads use memory; network, consensus, durable folds and real AccountsDB I/O are excluded.",
            "Transaction generation and signature verification are excluded; signatures are marked already verified.",
            "Both variants use the same committed source plus the fixture; unrelated unstaged edits are excluded.",
            "The unlocked accessor is only used in an isolated test binary with an immutable parent, never deployed.",
            "The host may be running a validator or other workloads; alternating pairs do not remove all scheduling/GC noise.",
        ],
    }
    try:
        report["cpu"] = json.loads(command_output(["lscpu", "--json"], repo))
    except (OSError, subprocess.CalledProcessError, json.JSONDecodeError):
        report["cpu"] = platform.processor()
    binaries = {variant: out / ("replay-" + variant + ".test") for variant in ("locked", "unlocked")}
    with tempfile.TemporaryDirectory(prefix="mithril-parent-lock-") as temp:
        source = Path(temp) / "source"
        subprocess.run(["git", "worktree", "add", "--detach", str(source), commit], cwd=repo, check=True, stdout=subprocess.DEVNULL)
        try:
            (source / fixture_path).write_bytes(fixture)
            accessor = source / "pkg/sealevel/execution_ctx.go"
            locked = accessor.read_text()
            before = "\tkey := [32]byte(pubkey)\n\tacct, err := slotCtx.ParentAccts.GetAccount(&key)"
            after = "\tacct, err := slotCtx.ParentAccts.GetAccountWithoutLock(pubkey)"
            if locked.count(before) != 1:
                raise RuntimeError("Expected exactly one locked parent accessor; review the baseline patch")
            unlocked = locked.replace(before, after)
            replacement = Path(temp) / "execution_ctx_unlocked.go"
            replacement.write_text(unlocked)
            overlay = Path(temp) / "unlocked-overlay.json"
            overlay.write_text(json.dumps({"Replace": {str(accessor): str(replacement)}}, indent=2) + "\n")
            (out / "baseline.patch").write_text("".join(difflib.unified_diff(locked.splitlines(True), unlocked.splitlines(True), fromfile="locked/execution_ctx.go", tofile="unlocked/execution_ctx.go")))
            for variant, binary in binaries.items():
                command = ["go", "test", "-p=1", "-c", "-o", str(binary)]
                if variant == "unlocked":
                    command += ["-overlay", str(overlay)]
                command += ["./pkg/replay"]
                with (out / ("build-" + variant + ".log")).open("w") as log:
                    subprocess.run(command, cwd=source, env=env, check=True, stdout=log, stderr=subprocess.STDOUT)
            report["binary_sha256"] = {variant: hashlib.sha256(binary.read_bytes()).hexdigest() for variant, binary in binaries.items()}
            samples = {"locked": [], "unlocked": []}
            for trial in range(args.trials):
                order = ("locked", "unlocked") if trial % 2 == 0 else ("unlocked", "locked")
                for variant in order:
                    result = out / f"{variant}-{trial + 1}.json"
                    env["MITHRIL_PARENT_ACCOUNT_LOCK_BENCH_OUT"] = str(result)
                    with (out / f"{variant}-{trial + 1}.log").open("w") as log:
                        subprocess.run([str(binaries[variant]), "-test.run=^TestParentAccountLockReplayMeasurement$", "-test.count=1", "-test.timeout=120s"], cwd=source, env=env, check=True, stdout=log, stderr=subprocess.STDOUT)
                    samples[variant].append(json.loads(result.read_text()))
                    print(f"completed {variant} trial {trial + 1}/{args.trials}", flush=True)
            results = []
            for index, transactions in enumerate((64, 256, 1024)):
                row = {"transactions_per_block": transactions}
                identities = set()
                means = {}
                for variant in ("locked", "unlocked"):
                    workloads = [trial["workloads"][index] for trial in samples[variant]]
                    for workload in workloads:
                        identities.add((workload["transaction_digest_sha256"], workload["bankhash_hex"], workload["consumed_cu_per_block"]))
                    durations = [duration for workload in workloads for duration in workload["process_block_ns"]]
                    total_cu = sum(workload["total_consumed_cu"] for workload in workloads)
                    means[variant] = [workload["total_process_block_ns"] / workload["measured_blocks"] for workload in workloads]
                    row[variant] = {
                        "measured_blocks": len(durations),
                        "average_consumed_cu_per_block": total_cu / len(durations),
                        "total_consumed_cu": total_cu,
                        "total_process_block_ns": sum(durations),
                        "cu_per_ns": total_cu / sum(durations),
                        "process_block": distribution(durations),
                    }
                if len(identities) != 1:
                    raise RuntimeError(f"Mismatched transactions, bank hash or consumed CU for {transactions} transactions")
                row["transaction_digest_sha256"], row["bankhash_hex"], row["consumed_cu_per_block"] = identities.pop()
                row["locked_mean_change_pct_by_pair"] = [(new / old - 1) * 100 for new, old in zip(means["locked"], means["unlocked"])]
                row["median_paired_mean_change_pct"] = statistics.median(row["locked_mean_change_pct_by_pair"])
                results.append(row)
            report["workloads"] = results
        finally:
            subprocess.run(["git", "worktree", "remove", "--force", str(source)], cwd=repo, check=True)
            for binary in binaries.values():
                binary.unlink(missing_ok=True)
    report["completed_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    (out / "results.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"results": str(out / "results.json"), "workloads": report["workloads"]}, indent=2))


if __name__ == "__main__":
    main()
