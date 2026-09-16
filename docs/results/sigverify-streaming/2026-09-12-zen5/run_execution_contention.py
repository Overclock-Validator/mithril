#!/usr/bin/env python3
"""Compare actual transfer load/execute under separate-process sigverify load.

This does not benchmark complete block replay, the shared Go scheduler, account
commit, or the dependency planner. Both processes use the same explicitly chosen
eight physical CPU cores. The signature workload uses the production verifier
pool; 200 ms availability is simulated, not a captured network trace.
"""

import argparse
import json
import os
from pathlib import Path
import random
import shutil
import statistics
import subprocess
import time


def cpu_numbers(spec):
    result = []
    for part in spec.split(","):
        bounds = [int(value) for value in part.split("-")]
        if len(bounds) == 1:
            result.append(bounds[0])
        elif len(bounds) == 2 and bounds[0] <= bounds[1]:
            result.extend(range(bounds[0], bounds[1] + 1))
        else:
            raise ValueError(f"invalid CPU list: {spec}")
    if len(result) != len(set(result)):
        raise ValueError("CPU list contains duplicates")
    return result


def cpu_topology(cpus):
    rows = []
    for cpu in cpus:
        root = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology")
        rows.append({
            "cpu": cpu,
            "package": int((root / "physical_package_id").read_text()),
            "core": int((root / "core_id").read_text()),
        })
    return rows


def parse_benchmarks(path):
    results = []
    for line in path.read_text().splitlines():
        fields = line.strip().split()
        if len(fields) < 4 or not fields[0].startswith("Benchmark") or not fields[1].isdigit():
            continue
        row = {"name": fields[0], "iterations": int(fields[1])}
        for index in range(2, len(fields) - 1, 2):
            try:
                row[fields[index + 1]] = float(fields[index])
            except ValueError:
                break
        results.append(row)
    return results


def atomic_json(path, payload):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n")
    temporary.replace(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--turbine-bin", required=True, type=Path)
    parser.add_argument("--replay-bin", required=True, type=Path)
    parser.add_argument("--fixtures", required=True, type=Path)
    parser.add_argument("--slot", required=True, type=int)
    parser.add_argument("--cpus", required=True, help="Exactly eight physical cores, one logical CPU each, e.g. 0-7")
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--probe-time", default="500ms", help="Go benchmark benchtime for one execution sample")
    parser.add_argument("--catchup-blocks", type=int, default=256)
    parser.add_argument("--tip-blocks", type=int, default=30)
    parser.add_argument("--warmup-ms", type=int, default=400)
    parser.add_argument("--backend", default="r51")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--timeout", type=int, default=120)
    args = parser.parse_args()
    if min(args.samples, args.timeout) < 1 or min(args.catchup_blocks, args.tip_blocks) < 2 or args.warmup_ms < 0:
        parser.error("samples/timeout must be positive, block counts >=2, warmup nonnegative")
    if not shutil.which("taskset"):
        parser.error("this runner requires Linux taskset")
    cpus = cpu_numbers(args.cpus)
    topology = cpu_topology(cpus)
    if len(cpus) != 8 or len({(row["package"], row["core"]) for row in topology}) != 8:
        parser.error("select exactly eight distinct physical CPU cores, one logical CPU per core")
    if not set(cpus).issubset(os.sched_getaffinity(0)):
        parser.error("selected CPUs are outside this process's allowed affinity")
    for name in ("turbine_bin", "replay_bin", "fixtures"):
        path = getattr(args, name).resolve(strict=True)
        setattr(args, name, path)
    if not (args.fixtures / f"block-{args.slot}.json").is_file():
        parser.error("selected slot does not exist in fixture directory")
    args.out = args.out.resolve()
    args.out.mkdir(parents=True, exist_ok=False)
    environment = os.environ.copy()
    environment["GOMAXPROCS"] = "8"
    # Do not inherit rendezvous paths or fixture limits from another experiment.
    for name in list(environment):
        if name.startswith("MITHRIL_SIGVERIFY_FLOW_"):
            del environment[name]
    affinity = ["taskset", "--cpu-list", args.cpus]
    probe_command = affinity + [
        str(args.replay_bin), "-test.run=^$",
        "-test.bench=^BenchmarkLoadAndExecuteTransferResultMode$/^lean$",
        f"-test.benchtime={args.probe_time}", "-test.count=1",
        f"-test.timeout={args.timeout}s",
    ]
    results = {
        "description": __doc__,
        "arguments": {key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items()},
        "topology": topology,
        "gomaxprocs_each_process": 8,
        "execution_probe_command": probe_command,
        "runs": [],
    }
    manifest = args.out / "comparison.json"
    atomic_json(manifest, results)

    def execution_probe(path):
        started = time.monotonic()
        with path.open("w") as output:
            subprocess.run(probe_command, env=environment, stdout=output, stderr=subprocess.STDOUT,
                           check=True, timeout=args.timeout)
        rows = parse_benchmarks(path)
        if len(rows) != 1 or "ns/op" not in rows[0]:
            raise RuntimeError(f"expected exactly one execution result in {path}")
        return {"wall_seconds": time.monotonic() - started, "benchmark": rows[0], "log": str(path)}

    conditions = [(workers, target, mode) for workers in (2, 4) for target in (4, 8)
                  for mode in ("catchup", "tip_200ms_overlap")]
    rng = random.Random(args.seed)
    try:
        for sample in range(args.samples):
            order = conditions.copy()
            rng.shuffle(order)
            for workers, target, mode in order:
                name = f"sample-{sample + 1}-{mode}-workers{workers}-target{target}"
                folder = args.out / name
                folder.mkdir()
                print(f"{name}: baseline execution", flush=True)
                baseline = execution_probe(folder / "execution-baseline.log")
                ready, start = folder / "verifier.ready", folder / "verifier.start"
                load_env = environment | {
                    "MITHRIL_SIGVERIFY_FLOW_BACKEND": args.backend,
                    "MITHRIL_SIGVERIFY_FLOW_FIXTURES": str(args.fixtures),
                    "MITHRIL_SIGVERIFY_FLOW_READY": str(ready),
                    "MITHRIL_SIGVERIFY_FLOW_START": str(start),
                }
                blocks = args.catchup_blocks if mode == "catchup" else args.tip_blocks
                benchmark = (f"^BenchmarkTransactionVerificationFlow$/^captured_{args.slot}$/"
                             f"^workers_{workers}$/^target_{target}$/^{mode}$")
                load_command = affinity + [str(args.turbine_bin), "-test.run=^$",
                                          f"-test.bench={benchmark}", f"-test.benchtime={blocks}x",
                                          "-test.count=1", f"-test.timeout={args.timeout}s"]
                load_log = folder / "sigverify-load.log"
                with load_log.open("w") as output:
                    load = subprocess.Popen(load_command, env=load_env, stdout=output, stderr=subprocess.STDOUT)
                    try:
                        deadline = time.monotonic() + 30
                        while not ready.exists():
                            if load.poll() is not None:
                                raise RuntimeError(f"signature workload failed before rendezvous; inspect {load_log}")
                            if time.monotonic() > deadline:
                                raise TimeoutError("signature workload readiness timed out")
                            time.sleep(0.01)
                        start.touch()
                        time.sleep(args.warmup_ms / 1000)
                        if load.poll() is not None:
                            raise RuntimeError("signature load ended during warmup; increase --catchup-blocks/--tip-blocks")
                        print(f"{name}: concurrent execution", flush=True)
                        concurrent = execution_probe(folder / "execution-concurrent.log")
                        # Reject short load windows: otherwise the probe's late
                        # iterations could quietly measure unloaded execution.
                        if load.poll() is not None:
                            raise RuntimeError("signature load ended before execution probe; increase workload block counts")
                        exit_code = load.wait(timeout=args.timeout)
                        if exit_code:
                            raise RuntimeError(f"signature workload exited {exit_code}; inspect {load_log}")
                    finally:
                        if load.poll() is None:
                            load.terminate()  # only the child benchmark process
                            try:
                                load.wait(timeout=5)
                            except subprocess.TimeoutExpired:
                                load.kill()
                                load.wait()
                load_rows = parse_benchmarks(load_log)
                if len(load_rows) != 1:
                    raise RuntimeError(f"expected one signature result in {load_log}")
                ratio = concurrent["benchmark"]["ns/op"] / baseline["benchmark"]["ns/op"]
                results["runs"].append({
                    "sample": sample + 1, "workers": workers, "target": target, "mode": mode,
                    "baseline": baseline, "concurrent": concurrent,
                    "execution_slowdown_ratio": ratio,
                    "sigverify": load_rows[0], "sigverify_command": load_command,
                    "sigverify_log": str(load_log),
                })
                atomic_json(manifest, results)
                print(f"{name}: execution ratio {ratio:.3f}x", flush=True)
    except Exception as error:
        results["error"] = str(error)
        atomic_json(manifest, results)
        raise

    summary = []
    for workers, target, mode in conditions:
        runs = [row for row in results["runs"] if (row["workers"], row["target"], row["mode"]) == (workers, target, mode)]
        summary.append({
            "workers": workers, "target": target, "mode": mode, "samples": len(runs),
            "median_execution_slowdown_ratio": statistics.median(row["execution_slowdown_ratio"] for row in runs),
            "median_execution_baseline_ns": statistics.median(row["baseline"]["benchmark"]["ns/op"] for row in runs),
            "median_execution_concurrent_ns": statistics.median(row["concurrent"]["benchmark"]["ns/op"] for row in runs),
            "median_sigverify_residual_p95_ms": statistics.median(row["sigverify"]["residual_p95-ms"] for row in runs),
            "median_sigverify_cpu_ms_per_block": statistics.median(row["sigverify"]["cpu-ms/block"] for row in runs),
        })
    results["summary"] = summary
    atomic_json(manifest, results)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
