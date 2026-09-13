"""Regenerate summary.json from the archived native measurements."""
import json
import statistics
import tarfile
from collections import defaultdict
from pathlib import Path

root = Path(__file__).resolve().parent
groups = defaultdict(list)
contention = []
metrics = ("ns/op", "full_to_ready_p50-ms", "full_to_ready_p95-ms", "cpu-ms/block",
           "B/op", "allocs/op", "collection_p50-ms", "completion_sigverify_p50-ms",
           "residual_p50-ms", "residual_p95-ms", "ready_p95-ms", "mean_width")
with tarfile.open(root / "raw-results.tar.gz") as archive:
    for member in archive.getmembers():
        if not member.isfile():
            continue
        path = Path(member.name)
        if path.name == "comparison.json" and path.parts[0] in ("contention", "contention-recheck"):
            contention.extend(json.load(archive.extractfile(member))["records"])
        if path.suffix != ".txt":
            continue
        parts = path.stem.split("-", 2)
        if len(parts) != 3 or not parts[1].isdigit():
            continue
        if path.parts[0] == "screening" and parts[0] in ("assembly", "pool"):
            kind = "screening_" + parts[0]
        elif path.parts[0] in ("final", "fallback") and parts[0] == "sample":
            kind = path.parts[0]
        else:
            continue
        for line in archive.extractfile(member).read().decode().splitlines():
            if not line.startswith("Benchmark"):
                continue
            fields = line.split()
            assert int(fields[1]) == 5
            values = {fields[i + 1]: float(fields[i]) for i in range(2, len(fields), 2)}
            groups[(kind, fields[0], parts[2])].append((int(parts[1]), values))

summary = []
for (kind, name, variant), samples in sorted(groups.items()):
    expected = 6 if kind == "final" else 3 if kind == "fallback" else 4
    assert len(samples) == expected, (kind, name, variant)
    assert sorted(sample for sample, _ in samples) == list(range(1, expected + 1))
    summary.append({"kind": kind, "name": name, "variant": variant, "samples": expected,
                    "medians": {key: statistics.median(row[key] for _, row in samples)
                                for key in metrics if key in samples[0][1]}})
assert len(summary) == 82
execution = []
for job in (8, 32, 64):
    for mode in ("catchup", "tip_200ms_overlap"):
        runs = [r for r in contention if r["sample"] in (2, 3, 4)
                and r["job_signatures"] == job and r["mode"] == mode]
        assert sorted(r["sample"] for r in runs) == [2, 3, 4]
        ratios = [r["execution_slowdown_ratio"] for r in runs]
        execution.append({"job_signatures": job, "mode": mode, "samples": 3,
                          "median_ratio": statistics.median(ratios),
                          "min_ratio": min(ratios), "max_ratio": max(ratios)})
(root / "summary.json").write_text(json.dumps({"benchmarks": summary, "execution": execution}, indent=2) + "\n")
