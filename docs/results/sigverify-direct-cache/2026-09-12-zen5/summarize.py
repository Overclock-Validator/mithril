"""Summarize alternating native baseline/candidate assembly benchmark runs."""

import json
import statistics
from pathlib import Path


root = Path(__file__).resolve().parent
rows = []
for path in sorted(root.glob("sample-*.txt")):
    for line in path.read_text().splitlines():
        if not line.startswith("BenchmarkEntryPrefetchAssembly/"):
            continue
        fields = line.split()
        rows.append({
            "sample": int(path.stem.split("-")[1]),
            "variant": path.stem.split("-")[2],
            "name": fields[0],
            "iterations": int(fields[1]),
            "metrics": {
                fields[i + 1]: float(fields[i])
                for i in range(2, len(fields), 2)
            },
        })

assert len(rows) == 6 * 2 * 8
summary = []
for name in sorted({row["name"] for row in rows}):
    variants = {
        variant: [row for row in rows if row["name"] == name and row["variant"] == variant]
        for variant in ("baseline", "candidate")
    }
    for samples in variants.values():
        assert len(samples) == 6
        assert sorted(row["sample"] for row in samples) == list(range(1, 7))
        assert all(row["iterations"] == 5 for row in samples)
    medians = {
        variant: {
            metric: statistics.median(row["metrics"][metric] for row in samples)
            for metric in samples[0]["metrics"]
        }
        for variant, samples in variants.items()
    }
    paired = {
        metric: [
            next(row for row in variants["candidate"] if row["sample"] == sample)["metrics"][metric]
            / next(row for row in variants["baseline"] if row["sample"] == sample)["metrics"][metric]
            for sample in range(1, 7)
        ]
        for metric in ("ns/op", "cpu-ms/block", "B/op", "full_to_ready_p50-ms")
    }
    summary.append({"name": name, "medians": medians, "paired_ratios": paired})

(root / "summary.json").write_text(json.dumps({"samples": rows, "summary": summary}, indent=2) + "\n")
