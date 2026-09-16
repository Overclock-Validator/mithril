"""Summarize the paired standalone bank benchmarks; run beside the raw logs."""
import json
from pathlib import Path
from statistics import median

root = Path(__file__).resolve().parent
samples = {}
for path in sorted(root.glob('round-*.txt')):
    variant = path.stem.rsplit('-', 1)[1]
    for line in path.read_text().splitlines():
        if not line.startswith('Benchmark'):
            continue
        fields = line.split()
        name = fields[0].rsplit('-', 1)[0]
        metrics = dict(zip(fields[3::2], map(float, fields[2::2])))
        samples.setdefault(variant + ':' + name, []).append(metrics)
summary = {key: {metric: median(s[metric] for s in values) for metric in values[0]}
           for key, values in sorted(samples.items())}
(root / 'summary.json').write_text(json.dumps({'samples': samples, 'medians': summary}, indent=2) + '\n')
print(json.dumps(summary, indent=2))
