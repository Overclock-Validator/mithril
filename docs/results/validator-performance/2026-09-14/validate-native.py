import hashlib
import json
import os
import pathlib
import subprocess
import tarfile
import time

root = pathlib.Path('/srv/mithril-pr-consolidation-20260914')
assert not root.exists(), 'refuse to overwrite an existing validation'
source = root / 'source'
source.mkdir(parents=True)
with tarfile.open('/srv/mithril-pr-consolidation-20260914-source.tar') as archive:
    archive.extractall(source, filter='data')
env = dict(os.environ, PATH='/usr/local/go/bin:' + os.environ['PATH'], GOMAXPROCS='2')
packages = ['./pkg/accounts', './pkg/alpenglow', './pkg/block', './pkg/blockprod/...',
            './pkg/config', './pkg/consensus', './pkg/costmodel', './pkg/merkletree',
            './pkg/metrics', './pkg/replay', './pkg/sbpf', './pkg/sealevel',
            './pkg/sigverify', './pkg/statsd', './pkg/tpu/txfixture', './pkg/turbine',
            './cmd/mithril/node']
checks = [
    ('native-race', ['go', 'test', '-race', '-p', '1', '-count=1', *packages]),
    ('native-vet', ['go', 'vet', '-p', '1', *packages]),
    ('native-build', ['go', 'build', '-p', '1', '-o', str(root / 'mithril-review'), './cmd/mithril']),
]
results = []
for label, command in checks:
    print('starting', label, flush=True)
    started = time.time()
    with (root / (label + '.log')).open('w') as output:
        result = subprocess.run(['nice', '-n', '15', *command], cwd=source, env=env,
                                stdout=output, stderr=subprocess.STDOUT)
    row = dict(label=label, command=command, exit_code=result.returncode,
               elapsed_seconds=time.time() - started)
    results.append(row)
    (root / 'validation.json').write_text(json.dumps(results, indent=2))
    print(json.dumps(row), flush=True)
    if result.returncode:
        print((root / (label + '.log')).read_text()[-4000:], flush=True)
binary = root / 'mithril-review'
if binary.exists():
    print('review_binary_sha256', hashlib.sha256(binary.read_bytes()).hexdigest(), flush=True)
raise SystemExit(any(row['exit_code'] for row in results))
