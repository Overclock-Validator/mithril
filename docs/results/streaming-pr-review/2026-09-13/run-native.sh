set -eu
review_dir=/srv/mithril-streaming-pr-review-20260913
mkdir "$review_dir"
tar -xzf /tmp/mithril-streaming-pr-review-20260913.tar.gz -C "$review_dir"
cd "$review_dir"
export PATH=/usr/local/go/bin:$PATH
export GOMAXPROCS=8
mkdir review-results
sha256sum /tmp/mithril-streaming-pr-review-20260913.tar.gz > review-results/source-sha256.txt
systemctl show mithril-alpenglow-pr259.service -p MainPID -p ActiveState > review-results/service-before.txt
nice -n 10 go test -race ./pkg/turbine ./pkg/replay ./pkg/sigverify ./pkg/txverify ./pkg/block ./pkg/statsd ./cmd/mithril/node > review-results/native-race.txt 2>&1
nice -n 10 go vet ./pkg/turbine ./pkg/replay ./pkg/sigverify ./pkg/txverify ./pkg/block ./pkg/statsd ./cmd/mithril/node > review-results/native-vet.txt 2>&1
nice -n 10 go build -o review-results/mithril ./cmd/mithril > review-results/native-build.txt 2>&1
nice -n 10 go test -c -o review-results/turbine.test ./pkg/turbine
MITHRIL_SIGVERIFY_FLOW_BACKEND=r51 nice -n 10 taskset -c 0-7 review-results/turbine.test -test.run='^$' -test.bench='^BenchmarkEntryPrefetchAssembly$/^generated_.*$/^workers_2$/^target_8$/.*' -test.benchtime=3x -test.count=1 > review-results/native-assembly.txt 2>&1
systemctl show mithril-alpenglow-pr259.service -p MainPID -p ActiveState > review-results/service-after.txt
sha256sum review-results/mithril review-results/turbine.test > review-results/binaries-sha256.txt
cat review-results/native-race.txt
cat review-results/native-assembly.txt
