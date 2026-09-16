set -eu
review_dir=/srv/mithril-leader-packing-pr-20260913
mkdir "$review_dir"
mkdir "$review_dir/candidate" "$review_dir/baseline" "$review_dir/results"
tar -xzf /tmp/mithril-leader-packing-pr-20260913.tar.gz -C "$review_dir/candidate"
tar -xf /tmp/mithril-leader-base-20260913.tar -C "$review_dir/baseline"
cp "$review_dir/candidate/pkg/blockprod/readonly_block_bench_test.go" "$review_dir/baseline/pkg/blockprod/"
cp "$review_dir/candidate/pkg/tpu/txfixture/readonly_pair.go" "$review_dir/baseline/pkg/tpu/txfixture/"
export PATH=/usr/local/go/bin:$PATH
export GOMAXPROCS=8
systemctl show mithril-alpenglow-pr259.service -p MainPID -p ActiveState > "$review_dir/results/service-before.txt"
sha256sum /tmp/mithril-leader-packing-pr-20260913.tar.gz /tmp/mithril-leader-base-20260913.tar > "$review_dir/results/source-sha256.txt"
lscpu -e=CPU,CORE,SOCKET > "$review_dir/results/topology.txt"
cd "$review_dir/candidate"
nice -n 10 go test -race ./pkg/accounts ./pkg/blockprod/... ./pkg/costmodel ./pkg/merkletree ./pkg/replay ./pkg/fees ./pkg/turbine ./pkg/tpu/txfixture ./pkg/config ./cmd/mithril/node > "$review_dir/results/native-race.txt" 2>&1
nice -n 10 go vet ./pkg/accounts ./pkg/blockprod/... ./pkg/costmodel ./pkg/merkletree ./pkg/replay ./pkg/fees ./pkg/turbine ./pkg/tpu/txfixture ./pkg/config ./cmd/mithril/node > "$review_dir/results/native-vet.txt" 2>&1
nice -n 10 go build -o "$review_dir/results/mithril" ./cmd/mithril > "$review_dir/results/native-build.txt" 2>&1
nice -n 10 go test -c -o "$review_dir/results/candidate.test" ./pkg/blockprod
cd "$review_dir/baseline"
nice -n 10 go test -run '^TestReadonlyPairBlockCapacityAndShredRoundTrip$' ./pkg/blockprod > "$review_dir/results/baseline-capacity.txt" 2>&1
nice -n 10 go test -c -o "$review_dir/results/baseline.test" ./pkg/blockprod
for round in 1 2 3; do
 if [ "$round" = 2 ]; then order='candidate baseline'; else order='baseline candidate'; fi
 for variant in $order; do
  nice -n 10 taskset -c 0-7 "$review_dir/results/$variant.test" -test.run='^$' -test.bench='^BenchmarkReadonlyPair(FullBlock|PreparedFullBlock)$' -test.benchtime=3x -test.count=1 > "$review_dir/results/round-$round-$variant.txt" 2>&1
 done
done
systemctl show mithril-alpenglow-pr259.service -p MainPID -p ActiveState > "$review_dir/results/service-after.txt"
sha256sum "$review_dir/results/"*.test "$review_dir/results/mithril" > "$review_dir/results/binary-sha256.txt"
cat "$review_dir/results/native-race.txt"
cat "$review_dir/results/round-"*.txt
