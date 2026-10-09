package turbine

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sigverify"
	"github.com/Overclock-Validator/mithril/pkg/txverify"
	"github.com/gagliardetto/solana-go"
)

// BenchmarkTransactionVerificationFlow measures the real verifier pool under
// simulated transaction availability, not actual network reception or replay.
// Decode and fixture generation are outside the timer. "tip" spreads complete
// components over 200 ms; this is an explicit workload model, not a claim about
// the cluster's observed component sizes or arrival distribution. No component
// waits for another component to fill a verification batch.
//
// Optional environment:
//
//	MITHRIL_SIGVERIFY_FLOW_BACKEND=r51 (use -run '^$' in a fresh test process)
//	MITHRIL_SIGVERIFY_FLOW_FIXTURES=/path/to/fixtures (block-*.json, base64 txs)
//	MITHRIL_SIGVERIFY_FLOW_COUNT=33760 (generated count or captured prefix limit)
//
// Without captured fixtures, use reproducible signed 228-byte and 1232-byte
// legacy memo transactions. They are signature workloads, not replay fixtures.
// Use -benchtime=3x or another fixed count when comparing configurations so the
// deliberate arrival waits do not change the number of observations.
func BenchmarkTransactionVerificationFlow(b *testing.B) {
	flowConfigureBackend(b)
	for _, fixture := range flowBenchmarkFixtures(b) {
		b.Run(fixture.name, func(b *testing.B) {
			for _, workers := range []int{2, 4} {
				b.Run(fmt.Sprintf("workers_%d", workers), func(b *testing.B) {
					for _, target := range []int{4, 8} {
						b.Run(fmt.Sprintf("target_%d", target), func(b *testing.B) {
							for _, scenario := range flowScenarios(fixture.blk) {
								b.Run(scenario.name, func(b *testing.B) {
									flowRunBenchmark(b, workers, target, fixture.blk, scenario)
								})
							}
						})
					}
				})
			}
		})
	}
}

type flowFixture struct {
	name string
	blk  *block.Block
}

type flowScenario struct {
	name        string
	components  [][]*solana.Transaction
	arrivalSpan time.Duration
	overlap     bool
}

func flowScenarios(blk *block.Block) []flowScenario {
	// 60 KiB is a benchmark parameter only. Include signed transaction bytes;
	// actual serialized entry/component overhead and shred recovery are omitted.
	components := flowComponents(blk.Transactions, 60*1024)
	scenarios := []flowScenario{
		{name: "catchup", components: [][]*solana.Transaction{blk.Transactions}},
		{name: "tip_200ms_after_complete", components: components, arrivalSpan: 200 * time.Millisecond},
		{name: "tip_200ms_overlap", components: components, arrivalSpan: 200 * time.Millisecond, overlap: true},
	}
	// Sparse components expose tail behavior without requiring microsecond
	// timer precision to deliver tens of thousands of tiny arrival events.
	for _, width := range []int{4, 7, 8} {
		txs := blk.Transactions[:min(256, len(blk.Transactions))]
		var small [][]*solana.Transaction
		for start := 0; start < len(txs); start += width {
			small = append(small, txs[start:min(start+width, len(txs))])
		}
		scenarios = append(scenarios, flowScenario{
			name: fmt.Sprintf("sparse_%dtx_200ms_overlap", width), components: small,
			arrivalSpan: 200 * time.Millisecond, overlap: true,
		})
	}
	return scenarios
}

func flowComponents(txs []*solana.Transaction, bytesPerComponent int) [][]*solana.Transaction {
	var components [][]*solana.Transaction
	start, size := 0, 0
	for i, tx := range txs {
		wireSize, err := txverify.TransactionWireSize(tx)
		if err != nil {
			panic(err) // fixtures are validated before entering this helper
		}
		if i > start && size+wireSize > bytesPerComponent {
			components = append(components, txs[start:i])
			start, size = i, 0
		}
		size += wireSize
	}
	if start < len(txs) {
		components = append(components, txs[start:])
	}
	return components
}

type flowObservation struct {
	latencies []time.Duration
	submits   []time.Duration
	feedLags  []time.Duration
	residual  time.Duration
	err       error
}

func flowArrivalOffset(index, count int, span time.Duration) time.Duration {
	if count <= 1 {
		return span
	}
	return time.Duration(int64(span) * int64(index) / int64(count-1))
}

func flowObserve(v *transactionVerifier, blk *block.Block, scenario flowScenario) flowObservation {
	observation := flowObservation{
		latencies: make([]time.Duration, len(scenario.components)),
		submits:   make([]time.Duration, len(scenario.components)),
		feedLags:  make([]time.Duration, len(scenario.components)),
	}
	started := time.Now()
	finalArrival := started.Add(scenario.arrivalSpan)
	if !scenario.overlap {
		time.Sleep(time.Until(finalArrival))
		submitStarted := time.Now()
		future, err := v.submitTransactions(context.Background(), blk.Transactions)
		submitDuration := time.Since(submitStarted)
		if err != nil {
			observation.err = err
			return observation
		}
		_, observation.err = future.wait()
		finished := future.finishedAt
		for i := range observation.latencies {
			available := started.Add(flowArrivalOffset(i, len(scenario.components), scenario.arrivalSpan))
			observation.latencies[i] = finished.Sub(available)
			observation.submits[i] = submitDuration
			observation.feedLags[i] = submitStarted.Sub(available)
		}
		observation.residual = max(0, finished.Sub(finalArrival))
		return observation
	}

	var waiters sync.WaitGroup
	errs := make([]error, len(scenario.components))
	finished := make([]time.Time, len(scenario.components))
	for i, txs := range scenario.components {
		available := started.Add(flowArrivalOffset(i, len(scenario.components), scenario.arrivalSpan))
		time.Sleep(time.Until(available))
		submitStarted := time.Now()
		observation.feedLags[i] = submitStarted.Sub(available)
		future, err := v.submitTransactions(context.Background(), txs)
		observation.submits[i] = time.Since(submitStarted)
		if err != nil {
			errs[i] = err
			break
		}
		waiters.Add(1)
		go func() {
			defer waiters.Done()
			_, errs[i] = future.wait()
			finished[i] = future.finishedAt
			observation.latencies[i] = finished[i].Sub(available)
		}()
	}
	waiters.Wait()
	for _, completed := range finished {
		observation.residual = max(observation.residual, completed.Sub(finalArrival))
	}
	for _, err := range errs {
		if err != nil {
			observation.err = err
			break
		}
	}
	return observation
}

func flowRunBenchmark(b *testing.B, workers, target int, blk *block.Block, scenario flowScenario) {
	v := newTransactionVerifierWithBatchTarget(workers, 2*workers*8, target, nil)
	defer v.closeAndWait()
	if err := v.verifyBlock(blk); err != nil {
		b.Fatal(err)
	}
	flowBenchmarkGate(b)
	var signatureCount int
	for _, component := range scenario.components {
		for _, tx := range component {
			signatureCount += len(tx.Signatures)
		}
	}
	var latencies, submits, feedLags, residuals []time.Duration
	before := sigverify.Stats()
	cpuBefore := flowCPUSeconds(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		observation := flowObserve(v, blk, scenario)
		if observation.err != nil {
			b.Fatal(observation.err)
		}
		latencies = append(latencies, observation.latencies...)
		submits = append(submits, observation.submits...)
		feedLags = append(feedLags, observation.feedLags...)
		residuals = append(residuals, observation.residual)
	}
	b.StopTimer()
	cpuSeconds := flowCPUSeconds(b) - cpuBefore
	after := sigverify.Stats()
	b.ReportMetric(1000*cpuSeconds/float64(b.N), "cpu-ms/block")
	b.ReportMetric(cpuSeconds/b.Elapsed().Seconds(), "avg_cpu_cores")
	b.ReportMetric(float64(b.N*signatureCount)/b.Elapsed().Seconds(), "signatures/s")
	b.ReportMetric(float64(signatureCount), "signatures/block")
	b.ReportMetric(float64(len(scenario.components)), "components/block")
	b.ReportMetric(float64(after.Signatures-before.Signatures)/float64(after.Batches-before.Batches), "mean_width")
	flowReportPercentiles(b, latencies, "ready")
	flowReportPercentiles(b, submits, "submit")
	flowReportPercentiles(b, feedLags, "feed_lag")
	flowReportPercentiles(b, residuals, "residual")
	if after.InternalFaultFallbacks != before.InternalFaultFallbacks {
		b.Fatal("signature verifier used an internal fault fallback")
	}
	if want := uint64(b.N * signatureCount); after.Signatures-before.Signatures != want {
		b.Fatalf("verified signature count = %d, want %d", after.Signatures-before.Signatures, want)
	}
}

// The optional rendezvous is outside the timer. An external contention runner
// starts the real execution probe after seeing READY, then creates START. Both
// paths are explicit files in that runner's output directory. This is only for
// coordination; ordinary benchmark invocations do no filesystem polling.
func flowBenchmarkGate(b *testing.B) {
	b.Helper()
	ready, start := os.Getenv("MITHRIL_SIGVERIFY_FLOW_READY"), os.Getenv("MITHRIL_SIGVERIFY_FLOW_START")
	if ready == "" && start == "" {
		return
	}
	if ready == "" || start == "" {
		b.Fatal("set both MITHRIL_SIGVERIFY_FLOW_READY and MITHRIL_SIGVERIFY_FLOW_START")
	}
	// Go's first N=1 calibration and warmup must finish before the external
	// execution probe starts. The runner selects a fixed N greater than one.
	if b.N == 1 {
		return
	}
	if err := os.WriteFile(ready, []byte(b.Name()+"\n"), 0600); err != nil {
		b.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, err := os.Stat(start); err == nil {
			return
		} else if !os.IsNotExist(err) {
			b.Fatal(err)
		}
		if time.Now().After(deadline) {
			b.Fatal("contention runner did not release benchmark within 30 seconds")
		}
		time.Sleep(time.Millisecond)
	}
}

func flowReportPercentiles(b *testing.B, values []time.Duration, prefix string) {
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	for _, percentile := range []int{50, 95} {
		index := max(0, (len(values)*percentile+99)/100-1)
		b.ReportMetric(float64(values[index])/float64(time.Millisecond), fmt.Sprintf("%s_p%d-ms", prefix, percentile))
	}
}

func flowCPUSeconds(tb testing.TB) float64 {
	tb.Helper()
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		tb.Fatal(err)
	}
	return float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6
}

var flowBackendOnce sync.Once
var flowBackendError error

func flowConfigureBackend(tb testing.TB) {
	tb.Helper()
	flowBackendOnce.Do(func() {
		if backend := os.Getenv("MITHRIL_SIGVERIFY_FLOW_BACKEND"); backend != "" {
			_, flowBackendError = sigverify.Configure(sigverify.Config{Backend: backend})
		}
	})
	if flowBackendError != nil {
		tb.Fatal(flowBackendError)
	}
}

func flowBenchmarkFixtures(tb testing.TB) []flowFixture {
	tb.Helper()
	count := 33760
	limit := false
	if value := os.Getenv("MITHRIL_SIGVERIFY_FLOW_COUNT"); value != "" {
		var err error
		count, err = strconv.Atoi(value)
		if err != nil || count < 1 {
			tb.Fatal("MITHRIL_SIGVERIFY_FLOW_COUNT must be a positive integer")
		}
		limit = true
	}
	if dir := os.Getenv("MITHRIL_SIGVERIFY_FLOW_FIXTURES"); dir != "" {
		paths, err := filepath.Glob(filepath.Join(dir, "block-*.json"))
		if err != nil || len(paths) == 0 {
			tb.Fatalf("captured fixtures: %v, files=%d", err, len(paths))
		}
		var fixtures []flowFixture
		for _, path := range paths {
			data, err := os.ReadFile(path)
			if err != nil {
				tb.Fatal(err)
			}
			var captured struct {
				Slot         uint64
				Transactions []string
			}
			if err := json.Unmarshal(data, &captured); err != nil {
				tb.Fatal(err)
			}
			blk := &block.Block{Slot: captured.Slot}
			for i, encoded := range captured.Transactions {
				if limit && i == count {
					break
				}
				wire, err := base64.StdEncoding.DecodeString(encoded)
				if err != nil {
					tb.Fatal(err)
				}
				tx, err := solana.TransactionFromBytes(wire)
				if err != nil {
					tb.Fatal(err)
				}
				if err := txverify.SanitizeTransaction(tx); err != nil {
					tb.Fatal(err)
				}
				blk.Transactions = append(blk.Transactions, tx)
			}
			if len(blk.Transactions) == 0 {
				tb.Fatalf("fixture %s contains no transactions", path)
			}
			fixtures = append(fixtures, flowFixture{fmt.Sprintf("captured_%d", blk.Slot), blk})
		}
		return fixtures
	}
	var fixtures []flowFixture
	for _, wireSize := range []int{228, txverify.MaxLegacyTransactionSize} {
		blk := &block.Block{Slot: 1, Transactions: make([]*solana.Transaction, count)}
		for i := range blk.Transactions {
			blk.Transactions[i] = flowGeneratedTransaction(tb, wireSize, uint64(i))
		}
		fixtures = append(fixtures, flowFixture{fmt.Sprintf("generated_%dB", wireSize), blk})
	}
	return fixtures
}

func flowGeneratedTransaction(tb testing.TB, wireSize int, index uint64) *solana.Transaction {
	tb.Helper()
	// Public, deterministic benchmark material, never a validator identity.
	var seed [ed25519.SeedSize]byte
	binary.LittleEndian.PutUint64(seed[:], index+1)
	private := ed25519.NewKeyFromSeed(seed[:])
	public := solana.PublicKeyFromBytes(private.Public().(ed25519.PublicKey))
	tx := &solana.Transaction{
		Signatures: make([]solana.Signature, 1),
		Message: solana.Message{
			Header:       solana.MessageHeader{NumRequiredSignatures: 1, NumReadonlyUnsignedAccounts: 1},
			AccountKeys:  []solana.PublicKey{public, solana.MemoProgramID},
			Instructions: []solana.CompiledInstruction{{ProgramIDIndex: 1, Accounts: []uint16{0}}},
		},
	}
	binary.LittleEndian.PutUint64(tx.Message.RecentBlockhash[:], index+1)
	baseSize, err := txverify.TransactionWireSize(tx)
	if err != nil {
		tb.Fatal(err)
	}
	padding := wireSize - baseSize
	for attempts := 0; attempts < 3 && padding >= 0; attempts++ {
		tx.Message.Instructions[0].Data = make([]byte, padding)
		size, err := txverify.TransactionWireSize(tx)
		if err != nil {
			tb.Fatal(err)
		}
		if size != wireSize {
			padding += wireSize - size
			continue
		}
		for i := range tx.Message.Instructions[0].Data {
			tx.Message.Instructions[0].Data[i] = 'a'
		}
		message, err := txverify.MessageBytes(tx)
		if err != nil {
			tb.Fatal(err)
		}
		copy(tx.Signatures[0][:], ed25519.Sign(private, message))
		if err := txverify.SanitizeTransaction(tx); err != nil {
			tb.Fatal(err)
		}
		return tx
	}
	tb.Fatalf("cannot construct a %d-byte transaction", wireSize)
	return nil
}

func TestTransactionVerificationFlowFixtureShape(t *testing.T) {
	for _, size := range []int{228, 1232} {
		first := flowGeneratedTransaction(t, size, 0)
		second := flowGeneratedTransaction(t, size, 1)
		for _, tx := range []*solana.Transaction{first, second} {
			wire, err := tx.MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			if len(wire) != size || len(tx.Signatures) != 1 {
				t.Fatalf("fixture wire=%d signatures=%d, want %d bytes and one signature", len(wire), len(tx.Signatures), size)
			}
			if err := txverify.VerifyTransaction(tx); err != nil {
				t.Fatal(err)
			}
		}
		if first.Signatures[0] == second.Signatures[0] || first.Message.AccountKeys[0] == second.Message.AccountKeys[0] {
			t.Fatal("different fixture indices reused a message signature or signer")
		}
	}
}

func TestTransactionVerificationFlowComponentBoundaries(t *testing.T) {
	txs := make([]*solana.Transaction, 7)
	for i := range txs {
		txs[i] = flowGeneratedTransaction(t, 228, uint64(i))
	}
	components := flowComponents(txs, 500)
	if len(components) != 4 {
		t.Fatalf("got %d components, want four", len(components))
	}
	next := 0
	for i, component := range components {
		want := 2
		if i == 3 {
			want = 1
		}
		if len(component) != want {
			t.Fatalf("component %d contains %d transactions, want %d", i, len(component), want)
		}
		for _, tx := range component {
			if tx != txs[next] {
				t.Fatal("component split changed transaction order or identity")
			}
			next++
		}
	}
	if flowArrivalOffset(0, 4, 200*time.Millisecond) != 0 || flowArrivalOffset(3, 4, 200*time.Millisecond) != 200*time.Millisecond {
		t.Fatal("availability schedule does not span the requested interval")
	}
}
