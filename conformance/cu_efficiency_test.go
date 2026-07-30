package conformance

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	sealevelPkg "github.com/Overclock-Validator/mithril/pkg/sealevel"
	"google.golang.org/protobuf/proto"
)

// Nanoseconds of real execution per compute unit charged, measured across the
// conformance corpus.
//
// Why this metric. Compute units are the network's model of how expensive an
// operation is; wall-clock time is how expensive it actually is for us. Where
// those disagree we have either an optimisation target (we are slow at
// something the network considers cheap) or, in the extreme, an asymmetry an
// adversary can lean on: a transaction that buys a lot of our real time for
// few compute units. Both are found the same way, by ranking ns/CU.
//
// The absolute numbers are not comparable to Agave -- different language,
// different machine. What is meaningful is the *spread within our own corpus*:
// a handler at ten times the corpus median is slow relative to how we price
// everything else we do, whatever the baseline.
//
// Run with:
//
//	MITHRIL_CU_EFFICIENCY=1 go test ./conformance/ -run TestCUEfficiency -v -timeout 30m
//
// Off by default: it executes the corpus several times over and is a
// measurement tool, not a correctness gate.

const (
	// cuEffReps is how many times each fixture is executed. The minimum across
	// repetitions is kept rather than the mean, because scheduling noise and
	// page faults only ever add time; the fastest observation is the closest
	// to the true cost.
	cuEffReps = 3

	// cuEffMinCU filters the per-fixture leaderboard. A fixture that consumes a
	// handful of compute units has a ratio dominated by fixed setup cost and
	// timer granularity, which produces enormous meaningless ns/CU values.
	// Aggregates are unaffected: they divide summed time by summed CU, so small
	// fixtures contribute in proportion to their size rather than as outliers.
	cuEffMinCU = 500
)

// cuEffLimit caps how many fixtures are measured per suite, from
// MITHRIL_CU_EFFICIENCY_LIMIT. The full corpus is ~22,000 fixtures and each is
// executed cuEffReps times, so a complete pass takes a while; a limited pass
// answers "which handlers look expensive" in a fraction of the time.
//
// Sampling is by directory order, which is stable but arbitrary -- fine for
// ranking handlers, not for claiming corpus-wide totals. The report says which
// mode produced it so a limited run is never mistaken for a full one.
func cuEffLimit() int {
	raw := os.Getenv("MITHRIL_CU_EFFICIENCY_LIMIT")
	if raw == "" {
		return 0
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 0
	}
	return n
}

// cuEffSuites selects which suites to measure, from a comma-separated
// MITHRIL_CU_EFFICIENCY_SUITES. Empty means all of them.
//
// This exists because the aggregate is a poor A/B instrument when a change
// only touches one suite. Measuring an sBPF interpreter change over all four
// suites dilutes it against native-program handlers that cannot have moved, and
// the untouched suites still contribute their own run-to-run variance to the
// total -- so the number gets both smaller and noisier than the real effect.
// Selecting the affected suite gives attribution the whole-corpus total cannot.
//
// It does not replace the full run. Use the whole corpus to find what is slow;
// use one suite to measure whether a change to it worked.
func cuEffSuites() map[string]bool {
	raw := strings.TrimSpace(os.Getenv("MITHRIL_CU_EFFICIENCY_SUITES"))
	if raw == "" {
		return nil
	}
	selected := make(map[string]bool)
	for _, name := range strings.Split(raw, ",") {
		if name = strings.TrimSpace(name); name != "" {
			selected[name] = true
		}
	}
	if len(selected) == 0 {
		return nil
	}
	return selected
}

type cuEffSample struct {
	suite     string
	fixture   string
	instrCode int32
	ns        int64
	cu        uint64
}

// cuEffPanic records a fixture that panicked. A corpus tool has to survive
// these rather than abort on the first one -- otherwise a single bad fixture
// hides every measurement after it, which is the same masking that has already
// bitten this suite twice. Collecting them is also useful in its own right:
// a panic reachable from fixture input is a robustness bug worth its own fix.
type cuEffPanic struct {
	suite   string
	fixture string
	value   string
}

// runFixtureSafely isolates one execution so a panic becomes a reported result
// instead of a dead test binary.
func runFixtureSafely(run cuEffRunner, fixture *InstrFixture) (ns int64, cu uint64, ok bool, panicked string) {
	defer func() {
		if r := recover(); r != nil {
			ok = false
			panicked = fmt.Sprintf("%v", r)
		}
	}()
	ns, cu, ok = run(fixture)
	return ns, cu, ok, ""
}

func (s cuEffSample) nsPerCU() float64 {
	if s.cu == 0 {
		return 0
	}
	return float64(s.ns) / float64(s.cu)
}

type cuEffGroup struct {
	key     string
	count   int
	totalNS int64
	totalCU uint64
	ratios  []float64
}

// aggregate divides summed time by summed CU rather than averaging per-fixture
// ratios. Averaging ratios would weight a 100-CU fixture the same as a
// 100,000-CU one and let noise at the small end dominate the answer.
func (g *cuEffGroup) aggregate() float64 {
	if g.totalCU == 0 {
		return 0
	}
	return float64(g.totalNS) / float64(g.totalCU)
}

func (g *cuEffGroup) median() float64 {
	if len(g.ratios) == 0 {
		return 0
	}
	sorted := append([]float64(nil), g.ratios...)
	sort.Float64s(sorted)
	return sorted[len(sorted)/2]
}

// cuEffRunner executes one fixture and reports elapsed time and CU consumed.
// Each suite supplies its own, because the harness setup differs; the contract
// is that setup happens outside the timed region and only the instruction is
// measured.
type cuEffRunner func(fixture *InstrFixture) (ns int64, cu uint64, ok bool)

// runVMProgramFixture times a vm-programs fixture. The execution context is
// rebuilt for every repetition -- execution mutates it, so a second run against
// the same context would measure something else entirely -- but that rebuild
// sits outside the clock.
func runVMProgramFixture(fixture *InstrFixture) (int64, uint64, bool) {
	best := int64(-1)
	var used uint64

	for rep := 0; rep < cuEffReps; rep++ {
		var execCtx *sealevelPkg.ExecutionCtx
		var instrAccts []sealevelPkg.InstructionAccount
		var programIndices []uint64
		var setupErr error

		withoutConformanceStdout(func() {
			execCtx, instrAccts, programIndices, setupErr = newVMProgramExecCtxAndInstrAccts(fixture)
		})
		if setupErr != nil || execCtx == nil {
			return 0, 0, false
		}

		var elapsed time.Duration
		withoutConformanceStdout(func() {
			start := time.Now()
			_ = execCtx.ProcessInstruction(fixture.Input.Data, instrAccts, programIndices)
			elapsed = time.Since(start)
		})

		if best < 0 || elapsed.Nanoseconds() < best {
			best = elapsed.Nanoseconds()
			used = execCtx.ComputeMeter.Used()
		}
	}

	if best < 0 {
		return 0, 0, false
	}
	return best, used, true
}

// runInstrFixture times a fixture from one of the instruction suites, which
// share newExecCtxAndInstrAcctsFromFixture.
func runInstrFixture(fixture *InstrFixture) (int64, uint64, bool) {
	best := int64(-1)
	var used uint64

	for rep := 0; rep < cuEffReps; rep++ {
		var execCtx *sealevelPkg.ExecutionCtx
		var instrAccts []sealevelPkg.InstructionAccount

		withoutConformanceStdout(func() {
			execCtx, instrAccts = newExecCtxAndInstrAcctsFromFixture(fixture)
		})
		if execCtx == nil {
			return 0, 0, false
		}

		var elapsed time.Duration
		withoutConformanceStdout(func() {
			start := time.Now()
			_ = execCtx.ProcessInstruction(fixture.Input.Data, instrAccts, []uint64{0})
			elapsed = time.Since(start)
		})

		if best < 0 || elapsed.Nanoseconds() < best {
			best = elapsed.Nanoseconds()
			used = execCtx.ComputeMeter.Used()
		}
	}

	if best < 0 {
		return 0, 0, false
	}
	return best, used, true
}

func cuEffInstrCode(fixture *InstrFixture) int32 {
	data := fixture.GetInput().GetData()
	if len(data) < 4 {
		return -1
	}
	return int32(uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24)
}

// collectCUEffSamples walks one fixture directory and times every fixture in it.
func collectCUEffSamples(t *testing.T, suite, basePath string, run cuEffRunner) ([]cuEffSample, []cuEffPanic) {
	t.Helper()

	entries, err := os.ReadDir(basePath)
	if err != nil {
		t.Logf("suite %s: not present in the pinned corpus, skipping (%v)", suite, err)
		return nil, nil
	}

	var samples []cuEffSample
	var panics []cuEffPanic
	var skippedSetup, skippedZeroCU int
	limit := cuEffLimit()

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if limit > 0 && len(samples) >= limit {
			break
		}
		path := filepath.Join(basePath, entry.Name())

		raw, err := os.ReadFile(path)
		if err != nil {
			skippedSetup++
			continue
		}
		fixture := &InstrFixture{}
		if err := proto.Unmarshal(raw, fixture); err != nil {
			skippedSetup++
			continue
		}
		if fixture.GetInput() == nil {
			skippedSetup++
			continue
		}

		ns, used, ok, panicked := runFixtureSafely(run, fixture)
		if panicked != "" {
			panics = append(panics, cuEffPanic{suite: suite, fixture: entry.Name(), value: panicked})
			continue
		}
		if !ok {
			skippedSetup++
			continue
		}
		// A fixture that consumed nothing has no ratio to report. These are
		// mostly instructions rejected before the meter is touched.
		if used == 0 {
			skippedZeroCU++
			continue
		}

		samples = append(samples, cuEffSample{
			suite:     suite,
			fixture:   entry.Name(),
			instrCode: cuEffInstrCode(fixture),
			ns:        ns,
			cu:        used,
		})
	}

	t.Logf("suite %s: %d measured, %d unrunnable, %d consumed no CU, %d panicked",
		suite, len(samples), skippedSetup, skippedZeroCU, len(panics))
	return samples, panics
}

// cuEffDump writes every sample to MITHRIL_CU_EFFICIENCY_DUMP as TSV so two
// runs can be paired by (suite, fixture) offline.
//
// Why bother, when the report already prints a corpus total. Comparing two
// builds by their totals throws away almost all the data: one run yields a
// single number, so separating a few-percent change takes many repeated pairs,
// and the run-to-run spread it fights is dominated by fixtures being wildly
// different sizes rather than by the change under test.
//
// Pairing per fixture removes that. Every fixture is compared against itself
// under the other build, so fixture-size variation cancels instead of being
// averaged over, and one pair of runs yields thousands of paired observations
// instead of one. Take the median of the per-fixture relative deltas and a
// distribution-free interval around it -- a sign test or a bootstrap over
// fixtures -- rather than a mean, which the long tail would dominate.
//
// Filter to fixtures worth timing before drawing conclusions: below roughly
// cuEffMinCU the per-fixture number is mostly timer granularity, and including
// those adds noise without adding signal.
func cuEffDump(t *testing.T, samples []cuEffSample) {
	path := strings.TrimSpace(os.Getenv("MITHRIL_CU_EFFICIENCY_DUMP"))
	if path == "" {
		return
	}
	var b strings.Builder
	b.WriteString("suite\tfixture\tinstr\tns\tcu\n")
	for _, s := range samples {
		fmt.Fprintf(&b, "%s\t%s\t%d\t%d\t%d\n", s.suite, s.fixture, s.instrCode, s.ns, s.cu)
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatalf("cu efficiency dump: %v", err)
	}
	t.Logf("wrote %d samples to %s", len(samples), path)
}

func TestCUEfficiency(t *testing.T) {
	if os.Getenv("MITHRIL_CU_EFFICIENCY") == "" {
		t.Skip("set MITHRIL_CU_EFFICIENCY=1 to run the compute-unit efficiency measurement")
	}

	suites := []struct {
		name string
		path string
		run  cuEffRunner
	}{
		{"vm-programs", "test-vectors/instr/fixtures/vm-programs", runVMProgramFixture},
		{"bpf-loader-v3", "test-vectors/instr/fixtures/bpf-loader-v3", runInstrFixture},
		{"system", "test-vectors/instr/fixtures/system", runInstrFixture},
		{"vote", "test-vectors/instr/fixtures/vote", runInstrFixture},
	}

	selected := cuEffSuites()
	if selected != nil {
		known := make(map[string]bool, len(suites))
		for _, s := range suites {
			known[s.name] = true
		}
		for name := range selected {
			if !known[name] {
				t.Fatalf("unknown suite %q in MITHRIL_CU_EFFICIENCY_SUITES; known: vm-programs, bpf-loader-v3, system, vote", name)
			}
		}
	}

	var all []cuEffSample
	var allPanics []cuEffPanic
	var ran []string
	for _, s := range suites {
		if selected != nil && !selected[s.name] {
			continue
		}
		ran = append(ran, s.name)
		samples, panics := collectCUEffSamples(t, s.name, s.path, s.run)
		all = append(all, samples...)
		allPanics = append(allPanics, panics...)
	}
	if len(all) == 0 {
		t.Skip("no fixtures measured; run `make conformance-vectors` first")
	}

	cuEffDump(t, all)

	var corpusNS int64
	var corpusCU uint64
	for _, s := range all {
		corpusNS += s.ns
		corpusCU += s.cu
	}
	baseline := float64(corpusNS) / float64(corpusCU)

	t.Logf("")
	t.Logf("=== corpus baseline ===")
	if n := cuEffLimit(); n > 0 {
		t.Logf("LIMITED RUN: at most %d fixtures per suite; totals are not corpus-wide", n)
	}
	if selected != nil {
		t.Logf("SUITE SUBSET: %s only; totals cover these suites, not the corpus", strings.Join(ran, ", "))
	}
	t.Logf("%d fixtures, %.3f ms total execution, %d CU total", len(all), float64(corpusNS)/1e6, corpusCU)
	t.Logf("baseline: %.1f ns/CU", baseline)
	t.Logf("")

	// Grouped by suite and instruction code, but only where the code identifies
	// a handler. In the native instruction suites it does. In vm-programs the
	// first four bytes are the invoked program's own discriminant, so grouping
	// on it yields thousands of singletons that crowd out every real signal --
	// vm-programs is therefore kept as one bucket and its detail comes from the
	// per-fixture table below.
	groups := map[string]*cuEffGroup{}
	for _, s := range all {
		key := fmt.Sprintf("%s/instr=%d", s.suite, s.instrCode)
		if s.suite == "vm-programs" {
			key = s.suite
		}
		g := groups[key]
		if g == nil {
			g = &cuEffGroup{key: key}
			groups[key] = g
		}
		g.count++
		g.totalNS += s.ns
		g.totalCU += s.cu
		g.ratios = append(g.ratios, s.nsPerCU())
	}

	// Singleton groups are noise: one fixture's ratio is one sample, and the
	// per-fixture table already covers individual outliers. Groups are for
	// spotting a handler that is consistently expensive.
	const minGroupCount = 3
	ordered := make([]*cuEffGroup, 0, len(groups))
	for _, g := range groups {
		if g.count < minGroupCount {
			continue
		}
		ordered = append(ordered, g)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].aggregate() > ordered[j].aggregate() })

	t.Logf("=== by suite and instruction code, worst first ===")
	t.Logf("%-34s %7s %10s %12s %9s %9s", "group", "count", "ns/CU", "vs baseline", "median", "total ms")
	for _, g := range ordered {
		t.Logf("%-34s %7d %10.1f %11.1fx %9.1f %9.2f",
			g.key, g.count, g.aggregate(), g.aggregate()/baseline, g.median(), float64(g.totalNS)/1e6)
	}
	t.Logf("")

	// Individual fixtures, filtered to those large enough for the ratio to
	// mean something. These are the concrete reproducers to profile.
	var big []cuEffSample
	for _, s := range all {
		if s.cu >= cuEffMinCU {
			big = append(big, s)
		}
	}
	sort.Slice(big, func(i, j int) bool { return big[i].nsPerCU() > big[j].nsPerCU() })

	limit := 25
	if len(big) < limit {
		limit = len(big)
	}
	t.Logf("=== worst individual fixtures (>= %d CU), worst first ===", cuEffMinCU)
	t.Logf("%-12s %10s %10s %10s %11s  %s", "suite", "ns", "CU", "ns/CU", "vs baseline", "fixture")
	for _, s := range big[:limit] {
		t.Logf("%-12s %10d %10d %10.1f %10.1fx  %s",
			s.suite, s.ns, s.cu, s.nsPerCU(), s.nsPerCU()/baseline, s.fixture)
	}

	if len(allPanics) > 0 {
		t.Logf("")
		t.Logf("=== fixtures that panicked (%d) ===", len(allPanics))
		byValue := map[string][]cuEffPanic{}
		for _, p := range allPanics {
			byValue[p.value] = append(byValue[p.value], p)
		}
		for value, group := range byValue {
			t.Logf("%dx %s", len(group), value)
			t.Logf("    e.g. %s/%s", group[0].suite, group[0].fixture)
		}
	}
}
