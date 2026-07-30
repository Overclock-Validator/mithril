// repair-sim runs deterministic, single-node Turbine repair scenarios.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/turbine/repairsim"
)

type environment struct {
	GoVersion string `json:"go_version"`
	GOOS      string `json:"goos"`
	GOARCH    string `json:"goarch"`
	CPU       string `json:"cpu"`
}

type report struct {
	Environment          environment            `json:"environment"`
	Ledger               repairsim.LedgerConfig `json:"ledger"`
	Network              repairsim.Config       `json:"network"`
	LedgerGenerationWall time.Duration          `json:"ledger_generation_wall_ns"`
	Result               repairsim.Result       `json:"result"`
}

func main() {
	var (
		scenarioFlag = flag.String("scenario", string(repairsim.ScenarioNearTip), "near-tip or deep-catchup")
		slots        = flag.Int("slots", 200, "number of deterministic slots")
		fecSets      = flag.Int("fec-sets", 4, "FEC sets generated per slot")
		entries      = flag.Int("entries", 0, "entries per slot (0 derives an exact FEC count)")
		seed         = flag.Int64("seed", 1, "deterministic content and network seed")
		availability = flag.String("availability", "", "complete, near-loss, sparse, or mixed")
		repair       = flag.Bool("repair", true, "enable repair requests")
		latency      = flag.Duration("repair-latency", 20*time.Millisecond, "synthetic one-way response latency")
		jitter       = flag.Duration("repair-jitter", 2*time.Millisecond, "deterministic +/- response jitter")
		loss         = flag.Float64("packet-loss", 0, "repair response loss probability [0,1]")
		duplicates   = flag.Float64("duplicates", 0.02, "duplicate response probability [0,1]")
		bandwidth    = flag.Int64("repair-bandwidth", 100*1024*1024, "synthetic repair bytes/sec (0 is unlimited)")
		concurrent   = flag.Int("max-concurrent", 256, "maximum outstanding repair shreds")
		corrupt      = flag.Int("corrupt-responses", 0, "corrupt the first N repair responses")
		naturalLate  = flag.Bool("natural-late", true, "schedule selected late live shreds during repair")
		spoolDir     = flag.String("spool-dir", "", "persistent shred-spool directory (empty uses a temporary directory)")
		output       = flag.String("output", "", "write JSON to this file instead of stdout")
		includeTrace = flag.Bool("trace", true, "include the logical event trace in JSON")
	)
	flag.Parse()

	scenario := repairsim.Scenario(*scenarioFlag)
	network := repairsim.DefaultConfig(scenario)
	network.Availability = repairsim.Availability(*availability)
	network.RepairEnabled = *repair
	network.RepairLatency = *latency
	network.RepairJitter = *jitter
	network.PacketLoss = *loss
	network.DuplicateProbability = *duplicates
	network.BandwidthBytesPerSec = *bandwidth
	network.MaxConcurrent = *concurrent
	network.CorruptResponses = *corrupt
	network.NaturalLateShreds = *naturalLate
	network.CollectTrace = *includeTrace
	network.Seed = *seed
	network.SpoolDir = *spoolDir
	if network.Availability == "" {
		network.Availability = repairsim.DefaultConfig(scenario).Availability
	}

	ledgerCfg := repairsim.LedgerConfig{
		StartSlot:      10_000,
		Slots:          *slots,
		FECsPerSlot:    *fecSets,
		EntriesPerSlot: *entries,
		Seed:           *seed,
		ShredVersion:   1,
		ReferenceTick:  63,
	}
	started := time.Now()
	ledger, err := repairsim.GenerateLedger(ledgerCfg)
	if err != nil {
		fatalf("generate ledger: %v", err)
	}
	generationWall := time.Since(started)
	result, err := repairsim.Run(ledger, network)
	if err != nil {
		fatalf("run simulation: %v", err)
	}
	report := report{
		Environment: environment{GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, CPU: cpuModel()},
		Ledger:      ledger.Config, Network: network, LedgerGenerationWall: generationWall, Result: result,
	}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fatalf("marshal report: %v", err)
	}
	encoded = append(encoded, '\n')
	if *output == "" {
		_, _ = os.Stdout.Write(encoded)
		return
	}
	if err := os.WriteFile(*output, encoded, 0o644); err != nil {
		fatalf("write %s: %v", *output, err)
	}
}

func cpuModel() string {
	if runtime.GOOS == "linux" {
		if data, err := os.ReadFile("/proc/cpuinfo"); err == nil {
			for _, line := range strings.Split(string(data), "\n") {
				if key, value, ok := strings.Cut(line, ":"); ok && strings.TrimSpace(key) == "model name" {
					return strings.TrimSpace(value)
				}
			}
		}
	}
	if runtime.GOOS == "darwin" {
		if out, err := exec.Command("sysctl", "-n", "machdep.cpu.brand_string").Output(); err == nil {
			return strings.TrimSpace(string(out))
		}
	}
	return "unknown"
}

func fatalf(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
