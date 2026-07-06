
// One-off probe: reports, for every program in a bench bundle, whether the
// JIT can compile it and why not. Run with:
//
//	BENCH_BUNDLE=/path/to/bundle go test -run TestJITProbe -v ./pkg/replay
package replay

import (
	"fmt"
	"os"
	"sort"
	"testing"

	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/Overclock-Validator/mithril/pkg/sbpf/jit"
	"github.com/Overclock-Validator/mithril/pkg/sbpf/loader"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

func TestJITProbe(t *testing.T) {
	dir := os.Getenv("BENCH_BUNDLE")
	if dir == "" {
		t.Skip("BENCH_BUNDLE not set")
	}
	manifest, err := ReadBenchManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	accts, err := readBenchAccounts(dir)
	if err != nil {
		t.Fatal(err)
	}
	byKey := make(map[solana.PublicKey]*accounts.Account, len(accts))
	for _, acct := range accts {
		byKey[acct.Key] = acct
	}

	slot := manifest.FirstSlot
	f := features.NewFeaturesDefault()
	for _, gate := range features.AllFeatureGates {
		acct, ok := byKey[gate.Address]
		if !ok || acct.Owner != a.FeatureAddr {
			continue
		}
		fa := features.UnmarshalFeatureAcct(acct.Data)
		if fa.ActivatedAt != nil && slot >= *fa.ActivatedAt {
			f.EnableFeature(gate, *fa.ActivatedAt)
		}
	}
	t.Logf("slot %d: VirtualAddressSpaceAdjustments active = %v", slot,
		f.IsActive(features.VirtualAddressSpaceAdjustments))
	t.Logf("slot %d: AccountDataDirectMapping active = %v", slot,
		f.IsActive(features.AccountDataDirectMapping))

	registry := sbpf.SyscallRegistry(func(u uint32) (sbpf.Syscall, bool) {
		return sealevel.Syscalls(f, false, u)
	})
	isSyscall := func(u uint32) bool {
		_, ok := registry(u)
		return ok
	}

	type result struct {
		key     solana.PublicKey
		version uint64
		insns   int
		gapsErr string
		flatErr string
	}
	var results []result
	counts := map[string]int{}

	for _, acct := range accts {
		if !acct.Executable {
			continue
		}
		var elf []byte
		switch acct.Owner {
		case a.BpfLoader2Addr, a.BpfLoaderDeprecatedAddr:
			elf = acct.Data
		case a.BpfLoaderUpgradeableAddr:
			st, err := sealevel.UnmarshalUpgradeableLoaderState(acct.Data)
			if err != nil || st.Type != sealevel.UpgradeableLoaderStateTypeProgram {
				continue
			}
			pd, ok := byKey[st.Program.ProgramDataAddress]
			if !ok || len(pd.Data) < 45 {
				continue
			}
			elf = pd.Data[45:]
		default:
			continue
		}

		ldr, err := loader.NewLoaderWithSyscalls(elf, registry, false, f)
		if err != nil {
			counts["loader: "+err.Error()]++
			continue
		}
		prog, err := ldr.Load()
		if err != nil {
			counts["load: "+err.Error()]++
			continue
		}
		if err := prog.Verify(); err != nil {
			counts["verify: "+err.Error()]++
			continue
		}

		gaps := !f.IsActive(features.VirtualAddressSpaceAdjustments) && prog.SbpfVersion.StackFrameGaps()
		errStr := func(err error) string {
			if err == nil {
				return "ok"
			}
			return err.Error()
		}
		_, gapsErr := jit.Compile(prog, gaps, isSyscall)
		_, flatErr := jit.Compile(prog, false, isSyscall)
		results = append(results, result{
			key:     acct.Key,
			version: uint64(prog.SbpfVersion.Version),
			insns:   len(prog.Text),
			gapsErr: errStr(gapsErr),
			flatErr: errStr(flatErr),
		})
	}

	okReal, okFlat := 0, 0
	for _, r := range results {
		if r.gapsErr == "ok" {
			okReal++
		}
		if r.flatErr == "ok" {
			okFlat++
		}
		counts["flat: "+r.flatErr]++
	}
	t.Logf("%d programs probed: %d compile with real stackGaps setting, %d with gaps disabled",
		len(results), okReal, okFlat)

	type kv struct {
		k string
		n int
	}
	var sorted []kv
	for k, n := range counts {
		sorted = append(sorted, kv{k, n})
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].n > sorted[j].n })
	for _, e := range sorted {
		t.Logf("%6d  %s", e.n, e.k)
	}

	// Per-program detail for the hot list from the last -program-stats run.
	hot := []string{
		"pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",
		"ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
		"EtrnLzgbS7nMMy5fbD42kXiUzGg8XQzJ972Xtk1cjWih",
		"LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",
		"BiSoNHVpsVZW2F7rx2eQ59yQwKxzU5NvBcmKshCSUypi",
		"JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",
		"cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG",
		"6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
		"pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ",
		"SV2EYYJyRz2YhfXwXnhNAevDEui5Q6yrfyo13WtupPF",
		"CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",
		"TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
		"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
	}
	byKeyStr := map[string]result{}
	for _, r := range results {
		byKeyStr[r.key.String()] = r
	}
	for i, k := range hot {
		r, ok := byKeyStr[k]
		if !ok {
			t.Logf("hot #%2d %s: NOT PROBED", i+1, k)
			continue
		}
		t.Logf("hot #%2d v%d %6d insns  real=[%s] flat=[%s]  %s",
			i+1, r.version, r.insns, r.gapsErr, r.flatErr, k)
	}
	fmt.Println()
}
