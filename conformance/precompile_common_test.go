package conformance

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/gagliardetto/solana-go"
	"google.golang.org/protobuf/proto"
)

// Firedancer publishes every precompile fixture in one flat directory and
// distinguishes them by the program id inside the fixture, not by a per-program
// subdirectory. Filtering therefore has to parse each fixture rather than glob a
// path. An earlier revision of these tests read
// test-vectors/precompile/fixtures/<program>, a layout that no longer exists
// upstream, so they failed on a missing directory before running a single case.
const precompileFixtureDir = "test-vectors/instr/fixtures/precompile"

var (
	ed25519PrecompileProgram   = solana.MustPublicKeyFromBase58("Ed25519SigVerify111111111111111111111111111")
	secp256k1PrecompileProgram = solana.MustPublicKeyFromBase58("KeccakSecp256k11111111111111111111111111111")
	secp256r1PrecompileProgram = solana.MustPublicKeyFromBase58("Secp256r1SigVerify1111111111111111111111111")
)

// loadPrecompileFixtures returns every fixture whose instruction targets the
// given precompile. It skips rather than fails when the corpus is absent: the
// vectors are a multi-gigabyte external checkout that is deliberately
// gitignored, so a developer without them should not see a red build.
func loadPrecompileFixtures(t *testing.T, program solana.PublicKey) []string {
	t.Helper()

	entries, err := os.ReadDir(precompileFixtureDir)
	if err != nil {
		t.Skipf("conformance corpus not available at %s (%v); run 'make conformance-vectors' to fetch it",
			precompileFixtureDir, err)
	}

	var matched []string
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".fix" {
			continue
		}
		path := filepath.Join(precompileFixtureDir, entry.Name())
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read fixture %s: %v", path, err)
		}
		fixture := &InstrFixture{}
		if err := proto.Unmarshal(raw, fixture); err != nil {
			t.Fatalf("decode fixture %s: %v", path, err)
		}
		if fixture.Input == nil || !bytes.Equal(fixture.Input.ProgramId, program[:]) {
			continue
		}
		matched = append(matched, path)
	}

	if len(matched) == 0 {
		t.Skipf("no fixtures in %s target %s", precompileFixtureDir, program)
	}
	return matched
}

// runPrecompileFixtures executes each fixture and reports every disagreement
// rather than stopping at the first, so a run reports a rate instead of one
// arbitrary case.
func runPrecompileFixtures(t *testing.T, label string, paths []string) {
	t.Helper()

	verbose := os.Getenv("MITHRIL_CONFORMANCE_VERBOSE") != ""
	var failures []string

	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read fixture %s: %v", path, err)
		}
		fixture := &InstrFixture{}
		if err := proto.Unmarshal(raw, fixture); err != nil {
			t.Fatalf("decode fixture %s: %v", path, err)
		}

		execCtx, instrAccts := newExecCtxAndInstrAcctsFromFixture(fixture)
		if verbose {
			printFixtureInfo(fixture)
		}

		err = execCtx.ProcessInstruction(fixture.Input.Data, instrAccts, []uint64{0})

		switch {
		case err == nil && fixture.Output.Result != 0:
			failures = append(failures, fmt.Sprintf(
				"%s: we accepted, fixture expects error %d", path, fixture.Output.Result-1))
		case err != nil && fixture.Output.Result == 0:
			failures = append(failures, fmt.Sprintf(
				"%s: we returned %v, fixture expects success", path, err))
		}
	}

	for _, failure := range failures {
		t.Errorf("%s", failure)
	}
	t.Logf("%s: %d/%d fixtures matched", label, len(paths)-len(failures), len(paths))
}
