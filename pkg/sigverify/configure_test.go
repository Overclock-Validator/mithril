package sigverify

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Backend selection is one-shot per process, by design: the library pins its
// backend on first use so the key cache can never hold tables in two formats.
// Every case here therefore needs its own process. Running them in-process
// would let the first Configure win and turn the rest into vacuous passes,
// which is worse than not testing them.
const configureChildEnv = "MITHRIL_SIGVERIFY_CONFIGURE_CHILD"

// runConfigureChild re-executes this test binary and runs only the named
// subtest, with the child marker set.
func runConfigureChild(t *testing.T, name string) (string, error) {
	t.Helper()

	cmd := exec.Command(os.Args[0], "-test.run", "^"+name+"$", "-test.v")
	cmd.Env = append(os.Environ(), configureChildEnv+"=1")
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func inChild() bool { return os.Getenv(configureChildEnv) == "1" }

// TestConfigureBackends covers every accepted name plus a rejected one.
//
// r51 has no fallback by contract: on a CPU without AVX512-IFMA it must fail
// startup rather than quietly running the portable backend, because an operator
// who asked for the accelerated path needs to know they did not get it. The
// assertion is written to hold on both kinds of machine -- it accepts either
// "r51 resolved" or "a clear error", and rejects the one outcome that would be
// a bug, namely success while some other backend is active.
func TestConfigureBackends(t *testing.T) {
	cases := []struct {
		name    string
		backend string
	}{
		{"Auto", BackendAuto},
		{"R51", BackendR51},
		{"Generic", BackendGeneric},
		{"Stdlib", BackendStdlib},
		{"Empty", ""},
		{"Unknown", "definitely-not-a-backend"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			full := "TestConfigureBackends/" + tc.name
			if !inChild() {
				out, err := runConfigureChild(t, full)
				require.NoError(t, err, "child output:\n%s", out)
				require.Contains(t, out, "PASS")
				return
			}

			resolved, err := Configure(Config{Backend: tc.backend})

			switch tc.backend {
			case "definitely-not-a-backend":
				require.Error(t, err, "an unknown backend must be rejected")
				assert.Contains(t, err.Error(), "must be one of")
				assert.Empty(t, resolved)
				// A rejected name must not become visible anywhere.
				assert.Equal(t, BackendAuto, Cfg.Backend,
					"a rejected backend must not be published to Cfg")

			case BackendR51:
				if err != nil {
					// The only acceptable failure is "this CPU cannot do it".
					assert.Contains(t, err.Error(), BackendR51)
					return
				}
				assert.Equal(t, BackendR51, resolved,
					"r51 must not silently resolve to a different backend")

			case "", BackendAuto:
				require.NoError(t, err)
				assert.NotEmpty(t, resolved)
				assert.Equal(t, BackendAuto, Cfg.Backend,
					"an empty backend must default to auto")

			default:
				require.NoError(t, err)
				assert.Equal(t, tc.backend, resolved)
				assert.Equal(t, tc.backend, Cfg.Backend)
			}
		})
	}
}

// Configure is a startup-only operation. A second call must be refused rather
// than partially applied: the library has already pinned its backend, so a late
// switch would leave the process in a state neither caller asked for.
func TestConfigureRefusesASecondCall(t *testing.T) {
	if !inChild() {
		out, err := runConfigureChild(t, t.Name())
		require.NoError(t, err, "child output:\n%s", out)
		require.Contains(t, out, "PASS")
		return
	}

	first, err := Configure(Config{Backend: BackendGeneric})
	require.NoError(t, err)
	require.Equal(t, BackendGeneric, first)

	second, err := Configure(Config{Backend: BackendStdlib})
	require.Error(t, err, "a second Configure must be refused")
	assert.Empty(t, second)
	assert.Contains(t, err.Error(), "already configured")

	// The refusal must leave the first selection intact rather than half-applied.
	assert.Equal(t, BackendGeneric, Cfg.Backend)
	assert.Equal(t, BackendGeneric, Backend())
}

// Re-stating the same backend is still a second call, and is still refused.
// Treating it as a harmless no-op would make "Configure ran twice" invisible,
// and the second caller has no way to know its configuration was ignored.
func TestConfigureRefusesARepeatOfTheSameBackend(t *testing.T) {
	if !inChild() {
		out, err := runConfigureChild(t, t.Name())
		require.NoError(t, err, "child output:\n%s", out)
		require.Contains(t, out, "PASS")
		return
	}

	_, err := Configure(Config{Backend: BackendGeneric})
	require.NoError(t, err)

	_, err = Configure(Config{Backend: BackendGeneric})
	require.Error(t, err, "repeating the same backend is still a second call")
}

// A verification must work without Configure ever being called: not every entry
// point into the codebase runs node startup, and defaulting to no backend at all
// would fail closed in a way that looks like a signature problem.
func TestVerificationWorksWithoutConfigure(t *testing.T) {
	if !inChild() {
		out, err := runConfigureChild(t, t.Name())
		require.NoError(t, err, "child output:\n%s", out)
		require.Contains(t, out, "PASS")
		return
	}

	signed := makeSigned(t, 0, true)
	assert.True(t, VerifyOne(&signed.pub, signed.msg, signed.sig),
		"a valid signature must verify before Configure is called")
	assert.NotEmpty(t, Backend(), "some backend must be active by default")
}

// Guards the subprocess plumbing itself. If the child marker or the -test.run
// pattern ever stops matching, every test above would spawn a child that runs
// nothing, exit zero, and report a pass without asserting anything.
func TestConfigureChildPlumbingActuallyRuns(t *testing.T) {
	if inChild() {
		fmt.Println("child-marker-observed")
		return
	}

	out, err := runConfigureChild(t, t.Name())
	require.NoError(t, err, "child output:\n%s", out)
	require.Contains(t, out, "child-marker-observed",
		"the child did not execute the subtest; the harness is not testing anything")
	require.Equal(t, 1, strings.Count(out, "child-marker-observed"),
		"the -test.run pattern matched more than the intended subtest")
}
