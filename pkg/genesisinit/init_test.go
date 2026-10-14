package genesisinit

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/genesis"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func genesisFixture(t *testing.T) *genesis.Genesis {
	t.Helper()
	key := func(n byte) string {
		var k solana.PublicKey
		for i := range k {
			k[i] = n
		}
		return k.String()
	}
	g, _, err := genesis.Build(context.Background(), genesis.Config{CreationTime: "2026-01-01T00:00:00Z", Validators: []genesis.Validator{{Identity: key(1), VoteAccount: key(2), StakeAccount: key(3), BLSPublicKey: "a55384b86c60c0e2a13ffdf9087c94495455083acf06ff23d8e7416ee1df1cafccfaad802b527ade7dfe1b02a6bfd79a", IdentityLamports: 500_000_000_000, VoteLamports: 2_000_000_000, StakeLamports: 10_000_000_000}}})
	require.NoError(t, err)
	return g
}
func TestInitializeReopen(t *testing.T) {
	root := filepath.Join(t.TempDir(), "db")
	g := genesisFixture(t)
	metadata, err := initialize(t.Context(), g, root, nil)
	require.NoError(t, err)
	s, err := state.CheckAndLoadValidState(root)
	require.NoError(t, err)
	require.True(t, s.HasGenesisRoot())
	require.True(t, s.HasDurableRoot())
	require.Equal(t, uint64(1), s.GetResumeSlot())
	require.Equal(t, uint64(0), s.DurableHighWater())
	require.ErrorContains(t, state.RejectGenesisLaunch(root), "not implemented")
	db, reopened, err := open(t.Context(), root)
	require.NoError(t, err)
	require.Equal(t, *metadata, *reopened)
	_, _, err = open(t.Context(), root)
	require.ErrorIs(t, err, accountsdb.ErrAccountsDbInUse)
	_, err = initialize(t.Context(), g, root, nil)
	require.ErrorIs(t, err, accountsdb.ErrAccountsDbInUse)
	require.NoError(t, db.Shutdown(context.Background()))
	_, err = initialize(t.Context(), g, root, nil)
	require.ErrorContains(t, err, "occupied")
	// Metadata tampering cannot silently turn into an absent recovery root.
	require.NoError(t, os.WriteFile(filepath.Join(root, state.GenesisBankFileName), []byte("{}"), 0644))
	_, _, err = open(t.Context(), root)
	require.ErrorContains(t, err, "checksum")
}
func TestInterruptedInitialization(t *testing.T) {
	for _, phase := range []string{"before-directory", "locked", "intent", "accounts", "index", "verified"} {
		t.Run(phase, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "db")
			canceled := errors.New("injected interruption")
			_, err := initialize(t.Context(), genesisFixture(t), root, func(stage string) error {
				if phase == stage {
					return canceled
				}
				return nil
			})
			require.ErrorIs(t, err, canceled)
			_, err = os.Stat(filepath.Join(root, state.StateFileName))
			require.True(t, os.IsNotExist(err))
			if phase != "before-directory" && phase != "locked" {
				require.ErrorContains(t, state.RejectGenesisLaunch(root), "preserved")
				_, err = initialize(t.Context(), genesisFixture(t), root, nil)
				require.ErrorContains(t, err, "occupied")
				_, _, err = open(t.Context(), root)
				require.ErrorContains(t, err, "no ready")
			}
		})
	}
}
func TestCancellationAndDestinationOwnership(t *testing.T) {
	root := t.TempDir()
	sentinel := filepath.Join(root, "unrelated")
	require.NoError(t, os.WriteFile(sentinel, []byte("keep"), 0644))
	_, err := initialize(t.Context(), genesisFixture(t), root, nil)
	require.ErrorContains(t, err, "occupied")
	content, err := os.ReadFile(sentinel)
	require.NoError(t, err)
	require.Equal(t, "keep", string(content))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = initialize(ctx, genesisFixture(t), filepath.Join(t.TempDir(), "new"), nil)
	require.ErrorIs(t, err, context.Canceled)
	target := t.TempDir()
	link := filepath.Join(t.TempDir(), "link")
	require.NoError(t, os.Symlink(target, link))
	_, err = initialize(t.Context(), genesisFixture(t), link, nil)
	require.Error(t, err)
	entries, err := os.ReadDir(target)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestConcurrentInitialization(t *testing.T) {
	root := filepath.Join(t.TempDir(), "db")
	g := genesisFixture(t)
	locked, release, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		_, err := initialize(context.Background(), g, root, func(stage string) error {
			if stage == "locked" {
				close(locked)
				<-release
			}
			return nil
		})
		finished <- err
	}()
	<-locked
	_, err := initialize(t.Context(), g, root, nil)
	close(release)
	require.ErrorIs(t, err, accountsdb.ErrAccountsDbInUse)
	require.NoError(t, <-finished)
}

func TestInitializationProcessCrash(t *testing.T) {
	if phase := os.Getenv("MITHRIL_TEST_GENESIS_CRASH_PHASE"); phase != "" {
		_, err := initialize(context.Background(), genesisFixture(t), os.Getenv("MITHRIL_TEST_GENESIS_CRASH_ROOT"), func(stage string) error {
			if stage == phase {
				os.Exit(23)
			}
			return nil
		})
		t.Fatalf("did not crash at %s: %v", phase, err)
	}
	for _, phase := range []string{"intent", "index", "verified"} {
		t.Run(phase, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "db")
			command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestInitializationProcessCrash$")
			command.Env = append(os.Environ(), "MITHRIL_TEST_GENESIS_CRASH_PHASE="+phase, "MITHRIL_TEST_GENESIS_CRASH_ROOT="+root)
			output, err := command.CombinedOutput()
			var exit *exec.ExitError
			require.ErrorAs(t, err, &exit, string(output))
			require.Equal(t, 23, exit.ExitCode(), string(output))
			s, err := state.LoadState(root)
			require.NoError(t, err)
			require.Nil(t, s)
			// The dead process's OS lock is released, but its partial store remains
			// fenced against both destructive init and startup fallback.
			guard, err := accountsdb.AcquireExclusiveAccountsDbStore(root)
			require.NoError(t, err)
			require.NoError(t, guard.Close())
			require.ErrorContains(t, state.RejectGenesisLaunch(root), "preserved")
			_, err = initialize(t.Context(), genesisFixture(t), root, nil)
			require.ErrorContains(t, err, "occupied")
		})
	}
}
