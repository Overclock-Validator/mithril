// Package sigverify is Mithril's single entry point for ed25519 transaction
// signature verification.
//
// It exists for two reasons.
//
// Predicate. Solana mainnet verifies transaction signatures with
// ed25519-dalek's verify_strict, which is Go's crypto/ed25519.Verify plus
// rejection of small-order A and small-order R. Verifying with plain stdlib
// therefore ACCEPTS a class of signatures mainnet REJECTS — a divergence from
// the invariant that Mithril reproduce mainnet state exactly. Every path here
// applies the strict predicate.
//
// Throughput. The underlying library verifies eight signatures per AVX-512
// group, so cost per signature is a strong function of how many signatures are
// handed over at once: on Zen 5 a lone signature costs ~22.9us while a group of
// eight costs ~6.1us each. Callers must therefore batch. Batch is the shape
// that makes that easy and allocation-free; Drain is the policy for filling it
// from a work channel.
package sigverify

import (
	stded25519 "crypto/ed25519"
	"fmt"
	"sync/atomic"

	narya "github.com/Overclock-Validator/narya-ed25519/ed25519"
)

// Backend names accepted by Config.Backend.
const (
	// BackendAuto prefers the AVX-512 backend and silently falls back to the
	// portable one when the CPU lacks AVX512-IFMA.
	BackendAuto = "auto"
	// BackendR51 forces the AVX-512 backend. Startup fails on a CPU without
	// AVX512-IFMA rather than silently degrading.
	BackendR51 = "r51"
	// BackendGeneric forces the portable pure-Go backend.
	BackendGeneric = "generic"
	// BackendStdlib bypasses the library entirely and calls crypto/ed25519
	// directly. This is the rollback switch: it restores the exact behaviour
	// Mithril had before this package existed, INCLUDING the non-strict
	// predicate, so it reintroduces the small-order divergence described above.
	// It exists so an operator can eliminate this package as a suspect without
	// rebuilding, not as a supported steady state.
	BackendStdlib = "stdlib"
)

// Config selects the verification backend and the group widths the pipelines
// aim for. It is deliberately tiny: the library's own defaults are good, and
// every knob here is a consensus-visible or performance-visible choice that an
// operator should have to state.
type Config struct {
	Backend string

	// BatchTarget is how many items a producer hands one worker, and the cap on
	// what a turbine worker coalesces. See BatchTarget.
	BatchTarget int

	// MaxDrain caps how many items a replay or TPU worker coalesces into one
	// batch. See MaxDrain.
	MaxDrain int
}

// Defaults returns the configuration used when the operator sets nothing.
func Defaults() Config {
	return Config{
		Backend:     BackendAuto,
		BatchTarget: DefaultBatchTarget,
		MaxDrain:    DefaultMaxDrain,
	}
}

// Cfg is the live configuration, set once by Configure during startup and
// read-only afterwards. It follows the same shape as replay.TrailingVerifierCfg.
var Cfg = Defaults()

// bypass is read on every verification, so it is an atomic rather than a plain
// bool: Configure runs during startup but the verifiers run on pool goroutines,
// and the race detector is correctly unhappy about an unsynchronised handoff.
var bypass atomic.Bool

// Configure resolves cfg and installs the backend. It returns the name of the
// backend actually selected, which the caller should log — with BackendAuto the
// resolved name is the only way an operator learns whether they got the
// accelerated path.
//
// It must be called exactly once, before any verification. Calling it twice
// returns an error rather than silently ignoring the second call, because the
// underlying library pins its backend on first use and a late switch would
// leave the process in a state neither caller asked for.
func Configure(cfg Config) (string, error) {
	if cfg.Backend == "" {
		cfg.Backend = Defaults().Backend
	}
	if cfg.BatchTarget == 0 {
		cfg.BatchTarget = Defaults().BatchTarget
	}
	if cfg.MaxDrain == 0 {
		cfg.MaxDrain = Defaults().MaxDrain
	}
	if err := validateWidths(cfg); err != nil {
		return "", err
	}
	Cfg = cfg
	batchTarget.Store(int64(cfg.BatchTarget))
	maxDrain.Store(int64(cfg.MaxDrain))

	// The strict predicate is not optional and not configurable: it is what
	// mainnet does. Set it before selecting a backend so no window exists in
	// which a verification could run under the compat predicate.
	narya.SetDefaultProfile(narya.DalekStrict)

	switch cfg.Backend {
	case BackendStdlib:
		bypass.Store(true)
		return BackendStdlib, nil

	case BackendAuto:
		// Try the accelerated backend, accept the portable one. An error here
		// means "this CPU lacks AVX512-IFMA", which is the expected answer on
		// most hardware and not a startup failure.
		if err := narya.SetBackend(BackendR51); err == nil {
			return narya.ActiveBackend(), nil
		}
		if err := narya.SetBackend(BackendGeneric); err != nil {
			return "", fmt.Errorf("sigverify: select portable backend: %w", err)
		}
		return narya.ActiveBackend(), nil

	case BackendR51, BackendGeneric:
		if err := narya.SetBackend(cfg.Backend); err != nil {
			return "", fmt.Errorf("sigverify: select backend %q: %w", cfg.Backend, err)
		}
		return narya.ActiveBackend(), nil

	default:
		return "", fmt.Errorf(
			"sigverify.backend must be one of %q, %q, %q, %q; got %q",
			BackendAuto, BackendR51, BackendGeneric, BackendStdlib, cfg.Backend)
	}
}

// validateWidths rejects group widths that would break the drain policy rather
// than merely tune it. A width below one would make FairShare's floor the only
// thing keeping a worker from returning the item it already holds, and a
// MaxDrain below BatchTarget would make the target unreachable by construction:
// the producer hands out BatchTarget items per worker and the consumer would
// refuse to take them.
func validateWidths(cfg Config) error {
	if cfg.BatchTarget < 1 {
		return fmt.Errorf("sigverify.batch_target must be >= 1; got %d", cfg.BatchTarget)
	}
	if cfg.MaxDrain < 1 {
		return fmt.Errorf("sigverify.max_drain must be >= 1; got %d", cfg.MaxDrain)
	}
	if cfg.MaxDrain < cfg.BatchTarget {
		return fmt.Errorf(
			"sigverify.max_drain (%d) must be >= sigverify.batch_target (%d), or workers would refuse groups the producer hands them",
			cfg.MaxDrain, cfg.BatchTarget)
	}
	if cfg.BatchTarget > MaxConfigurableDrain || cfg.MaxDrain > MaxConfigurableDrain {
		return fmt.Errorf(
			"sigverify group widths must be <= %d; got batch_target=%d max_drain=%d",
			MaxConfigurableDrain, cfg.BatchTarget, cfg.MaxDrain)
	}
	return nil
}

// Backend reports the backend in use, for metrics and diagnostics.
func Backend() string {
	if bypass.Load() {
		return BackendStdlib
	}
	return narya.ActiveBackend()
}

// InternalFaultFallbacks reports how many times the accelerated backend hit an
// internal fault and recomputed the work on the portable backend. It should be
// zero forever; a nonzero value is a bug in the accelerated backend, not an
// input-dependent condition, and is worth alerting on.
func InternalFaultFallbacks() uint64 {
	if bypass.Load() {
		return 0
	}
	return narya.ActiveBackendStats().InternalFaultFallbacks
}

// VerifyOne verifies a single signature under the strict predicate.
//
// Prefer Batch. This costs roughly 3.7x per signature what the same work costs
// inside a group of eight, so it is for paths that genuinely have one signature
// and no way to accumulate more.
func VerifyOne(pub *[32]byte, msg, sig []byte) bool {
	if pub == nil {
		return false
	}
	if bypass.Load() {
		return stded25519.Verify(pub[:], msg, sig)
	}
	return narya.VerifyStrict(pub[:], msg, sig)
}

// Batch accumulates signatures and verifies them in one call. It is reusable
// worker-local scratch: Reset keeps the backing arrays, so a worker that loops
// on Reset/Add/Verify allocates nothing after the first batch.
//
// A Batch is not safe for concurrent use. Give each worker its own.
type Batch struct {
	pubs []*[32]byte
	msgs [][]byte
	sigs [][]byte
	ok   []bool
}

// Reset empties the batch while retaining capacity.
func (b *Batch) Reset() {
	// Clear the pointer-bearing slots so a finished batch does not pin public
	// keys, messages, and signatures alive until the worker's next batch
	// happens to overwrite that index.
	clear(b.pubs)
	clear(b.msgs)
	clear(b.sigs)
	b.pubs = b.pubs[:0]
	b.msgs = b.msgs[:0]
	b.sigs = b.sigs[:0]
	b.ok = b.ok[:0]
}

// Add appends one signature. pub must remain valid until Verify returns.
func (b *Batch) Add(pub *[32]byte, msg, sig []byte) {
	b.pubs = append(b.pubs, pub)
	b.msgs = append(b.msgs, msg)
	b.sigs = append(b.sigs, sig)
	b.ok = append(b.ok, false)
}

// Len reports how many signatures are queued.
func (b *Batch) Len() int { return len(b.pubs) }

// Verify checks every queued signature and reports whether all of them passed.
// Per-signature verdicts are available from OK afterwards either way, so a
// caller that needs to identify WHICH signature failed does not have to
// re-verify anything.
func (b *Batch) Verify() bool {
	if len(b.pubs) == 0 {
		return true
	}
	if bypass.Load() {
		all := true
		for i, pub := range b.pubs {
			verdict := pub != nil && stded25519.Verify(pub[:], b.msgs[i], b.sigs[i])
			b.ok[i] = verdict
			all = all && verdict
		}
		return all
	}
	return narya.VerifyBatchStrict(b.pubs, b.msgs, b.sigs, b.ok)
}

// OK reports the verdict for the i'th queued signature. It is only meaningful
// after Verify.
func (b *Batch) OK(i int) bool { return b.ok[i] }
