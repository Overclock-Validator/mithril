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
	"fmt"
	"sync"

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
	// BackendStdlib uses Go's crypto/ed25519-backed arithmetic, but with the
	// strictness checks applied first.
	BackendStdlib = "stdlib"
)

// Config selects the verification backend. It is deliberately tiny: the
// library's own defaults are good, and every knob here is a consensus-visible
// or performance-visible choice that an operator should have to state.
type Config struct {
	Backend string
}

// Defaults returns the configuration used when the operator sets nothing.
func Defaults() Config { return Config{Backend: BackendAuto} }

// Cfg is the live configuration, set once by Configure during startup and
// read-only afterwards. It follows the same shape as replay.TrailingVerifierCfg.
var Cfg = Defaults()

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

	configureMu.Lock()
	defer configureMu.Unlock()

	if configuredBackend != "" {
		return "", fmt.Errorf(
			"sigverify: already configured with backend %q; Configure is a startup-only operation",
			configuredBackend)
	}

	// Validate before publishing anything. Assigning Cfg first would leave a
	// rejected backend name visible to Backend() and to the startup log.
	switch cfg.Backend {
	case BackendAuto, BackendR51, BackendGeneric, BackendStdlib:
	default:
		return "", fmt.Errorf(
			"sigverify.backend must be one of %q, %q, %q, %q; got %q",
			BackendAuto, BackendR51, BackendGeneric, BackendStdlib, cfg.Backend)
	}

	resolved, err := installBackend(cfg.Backend)
	if err != nil {
		return "", err
	}

	Cfg = cfg
	configuredBackend = resolved
	return resolved, nil
}

// configureMu guards the one-shot handoff. Configure runs during startup while
// verification runs on pool goroutines, so the published state needs a barrier
// even though the write happens once.
var (
	configureMu       sync.Mutex
	configuredBackend string
)

func installBackend(backend string) (string, error) {
	// The strict predicate is not optional and not configurable: it is what
	// mainnet does. Set it before selecting a backend so no window exists in
	// which a verification could run under the compat predicate.
	narya.SetDefaultProfile(narya.DalekStrict)

	switch backend {
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

	case BackendR51, BackendGeneric, BackendStdlib:
		// Deliberately no fallback. BackendR51 on a CPU without AVX512-IFMA is
		// an operator asking for something the hardware cannot provide, and
		// silently degrading would hide that.
		if err := narya.SetBackend(backend); err != nil {
			return "", fmt.Errorf("sigverify: select backend %q: %w", backend, err)
		}
		return narya.ActiveBackend(), nil
	}

	// Unreachable: Configure validates the name before calling in.
	return "", fmt.Errorf("sigverify: unhandled backend %q", backend)
}

// Backend reports the backend in use, for metrics and diagnostics.
func Backend() string {
	return narya.ActiveBackend()
}

// InternalFaultFallbacks reports how many times the accelerated backend hit an
// internal fault and recomputed the work on the portable backend. It should be
// zero forever; a nonzero value is a bug in the accelerated backend, not an
// input-dependent condition, and is worth alerting on.
func InternalFaultFallbacks() uint64 {
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
	// Recorded here rather than at each drain site so a new caller cannot forget
	// to instrument itself. See stats.go for why width, not count, is the metric.
	observeBatchWidth(len(b.pubs))

	if len(b.pubs) == 0 {
		return true
	}
	return narya.VerifyBatchStrict(b.pubs, b.msgs, b.sigs, b.ok)
}

// OK reports the verdict for the i'th queued signature. It is only meaningful
// after Verify.
func (b *Batch) OK(i int) bool { return b.ok[i] }
