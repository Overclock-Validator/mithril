package lightbringer

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/overcast"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Manager handles the lifecycle of a Lightbringer child process:
// config generation, spawning, log capture, health checking, and shutdown.
type Manager struct {
	binaryPath string
	configDir  string
	tomlCfg    LightbringerTOML
	grpcAddr   string
	logWriter  io.Writer

	cmd      *exec.Cmd
	done     chan struct{} // closed when the child process exits
	mu       sync.Mutex
	running  atomic.Bool
	stopping atomic.Bool // set by Stop to prevent MonitorAndRestart from restarting
}

// ManagerConfig holds the parameters needed to create a Manager.
type ManagerConfig struct {
	BinaryPath string
	ConfigDir  string
	GrpcAddr   string
	TOML       LightbringerTOML
	LogWriter  io.Writer // where captured stdout/stderr is written
}

// NewManager creates a new Lightbringer process manager.
func NewManager(cfg ManagerConfig) *Manager {
	return &Manager{
		binaryPath: cfg.BinaryPath,
		configDir:  cfg.ConfigDir,
		tomlCfg:    cfg.TOML,
		grpcAddr:   cfg.GrpcAddr,
		logWriter:  cfg.LogWriter,
	}
}

// WriteConfig generates Lightbringer.toml in the configured directory.
// Returns an error if required fields are missing.
func (m *Manager) WriteConfig() (string, error) {
	if err := m.tomlCfg.Validate(); err != nil {
		return "", fmt.Errorf("invalid lightbringer config: %w", err)
	}
	if err := os.MkdirAll(m.configDir, 0700); err != nil {
		return "", fmt.Errorf("failed to create config directory %s: %w", m.configDir, err)
	}
	return m.tomlCfg.WriteConfigFile(m.configDir)
}

// Start spawns the Lightbringer process and begins capturing its output.
// On Linux, Pdeathsig ensures the kernel sends SIGTERM to Lightbringer if
// Mithril exits for any reason (including os.Exit, SIGKILL, panic).
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running.Load() {
		return fmt.Errorf("already running")
	}

	m.stopping.Store(false) // reset in case of previous Stop()

	// Resolve binary path to absolute before exec. Relative paths like
	// ./lightbringer are validated from cwd but cmd.Dir causes chdir(configDir)
	// before execve on Unix, so the binary would be looked up from configDir.
	// Making the path absolute here ensures consistent resolution.
	binaryAbs := m.binaryPath
	if !filepath.IsAbs(binaryAbs) {
		abs, err := filepath.Abs(binaryAbs)
		if err != nil {
			return fmt.Errorf("cannot resolve binary path %s: %w", m.binaryPath, err)
		}
		binaryAbs = abs
	}
	if _, err := os.Stat(binaryAbs); err != nil {
		return fmt.Errorf("binary not found at %s: %w", binaryAbs, err)
	}

	// Spawn in a dedicated goroutine with LockOSThread to keep Pdeathsig valid.
	// Pdeathsig fires when the *OS thread* that called fork() dies. Without
	// LockOSThread, Go may recycle the thread, triggering a premature death
	// signal while Mithril is still alive (Go issue #27505).
	type startResult struct {
		cmd    *exec.Cmd
		stdout io.ReadCloser
		stderr io.ReadCloser
		err    error
	}
	resultCh := make(chan startResult, 1)
	done := make(chan struct{})

	go func() {
		runtime.LockOSThread()
		// Do NOT defer UnlockOSThread here — we unlock only after Wait returns

		cmd := exec.Command(binaryAbs)
		cmd.Dir = m.configDir
		cmd.SysProcAttr = childSysProcAttr() // Pdeathsig on Linux, empty on other platforms

		stdout, err := cmd.StdoutPipe()
		if err != nil {
			resultCh <- startResult{err: fmt.Errorf("stdout pipe: %w", err)}
			runtime.UnlockOSThread()
			return
		}
		stderr, err := cmd.StderrPipe()
		if err != nil {
			resultCh <- startResult{err: fmt.Errorf("stderr pipe: %w", err)}
			runtime.UnlockOSThread()
			return
		}

		if err := cmd.Start(); err != nil {
			resultCh <- startResult{err: fmt.Errorf("failed to start: %w", err)}
			runtime.UnlockOSThread()
			return
		}

		m.running.Store(true)
		resultCh <- startResult{cmd: cmd, stdout: stdout, stderr: stderr}

		// Block this goroutine (and its locked thread) until the child exits.
		// This keeps Pdeathsig valid for the entire lifetime of the child.
		waitErr := cmd.Wait()

		// Signal exit to the manager
		m.running.Store(false)
		deliberateStop := m.stopping.Load()
		if waitErr != nil && deliberateStop {
			mlog.Log.Infof("lightbringer: process exited after stop: %v", waitErr)
		} else if waitErr != nil {
			mlog.Log.Warnf("lightbringer: process exited with error: %v", waitErr)
		} else {
			mlog.Log.Infof("lightbringer: process exited cleanly")
		}
		close(done)

		runtime.UnlockOSThread()
	}()

	res := <-resultCh
	if res.err != nil {
		return res.err
	}

	m.cmd = res.cmd
	m.done = done

	mlog.Log.Infof("lightbringer: started process (pid=%d, binary=%s)",
		res.cmd.Process.Pid, m.binaryPath)

	go m.captureOutput("stdout", res.stdout)
	go m.captureOutput("stderr", res.stderr)

	return nil
}

// WaitReady polls the gRPC slot-stream service until the real Lightbringer
// protocol responds, or the timeout expires.
func (m *Manager) WaitReady(timeout time.Duration) error {
	return m.WaitReadyContext(context.Background(), timeout)
}

// WaitReadyContext polls the gRPC slot-stream service until the real
// Lightbringer protocol responds, the timeout expires, or ctx is cancelled.
func (m *Manager) WaitReadyContext(ctx context.Context, timeout time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	pollInterval := 500 * time.Millisecond

	for {
		select {
		case <-waitCtx.Done():
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("gRPC endpoint %s not ready after %s", m.grpcAddr, timeout)
		default:
		}

		if !m.running.Load() {
			return fmt.Errorf("process exited before becoming ready")
		}

		dialTimeout := 2 * time.Second
		if deadline, ok := waitCtx.Deadline(); ok {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return fmt.Errorf("gRPC endpoint %s not ready after %s", m.grpcAddr, timeout)
			}
			if remaining < dialTimeout {
				dialTimeout = remaining
			}
		}

		if err := probeSlotStreamContext(waitCtx, m.grpcAddr, dialTimeout); err == nil {
			mlog.Log.Infof("lightbringer: gRPC slot stream ready at %s", m.grpcAddr)
			return nil
		}

		timer := time.NewTimer(pollInterval)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("gRPC endpoint %s not ready after %s", m.grpcAddr, timeout)
		case <-timer.C:
		}
	}
}

func probeSlotStreamContext(parent context.Context, addr string, timeout time.Duration) error {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	conn, err := grpc.NewClient(addr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()

	client := overcast.NewSlotStreamClient(conn)
	return probeSlotStreamClient(ctx, client)
}

func probeSlotStreamClient(ctx context.Context, client overcast.SlotStreamClient) error {
	stream, err := client.StreamSlots(ctx, &overcast.SlotStreamRequest{})
	if err != nil {
		return err
	}
	_, err = stream.Recv()
	if err == nil {
		return nil
	}
	switch status.Code(err) {
	case codes.DeadlineExceeded:
		// The service accepted the SlotStream method but did not have a slot
		// ready before the probe timeout. That is still a valid readiness
		// signal: the target is Lightbringer, not an arbitrary TCP listener or
		// unrelated gRPC service.
		return nil
	default:
		return err
	}
}

// captureOutput reads from a pipe line-by-line and writes to the log writer.
func (m *Manager) captureOutput(name string, reader io.ReadCloser) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 64*1024), 256*1024)

	for scanner.Scan() {
		line := scanner.Text()
		if m.logWriter != nil {
			fmt.Fprintf(m.logWriter, "%s\n", line)
		}
	}
	if err := scanner.Err(); err != nil {
		mlog.Log.Warnf("lightbringer: %s capture error: %v", name, err)
	}
}

// Stop sends SIGTERM to the Lightbringer process and waits for it to exit.
// If the process doesn't exit within the timeout, it sends SIGKILL.
func (m *Manager) Stop(timeout time.Duration) error {
	m.stopping.Store(true) // signal MonitorAndRestart to not restart
	m.mu.Lock()
	if !m.running.Load() {
		m.mu.Unlock()
		return nil
	}

	proc := m.cmd.Process
	done := m.done
	m.mu.Unlock()

	mlog.Log.Infof("lightbringer: sending SIGTERM to pid %d", proc.Pid)

	if err := proc.Signal(syscall.SIGTERM); err != nil {
		mlog.Log.Warnf("lightbringer: SIGTERM failed: %v", err)
	}

	select {
	case <-done:
		mlog.Log.Infof("lightbringer: stopped cleanly")
		return nil
	case <-time.After(timeout):
		mlog.Log.Warnf("lightbringer: did not stop within %s, sending SIGKILL", timeout)
		if err := proc.Signal(syscall.SIGKILL); err != nil {
			mlog.Log.Warnf("lightbringer: SIGKILL failed: %v", err)
		}

		select {
		case <-done:
			return nil
		case <-time.After(5 * time.Second):
			return fmt.Errorf("process %d did not exit after SIGKILL", proc.Pid)
		}
	}
}

// IsRunning returns true if the Lightbringer process is currently running.
func (m *Manager) IsRunning() bool {
	return m.running.Load()
}

// Done returns a channel that is closed when the Lightbringer process exits.
// Returns nil if Start has not been called.
func (m *Manager) Done() <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.done
}

// Pid returns the PID of the running Lightbringer process, or 0 if not running.
func (m *Manager) Pid() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.running.Load() || m.cmd == nil || m.cmd.Process == nil {
		return 0
	}
	return m.cmd.Process.Pid
}

// MonitorAndRestart watches for unexpected Lightbringer exits and restarts
// with exponential backoff. Stops monitoring when stopCh is closed.
// maxRetries=0 means unlimited retries. Returns when stopped or max retries exceeded.
func (m *Manager) MonitorAndRestart(stopCh <-chan struct{}, maxRetries int) {
	const initialBackoff = 2 * time.Second
	// Uptime past this counts as a healthy run, not a crash loop.
	const stabilityWindow = 60 * time.Second
	backoff := initialBackoff
	maxBackoff := 60 * time.Second
	retries := 0
	lastStartAt := time.Now() // the caller already Start()ed the process we monitor

	for {
		done := m.Done()
		if done == nil {
			return
		}

		select {
		case <-stopCh:
			return
		case <-done:
		}

		// Reset crash-loop counter/backoff after a healthy run so an isolated crash gets the full retry budget.
		if time.Since(lastStartAt) >= stabilityWindow {
			retries = 0
			backoff = initialBackoff
		}

		// Process exited — wait for running flag to be cleared
		// (small window between done closing and running.Store(false))
		for i := 0; i < 10 && m.running.Load(); i++ {
			time.Sleep(10 * time.Millisecond)
		}

		// Check if this was a deliberate stop
		if m.stopping.Load() {
			return
		}
		select {
		case <-stopCh:
			return
		default:
		}

		// Retry transient WriteConfig/Start failures within the backoff+maxRetries budget. m.done is refreshed only on successful Start, so don't wait on it after a failed start.
		for {
			retries++
			if maxRetries > 0 && retries > maxRetries {
				mlog.Log.Errorf("lightbringer: exceeded %d restart attempts, giving up", maxRetries)
				return
			}

			mlog.Log.Warnf("lightbringer: not running; restart attempt %d scheduled after %s", retries, backoff)
			select {
			case <-stopCh:
				mlog.Log.Infof("lightbringer: restart attempt %d cancelled", retries)
				return
			case <-time.After(backoff):
			}

			// Exponential backoff for the next attempt.
			backoff = backoff * 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}

			if m.stopping.Load() {
				return
			}

			if _, err := m.WriteConfig(); err != nil {
				mlog.Log.Errorf("lightbringer: failed to write config for restart (attempt %d): %v", retries, err)
				continue // transient — retry within budget
			}
			if err := m.Start(); err != nil {
				mlog.Log.Errorf("lightbringer: failed to restart (attempt %d): %v", retries, err)
				continue // transient — retry within budget
			}

			lastStartAt = time.Now()
			mlog.Log.Infof("lightbringer: restarted successfully (attempt %d)", retries)
			break // success
		}
	}
}
