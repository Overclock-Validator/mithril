package lightbringer

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/overcast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// createFakeBinary creates a simple shell script that acts as a fake Lightbringer process.
// It sleeps until killed, writing a startup message to stdout.
func createFakeBinary(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "fake-lightbringer")
	script := "#!/bin/sh\necho \"fake lightbringer started\"\nsleep 300\n"
	err := os.WriteFile(path, []byte(script), 0755)
	require.NoError(t, err)
	return path
}

// createFastExitBinary creates a binary that exits immediately with a message.
func createFastExitBinary(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "fast-exit-lightbringer")
	script := "#!/bin/sh\necho \"started and exiting\"\nexit 0\n"
	err := os.WriteFile(path, []byte(script), 0755)
	require.NoError(t, err)
	return path
}

func TestManager_WriteConfig(t *testing.T) {
	dir := t.TempDir()
	mgr := NewManager(ManagerConfig{
		BinaryPath: "/nonexistent",
		ConfigDir:  dir,
		GrpcAddr:   "127.0.0.1:3001",
		TOML: LightbringerTOML{
			GossipEntrypoint: "10.0.0.1:8000",
			Storage:          "/data/shreds",
			RpcAddr:          "127.0.0.1:3000",
			GrpcAddr:         "127.0.0.1:3001",
		},
	})

	path, err := mgr.WriteConfig()
	require.NoError(t, err)

	content, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Contains(t, string(content), `gossip_entrypoint = "10.0.0.1:8000"`)
}

func TestManager_WriteConfig_ValidationFails(t *testing.T) {
	dir := t.TempDir()
	mgr := NewManager(ManagerConfig{
		BinaryPath: "/nonexistent",
		ConfigDir:  dir,
		GrpcAddr:   "127.0.0.1:3001",
		TOML: LightbringerTOML{
			// Missing GossipEntrypoint
			GrpcAddr: "127.0.0.1:3001",
		},
	})

	_, err := mgr.WriteConfig()
	assert.ErrorContains(t, err, "gossip_entrypoint is required")
}

func TestManager_StartStop(t *testing.T) {
	dir := t.TempDir()
	binaryPath := createFakeBinary(t, dir)

	var logBuf bytes.Buffer
	mgr := NewManager(ManagerConfig{
		BinaryPath: binaryPath,
		ConfigDir:  dir,
		GrpcAddr:   "127.0.0.1:39999",
		TOML: LightbringerTOML{
			GossipEntrypoint: "1.2.3.4:8000",
			Storage:          filepath.Join(dir, "shreds"),
			RpcAddr:          "127.0.0.1:39998",
			GrpcAddr:         "127.0.0.1:39999",
		},
		LogWriter: &logBuf,
	})

	_, err := mgr.WriteConfig()
	require.NoError(t, err)

	err = mgr.Start()
	require.NoError(t, err)
	assert.True(t, mgr.IsRunning())
	assert.Greater(t, mgr.Pid(), 0)

	// wait for startup line
	time.Sleep(200 * time.Millisecond)

	err = mgr.Stop(5 * time.Second)
	require.NoError(t, err)
	assert.False(t, mgr.IsRunning())
	assert.Equal(t, 0, mgr.Pid())

	assert.Contains(t, logBuf.String(), "fake lightbringer started")
}

func TestManager_StartFailsBinaryNotFound(t *testing.T) {
	dir := t.TempDir()
	mgr := NewManager(ManagerConfig{
		BinaryPath: filepath.Join(dir, "nonexistent-binary"),
		ConfigDir:  dir,
		GrpcAddr:   "127.0.0.1:3001",
		TOML: LightbringerTOML{
			GossipEntrypoint: "1.2.3.4:8000",
			GrpcAddr:         "127.0.0.1:3001",
		},
	})

	err := mgr.Start()
	assert.ErrorContains(t, err, "binary not found")
}

func TestManager_DoubleStartFails(t *testing.T) {
	dir := t.TempDir()
	binaryPath := createFakeBinary(t, dir)

	mgr := NewManager(ManagerConfig{
		BinaryPath: binaryPath,
		ConfigDir:  dir,
		GrpcAddr:   "127.0.0.1:39999",
		TOML: LightbringerTOML{
			GossipEntrypoint: "1.2.3.4:8000",
			Storage:          filepath.Join(dir, "shreds"),
			RpcAddr:          "127.0.0.1:39998",
			GrpcAddr:         "127.0.0.1:39999",
		},
	})

	_, err := mgr.WriteConfig()
	require.NoError(t, err)

	err = mgr.Start()
	require.NoError(t, err)
	defer mgr.Stop(5 * time.Second)

	err = mgr.Start()
	assert.ErrorContains(t, err, "already running")
}

func TestManager_StopWhenNotRunning(t *testing.T) {
	mgr := NewManager(ManagerConfig{
		BinaryPath: "/nonexistent",
		ConfigDir:  t.TempDir(),
		GrpcAddr:   "127.0.0.1:3001",
	})

	// Stop on a never-started manager should be no-op
	err := mgr.Stop(1 * time.Second)
	assert.NoError(t, err)
}

func TestManager_DoneChannelClosesOnExit(t *testing.T) {
	dir := t.TempDir()
	binaryPath := createFastExitBinary(t, dir)

	mgr := NewManager(ManagerConfig{
		BinaryPath: binaryPath,
		ConfigDir:  dir,
		GrpcAddr:   "127.0.0.1:39999",
		TOML: LightbringerTOML{
			GossipEntrypoint: "1.2.3.4:8000",
			Storage:          filepath.Join(dir, "shreds"),
			RpcAddr:          "127.0.0.1:39998",
			GrpcAddr:         "127.0.0.1:39999",
		},
	})

	_, err := mgr.WriteConfig()
	require.NoError(t, err)

	err = mgr.Start()
	require.NoError(t, err)

	select {
	case <-mgr.Done():
		// expected
	case <-time.After(5 * time.Second):
		t.Fatal("done channel did not close after process exit")
	}

	assert.False(t, mgr.IsRunning())
}

func TestManager_DoneBeforeStart(t *testing.T) {
	mgr := NewManager(ManagerConfig{
		BinaryPath: "/nonexistent",
		ConfigDir:  t.TempDir(),
		GrpcAddr:   "127.0.0.1:3001",
	})

	done := mgr.Done()
	assert.Nil(t, done)
}

func TestManager_WaitReadyFailsWhenProcessDies(t *testing.T) {
	dir := t.TempDir()
	binaryPath := createFastExitBinary(t, dir)

	mgr := NewManager(ManagerConfig{
		BinaryPath: binaryPath,
		ConfigDir:  dir,
		GrpcAddr:   "127.0.0.1:39999",
		TOML: LightbringerTOML{
			GossipEntrypoint: "1.2.3.4:8000",
			Storage:          filepath.Join(dir, "shreds"),
			RpcAddr:          "127.0.0.1:39998",
			GrpcAddr:         "127.0.0.1:39999",
		},
	})

	_, err := mgr.WriteConfig()
	require.NoError(t, err)

	err = mgr.Start()
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond) // let it exit

	err = mgr.WaitReady(3 * time.Second)
	assert.ErrorContains(t, err, "process exited before becoming ready")
}

func TestManager_WaitReadyContextCancels(t *testing.T) {
	dir := t.TempDir()
	binaryPath := createFakeBinary(t, dir)

	mgr := NewManager(ManagerConfig{
		BinaryPath: binaryPath,
		ConfigDir:  dir,
		GrpcAddr:   "127.0.0.1:39999",
		TOML: LightbringerTOML{
			GossipEntrypoint: "1.2.3.4:8000",
			Storage:          filepath.Join(dir, "shreds"),
			RpcAddr:          "127.0.0.1:39998",
			GrpcAddr:         "127.0.0.1:39999",
		},
	})

	_, err := mgr.WriteConfig()
	require.NoError(t, err)

	err = mgr.Start()
	require.NoError(t, err)
	defer mgr.Stop(5 * time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = mgr.WaitReadyContext(ctx, 30*time.Second)
	require.ErrorIs(t, err, context.Canceled)
}

type fakeSlotStreamClient struct {
	stream grpc.ServerStreamingClient[overcast.SlotResponse]
	err    error
}

func (f fakeSlotStreamClient) StreamSlots(ctx context.Context, in *overcast.SlotStreamRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[overcast.SlotResponse], error) {
	return f.stream, f.err
}

type fakeSlotStream struct {
	ctx     context.Context
	resp    *overcast.SlotResponse
	recvErr error
}

func (f fakeSlotStream) Recv() (*overcast.SlotResponse, error) {
	if f.resp != nil {
		return f.resp, nil
	}
	return nil, f.recvErr
}

func (f fakeSlotStream) Header() (metadata.MD, error) { return nil, nil }
func (f fakeSlotStream) Trailer() metadata.MD         { return nil }
func (f fakeSlotStream) CloseSend() error             { return nil }
func (f fakeSlotStream) Context() context.Context {
	if f.ctx != nil {
		return f.ctx
	}
	return context.Background()
}
func (f fakeSlotStream) SendMsg(any) error { return nil }
func (f fakeSlotStream) RecvMsg(any) error { return f.recvErr }

func TestProbeSlotStreamClientRejectsWrongGRPCService(t *testing.T) {
	err := probeSlotStreamClient(context.Background(), fakeSlotStreamClient{
		stream: fakeSlotStream{recvErr: status.Error(codes.Unimplemented, "unknown service")},
	})
	require.Error(t, err)
	assert.Equal(t, codes.Unimplemented, status.Code(err))
}

func TestProbeSlotStreamClientDeadlineAfterAcceptedMethodIsReady(t *testing.T) {
	err := probeSlotStreamClient(context.Background(), fakeSlotStreamClient{
		stream: fakeSlotStream{recvErr: status.Error(codes.DeadlineExceeded, "no slots yet")},
	})
	require.NoError(t, err)
}

func TestProbeSlotStreamClientResponseIsReady(t *testing.T) {
	err := probeSlotStreamClient(context.Background(), fakeSlotStreamClient{
		stream: fakeSlotStream{resp: &overcast.SlotResponse{}},
	})
	require.NoError(t, err)
}
