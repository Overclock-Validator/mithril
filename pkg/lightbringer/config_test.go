package lightbringer

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidate_RequiredFields(t *testing.T) {
	tests := []struct {
		name    string
		cfg     LightbringerTOML
		wantErr string
	}{
		{
			name:    "empty gossip entrypoint",
			cfg:     LightbringerTOML{GrpcAddr: "127.0.0.1:3001"},
			wantErr: "gossip_entrypoint is required",
		},
		{
			name:    "empty grpc addr",
			cfg:     LightbringerTOML{GossipEntrypoint: "1.2.3.4:8000"},
			wantErr: "grpc_addr is required",
		},
		{
			name: "valid minimal config",
			cfg: LightbringerTOML{
				GossipEntrypoint: "1.2.3.4:8000",
				GrpcAddr:         "127.0.0.1:3001",
			},
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

func TestValidate_GossipPorts(t *testing.T) {
	base := LightbringerTOML{
		GossipEntrypoint: "1.2.3.4:8000",
		Storage:          "/data/shreds",
		RpcAddr:          "127.0.0.1:3000",
		GrpcAddr:         "127.0.0.1:3001",
		GossipPort:       55000,
		PortRangeStart:   55001,
		PortRangeEnd:     55100,
	}

	tests := []struct {
		name    string
		mutate  func(*LightbringerTOML)
		wantErr string
	}{
		{name: "valid explicit ports"},
		{
			name: "narrow range",
			mutate: func(cfg *LightbringerTOML) {
				cfg.PortRangeEnd = 55010
			},
			wantErr: "at least 25",
		},
		{
			name: "overlap",
			mutate: func(cfg *LightbringerTOML) {
				cfg.GossipPort = 55010
			},
			wantErr: "must not overlap",
		},
		{
			name: "quic overflow",
			mutate: func(cfg *LightbringerTOML) {
				cfg.PortRangeStart = 65500
				cfg.PortRangeEnd = 65535
			},
			wantErr: "must fit",
		},
		{
			name: "zero values use lightbringer defaults",
			mutate: func(cfg *LightbringerTOML) {
				cfg.GossipPort = 0
				cfg.PortRangeStart = 0
				cfg.PortRangeEnd = 0
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			if tt.mutate != nil {
				tt.mutate(&cfg)
			}
			err := cfg.Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

func TestValidate_InfluxDBRequiresCompleteSection(t *testing.T) {
	base := LightbringerTOML{
		GossipEntrypoint: "1.2.3.4:8000",
		Storage:          "/data/shreds",
		RpcAddr:          "127.0.0.1:3000",
		GrpcAddr:         "127.0.0.1:3001",
		InfluxdbHost:     "http://127.0.0.1:18181",
		InfluxdbDatabase: "lightbringer",
		InfluxdbToken:    "token",
	}

	tests := []struct {
		name    string
		mutate  func(*LightbringerTOML)
		wantErr string
	}{
		{name: "complete section"},
		{
			name: "host only",
			mutate: func(cfg *LightbringerTOML) {
				cfg.InfluxdbDatabase = ""
				cfg.InfluxdbToken = ""
			},
			wantErr: "influxdb.database is required",
		},
		{
			name: "missing token",
			mutate: func(cfg *LightbringerTOML) {
				cfg.InfluxdbToken = ""
			},
			wantErr: "influxdb.token is required",
		},
		{
			name: "wrong host scheme",
			mutate: func(cfg *LightbringerTOML) {
				cfg.InfluxdbHost = "ws://127.0.0.1:18181"
			},
			wantErr: "influxdb.host must use one of",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			if tt.mutate != nil {
				tt.mutate(&cfg)
			}
			err := cfg.Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

func TestValidate_BlockConfirmationRequiresCompleteSection(t *testing.T) {
	base := LightbringerTOML{
		GossipEntrypoint:    "1.2.3.4:8000",
		Storage:             "/data/shreds",
		RpcAddr:             "127.0.0.1:3000",
		GrpcAddr:            "127.0.0.1:3001",
		BlockConfirmRpcHTTP: "http://127.0.0.1:8900",
		BlockConfirmRpcWS:   "ws://127.0.0.1:8900",
	}

	tests := []struct {
		name    string
		mutate  func(*LightbringerTOML)
		wantErr string
	}{
		{name: "complete section"},
		{
			name: "missing websocket",
			mutate: func(cfg *LightbringerTOML) {
				cfg.BlockConfirmRpcWS = ""
			},
			wantErr: "block_confirmation.rpc_websocket is required",
		},
		{
			name: "missing http",
			mutate: func(cfg *LightbringerTOML) {
				cfg.BlockConfirmRpcHTTP = ""
			},
			wantErr: "block_confirmation.rpc_http is required",
		},
		{
			name: "http field uses ws scheme",
			mutate: func(cfg *LightbringerTOML) {
				cfg.BlockConfirmRpcHTTP = "ws://127.0.0.1:8900"
			},
			wantErr: "block_confirmation.rpc_http must use one of",
		},
		{
			name: "websocket field uses http scheme",
			mutate: func(cfg *LightbringerTOML) {
				cfg.BlockConfirmRpcWS = "http://127.0.0.1:8900"
			},
			wantErr: "block_confirmation.rpc_websocket must use one of",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			if tt.mutate != nil {
				tt.mutate(&cfg)
			}
			err := cfg.Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

func TestGenerateTOML_RequiredFieldsOnly(t *testing.T) {
	cfg := LightbringerTOML{
		GossipEntrypoint: "1.2.3.4:8000",
		Storage:          "/data/shreds",
		RpcAddr:          "127.0.0.1:3000",
		GrpcAddr:         "127.0.0.1:3001",
	}

	toml := cfg.GenerateTOML()

	assert.Contains(t, toml, `gossip_entrypoint = "1.2.3.4:8000"`)
	assert.Contains(t, toml, `storage = "/data/shreds"`)
	assert.Contains(t, toml, `rpc_addr = "127.0.0.1:3000"`)
	assert.Contains(t, toml, `grpc_addr = "127.0.0.1:3001"`)
	assert.NotContains(t, toml, "[influxdb]")
	assert.NotContains(t, toml, "[block_confirmation]")
}

func TestGenerateTOML_WithGossipConfig(t *testing.T) {
	cfg := LightbringerTOML{
		GossipEntrypoint: "1.2.3.4:8000",
		Storage:          "/data/shreds",
		RpcAddr:          "127.0.0.1:3000",
		GrpcAddr:         "127.0.0.1:3001",
		GossipPort:       55000,
		PortRangeStart:   55001,
		PortRangeEnd:     55100,
	}

	toml := cfg.GenerateTOML()

	assert.Contains(t, toml, "[gossip]")
	assert.Contains(t, toml, "gossip_port = 55000")
	assert.Contains(t, toml, "port_range_start = 55001")
	assert.Contains(t, toml, "port_range_end = 55100")
}

func TestGenerateTOML_WithInfluxDB(t *testing.T) {
	cfg := LightbringerTOML{
		GossipEntrypoint: "1.2.3.4:8000",
		Storage:          "./shreds",
		RpcAddr:          "127.0.0.1:3000",
		GrpcAddr:         "127.0.0.1:3001",
		InfluxdbHost:     "http://localhost:8181",
		InfluxdbDatabase: "metrics",
		InfluxdbToken:    "secret-token",
	}

	toml := cfg.GenerateTOML()

	assert.Contains(t, toml, "[influxdb]")
	assert.Contains(t, toml, `host = "http://localhost:8181"`)
	assert.Contains(t, toml, `database = "metrics"`)
	assert.Contains(t, toml, `token = "secret-token"`)
	assert.NotContains(t, toml, "[block_confirmation]")
}

func TestGenerateTOML_WithBlockConfirmation(t *testing.T) {
	cfg := LightbringerTOML{
		GossipEntrypoint:    "1.2.3.4:8000",
		Storage:             "./shreds",
		RpcAddr:             "127.0.0.1:3000",
		GrpcAddr:            "127.0.0.1:3001",
		BlockConfirmRpcHTTP: "http://localhost:8899",
		BlockConfirmRpcWS:   "ws://localhost:8899",
	}

	toml := cfg.GenerateTOML()

	assert.Contains(t, toml, "[block_confirmation]")
	assert.Contains(t, toml, `rpc_http = "http://localhost:8899"`)
	assert.Contains(t, toml, `rpc_websocket = "ws://localhost:8899"`)
	assert.NotContains(t, toml, "[influxdb]")
}

func TestGenerateTOML_QuietOmittedByDefault(t *testing.T) {
	cfg := LightbringerTOML{
		GossipEntrypoint: "1.2.3.4:8000",
		Storage:          "/data/shreds",
		RpcAddr:          "127.0.0.1:3000",
		GrpcAddr:         "127.0.0.1:3001",
	}

	toml := cfg.GenerateTOML()

	// When Quiet is false (zero value), no [log] section should be emitted.
	// This preserves backward compatibility with older Lightbringer binaries.
	assert.NotContains(t, toml, "[log]")
	assert.NotContains(t, toml, "quiet")
}

func TestGenerateTOML_QuietEmitsLogSection(t *testing.T) {
	cfg := LightbringerTOML{
		GossipEntrypoint: "1.2.3.4:8000",
		Storage:          "/data/shreds",
		RpcAddr:          "127.0.0.1:3000",
		GrpcAddr:         "127.0.0.1:3001",
		Quiet:            true,
	}

	toml := cfg.GenerateTOML()

	assert.Contains(t, toml, "[log]")
	assert.Contains(t, toml, "quiet = true")
}

func TestGenerateTOML_QuietWithOtherOptionalSections(t *testing.T) {
	cfg := LightbringerTOML{
		GossipEntrypoint:    "1.2.3.4:8000",
		Storage:             "./shreds",
		RpcAddr:             "127.0.0.1:3000",
		GrpcAddr:            "127.0.0.1:3001",
		InfluxdbHost:        "http://localhost:8181",
		InfluxdbDatabase:    "db",
		InfluxdbToken:       "tok",
		BlockConfirmRpcHTTP: "http://localhost:8899",
		BlockConfirmRpcWS:   "ws://localhost:8899",
		Quiet:               true,
	}

	toml := cfg.GenerateTOML()

	// All three optional sections should be present and ordered consistently.
	assert.Contains(t, toml, "[influxdb]")
	assert.Contains(t, toml, "[block_confirmation]")
	assert.Contains(t, toml, "[log]")
	assert.Less(t, strings.Index(toml, "[influxdb]"), strings.Index(toml, "[block_confirmation]"))
	assert.Less(t, strings.Index(toml, "[block_confirmation]"), strings.Index(toml, "[log]"))
}

func TestGenerateTOML_BothOptionalSections(t *testing.T) {
	cfg := LightbringerTOML{
		GossipEntrypoint:    "1.2.3.4:8000",
		Storage:             "./shreds",
		RpcAddr:             "127.0.0.1:3000",
		GrpcAddr:            "127.0.0.1:3001",
		InfluxdbHost:        "http://localhost:8181",
		InfluxdbDatabase:    "db",
		InfluxdbToken:       "tok",
		BlockConfirmRpcHTTP: "http://localhost:8899",
		BlockConfirmRpcWS:   "ws://localhost:8899",
	}

	toml := cfg.GenerateTOML()

	// Both sections present
	assert.Contains(t, toml, "[influxdb]")
	assert.Contains(t, toml, "[block_confirmation]")
	// influxdb appears before block_confirmation
	assert.Less(t, strings.Index(toml, "[influxdb]"), strings.Index(toml, "[block_confirmation]"))
}

func TestGenerateTOML_SpecialCharactersInPaths(t *testing.T) {
	cfg := LightbringerTOML{
		GossipEntrypoint: "1.2.3.4:8000",
		Storage:          "/data/path with spaces/shreds",
		RpcAddr:          "127.0.0.1:3000",
		GrpcAddr:         "127.0.0.1:3001",
	}

	toml := cfg.GenerateTOML()

	// %q should properly escape the path
	assert.Contains(t, toml, `storage = "/data/path with spaces/shreds"`)
}

func TestWriteConfigFile_CreatesValidFile(t *testing.T) {
	dir := t.TempDir()

	cfg := LightbringerTOML{
		GossipEntrypoint: "10.0.0.1:8000",
		Storage:          "/tmp/shreds",
		RpcAddr:          "0.0.0.0:3000",
		GrpcAddr:         "0.0.0.0:3001",
	}

	path, err := cfg.WriteConfigFile(dir)
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(dir, "Lightbringer.toml"), path)

	// Read back and verify
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Contains(t, string(content), `gossip_entrypoint = "10.0.0.1:8000"`)
	assert.Contains(t, string(content), `grpc_addr = "0.0.0.0:3001"`)

	// The config can carry an InfluxDB token, so the file must be private
	// (0600). Permission bits aren't meaningful on Windows.
	if runtime.GOOS != "windows" {
		info, statErr := os.Stat(path)
		require.NoError(t, statErr)
		assert.Equal(t, os.FileMode(0600), info.Mode().Perm(),
			"Lightbringer config may hold a secret token; must not be group/world-readable")
	}
}

func TestWriteConfigFile_OverwritesExisting(t *testing.T) {
	dir := t.TempDir()

	cfg1 := LightbringerTOML{
		GossipEntrypoint: "1.1.1.1:8000",
		GrpcAddr:         "127.0.0.1:3001",
		Storage:          "./s1",
		RpcAddr:          "127.0.0.1:3000",
	}
	cfg2 := LightbringerTOML{
		GossipEntrypoint: "2.2.2.2:8000",
		GrpcAddr:         "127.0.0.1:3001",
		Storage:          "./s2",
		RpcAddr:          "127.0.0.1:3000",
	}

	// Write first
	_, err := cfg1.WriteConfigFile(dir)
	require.NoError(t, err)

	// Overwrite
	path, err := cfg2.WriteConfigFile(dir)
	require.NoError(t, err)

	content, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Contains(t, string(content), `gossip_entrypoint = "2.2.2.2:8000"`)
	assert.NotContains(t, string(content), "1.1.1.1")
}

func TestWriteConfigFile_NoTempFileLeftOnSuccess(t *testing.T) {
	dir := t.TempDir()

	cfg := LightbringerTOML{
		GossipEntrypoint: "1.2.3.4:8000",
		GrpcAddr:         "127.0.0.1:3001",
		Storage:          "./shreds",
		RpcAddr:          "127.0.0.1:3000",
	}

	_, err := cfg.WriteConfigFile(dir)
	require.NoError(t, err)

	// Only Lightbringer.toml should exist — no temp files
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, entries, 1)
	assert.Equal(t, "Lightbringer.toml", entries[0].Name())
}

// The exported wrapper (used by doctor) must enforce the same rules as runtime.
func TestValidateGossipPorts_ExportedWrapper(t *testing.T) {
	require.NoError(t, ValidateGossipPorts(55000, 55001, 55100))
	require.ErrorContains(t, ValidateGossipPorts(55010, 55001, 55100), "must not overlap")
	require.ErrorContains(t, ValidateGossipPorts(55000, 55001, 55010), "at least 25")
	require.ErrorContains(t, ValidateGossipPorts(70000, 55001, 55100), "out of range")
}
