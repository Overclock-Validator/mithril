package mcpcmd

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"

	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/Overclock-Validator/mithril/pkg/mcp"
	"github.com/spf13/viper"
)

type resolvedConfigOverrides struct {
	Profile *mcp.Profile
}

func resolvedConfigWithOverrides(overrides resolvedConfigOverrides) (mcp.Config, error) {
	if rawProfile, configured := os.LookupEnv("MITHRIL_MCP_PROFILE"); overrides.Profile == nil && configured && rawProfile != "" {
		if _, err := mcp.ParseProfile(rawProfile); err != nil {
			return mcp.Config{}, err
		}
	}
	cfg := mcp.ConfigFromEnv()
	if overrides.Profile != nil {
		cfg.Profile = *overrides.Profile
	}
	sp := defaultStoragePathsReadOnly()
	accountsEnv, accountsEnvSet := os.LookupEnv("MITHRIL_ACCOUNTS_PATH")
	snapshotsEnv, snapshotsEnvSet := os.LookupEnv("MITHRIL_SNAPSHOTS_PATH")
	shredstoreEnv, shredstoreEnvSet := os.LookupEnv("MITHRIL_SHREDSTORE_PATH")
	logDirEnv, logDirEnvSet := os.LookupEnv("MITHRIL_LOG_DIR")
	statePathEnv, statePathEnvSet := os.LookupEnv("MITHRIL_STATE_PATH")
	replayPathEnv, replayPathEnvSet := os.LookupEnv("MITHRIL_REPLAY_PATH")
	metricsEnv, metricsEnvSet := os.LookupEnv("MITHRIL_METRICS_URL")
	rpcEnv, rpcEnvSet := os.LookupEnv("MITHRIL_RPC_URL")
	pprofEnv, pprofEnvSet := os.LookupEnv("MITHRIL_PPROF_URL")
	blockSourceEnv, blockSourceEnvSet := os.LookupEnv("MITHRIL_BLOCK_SOURCE")

	// ConfigFromEnv supplies conventional defaults for standalone MCP use. An
	// explicitly present environment variable, including an empty one, is still
	// an override when a node config is merged below.
	if accountsEnvSet {
		cfg.AccountsDir = accountsEnv
	}
	if snapshotsEnvSet {
		cfg.SnapshotsDir = snapshotsEnv
	}
	if shredstoreEnvSet {
		cfg.ShredstoreDir = shredstoreEnv
	}
	if logDirEnvSet {
		cfg.LogDir = logDirEnv
	}
	if statePathEnvSet {
		cfg.StatePath = statePathEnv
	}
	if replayPathEnvSet {
		cfg.ReplayPath = replayPathEnv
	}
	if metricsEnvSet {
		cfg.MetricsURL = metricsEnv
	}
	if rpcEnvSet {
		cfg.RPCURL = rpcEnv
	}
	if pprofEnvSet {
		cfg.PprofURL = pprofEnv
	}
	if blockSourceEnvSet {
		cfg.BlockSource = blockSourceEnv
	}

	logsExplicitlyDisabled := logDirEnvSet && logDirEnv == ""
	pprofExplicitlyDisabled := pprofEnvSet && pprofEnv == ""
	if config.ConfigFile != "" {
		configured, err := nodeSettingsFromConfig(config.ConfigFile)
		if err != nil {
			return mcp.Config{}, err
		}
		if !accountsEnvSet && configured.Accounts != "" {
			sp.Accounts = configured.Accounts
		}
		if !snapshotsEnvSet && configured.Snapshots != "" {
			sp.Snapshots = configured.Snapshots
		}
		if !shredstoreEnvSet && configured.Shredstore != "" {
			sp.Shredstore = configured.Shredstore
		}
		if !logDirEnvSet {
			sp.Logs = configured.Logs
			logsExplicitlyDisabled = configured.Logs == ""
		}
		if !rpcEnvSet {
			cfg.RPCURL = loopbackHTTPURL(configured.RPCPort)
		}
		if !pprofEnvSet {
			cfg.PprofURL = loopbackHTTPURL(int(configured.PprofPort))
			pprofExplicitlyDisabled = configured.PprofPort <= 0
		}
		if !blockSourceEnvSet {
			cfg.BlockSource = configured.BlockSource
		}
	}
	if !accountsEnvSet {
		cfg.AccountsDir = sp.Accounts
	}
	if !snapshotsEnvSet {
		cfg.SnapshotsDir = sp.Snapshots
	}
	if !shredstoreEnvSet {
		cfg.ShredstoreDir = sp.Shredstore
	}
	if cfg.LogDir == "" && !logDirEnvSet && !logsExplicitlyDisabled {
		cfg.LogDir = sp.Logs
	}
	if cfg.StatePath == "" && !statePathEnvSet && cfg.AccountsDir != "" {
		cfg.StatePath = filepath.Join(cfg.AccountsDir, "mithril_state.json")
	}
	if cfg.ReplayPath == "" && !replayPathEnvSet {
		// The effective log directory may come from an environment override.
		// An explicitly empty storage.logs disables both file-backed surfaces.
		if cfg.LogDir != "" {
			cfg.ReplayPath = filepath.Join(cfg.LogDir, "replay_timings.jsonl")
		}
	}
	if cfg.PprofURL == "" && !pprofEnvSet && !pprofExplicitlyDisabled {
		cfg.PprofURL = "http://127.0.0.1:6060" // Conventional local pprof target.
	}
	for name, path := range map[string]string{
		"MCP accounts directory":   cfg.AccountsDir,
		"MCP snapshots directory":  cfg.SnapshotsDir,
		"MCP shredstore directory": cfg.ShredstoreDir,
		"MCP log directory":        cfg.LogDir,
		"MCP state path":           cfg.StatePath,
		"MCP replay path":          cfg.ReplayPath,
		"MCP node cgroup path":     cfg.NodeCgroupPath,
	} {
		if path != "" && !filepath.IsAbs(path) {
			return mcp.Config{}, fmt.Errorf("%s must be absolute so MCP client launch directories cannot change it", name)
		}
	}
	if cfg.BlockSource != "" {
		source, err := mcp.ParseBlockSource(cfg.BlockSource)
		if err != nil {
			return mcp.Config{}, err
		}
		cfg.BlockSource = source
	}
	return cfg, nil
}

// configuredNodeSettings contains node settings that affect MCP's local
// observation targets.
type configuredNodeSettings struct {
	Accounts    string
	Snapshots   string
	Shredstore  string
	Logs        string
	BlockSource string
	RPCPort     int
	PprofPort   int64
}

func configuredNodeBlockSource(v *viper.Viper) (string, error) {
	cluster := v.GetString("network.cluster")
	if cluster == "" {
		cluster = "alpenglow"
	}

	var defaultSource string
	switch cluster {
	case "alpenglow":
		defaultSource = "turbine"
	case "mainnet-beta", "testnet", "devnet":
		defaultSource = "rpc"
	default:
		return "", fmt.Errorf("invalid network.cluster %q - must be 'alpenglow', 'mainnet-beta', 'testnet', or 'devnet'", cluster)
	}

	source := v.GetString("block.source")
	sourceExplicit := source != ""
	if !sourceExplicit {
		source = defaultSource
	}
	if v.GetBool("lightbringer.enabled") && !sourceExplicit {
		source = "lightbringer"
	}

	switch source {
	case "rpc", "lightbringer", "turbine":
		return source, nil
	default:
		return "", fmt.Errorf("invalid block.source %q - must be 'rpc', 'lightbringer', or 'turbine'", source)
	}
}

func nodeSettingsFromConfig(path string) (configuredNodeSettings, error) {
	v := viper.New()
	config.ApplyDefaults(v)
	v.SetConfigFile(path)
	v.SetConfigType("toml")
	if err := v.ReadInConfig(); err != nil {
		return configuredNodeSettings{}, fmt.Errorf("read MCP node config %q: %w", path, err)
	}
	accounts := v.GetString("storage.accounts")
	if accounts == "" {
		accounts = v.GetString("ledger.accounts_path")
	}
	snapshots := v.GetString("snapshot.download_path")
	if snapshots == "" {
		snapshots = v.GetString("storage.snapshots")
	}
	shredstore := v.GetString("storage.shredstore")
	if shredstore == "" {
		shredstore = v.GetString("storage.blockstore")
	}
	if shredstore == "" {
		shredstore = v.GetString("ledger.path")
	}
	blockSource, err := configuredNodeBlockSource(v)
	if err != nil {
		return configuredNodeSettings{}, err
	}
	// These are the node command's effective flag defaults when an explicit
	// config omits the corresponding keys.
	rpcPort := 0
	pprofPort := int64(-1)
	settings := configuredNodeSettings{
		Accounts:    accounts,
		Snapshots:   snapshots,
		Shredstore:  shredstore,
		Logs:        "/mnt/mithril-logs",
		BlockSource: blockSource,
		RPCPort:     rpcPort,
		PprofPort:   pprofPort,
	}
	if v.InConfig("storage.logs") {
		settings.Logs = v.GetString("storage.logs")
	}
	if v.InConfig("rpc.port") {
		parsed, err := configuredInteger(v, "rpc.port")
		if err != nil {
			return configuredNodeSettings{}, err
		}
		if parsed < 0 || parsed > 65535 {
			return configuredNodeSettings{}, errors.New("rpc.port must be between 0 and 65535")
		}
		value := int(parsed)
		settings.RPCPort = value
	}
	// Match node.go exactly: the legacy key is consulted only when the tuning
	// value is explicitly zero. A missing tuning key resolves to the -1 flag
	// default, so a legacy-only key is ignored by the current node.
	if v.InConfig("tuning.pprof.port") {
		value, err := configuredInteger(v, "tuning.pprof.port")
		if err != nil {
			return configuredNodeSettings{}, err
		}
		if value == 0 {
			value = -1
			if v.InConfig("development.pprof.port") {
				value, err = configuredInteger(v, "development.pprof.port")
				if err != nil {
					return configuredNodeSettings{}, err
				}
			}
		}
		if value < -1 || value > 65535 {
			return configuredNodeSettings{}, errors.New("pprof port must be -1 or between 0 and 65535")
		}
		settings.PprofPort = value
	}
	return settings, nil
}

func configuredInteger(v *viper.Viper, key string) (int64, error) {
	raw := reflect.ValueOf(v.Get(key))
	if !raw.IsValid() {
		return 0, fmt.Errorf("%s must be an integer", key)
	}
	switch raw.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return raw.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		value := raw.Uint()
		if value <= uint64(math.MaxInt64) {
			return int64(value), nil
		}
	}
	return 0, fmt.Errorf("%s must be an integer", key)
}

func loopbackHTTPURL(port int) string {
	if port <= 0 {
		return ""
	}
	return fmt.Sprintf("http://127.0.0.1:%d", port)
}

func defaultStoragePathsReadOnly() config.StoragePaths {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		home = "."
	}
	return discoverStoragePaths(home, os.Stat)
}

// discoverStoragePaths selects the production layout when its accounts root is
// present. MCP only reads these paths, so existence, not write access, is the
// relevant signal and no write probe is necessary.
func discoverStoragePaths(home string, stat func(string) (os.FileInfo, error)) config.StoragePaths {
	if info, err := stat("/mnt/mithril-accounts"); err == nil && info.IsDir() {
		return config.StoragePaths{
			Accounts:   "/mnt/mithril-accounts",
			Snapshots:  "/mnt/mithril-ledger/snapshots",
			Logs:       "/mnt/mithril-logs",
			Shredstore: "/mnt/mithril-ledger/shredstore",
		}
	}
	base := filepath.Join(home, ".mithril")
	return config.StoragePaths{
		Accounts:   filepath.Join(base, "accounts"),
		Snapshots:  filepath.Join(base, "snapshots"),
		Logs:       filepath.Join(base, "logs"),
		Shredstore: filepath.Join(base, "shredstore"),
	}
}
