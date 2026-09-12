// Package mcpcmd defines the `mithril mcp` commands.
package mcpcmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/Overclock-Validator/mithril/pkg/mcp"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

// MCPCmd runs the MCP server or one of its setup commands.
var MCPCmd = cobra.Command{
	Use:           "mcp",
	Short:         "Inspect a Mithril node over MCP",
	Args:          cobra.NoArgs,
	SilenceErrors: true,
	SilenceUsage:  true,
	RunE:          runServe,
	Long: `Run MCP over stdio so a compatible client can inspect Mithril's RPC,
metrics, logs, state, and replay data.

An MCP client launches the stdio server as a child process. It is not a daemon
or listening service. For remote use, make "ssh -T NODE mithril mcp" the
client's SSH remote command.

The default monitor profile is read-only. Diagnostic adds profiling and
simulation tools.

Run "mithril mcp setup codex", "mithril mcp setup claude", or "mithril mcp
setup vscode" to add the server in one command. Any stdio-capable MCP client
can use "mithril mcp config" to print a client-neutral command-and-arguments
entry. File paths must be absolute because clients may launch the server from
another directory.

MCP stdio has no authentication of its own, so local process access or SSH
identity is the authorization boundary.`,
	Example: `  # Local node
  mithril mcp setup codex

  # Remote node
  mithril mcp setup codex --ssh node-alias --remote-binary /absolute/path/to/mithril`,
}

var serveCmd = cobra.Command{
	Use:           "serve",
	Short:         "Run the MCP server over stdio",
	Long:          "Serve MCP over stdio until the launching client disconnects. For remote use, run this as the SSH remote command. --profile overrides MITHRIL_MCP_PROFILE.",
	Hidden:        true,
	Args:          cobra.NoArgs,
	SilenceErrors: true,
	SilenceUsage:  true,
	RunE:          runServe,
}

var interactiveStdio = func() bool {
	return term.IsTerminal(int(os.Stdin.Fd())) && term.IsTerminal(int(os.Stdout.Fd()))
}

func runServe(cmd *cobra.Command, _ []string) error {
	cfg, err := resolvedServeConfig(cmd)
	if err != nil {
		return err
	}
	if cmd.Name() == "mcp" && interactiveStdio() {
		if err := mcp.ValidateServeConfig(cfg); err != nil {
			return err
		}
		fmt.Fprintln(cmd.OutOrStdout(), `Mithril MCP is a stdio server launched by an MCP client; it is not an interactive shell.
Run "mithril mcp config" to print a client-neutral configuration entry.`)
		return nil
	}
	return mcp.Serve(cmd.Context(), cfg)
}

type stdioConfigEntry struct {
	Type    string   `json:"type"`
	Command string   `json:"command"`
	Args    []string `json:"args"`
}

func generatedStdioConfig(cmd *cobra.Command) (stdioConfigEntry, error) {
	if configSSHTarget != "" || configRemoteBinary != "" || configRemoteConfig != "" {
		if configSSHTarget == "" {
			return stdioConfigEntry{}, errors.New("--remote-binary and --remote-config require --ssh")
		}
		if config.ConfigFile != "" {
			return stdioConfigEntry{}, errors.New("local --config cannot be combined with --ssh; use --remote-config")
		}
		return remoteStdioConfig(configSSHTarget, configRemoteBinary, configProfile, configRemoteConfig)
	}

	rawExecutable, err := currentExecutable()
	if err != nil {
		return stdioConfigEntry{}, fmt.Errorf("locate Mithril executable: %w", err)
	}
	return portableStdioConfig(rawExecutable, configProfile, config.ConfigFile)
}

var configCmd = cobra.Command{
	Use:           "config",
	Short:         "Print a common stdio MCP server entry",
	SilenceErrors: true,
	SilenceUsage:  true,
	Long: `Print a common client-neutral JSON server entry for MCP hosts that support
stdio. MCP does not standardize host configuration files, so wrap its command
and args fields as required by your client. This command only prints JSON; it
does not modify client configuration.

With --ssh, the entry launches Mithril on a remote node through SSH. The remote
binary must be an absolute path. Use --remote-config for a config file on that
node; the root --config flag always refers to a local file. Before adding the
entry to a client, verify noninteractive key authentication and known_hosts
with "ssh NODE true"; MCP cannot answer password or host-key prompts.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		entry, err := generatedStdioConfig(cmd)
		if err != nil {
			return err
		}
		encoder := json.NewEncoder(cmd.OutOrStdout())
		encoder.SetIndent("", "  ")
		return encoder.Encode(entry)
	},
}

var runMCPClient = func(cmd *cobra.Command, executable string, args []string) error {
	client := exec.CommandContext(cmd.Context(), executable, args...)
	client.Stdin = cmd.InOrStdin()
	client.Stdout = cmd.OutOrStdout()
	client.Stderr = cmd.ErrOrStderr()
	if err := client.Run(); err != nil {
		return fmt.Errorf("run %s: %w", executable, err)
	}
	return nil
}

var setupCmd = cobra.Command{
	Use:           "setup CLIENT",
	Short:         "Add Mithril MCP to Codex, Claude Code, or VS Code",
	Args:          cobra.ExactArgs(1),
	SilenceErrors: true,
	SilenceUsage:  true,
	RunE: func(cmd *cobra.Command, args []string) error {
		if args[0] != "codex" && args[0] != "claude" && args[0] != "vscode" {
			return fmt.Errorf("unsupported MCP client %q; supported clients: codex, claude, or vscode", args[0])
		}
		entry, err := generatedStdioConfig(cmd)
		if err != nil {
			return err
		}
		if args[0] == "codex" {
			clientArgs := []string{"mcp", "add", "mithril", "--", entry.Command}
			return runMCPClient(cmd, "codex", append(clientArgs, entry.Args...))
		}
		if args[0] == "claude" {
			configJSON, err := json.Marshal(entry)
			if err != nil {
				return fmt.Errorf("encode Claude MCP config: %w", err)
			}
			return runMCPClient(cmd, "claude", []string{"mcp", "add-json", "--scope", "user", "mithril", string(configJSON)})
		}
		configJSON, err := json.Marshal(struct {
			Name    string   `json:"name"`
			Command string   `json:"command"`
			Args    []string `json:"args"`
		}{Name: "mithril", Command: entry.Command, Args: entry.Args})
		if err != nil {
			return fmt.Errorf("encode VS Code MCP config: %w", err)
		}
		return runMCPClient(cmd, "code", []string{"--add-mcp", string(configJSON)})
	},
}

var (
	configProfile      string
	configSSHTarget    string
	configRemoteBinary string
	configRemoteConfig string
)

var currentExecutable = os.Executable

func durableExecutablePath(raw string) (string, error) {
	executable, err := filepath.Abs(raw)
	if err != nil {
		return "", fmt.Errorf("resolve Mithril executable path: %w", err)
	}
	if strings.Contains(filepath.ToSlash(executable), "/go-build") || pathWithinConfiguredGoCache(executable) {
		return "", errors.New("cannot configure an ephemeral 'go run' binary; build or install Mithril first")
	}
	return executable, nil
}

func pathWithinConfiguredGoCache(path string) bool {
	cache := os.Getenv("GOCACHE")
	if cache == "" || cache == "off" {
		return false
	}
	cache, err := filepath.Abs(cache)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(cache, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func portableStdioConfig(rawExecutable, rawProfile, rawConfigPath string) (stdioConfigEntry, error) {
	executable, err := durableExecutablePath(rawExecutable)
	if err != nil {
		return stdioConfigEntry{}, err
	}
	args := []string{"mcp"}
	if rawConfigPath != "" {
		configPath, err := filepath.Abs(rawConfigPath)
		if err != nil {
			return stdioConfigEntry{}, fmt.Errorf("resolve config path: %w", err)
		}
		args = append(args, "--config", configPath)
	}
	if rawProfile != "" {
		profile, err := mcp.ParseProfile(rawProfile)
		if err != nil {
			return stdioConfigEntry{}, err
		}
		args = append(args, "--profile", string(profile))
	}
	return stdioConfigEntry{Type: "stdio", Command: executable, Args: args}, nil
}

func remoteStdioConfig(target, remoteBinary, rawProfile, remoteConfig string) (stdioConfigEntry, error) {
	if err := validateSSHTarget(target); err != nil {
		return stdioConfigEntry{}, err
	}
	if err := validateRemoteAbsolutePath("--remote-binary", remoteBinary); err != nil {
		return stdioConfigEntry{}, err
	}
	if remoteConfig != "" {
		if err := validateRemoteAbsolutePath("--remote-config", remoteConfig); err != nil {
			return stdioConfigEntry{}, err
		}
	}

	remoteArgs := []string{remoteBinary, "mcp"}
	if remoteConfig != "" {
		remoteArgs = append(remoteArgs, "--config", remoteConfig)
	}
	if rawProfile != "" {
		profile, err := mcp.ParseProfile(rawProfile)
		if err != nil {
			return stdioConfigEntry{}, err
		}
		remoteArgs = append(remoteArgs, "--profile", string(profile))
	}
	quoted := make([]string, len(remoteArgs))
	for i, arg := range remoteArgs {
		quoted[i] = posixShellQuote(arg)
	}
	remoteCommand := "exec " + strings.Join(quoted, " ")
	// Preserve host aliases and identity settings, but isolate this stdio session
	// from inherited forwarding, multiplexing, and command/lifecycle overrides.
	return stdioConfigEntry{
		Type:    "stdio",
		Command: "ssh",
		Args: []string{
			"-T", "-a", "-x",
			"-o", "IgnoreUnknown=ForkAfterAuthentication,SessionType,StdinNull,RemoteCommand",
			"-o", "BatchMode=yes",
			"-o", "ConnectTimeout=10",
			"-o", "ServerAliveInterval=15",
			"-o", "ServerAliveCountMax=2",
			"-o", "ClearAllForwardings=yes",
			"-o", "Tunnel=no",
			"-o", "PermitLocalCommand=no",
			"-o", "StdinNull=no",
			"-o", "ForkAfterAuthentication=no",
			"-o", "SessionType=default",
			"-o", "RemoteCommand=none",
			"-o", "ControlPath=none",
			"--", target, remoteCommand,
		},
	}, nil
}

func validateSSHTarget(target string) error {
	if target == "" || strings.HasPrefix(target, "-") {
		return errors.New("--ssh must be a non-empty SSH host or alias")
	}
	if strings.Count(target, "@") > 1 {
		return errors.New("--ssh must contain at most one user separator")
	}
	host := target
	if separator := strings.IndexByte(target, '@'); separator >= 0 {
		if separator == 0 || separator == len(target)-1 {
			return errors.New("--ssh user and host must both be non-empty")
		}
		host = target[separator+1:]
	}
	if strings.ContainsAny(host, "[]") &&
		!(strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") && strings.Count(host, "[") == 1 && strings.Count(host, "]") == 1) {
		return errors.New("--ssh has invalid host brackets")
	}
	if strings.HasPrefix(host, "-") {
		return errors.New("--ssh host cannot start with '-'")
	}
	for _, r := range target {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case strings.ContainsRune("._-@:+%[]", r):
		default:
			return errors.New("--ssh contains an unsupported character")
		}
	}
	if strings.IndexFunc(host, func(r rune) bool {
		return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9'
	}) < 0 {
		return errors.New("--ssh must contain a host or alias name")
	}
	return nil
}

func validateRemoteAbsolutePath(flagName, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required and must be an absolute path", flagName)
	}
	if !utf8.ValidString(value) || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return fmt.Errorf("%s contains an unsupported character", flagName)
	}
	if !path.IsAbs(value) || path.Clean(value) != value || value == "/" {
		return fmt.Errorf("%s must be a clean absolute path", flagName)
	}
	return nil
}

func posixShellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func resolvedServeConfig(cmd *cobra.Command) (mcp.Config, error) {
	var overrides resolvedConfigOverrides
	profileName, err := cmd.Flags().GetString("profile")
	if err != nil {
		return mcp.Config{}, err
	}
	if profileName != "" {
		profile, err := mcp.ParseProfile(profileName)
		if err != nil {
			return mcp.Config{}, err
		}
		overrides.Profile = &profile
	}
	return resolvedConfigWithOverrides(overrides)
}

func bindServeFlags(cmd *cobra.Command) {
	cmd.Flags().String("profile", "", "override MCP profile (monitor or diagnostic)")
}

func bindConfigFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&configProfile, "profile", "", "include an explicit MCP profile (monitor or diagnostic)")
	cmd.Flags().StringVar(&configSSHTarget, "ssh", "", "use SSH for this host or alias")
	cmd.Flags().StringVar(&configRemoteBinary, "remote-binary", "", "absolute path to the Mithril binary used with --ssh")
	cmd.Flags().StringVar(&configRemoteConfig, "remote-config", "", "absolute node config path used with --ssh")
}

func init() {
	bindServeFlags(&MCPCmd)
	bindServeFlags(&serveCmd)
	bindConfigFlags(&configCmd)
	setupCmd.Flags().StringVar(&configSSHTarget, "ssh", "", "use SSH for this host or alias")
	setupCmd.Flags().StringVar(&configRemoteBinary, "remote-binary", "", "absolute path to the Mithril binary used with --ssh")
	setupCmd.Flags().StringVar(&configRemoteConfig, "remote-config", "", "absolute node config path used with --ssh")
	MCPCmd.AddCommand(&serveCmd)
	MCPCmd.AddCommand(&configCmd)
	MCPCmd.AddCommand(&setupCmd)
}
