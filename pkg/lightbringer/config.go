package lightbringer

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const configFileName = "Lightbringer.toml"

const (
	defaultGossipPort       = 65400
	defaultPortRangeStart   = 65401
	defaultPortRangeEnd     = 65500
	minValidatorPortWidth   = 25
	validatorQUICPortOffset = 6
)

// LightbringerTOML represents the Lightbringer.toml structure that Lightbringer expects.
// This mirrors the Rust ConfigRaw struct in the Lightbringer source.
type LightbringerTOML struct {
	GossipEntrypoint string
	Storage          string
	RpcAddr          string
	GrpcAddr         string
	GossipPort       int
	PortRangeStart   int
	PortRangeEnd     int

	// Optional sections
	InfluxdbHost     string
	InfluxdbDatabase string
	InfluxdbToken    string

	BlockConfirmRpcHTTP string
	BlockConfirmRpcWS   string

	// Quiet emits [log] quiet = true so Lightbringer logs at Warn level only.
	Quiet bool
}

// Validate checks that required fields are present and well-formed.
func (c *LightbringerTOML) Validate() error {
	if c.GossipEntrypoint == "" {
		return fmt.Errorf("gossip_entrypoint is required")
	}
	if err := validateHostPort(c.GossipEntrypoint, "gossip_entrypoint"); err != nil {
		return err
	}
	if c.GrpcAddr == "" {
		return fmt.Errorf("grpc_addr is required")
	}
	if err := validateHostPort(c.GrpcAddr, "grpc_addr"); err != nil {
		return err
	}
	if c.RpcAddr != "" {
		if err := validateHostPort(c.RpcAddr, "rpc_addr"); err != nil {
			return err
		}
	}
	if err := validateGossipPorts(c.GossipPort, c.PortRangeStart, c.PortRangeEnd); err != nil {
		return err
	}
	if err := validateInfluxDB(c.InfluxdbHost, c.InfluxdbDatabase, c.InfluxdbToken); err != nil {
		return err
	}
	if err := validateBlockConfirmation(c.BlockConfirmRpcHTTP, c.BlockConfirmRpcWS); err != nil {
		return err
	}
	return nil
}

// validateHostPort checks that addr is a valid host:port with a numeric port in range.
func validateHostPort(addr, field string) error {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%s %q is not valid host:port: %w", field, addr, err)
	}
	if host == "" {
		return fmt.Errorf("%s has empty host", field)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("%s port %q is not numeric", field, portStr)
	}
	if port < 1 || port > 65535 {
		return fmt.Errorf("%s port %d is out of range 1-65535", field, port)
	}
	return nil
}

func effectiveGossipPorts(gossipPort, portRangeStart, portRangeEnd int) (int, int, int) {
	if gossipPort == 0 {
		gossipPort = defaultGossipPort
	}
	if portRangeStart == 0 {
		portRangeStart = defaultPortRangeStart
	}
	if portRangeEnd == 0 {
		portRangeEnd = defaultPortRangeEnd
	}
	return gossipPort, portRangeStart, portRangeEnd
}

// ValidateGossipPorts exposes the runtime gossip-port validation so doctor
// enforces the exact same rules the node applies at startup.
func ValidateGossipPorts(gossipPort, portRangeStart, portRangeEnd int) error {
	return validateGossipPorts(gossipPort, portRangeStart, portRangeEnd)
}

func validateGossipPorts(gossipPort, portRangeStart, portRangeEnd int) error {
	effectiveGossipPort, effectiveRangeStart, effectiveRangeEnd := effectiveGossipPorts(gossipPort, portRangeStart, portRangeEnd)
	values := []struct {
		field string
		value int
	}{
		{"gossip.gossip_port", effectiveGossipPort},
		{"gossip.port_range_start", effectiveRangeStart},
		{"gossip.port_range_end", effectiveRangeEnd},
	}
	for _, item := range values {
		if item.value < 1 || item.value > 65535 {
			return fmt.Errorf("%s %d is out of range 1-65535", item.field, item.value)
		}
	}
	if effectiveRangeStart > effectiveRangeEnd {
		return fmt.Errorf("gossip.port_range_start must be <= gossip.port_range_end")
	}
	if effectiveRangeEnd-effectiveRangeStart < minValidatorPortWidth {
		return fmt.Errorf("gossip.port_range_end - gossip.port_range_start must be at least %d", minValidatorPortWidth)
	}
	if effectiveRangeEnd+validatorQUICPortOffset > 65535 {
		return fmt.Errorf("gossip.port_range_end + %d must fit in 65535", validatorQUICPortOffset)
	}
	if effectiveGossipPort >= effectiveRangeStart && effectiveGossipPort <= effectiveRangeEnd {
		return fmt.Errorf("gossip.gossip_port must not overlap gossip.port_range_start..=gossip.port_range_end")
	}
	return nil
}

func validateInfluxDB(host, database, token string) error {
	host = strings.TrimSpace(host)
	database = strings.TrimSpace(database)
	token = strings.TrimSpace(token)
	if host == "" && database == "" && token == "" {
		return nil
	}
	if host == "" {
		return fmt.Errorf("influxdb.host is required when InfluxDB is configured")
	}
	if database == "" {
		return fmt.Errorf("influxdb.database is required when InfluxDB is configured")
	}
	if token == "" {
		return fmt.Errorf("influxdb.token is required when InfluxDB is configured")
	}
	if err := validateURLWithSchemes(host, "influxdb.host", "http", "https"); err != nil {
		return err
	}
	return nil
}

func validateBlockConfirmation(rpcHTTP, rpcWS string) error {
	rpcHTTP = strings.TrimSpace(rpcHTTP)
	rpcWS = strings.TrimSpace(rpcWS)
	if rpcHTTP == "" && rpcWS == "" {
		return nil
	}
	if rpcHTTP == "" {
		return fmt.Errorf("block_confirmation.rpc_http is required when block confirmation is configured")
	}
	if rpcWS == "" {
		return fmt.Errorf("block_confirmation.rpc_websocket is required when block confirmation is configured")
	}
	if err := validateURLWithSchemes(rpcHTTP, "block_confirmation.rpc_http", "http", "https"); err != nil {
		return err
	}
	if err := validateURLWithSchemes(rpcWS, "block_confirmation.rpc_websocket", "ws", "wss"); err != nil {
		return err
	}
	return nil
}

func validateURLWithSchemes(raw, field string, allowedSchemes ...string) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("%s %q is not a valid URL", field, raw)
	}
	for _, scheme := range allowedSchemes {
		if parsed.Scheme == scheme {
			return nil
		}
	}
	return fmt.Errorf("%s must use one of: %s", field, strings.Join(allowedSchemes, ", "))
}

// GenerateTOML produces a valid Lightbringer.toml string from the config.
func (c *LightbringerTOML) GenerateTOML() string {
	var b strings.Builder

	fmt.Fprintf(&b, "gossip_entrypoint = %q\n", c.GossipEntrypoint)
	fmt.Fprintf(&b, "storage = %q\n", c.Storage)
	fmt.Fprintf(&b, "rpc_addr = %q\n", c.RpcAddr)
	fmt.Fprintf(&b, "grpc_addr = %q\n", c.GrpcAddr)

	if c.GossipPort != 0 || c.PortRangeStart != 0 || c.PortRangeEnd != 0 {
		b.WriteString("\n[gossip]\n")
		if c.GossipPort != 0 {
			fmt.Fprintf(&b, "gossip_port = %d\n", c.GossipPort)
		}
		if c.PortRangeStart != 0 {
			fmt.Fprintf(&b, "port_range_start = %d\n", c.PortRangeStart)
		}
		if c.PortRangeEnd != 0 {
			fmt.Fprintf(&b, "port_range_end = %d\n", c.PortRangeEnd)
		}
	}

	// Trim to match Validate(): blank means unconfigured; padded values would emit invalid TOML strings.
	if host := strings.TrimSpace(c.InfluxdbHost); host != "" {
		b.WriteString("\n[influxdb]\n")
		fmt.Fprintf(&b, "host = %q\n", host)
		fmt.Fprintf(&b, "database = %q\n", strings.TrimSpace(c.InfluxdbDatabase))
		fmt.Fprintf(&b, "token = %q\n", strings.TrimSpace(c.InfluxdbToken))
	}

	if rpcHTTP := strings.TrimSpace(c.BlockConfirmRpcHTTP); rpcHTTP != "" {
		b.WriteString("\n[block_confirmation]\n")
		fmt.Fprintf(&b, "rpc_http = %q\n", rpcHTTP)
		fmt.Fprintf(&b, "rpc_websocket = %q\n", strings.TrimSpace(c.BlockConfirmRpcWS))
	}

	// Emit [log] section only when quiet mode is enabled.
	// Lightbringer's default (info) applies when the section is absent, preserving backward compatibility.
	if c.Quiet {
		b.WriteString("\n[log]\nquiet = true\n")
	}

	return b.String()
}

// WriteConfigFile writes Lightbringer.toml to the given directory using an atomic
// write pattern (write to temp file, then rename) to prevent partial writes.
func (c *LightbringerTOML) WriteConfigFile(dir string) (string, error) {
	content := c.GenerateTOML()
	targetPath := filepath.Join(dir, configFileName)

	// Write to temp file in same directory (required for atomic rename on same filesystem).
	// CreateTemp uses mode 0600; we keep this restrictive since the file may contain tokens.
	tmpFile, err := os.CreateTemp(dir, "Lightbringer.toml.tmp.*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp config file: %w", err)
	}
	// Ensure restrictive permissions regardless of umask
	if err := tmpFile.Chmod(0600); err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		return "", fmt.Errorf("failed to set config file permissions: %w", err)
	}
	tmpPath := tmpFile.Name()

	if _, err := tmpFile.WriteString(content); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return "", fmt.Errorf("failed to write config content: %w", err)
	}

	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return "", fmt.Errorf("failed to sync config file: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpPath)
		return "", fmt.Errorf("failed to close temp config file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tmpPath, targetPath); err != nil {
		os.Remove(tmpPath)
		return "", fmt.Errorf("failed to rename config file: %w", err)
	}

	return targetPath, nil
}
