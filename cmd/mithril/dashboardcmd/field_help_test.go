package dashboardcmd

import (
	"strings"
	"testing"
)

// Every non-separator editable field has plain-language help.
func TestFieldHelp_CoversEveryEditableField(t *testing.T) {
	m := newModel("config.toml") // same field list the UI uses
	for _, f := range m.editFields {
		if f.isSep {
			continue
		}
		if h := fieldHelp(f.section, f.key); strings.TrimSpace(h) == "" {
			t.Errorf("no plain-language help for %s.%s (%q)", f.section, f.key, f.label)
		}
	}
}

func TestFieldHelp_FlagsFirewallSensitiveFields(t *testing.T) {
	// RPC port and Lightbringer UDP ports have firewall implications.
	for _, kv := range [][2]string{{"rpc", "port"}, {"lightbringer", "gossip_port"}, {"lightbringer", "port_range_start"}} {
		if !strings.Contains(strings.ToLower(fieldHelp(kv[0], kv[1])), "firewall") {
			t.Errorf("%s.%s help should mention firewall", kv[0], kv[1])
		}
	}
}
