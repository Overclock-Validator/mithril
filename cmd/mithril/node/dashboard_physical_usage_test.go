package node

import "testing"

func TestPhysicalCensusExplicitOptIn(t *testing.T) {
	t.Setenv("MITHRIL_DASHBOARD_PHYSICAL_CENSUS", "")
	if physicalCensusEnabled() {
		t.Fatal("default enabled physical census")
	}
	t.Setenv("MITHRIL_DASHBOARD_PHYSICAL_CENSUS", "true")
	if physicalCensusEnabled() {
		t.Fatal("unexpected value enabled physical census")
	}
	t.Setenv("MITHRIL_DASHBOARD_PHYSICAL_CENSUS", "1")
	if !physicalCensusEnabled() {
		t.Fatal("explicit opt-in ignored")
	}
}
