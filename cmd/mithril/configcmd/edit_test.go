package configcmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestEditor_ScrLightbringerQuiet sets m.lbQuiet from a menu selection.
func TestEditor_ScrLightbringerQuiet_True(t *testing.T) {
	m := &editModel{screen: edScrLightbringerQuiet, lbEnabled: true, lbQuiet: false}
	m.handleSelect("true")
	assert.True(t, m.lbQuiet, "picking 'true' should set lbQuiet=true")
}

func TestEditor_ScrLightbringerQuiet_False(t *testing.T) {
	m := &editModel{screen: edScrLightbringerQuiet, lbEnabled: true, lbQuiet: true}
	m.handleSelect("false")
	assert.False(t, m.lbQuiet, "picking 'false' should set lbQuiet=false")
}

// TestEditor_DisableLB_ResetsQuiet verifies that "disable" resets m.lbQuiet to
// the default so a later re-enable starts clean (no stale quiet state carried over).
func TestEditor_DisableLB_ResetsQuietDefault(t *testing.T) {
	m := &editModel{
		screen:    edScrLightbringer,
		lbEnabled: true,
		lbQuiet:   true, // previously enabled quiet
	}
	m.handleSelect("disable")
	assert.False(t, m.lbEnabled, "disable should set lbEnabled=false")
	assert.True(t, m.lbQuiet, "disable should reset lbQuiet to the default to avoid stale state on re-enable")
}

// TestEditor_EnableLB_PreservesQuiet ensures enable does not clobber quiet.
func TestEditor_EnableLB_PreservesQuiet(t *testing.T) {
	m := &editModel{
		screen:    edScrLightbringer,
		lbEnabled: false,
		lbQuiet:   true,
	}
	m.handleSelect("enable")
	assert.True(t, m.lbEnabled)
	assert.True(t, m.lbQuiet, "enable should not modify lbQuiet")
}

// TestEditor_QuietMenuItem_ShownOnlyWhenLBEnabled verifies the conditional
// menu rendering — the "Quiet logs" entry must not appear when LB is off.
func TestEditor_QuietMenuItem_HiddenWhenLBDisabled(t *testing.T) {
	m := editModel{screen: edScrLightbringer, lbEnabled: false}
	items := m.currentItems()
	for _, it := range items {
		assert.NotEqual(t, "quiet", it.value, "Quiet logs entry must not appear when lbEnabled=false")
	}
}

func TestEditor_QuietMenuItem_ShownWhenLBEnabled(t *testing.T) {
	m := editModel{screen: edScrLightbringer, lbEnabled: true}
	items := m.currentItems()
	found := false
	for _, it := range items {
		if it.value == "quiet" {
			found = true
			break
		}
	}
	assert.True(t, found, "Quiet logs entry should appear when lbEnabled=true")
}

func TestFormatTOMLValueRejectsNumericPrefixes(t *testing.T) {
	assert.Equal(t, `"203.0.113.5"`, formatTOMLValue("203.0.113.5"))
	assert.Equal(t, `"12ms"`, formatTOMLValue("12ms"))
	assert.Equal(t, "12.5", formatTOMLValue("12.5"))
}
