//go:build linux

package replay

import "os"

// readPressureTotals reads Linux PSI. ok is false when PSI is unavailable
// (kernel without CONFIG_PSI, or psi=0).
func readPressureTotals() (pressureTotals, bool) {
	var t pressureTotals
	for _, f := range []struct {
		path string
		dst  *uint64
	}{{"/proc/pressure/cpu", &t.cpu}, {"/proc/pressure/io", &t.io}, {"/proc/pressure/memory", &t.memory}} {
		data, err := os.ReadFile(f.path)
		if err != nil {
			return pressureTotals{}, false
		}
		v, ok := parsePressureSomeTotal(data)
		if !ok {
			return pressureTotals{}, false
		}
		*f.dst = v
	}
	return t, true
}
