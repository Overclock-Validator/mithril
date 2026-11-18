//go:build !linux

package replay

func readPressureTotals() (pressureTotals, bool) { return pressureTotals{}, false }
