//go:build !linux && !darwin

package accountsdb

import "context"

type physicalCensusTestHooks struct{}

func observePhysicalUsage(context.Context, []string, PhysicalUsageLimits, *physicalCensusTestHooks) (PhysicalUsage, string) {
	return PhysicalUsage{}, "unsupported_platform"
}
