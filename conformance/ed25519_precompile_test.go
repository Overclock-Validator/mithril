package conformance

import "testing"

func TestConformance_Precompile_Ed25519_Program(t *testing.T) {
	runPrecompileFixtures(t, "ed25519",
		loadPrecompileFixtures(t, ed25519PrecompileProgram))
}
