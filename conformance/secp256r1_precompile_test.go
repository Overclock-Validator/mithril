package conformance

import "testing"

// secp256r1 is the newest precompile and carries the largest fixture set in the
// corpus, but had no conformance test at all until now.
func TestConformance_Precompile_Secp256r1_Program(t *testing.T) {
	runPrecompileFixtures(t, "secp256r1",
		loadPrecompileFixtures(t, secp256r1PrecompileProgram))
}
