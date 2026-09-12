package conformance

import "testing"

func TestConformance_Precompile_Secp256k1_Program(t *testing.T) {
	runPrecompileFixtures(t, "secp256k1",
		loadPrecompileFixtures(t, secp256k1PrecompileProgram))
}
