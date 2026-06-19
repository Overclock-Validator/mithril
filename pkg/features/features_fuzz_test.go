package features

import (
	"bytes"
	"crypto/rand"
	"testing"
)

// Fuzzes FeatureGate creation and comparison
func FuzzFeatureGateCreation(f *testing.F) {
	// Seed with various name and address combinations
	f.Add("test_feature", make([]byte, 32))
	f.Add("", make([]byte, 32)) // Empty name
	f.Add("very_long_feature_name_that_is_descriptive", make([]byte, 32))

	zeroAddr := make([]byte, 32)
	f.Add("zero_address", zeroAddr)

	maxAddr := make([]byte, 32)
	for i := range maxAddr {
		maxAddr[i] = 0xff
	}
	f.Add("max_address", maxAddr)

	f.Fuzz(func(t *testing.T, name string, addrBytes []byte) {
		// Ensure address is exactly 32 bytes
		if len(addrBytes) != 32 {
			if len(addrBytes) < 32 {
				addrBytes = append(addrBytes, make([]byte, 32-len(addrBytes))...)
			} else {
				addrBytes = addrBytes[:32]
			}
		}

		var addr [32]byte
		copy(addr[:], addrBytes)

		gate := FeatureGate{
			Name:    name,
			Address: addr,
		}

		// Verify fields are preserved
		if gate.Name != name {
			t.Error("Feature gate name not preserved")
		}
		if !bytes.Equal(gate.Address[:], addr[:]) {
			t.Error("Feature gate address not preserved")
		}

		// Verify gate can be used as map key
		testMap := make(map[FeatureGate]bool)
		testMap[gate] = true
		if !testMap[gate] {
			t.Error("Feature gate cannot be used as map key")
		}

		// Verify same address produces equal gates (name may differ)
		gate2 := FeatureGate{
			Name:    name + "_different",
			Address: addr,
		}
		// Gates with same address but different names should have same address
		if !bytes.Equal(gate.Address[:], gate2.Address[:]) {
			t.Error("Gates with same address bytes should have equal addresses")
		}
	})
}

// Fuzzes Features map operations including enable, disable, and activation checks
func FuzzFeaturesEnableDisable(f *testing.F) {
	// Seed with various activation slots
	f.Add(uint64(0))
	f.Add(uint64(1))
	f.Add(uint64(1000000))
	f.Add(uint64(0xFFFFFFFFFFFFFFFF)) // max uint64

	f.Fuzz(func(t *testing.T, activationSlot uint64) {
		features := NewFeaturesDefault()

		// Create test feature gate
		var addr [32]byte
		rand.Read(addr[:])
		gate := FeatureGate{
			Name:    "test_feature",
			Address: addr,
		}

		// Initially feature should not be active
		if features.IsActive(gate) {
			t.Error("New feature should not be active initially")
		}

		slot, ok := features.ActivationSlot(gate)
		if ok {
			t.Error("Inactive feature should not return activation slot")
		}
		if slot != 0 {
			t.Errorf("Inactive feature should return slot 0, got %d", slot)
		}

		// Enable the feature
		features.EnableFeature(gate, activationSlot)

		// Feature should now be active
		if !features.IsActive(gate) {
			t.Error("Enabled feature should be active")
		}

		// Activation slot should be retrievable
		slot, ok = features.ActivationSlot(gate)
		if !ok {
			t.Error("Active feature should return activation slot")
		}
		if slot != activationSlot {
			t.Errorf("Activation slot mismatch: got %d, want %d", slot, activationSlot)
		}

		// Disable the feature
		features.DisableFeature(gate)

		// Feature should no longer be active
		if features.IsActive(gate) {
			t.Error("Disabled feature should not be active")
		}

		// Activation slot should not be available for disabled feature
		slot, ok = features.ActivationSlot(gate)
		if ok {
			t.Error("Disabled feature should not return activation slot")
		}

		// Re-enable with different slot
		newSlot := activationSlot + 1
		features.EnableFeature(gate, newSlot)

		if !features.IsActive(gate) {
			t.Error("Re-enabled feature should be active")
		}

		slot, ok = features.ActivationSlot(gate)
		if !ok || slot != newSlot {
			t.Errorf("Re-enabled feature should have new activation slot %d, got %d (ok=%v)",
				newSlot, slot, ok)
		}
	})
}

// Fuzzes multiple feature gates simultaneously to test map consistency
func FuzzMultipleFeatures(f *testing.F) {
	// Seed with various feature counts
	f.Add(uint8(1))
	f.Add(uint8(5))
	f.Add(uint8(20))
	f.Add(uint8(100))

	f.Fuzz(func(t *testing.T, numFeatures uint8) {
		// Limit to prevent OOM
		if numFeatures == 0 || numFeatures > 128 {
			t.Skip("Invalid feature count")
		}

		features := NewFeaturesDefault()

		// Create multiple feature gates
		gates := make([]FeatureGate, numFeatures)
		expectedEnabled := make(map[FeatureGate]bool)

		for i := uint8(0); i < numFeatures; i++ {
			var addr [32]byte
			rand.Read(addr[:])
			gates[i] = FeatureGate{
				Name:    "feature_" + string(rune(i)),
				Address: addr,
			}

			// Enable even-indexed features
			if i%2 == 0 {
				slot := uint64(i) * 1000
				features.EnableFeature(gates[i], slot)
				expectedEnabled[gates[i]] = true
			}
		}

		// Verify each feature has correct state
		for i, gate := range gates {
			isActive := features.IsActive(gate)
			shouldBeActive := (i%2 == 0)

			if isActive != shouldBeActive {
				t.Errorf("Feature %d: IsActive=%v, expected %v", i, isActive, shouldBeActive)
			}

			if shouldBeActive {
				slot, ok := features.ActivationSlot(gate)
				if !ok {
					t.Errorf("Feature %d: should have activation slot", i)
				}
				expectedSlot := uint64(i) * 1000
				if slot != expectedSlot {
					t.Errorf("Feature %d: activation slot=%d, expected %d", i, slot, expectedSlot)
				}
			}
		}

		// Verify AllEnabled() returns correct count
		enabledStrs := features.AllEnabled()
		enabledCount := len(enabledStrs)
		expectedCount := (int(numFeatures) + 1) / 2 // Ceiling division for even count

		if enabledCount != expectedCount {
			t.Errorf("AllEnabled returned %d features, expected %d", enabledCount, expectedCount)
		}
	})
}

// Fuzzes FeatureGate address uniqueness and collision detection
func FuzzFeatureGateAddressUniqueness(f *testing.F) {
	// Seed with address patterns
	f.Add(byte(0x00), byte(0xFF))
	f.Add(byte(0x01), byte(0x01))
	f.Add(byte(0xAB), byte(0xCD))

	f.Fuzz(func(t *testing.T, b1, b2 byte) {
		// Create two addresses differing only in first two bytes
		var addr1, addr2 [32]byte
		addr1[0], addr1[1] = b1, b2
		addr2[0], addr2[1] = b2, b1

		gate1 := FeatureGate{Name: "feature1", Address: addr1}
		gate2 := FeatureGate{Name: "feature2", Address: addr2}

		features := NewFeaturesDefault()

		// Enable both features with different slots
		features.EnableFeature(gate1, 1000)
		features.EnableFeature(gate2, 2000)

		// If addresses are different, features should be independent
		if !bytes.Equal(addr1[:], addr2[:]) {
			slot1, ok1 := features.ActivationSlot(gate1)
			slot2, ok2 := features.ActivationSlot(gate2)

			if !ok1 || !ok2 {
				t.Error("Both features should be active")
			}

			if slot1 != 1000 {
				t.Errorf("Feature 1 slot should be 1000, got %d", slot1)
			}
			if slot2 != 2000 {
				t.Errorf("Feature 2 slot should be 2000, got %d", slot2)
			}
		} else {
			// If addresses are same, second EnableFeature should overwrite first
			slot, ok := features.ActivationSlot(gate2)
			if !ok {
				t.Error("Feature with duplicate address should be active")
			}
			if slot != 2000 {
				t.Errorf("Latest enabled slot should be 2000, got %d", slot)
			}
		}
	})
}

// Fuzzes AllEnabled() output format and consistency
func FuzzAllEnabled(f *testing.F) {
	// Seed with various counts
	f.Add(uint8(0))
	f.Add(uint8(1))
	f.Add(uint8(10))

	f.Fuzz(func(t *testing.T, numEnabled uint8) {
		// Limit to prevent timeout
		if numEnabled > 50 {
			numEnabled = numEnabled % 50
		}

		features := NewFeaturesDefault()

		// Create and enable features
		for i := uint8(0); i < numEnabled; i++ {
			var addr [32]byte
			rand.Read(addr[:])
			gate := FeatureGate{
				Name:    "enabled_" + string(rune(i)),
				Address: addr,
			}
			features.EnableFeature(gate, uint64(i))
		}

		// Create disabled features
		for i := uint8(0); i < numEnabled; i++ {
			var addr [32]byte
			rand.Read(addr[:])
			gate := FeatureGate{
				Name:    "disabled_" + string(rune(i)),
				Address: addr,
			}
			features.DisableFeature(gate)
		}

		// Get all enabled
		enabled := features.AllEnabled()

		// Should return exactly numEnabled features
		if len(enabled) != int(numEnabled) {
			t.Errorf("AllEnabled returned %d features, expected %d", len(enabled), numEnabled)
		}

		// Each string should contain "enabled" (part of feature names)
		for i, str := range enabled {
			if str == "" {
				t.Errorf("AllEnabled[%d] is empty string", i)
			}
			// Verify it's a valid string (no panic on iteration)
			_ = len(str)
		}
	})
}
