package replay

import "testing"

func TestOptionalUnrootedState(t *testing.T) {
	var absent *unrootedTail
	if optionalUnrootedState(absent) != nil {
		t.Fatal("absent overlay must remain a nil interface")
	}
	present := &unrootedTail{}
	if optionalUnrootedState(present) != present {
		t.Fatal("live overlay identity must be preserved")
	}
}
