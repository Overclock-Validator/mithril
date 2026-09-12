package node

import (
	"bytes"
	"os"
	"os/exec"
	"strings"
	"testing"
)

const panicRedactionHelperEnv = "MITHRIL_TEST_PANIC_REDACTION"

func TestReplayPanicReplacementDoesNotLeakOriginal(t *testing.T) {
	if os.Getenv(panicRedactionHelperEnv) == "1" {
		captured, recovered := captureSanitizedPanic(func() {
			panic("replay failed at https://rpc.example.com/private?api-key=TOP_SECRET")
		})
		if !recovered {
			panic("test panic was not recovered")
		}
		panic(captured.Value)
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestReplayPanicReplacementDoesNotLeakOriginal$")
	cmd.Env = append(os.Environ(), panicRedactionHelperEnv+"=1")
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("helper process did not panic")
	}
	if bytes.Contains(output, []byte("TOP_SECRET")) || bytes.Contains(output, []byte("[recovered]")) {
		t.Fatalf("panic output retained the recovered secret-bearing value:\n%s", output)
	}
	if !bytes.Contains(output, []byte("panic: replay failed at https://rpc.example.com")) {
		t.Fatalf("panic output is missing the sanitized replacement:\n%s", output)
	}
}

func panicFromRecoveryTest() {
	panic("replay failed at https://rpc.example.com/private?api-key=STACK_SECRET")
}

func TestCaptureSanitizedPanicPreservesOriginalStack(t *testing.T) {
	captured, recovered := captureSanitizedPanic(panicFromRecoveryTest)
	if !recovered {
		t.Fatal("test panic was not recovered")
	}
	if !strings.Contains(captured.Stack, "panicFromRecoveryTest") {
		t.Fatalf("captured stack is missing the faulting frame:\n%s", captured.Stack)
	}
	if strings.Contains(captured.Value+captured.Stack, "STACK_SECRET") {
		t.Fatalf("captured panic leaked a credential: %+v", captured)
	}
	if len(captured.Stack) > maxSanitizedPanicStackBytes {
		t.Fatalf("captured stack length = %d, want at most %d", len(captured.Stack), maxSanitizedPanicStackBytes)
	}
}

func TestSanitizePanicStackRedactsAndBounds(t *testing.T) {
	raw := []byte("frame https://rpc.example.com/private?api-key=STACK_SECRET\n" + strings.Repeat("x", maxSanitizedPanicStackBytes))
	got := sanitizePanicStack(raw)
	if strings.Contains(got, "STACK_SECRET") || !strings.Contains(got, "https://rpc.example.com") {
		t.Fatalf("stack sanitization failed: %q", got)
	}
	if len(got) > maxSanitizedPanicStackBytes || !strings.HasSuffix(got, "...[truncated]") {
		t.Fatalf("bounded stack length/suffix = %d/%q", len(got), got[len(got)-min(len(got), 32):])
	}
}
