package progress

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// empty path -> usable no-op emitter (feature disabled).
func TestJSONLEmitter_NilPathIsNoOp(t *testing.T) {
	e, err := NewJSONLEmitter("")
	require.NoError(t, err)
	require.NotNil(t, e)
	e.Emit(map[string]any{"phase": "test", "n": 1})
	e.Close()
}

// nil receiver methods must not panic (zero value = disabled).
func TestJSONLEmitter_NilReceiver(t *testing.T) {
	var e *JSONLEmitter
	e.Emit(map[string]any{"x": 1})
	e.Close()
}

// each Emit produces one well-formed JSON line.
func TestJSONLEmitter_AppendsOneLinePerEmit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "progress.jsonl")

	e, err := NewJSONLEmitter(path)
	require.NoError(t, err)
	defer e.Close()

	e.Emit(map[string]any{"phase": "snapshot_download", "done": 100, "total": 1000})
	e.Emit(map[string]any{"phase": "snapshot_download", "done": 500, "total": 1000})
	e.Emit(map[string]any{"phase": "ready"})
	e.Close()

	lines := readAllLines(t, path)
	require.Len(t, lines, 3)

	for i, line := range lines {
		var ev map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &ev),
			"line %d should be valid JSON: %s", i, line)
		assert.NotEmpty(t, ev["phase"], "line %d should have 'phase' field", i)
		assert.NotEmpty(t, ev["ts"], "emitter should stamp 'ts' on every event")
	}
}

// emitter stamps a 'ts' (RFC3339) on every event automatically.
func TestJSONLEmitter_StampsTimestamp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "progress.jsonl")

	e, err := NewJSONLEmitter(path)
	require.NoError(t, err)
	defer e.Close()

	e.Emit(map[string]any{"phase": "x"})
	e.Close()

	lines := readAllLines(t, path)
	require.Len(t, lines, 1)
	var ev map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &ev))
	tsRaw, ok := ev["ts"].(string)
	require.True(t, ok, "ts should be a string")
	assert.Contains(t, tsRaw, "T") // RFC3339 date/time separator
}

// emitter's own 'ts' wins over a caller-supplied one.
func TestJSONLEmitter_CallerCannotOverrideTimestamp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "progress.jsonl")

	e, err := NewJSONLEmitter(path)
	require.NoError(t, err)
	defer e.Close()

	e.Emit(map[string]any{"phase": "x", "ts": "1999-01-01T00:00:00Z"})
	e.Close()

	lines := readAllLines(t, path)
	require.Len(t, lines, 1)
	var ev map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &ev))
	assert.NotEqual(t, "1999-01-01T00:00:00Z", ev["ts"])
}

// concurrent Emits produce well-formed lines (mutex prevents interleaving).
func TestJSONLEmitter_ConcurrentEmitsSerialize(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "progress.jsonl")

	e, err := NewJSONLEmitter(path)
	require.NoError(t, err)

	var wg sync.WaitGroup
	const writers = 8
	const perWriter = 50
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < perWriter; j++ {
				e.Emit(map[string]any{"phase": "x", "writer": id, "seq": j})
			}
		}(i)
	}
	wg.Wait()
	e.Close()

	lines := readAllLines(t, path)
	assert.Equal(t, writers*perWriter, len(lines))
	for i, line := range lines {
		var ev map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &ev),
			"line %d garbled: %q", i, line)
	}
}

// reopening appends (not truncates) so prior-run history survives restart.
func TestJSONLEmitter_AppendsToExistingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "progress.jsonl")

	e1, err := NewJSONLEmitter(path)
	require.NoError(t, err)
	e1.Emit(map[string]any{"phase": "first"})
	e1.Close()

	e2, err := NewJSONLEmitter(path)
	require.NoError(t, err)
	e2.Emit(map[string]any{"phase": "second"})
	e2.Close()

	lines := readAllLines(t, path)
	require.Len(t, lines, 2)
}

func readAllLines(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	var out []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		out = append(out, scanner.Text())
	}
	require.NoError(t, scanner.Err())
	return out
}
