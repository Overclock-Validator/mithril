package configcmd

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

// pasteMsg builds the KeyRunes message bubbletea delivers for a paste.
func pasteMsg(s string) tea.KeyMsg {
	return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(s), Paste: true}
}

// A multi-rune paste is inserted as a whole string.
func TestUpdateInput_PasteInserted(t *testing.T) {
	m := editModel{inputVal: "", inputCur: 0}
	out, _ := m.updateInput(pasteMsg("203.0.113.10:8000"))
	got := out.(editModel)
	if got.inputVal != "203.0.113.10:8000" {
		t.Fatalf("paste not inserted: got %q", got.inputVal)
	}
	if got.inputCur != len("203.0.113.10:8000") {
		t.Errorf("cursor = %d, want %d", got.inputCur, len("203.0.113.10:8000"))
	}
}

func TestUpdateInput_PasteIntoMiddle(t *testing.T) {
	m := editModel{inputVal: "abXY", inputCur: 2}
	out, _ := m.updateInput(pasteMsg("CD"))
	got := out.(editModel)
	if got.inputVal != "abCDXY" {
		t.Errorf("mid-insert wrong: got %q want abCDXY", got.inputVal)
	}
	if got.inputCur != 4 {
		t.Errorf("cursor = %d, want 4 (after inserted CD)", got.inputCur)
	}
}

func TestUpdateInput_PasteAllControlIsNoOp(t *testing.T) {
	m := editModel{inputVal: "abc", inputCur: 3}
	out, _ := m.updateInput(pasteMsg("\n\t\x1b\x00"))
	got := out.(editModel)
	if got.inputVal != "abc" || got.inputCur != 3 {
		t.Errorf("all-control paste should be a no-op: got %q cur %d", got.inputVal, got.inputCur)
	}
}

// Embedded newlines are stripped from a paste (TOML-injection guard).
func TestUpdateInput_PasteStripsNewlineInjection(t *testing.T) {
	m := editModel{}
	out, _ := m.updateInput(pasteMsg("1.2.3.4:8000\nadmin = \"evil\""))
	got := out.(editModel).inputVal
	if strings.ContainsAny(got, "\n\r") {
		t.Fatalf("newline survived paste (TOML-injection risk): %q", got)
	}
}

// Pasted ANSI/ESC bytes are stripped.
func TestUpdateInput_PasteStripsEscape(t *testing.T) {
	m := editModel{}
	out, _ := m.updateInput(pasteMsg("ip\x1b[31mX"))
	if got := out.(editModel).inputVal; strings.ContainsRune(got, 0x1b) {
		t.Fatalf("ESC survived paste: %q", got)
	}
}

// A single typed character inserts.
func TestUpdateInput_SingleCharStillWorks(t *testing.T) {
	m := editModel{inputVal: "ab", inputCur: 2}
	out, _ := m.updateInput(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("c")})
	if got := out.(editModel).inputVal; got != "abc" {
		t.Errorf("typing broke: got %q want abc", got)
	}
}
