package transactionhistory

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"math"
	"runtime"
	"strings"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func testMetadata() *Metadata {
	return &Metadata{
		AccountKeys: []solana.PublicKey{{1}}, Fee: 5, ComputeUnitsConsumed: 6,
		Error: json.RawMessage("null"), PreBalances: []uint64{2}, PostBalances: []uint64{3},
		InnerInstructions: []InnerInstructionGroup{{Index: 0, Instructions: []InnerInstruction{{
			ProgramIDIndex: 0, Accounts: []uint8{0}, Data: []byte{7}, StackHeight: 2,
		}}}},
		PreTokenBalances:  []TokenBalance{{AccountIndex: 0, Mint: solana.PublicKey{2}, Owner: solana.PublicKey{3}, ProgramID: solana.PublicKey{4}, Amount: 8, Decimals: 20}},
		PostTokenBalances: []TokenBalance{{AccountIndex: 0, Mint: solana.PublicKey{2}, Owner: solana.PublicKey{3}, ProgramID: solana.PublicKey{4}, Amount: 9, Decimals: 255}},
		ReturnData:        &ReturnData{ProgramID: solana.PublicKey{5}, Data: []byte{10}},
	}
}

func TestMetadataExactBinaryLayout(t *testing.T) {
	m := testMetadata()
	m.InnerInstructions, m.PreTokenBalances, m.PostTokenBalances, m.ReturnData = nil, nil, nil, nil
	wire, err := m.MarshalBinary()
	require.NoError(t, err)
	// Independent fixed-width layout: version, fee, CU, error vector, one
	// key/pre/post row, three empty counts, and the absent-return tag.
	want := make([]byte, 89)
	want[0], want[4], want[12], want[20] = 1, 5, 6, 4
	copy(want[28:], "null")
	want[32], want[34], want[66], want[74] = 1, 1, 2, 3
	require.Equal(t, want, wire)
	require.Equal(t, len(want), cap(wire))
	size, err := m.encodedSize()
	require.NoError(t, err)
	require.Equal(t, len(want), size)

	m = testMetadata()
	wire, err = m.MarshalBinary()
	require.NoError(t, err)
	// One group (3), one instruction (21 + 1 + 1), two token rows
	// (2 * 107), and a present return value (32 + 8 + 1).
	require.Len(t, wire, 89+3+23+214+41)
	require.Equal(t, len(wire), cap(wire))
	size, err = m.encodedSize()
	require.NoError(t, err)
	require.Equal(t, len(wire), size)
	decoded, err := UnmarshalMetadata(wire)
	require.NoError(t, err)
	require.Equal(t, m, decoded)
}

func TestMetadataMaximumScalarRoundTrip(t *testing.T) {
	m := testMetadata()
	m.Fee, m.ComputeUnitsConsumed = math.MaxUint64, math.MaxUint64
	m.Error = json.RawMessage(`"` + strings.Repeat("x", metadataErrorBytes-2) + `"`)
	m.AccountKeys = make([]solana.PublicKey, metadataAccounts)
	m.PreBalances, m.PostBalances = make([]uint64, metadataAccounts), make([]uint64, metadataAccounts)
	m.PreTokenBalances, m.PostTokenBalances = make([]TokenBalance, metadataAccounts), make([]TokenBalance, metadataAccounts)
	for i := range m.AccountKeys {
		m.AccountKeys[i][0] = byte(i)
		m.PreBalances[i], m.PostBalances[i] = math.MaxUint64, math.MaxUint64
		balance := TokenBalance{AccountIndex: uint16(i), Mint: solana.PublicKey{1, byte(i)}, Owner: solana.PublicKey{2, byte(i)}, ProgramID: solana.PublicKey{3, byte(i)}, Amount: math.MaxUint64, Decimals: byte(i)}
		m.PreTokenBalances[i], m.PostTokenBalances[i] = balance, balance
	}
	m.InnerInstructions = make([]InnerInstructionGroup, metadataCPIEntries)
	for i := range m.InnerInstructions {
		m.InnerInstructions[i] = InnerInstructionGroup{Index: uint8(i * 4), Instructions: []InnerInstruction{{
			ProgramIDIndex: 255, Accounts: bytes.Repeat([]byte{255}, 255), Data: bytes.Repeat([]byte{255}, metadataCPIDataBytes), StackHeight: math.MaxUint32,
		}}}
	}
	m.InnerInstructions[len(m.InnerInstructions)-1].Index = 255
	m.ReturnData = &ReturnData{ProgramID: solana.PublicKey{255}, Data: bytes.Repeat([]byte{255}, metadataReturnBytes)}
	wire, err := m.MarshalBinary()
	require.NoError(t, err)
	// All independently bounded collections at their maxima still fit the
	// outer 1 MiB cap. The 10 KiB per-CPI cap makes exactly 1 MiB unreachable.
	require.Len(t, wire, 743437)
	size, err := m.encodedSize()
	require.NoError(t, err)
	require.Equal(t, size, len(wire))
	require.Equal(t, size, cap(wire))
	got, err := UnmarshalMetadata(wire)
	require.NoError(t, err)
	require.Equal(t, m, got)
	again, err := got.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, wire, again)
}

func TestMetadataEmptyCollectionsAndReturnPresence(t *testing.T) {
	var absent, present []byte
	for _, name := range []string{"absent", "nil-data", "empty-data"} {
		t.Run(name, func(t *testing.T) {
			m := testMetadata()
			m.InnerInstructions, m.PreTokenBalances, m.PostTokenBalances, m.ReturnData = nil, nil, nil, nil
			if name != "absent" {
				m.ReturnData = &ReturnData{}
				if name == "empty-data" {
					m.ReturnData.Data = []byte{}
				}
			}
			wire, err := m.MarshalBinary()
			require.NoError(t, err)
			got, err := UnmarshalMetadata(wire)
			require.NoError(t, err)
			require.NotNil(t, got.InnerInstructions)
			require.Empty(t, got.InnerInstructions)
			require.NotNil(t, got.PreTokenBalances)
			require.Empty(t, got.PreTokenBalances)
			require.NotNil(t, got.PostTokenBalances)
			require.Empty(t, got.PostTokenBalances)
			if name == "absent" {
				require.Nil(t, got.ReturnData)
				absent = wire
			} else {
				require.NotNil(t, got.ReturnData)
				require.NotNil(t, got.ReturnData.Data)
				require.Empty(t, got.ReturnData.Data)
				if present != nil {
					require.Equal(t, present, wire, "nil and empty data have the same present-return encoding")
				}
				present = wire
			}
		})
	}
	require.NotEqual(t, absent, present)
	require.Equal(t, len(absent)+40, len(present))

	m := testMetadata()
	m.InnerInstructions[0].Instructions[0].Accounts = nil
	m.InnerInstructions[0].Instructions[0].Data = nil
	wire, err := m.MarshalBinary()
	require.NoError(t, err)
	got, err := UnmarshalMetadata(wire)
	require.NoError(t, err)
	require.NotNil(t, got.InnerInstructions[0].Instructions[0].Accounts)
	require.Empty(t, got.InnerInstructions[0].Instructions[0].Accounts)
	require.NotNil(t, got.InnerInstructions[0].Instructions[0].Data)
	require.Empty(t, got.InnerInstructions[0].Instructions[0].Data)
}

func TestMetadataOwnsEncodedAndDecodedData(t *testing.T) {
	m := testMetadata()
	wire, err := m.MarshalBinary()
	require.NoError(t, err)
	wantWire := bytes.Clone(wire)
	m.AccountKeys[0][0]++
	m.Error[0] = 'x'
	m.PreBalances[0]++
	m.PostBalances[0]++
	m.InnerInstructions[0].Index++
	m.InnerInstructions[0].Instructions[0].Accounts[0]++
	m.InnerInstructions[0].Instructions[0].Data[0]++
	m.PreTokenBalances[0].Mint[0]++
	m.PostTokenBalances[0].Amount++
	m.ReturnData.ProgramID[0]++
	m.ReturnData.Data[0]++
	require.Equal(t, wantWire, wire)
	got, err := UnmarshalMetadata(wire)
	require.NoError(t, err)
	for i := range wire {
		wire[i] ^= 255
	}
	require.Equal(t, testMetadata(), got)
	again, err := got.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, wantWire, again)
}

func TestMetadataStructuralErrorContract(t *testing.T) {
	// The adapter supplies TransactionError JSON. The codec deliberately does
	// not duplicate Solana's enum or reject future string/object variants.
	for _, value := range []string{"null", " \nnull\t", `"FutureError"`, `{}`, `{"FutureError":{"value":1}}`, `"` + strings.Repeat("x", metadataErrorBytes-2) + `"`} {
		m := testMetadata()
		m.Error = json.RawMessage(value)
		wire, err := m.MarshalBinary()
		require.NoError(t, err)
		got, err := UnmarshalMetadata(wire)
		require.NoError(t, err)
		require.Equal(t, m.Error, got.Error)
	}
	for _, value := range []string{"", " ", "nul", "null null", "true", "1", "[]", "{} trailing", `"unterminated`} {
		m := testMetadata()
		m.Error = json.RawMessage(value)
		wire, err := m.MarshalBinary()
		require.Error(t, err, value)
		require.Nil(t, wire)
	}
}

func TestMetadataMarshalRejectsInvalidBoundsAndOrder(t *testing.T) {
	cases := []struct {
		name   string
		change func(*Metadata)
	}{
		{"no-keys", func(m *Metadata) { m.AccountKeys = nil }},
		{"too-many-keys", func(m *Metadata) { m.AccountKeys = make([]solana.PublicKey, metadataAccounts+1) }},
		{"pre-count", func(m *Metadata) { m.PreBalances = nil }},
		{"post-count", func(m *Metadata) { m.PostBalances = nil }},
		{"error-bytes", func(m *Metadata) { m.Error = json.RawMessage(`"` + strings.Repeat("x", metadataErrorBytes-1) + `"`) }},
		{"groups", func(m *Metadata) { m.InnerInstructions = make([]InnerInstructionGroup, metadataCPIEntries+1) }},
		{"empty-group", func(m *Metadata) { m.InnerInstructions[0].Instructions = nil }},
		{"group-duplicate", func(m *Metadata) { m.InnerInstructions = append(m.InnerInstructions, m.InnerInstructions[0]) }},
		{"group-reversed", func(m *Metadata) {
			m.InnerInstructions = append(m.InnerInstructions, m.InnerInstructions[0])
			m.InnerInstructions[0].Index = 1
		}},
		{"instructions", func(m *Metadata) {
			m.InnerInstructions[0].Instructions = make([]InnerInstruction, metadataCPIEntries+1)
		}},
		{"total-instructions", func(m *Metadata) {
			m.InnerInstructions[0].Instructions = make([]InnerInstruction, metadataCPIEntries)
			m.InnerInstructions = append(m.InnerInstructions, InnerInstructionGroup{Index: 1, Instructions: []InnerInstruction{{}}})
		}},
		{"program-index", func(m *Metadata) { m.InnerInstructions[0].Instructions[0].ProgramIDIndex = 1 }},
		{"account-index", func(m *Metadata) { m.InnerInstructions[0].Instructions[0].Accounts[0] = 1 }},
		{"account-count", func(m *Metadata) { m.InnerInstructions[0].Instructions[0].Accounts = make([]uint8, 256) }},
		{"instruction-data", func(m *Metadata) { m.InnerInstructions[0].Instructions[0].Data = make([]byte, MaxMetadataBytes+1) }},
		{"instruction-data-capture-limit", func(m *Metadata) { m.InnerInstructions[0].Instructions[0].Data = make([]byte, metadataCPIDataBytes+1) }},
		{"pre-token-index", func(m *Metadata) { m.PreTokenBalances[0].AccountIndex = 1 }},
		{"post-token-index", func(m *Metadata) { m.PostTokenBalances[0].AccountIndex = 1 }},
		{"token-count", func(m *Metadata) { m.PreTokenBalances = append(m.PreTokenBalances, m.PreTokenBalances[0]) }},
		{"return-data", func(m *Metadata) { m.ReturnData.Data = make([]byte, metadataReturnBytes+1) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := testMetadata()
			tc.change(m)
			wire, err := m.MarshalBinary()
			require.Error(t, err)
			require.Nil(t, wire)
		})
	}
	var absent *Metadata
	wire, err := absent.MarshalBinary()
	require.Error(t, err)
	require.Nil(t, wire)
	for _, post := range []bool{false, true} {
		for _, order := range [][]uint16{{0, 0}, {1, 0}} {
			m := testMetadata()
			m.AccountKeys = append(m.AccountKeys, solana.PublicKey{2})
			m.PreBalances, m.PostBalances = append(m.PreBalances, 1), append(m.PostBalances, 1)
			balances := []TokenBalance{{AccountIndex: order[0]}, {AccountIndex: order[1]}}
			if post {
				m.PostTokenBalances = balances
			} else {
				m.PreTokenBalances = balances
			}
			wire, err := m.MarshalBinary()
			require.Error(t, err)
			require.Nil(t, wire)
		}
	}
}

func TestMetadataExactSizeLimitAndEarlyRefusal(t *testing.T) {
	m := testMetadata()
	m.InnerInstructions[0].Instructions[0].Data = nil
	base, err := m.encodedSize()
	require.NoError(t, err)
	m.InnerInstructions[0].Instructions[0].Data = make([]byte, metadataCPIDataBytes)
	wire, err := m.MarshalBinary()
	require.NoError(t, err)
	require.Len(t, wire, base+metadataCPIDataBytes)
	require.Equal(t, len(wire), cap(wire))
	got, err := UnmarshalMetadata(wire)
	require.NoError(t, err)
	require.Equal(t, m, got)

	// Preserve the complete surrounding record while supplying one byte beyond
	// the CPI ceiling. A remaining-bytes check alone must not accept this input.
	dataEnd := 109 + metadataCPIDataBytes
	bad := append(bytes.Clone(wire[:dataEnd]), 0)
	bad = append(bad, wire[dataEnd:]...)
	binary.LittleEndian.PutUint64(bad[101:], metadataCPIDataBytes+1)
	got, err = UnmarshalMetadata(bad)
	require.Error(t, err)
	require.Nil(t, got)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	got, err = UnmarshalMetadata(bad)
	runtime.ReadMemStats(&after)
	require.Error(t, err)
	require.Nil(t, got)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(8<<10), "oversized CPI payload allocated before its capture limit")

	m.InnerInstructions[0].Instructions[0].Data = append(m.InnerInstructions[0].Instructions[0].Data, 0)
	var rejected []byte
	var refusal error
	allocs := testing.AllocsPerRun(10, func() { rejected, refusal = m.MarshalBinary() })
	require.Error(t, refusal)
	require.Nil(t, rejected)
	require.Zero(t, allocs, "oversized encoding must be refused before allocating its output")
	oversized := make([]byte, MaxMetadataBytes+1)
	allocs = testing.AllocsPerRun(10, func() { got, refusal = UnmarshalMetadata(oversized) })
	require.Error(t, refusal)
	require.Nil(t, got)
	require.Zero(t, allocs, "outer bound must precede decoder allocations")
}

func TestMetadataRejectsEveryTruncationAndTrailingData(t *testing.T) {
	wire, err := testMetadata().MarshalBinary()
	require.NoError(t, err)
	for end := 0; end < len(wire); end++ {
		got, err := UnmarshalMetadata(wire[:end])
		require.Error(t, err, "proper prefix %d", end)
		require.Nil(t, got, "proper prefix %d must not publish partial metadata", end)
	}
	got, err := UnmarshalMetadata(append(bytes.Clone(wire), 0))
	require.Error(t, err)
	require.Nil(t, got)
}

func TestMetadataRejectsHostileWireCountsAndIndexes(t *testing.T) {
	wire, err := testMetadata().MarshalBinary()
	require.NoError(t, err)
	// Offsets are fixed by the independent 370-byte layout check above. These
	// mutations bypass MarshalBinary so decoder validation is tested directly.
	cases := []struct {
		name          string
		offset, width int
		value         uint64
	}{
		{"version-zero", 0, 4, 0}, {"version-future", 0, 4, math.MaxUint32},
		{"error-max-length", 20, 8, math.MaxUint64}, {"error-over-limit", 20, 8, metadataErrorBytes + 1},
		{"key-count-zero", 32, 2, 0}, {"key-count-over-limit", 32, 2, metadataAccounts + 1}, {"key-count-absent-rows", 32, 2, metadataAccounts},
		{"group-count", 82, 2, metadataCPIEntries + 1}, {"group-absent-rows", 82, 2, metadataCPIEntries},
		{"instruction-count", 85, 2, metadataCPIEntries + 1}, {"instruction-absent-rows", 85, 2, metadataCPIEntries},
		{"program-index", 87, 1, 1}, {"account-index", 100, 1, 1},
		{"cpi-account-max-length", 92, 8, math.MaxUint64}, {"cpi-account-count", 92, 8, 256},
		{"cpi-data-max-length", 101, 8, math.MaxUint64}, {"cpi-data-over-limit", 101, 8, metadataCPIDataBytes + 1}, {"cpi-data-absent", 101, 8, metadataCPIDataBytes},
		{"pre-token-count", 110, 2, metadataAccounts + 1}, {"pre-token-absent-rows", 110, 2, metadataAccounts}, {"pre-token-index", 112, 2, 1},
		{"post-token-count", 219, 2, metadataAccounts + 1}, {"post-token-absent-rows", 219, 2, metadataAccounts}, {"post-token-index", 221, 2, 1},
		{"return-tag", 328, 1, 2}, {"return-max-length", 361, 8, math.MaxUint64}, {"return-over-limit", 361, 8, metadataReturnBytes + 1}, {"return-absent-data", 361, 8, metadataReturnBytes},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bad := bytes.Clone(wire)
			switch tc.width {
			case 1:
				bad[tc.offset] = byte(tc.value)
			case 2:
				binary.LittleEndian.PutUint16(bad[tc.offset:], uint16(tc.value))
			case 4:
				binary.LittleEndian.PutUint32(bad[tc.offset:], uint32(tc.value))
			case 8:
				binary.LittleEndian.PutUint64(bad[tc.offset:], tc.value)
			}
			got, err := UnmarshalMetadata(bad)
			require.Error(t, err)
			require.Nil(t, got)
		})
	}
	for _, value := range []string{"true", "nulx", "[]  ", "   1"} {
		bad := bytes.Clone(wire)
		copy(bad[28:32], value)
		got, err := UnmarshalMetadata(bad)
		require.Error(t, err, value)
		require.Nil(t, got)
	}
	for _, firstIndex := range []byte{0, 1} {
		bad := append(bytes.Clone(wire[:110]), wire[84:110]...)
		bad = append(bad, wire[110:]...)
		binary.LittleEndian.PutUint16(bad[82:], 2)
		bad[84] = firstIndex // Duplicate (0,0), then descending (1,0).
		got, err := UnmarshalMetadata(bad)
		require.Error(t, err)
		require.Nil(t, got)
	}
	// Individually legal group counts must still share the 64-entry ceiling.
	bad := bytes.Clone(wire[:84])
	binary.LittleEndian.PutUint16(bad[82:], 2)
	bad = append(bad, 0, 64, 0)
	for i := 0; i < 64; i++ {
		bad = append(bad, wire[87:110]...)
	}
	bad = append(bad, 1, 1, 0)
	bad = append(bad, wire[87:110]...)
	bad = append(bad, wire[110:]...)
	got, err := UnmarshalMetadata(bad)
	require.Error(t, err)
	require.Nil(t, got)
	// Keep the rest of the record structurally aligned after removing the
	// instruction, so this reaches the empty-group semantic check.
	bad = append(bytes.Clone(wire[:87]), wire[110:]...)
	binary.LittleEndian.PutUint16(bad[85:], 0)
	got, err = UnmarshalMetadata(bad)
	require.Error(t, err)
	require.Nil(t, got)

	m := testMetadata()
	m.AccountKeys = append(m.AccountKeys, solana.PublicKey{2})
	m.PreBalances, m.PostBalances = append(m.PreBalances, 1), append(m.PostBalances, 1)
	m.PreTokenBalances = append(m.PreTokenBalances, TokenBalance{AccountIndex: 1})
	m.PostTokenBalances = append(m.PostTokenBalances, TokenBalance{AccountIndex: 1})
	twoTokens, err := m.MarshalBinary()
	require.NoError(t, err)
	require.Len(t, twoTokens, 632)
	for _, rows := range []int{160, 376} { // Pre- and post-token row starts.
		for _, order := range [][]uint16{{0, 0}, {1, 0}} {
			bad := bytes.Clone(twoTokens)
			binary.LittleEndian.PutUint16(bad[rows:], order[0])
			binary.LittleEndian.PutUint16(bad[rows+tokenBalanceBytes:], order[1])
			got, err := UnmarshalMetadata(bad)
			require.Error(t, err)
			require.Nil(t, got)
		}
	}

	// A declared but absent, otherwise legal 10 KiB CPI payload must not be
	// allocated. This is a narrow per-call regression guard, not an RSS claim.
	bad = bytes.Clone(wire[:109])
	binary.LittleEndian.PutUint64(bad[101:], metadataCPIDataBytes)
	got, err = UnmarshalMetadata(bad) // Warm error formatting before measurement.
	require.Error(t, err)
	require.Nil(t, got)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	got, err = UnmarshalMetadata(bad)
	runtime.ReadMemStats(&after)
	require.Error(t, err)
	require.Nil(t, got)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(8<<10), "absent payload allocated before remaining-byte validation")
}

func FuzzUnmarshalMetadata(f *testing.F) {
	wire, err := testMetadata().MarshalBinary()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(wire)
	f.Add([]byte{})
	f.Add([]byte{255, 255, 255, 255})
	f.Fuzz(func(t *testing.T, data []byte) {
		m, err := UnmarshalMetadata(data)
		if err != nil {
			if m != nil {
				t.Fatal("invalid input returned partial metadata")
			}
			return
		}
		encoded, err := m.MarshalBinary()
		if err != nil {
			t.Fatalf("decoded invalid metadata: %v", err)
		}
		if !bytes.Equal(data, encoded) {
			t.Fatal("accepted encoding did not round-trip exactly")
		}
	})
}
