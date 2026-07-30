package rsrecover

import (
	"bytes"
	"errors"
	"math/rand"
	"testing"

	"github.com/klauspost/reedsolomon"
)

func encodedFixture(t testing.TB, shardSize int) [][]byte {
	t.Helper()
	encoder, err := reedsolomon.New(DataShards, CodingShards)
	if err != nil {
		t.Fatal(err)
	}
	shards := make([][]byte, TotalShards)
	for shard := 0; shard < DataShards; shard++ {
		shards[shard] = make([]byte, shardSize)
		for index := range shards[shard] {
			shards[shard][index] = byte(shard*73 + index*29 + 11)
		}
	}
	for shard := DataShards; shard < TotalShards; shard++ {
		shards[shard] = make([]byte, shardSize)
	}
	if err := encoder.Encode(shards); err != nil {
		t.Fatal(err)
	}
	return shards
}

func availableFixture(original [][]byte, missingData, codingPositions []int) [][]byte {
	available := make([][]byte, TotalShards)
	missing := make(map[int]struct{}, len(missingData))
	for _, index := range missingData {
		missing[index] = struct{}{}
	}
	for index := 0; index < DataShards; index++ {
		if _, absent := missing[index]; !absent {
			available[index] = original[index]
		}
	}
	for _, position := range codingPositions {
		available[DataShards+position] = original[DataShards+position]
	}
	return available
}

func TestCodingMatrixClosedFormAndInvolution(t *testing.T) {
	for row := 0; row < CodingShards; row++ {
		for column := 0; column < DataShards; column++ {
			if got := codingCoefficient(row, column); got == 0 {
				t.Fatalf("coding coefficient [%d,%d] is zero", row, column)
			}
		}
	}
	for row := 0; row < DataShards; row++ {
		for column := 0; column < DataShards; column++ {
			var got byte
			for inner := 0; inner < DataShards; inner++ {
				got ^= gfMul(codingCoefficient(row, inner), codingCoefficient(inner, column))
			}
			want := byte(0)
			if row == column {
				want = 1
			}
			if got != want {
				t.Fatalf("C*C[%d,%d] = %#02x, want %#02x", row, column, got, want)
			}
		}
	}
}

func TestCauchyInverseMatchesIndependentGaussJordan(t *testing.T) {
	random := rand.New(rand.NewSource(0x434155434859))
	for _, dimension := range []int{1, 2, 4, 8, 16, 24, 32} {
		for sample := 0; sample < 16; sample++ {
			codingPermutation := random.Perm(CodingShards)
			missingPermutation := random.Perm(DataShards)
			coding := make([]uint8, dimension)
			missing := make([]uint8, dimension)
			matrix := make([][]byte, dimension)
			for row := 0; row < dimension; row++ {
				coding[row] = uint8(codingPermutation[row])
				missing[row] = uint8(missingPermutation[row])
				matrix[row] = make([]byte, dimension)
				for column := 0; column < dimension; column++ {
					matrix[row][column] = codingCoefficient(int(coding[row]), missingPermutation[column])
				}
			}
			got := invertCauchy(coding, missing)
			want, err := invert(matrix)
			if err != nil {
				t.Fatalf("dimension=%d sample=%d: %v", dimension, sample, err)
			}
			for row := range want {
				if !bytes.Equal(got[row], want[row]) {
					t.Fatalf("dimension=%d sample=%d row=%d: Cauchy inverse differs", dimension, sample, row)
				}
			}
		}
	}
}

func TestRequestedRowProofGateDetectsCoefficientMutation(t *testing.T) {
	original := encodedFixture(t, 16)
	available := availableFixture(original, []int{9}, []int{4})
	presence, err := Presence(available)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := PrepareRecoverOneData(presence, 9)
	if err != nil {
		t.Fatal(err)
	}
	if !verifyRequestedRows(plan.sources[:], [][]byte{plan.coefficients[:]}, []uint8{plan.missing}) {
		t.Fatal("valid direct row failed its proof gate")
	}
	plan.coefficients[17] ^= 1
	if verifyRequestedRows(plan.sources[:], [][]byte{plan.coefficients[:]}, []uint8{plan.missing}) {
		t.Fatal("mutated direct row passed its proof gate")
	}
}

func TestRecoverOneDataAllPositionsAndCodingRows(t *testing.T) {
	original := encodedFixture(t, 97)
	for missing := 0; missing < DataShards; missing++ {
		var reference []byte
		for coding := 0; coding < CodingShards; coding++ {
			available := availableFixture(original, []int{missing}, []int{coding})
			presence, err := Presence(available)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := PrepareRecoverOneData(presence, missing)
			if err != nil {
				t.Fatalf("missing=%d coding=%d: %v", missing, coding, err)
			}
			dst := bytes.Repeat([]byte{0xcc}, len(original[missing]))
			if err := plan.Recover(available, dst); err != nil {
				t.Fatalf("missing=%d coding=%d: %v", missing, coding, err)
			}
			if !bytes.Equal(dst, original[missing]) {
				t.Fatalf("missing=%d coding=%d: recovered bytes differ", missing, coding)
			}
			if coding == 0 {
				reference = append([]byte(nil), dst...)
			} else if !bytes.Equal(dst, reference) {
				t.Fatalf("missing=%d coding=%d: valid coding choice changed output", missing, coding)
			}
		}
	}
}

func TestRecoverOneDataRejectsUnsafePatternsAtomically(t *testing.T) {
	original := encodedFixture(t, 64)
	available := availableFixture(original, []int{7}, []int{3})
	presence, err := Presence(available)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := PrepareRecoverOneData(presence, 7)
	if err != nil {
		t.Fatal(err)
	}

	changed := append([][]byte(nil), available...)
	changed[DataShards+4] = original[DataShards+4]
	dst := bytes.Repeat([]byte{0xa7}, len(original[0]))
	want := append([]byte(nil), dst...)
	if err := plan.Recover(changed, dst); !errors.Is(err, ErrPatternChanged) {
		t.Fatalf("changed pattern error = %v, want ErrPatternChanged", err)
	}
	if !bytes.Equal(dst, want) {
		t.Fatal("destination changed after a validation error")
	}

	insufficient := availableFixture(original, []int{7, 8}, []int{3})
	insufficientPresence, err := Presence(insufficient)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareRecoverOneData(insufficientPresence, 7); !errors.Is(err, ErrInvalidPattern) {
		t.Fatalf("second missing data error = %v, want ErrInvalidPattern", err)
	}
}

func TestRecoverDataSubsetEveryTwoDataErasures(t *testing.T) {
	original := encodedFixture(t, 31)
	for first := 0; first < DataShards; first++ {
		for second := first + 1; second < DataShards; second++ {
			missing := []int{first, second}
			available := availableFixture(original, missing, []int{0, 1})
			presence, err := Presence(available)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := PrepareRecoverDataSubset(presence)
			if err != nil {
				t.Fatalf("missing=%v: %v", missing, err)
			}
			dst := [][]byte{make([]byte, len(original[0])), make([]byte, len(original[0]))}
			if err := plan.Recover(available, dst); err != nil {
				t.Fatalf("missing=%v: %v", missing, err)
			}
			if !bytes.Equal(dst[0], original[first]) || !bytes.Equal(dst[1], original[second]) {
				t.Fatalf("missing=%v: recovered data differs", missing)
			}
		}
	}
}

func TestRecoverDataSubsetDifferentialPatterns(t *testing.T) {
	original := encodedFixture(t, 987)
	random := rand.New(rand.NewSource(0x4e41525941))
	for _, missingCount := range []int{2, 4, 8, 16, 24, 32} {
		for sample := 0; sample < 8; sample++ {
			permutation := random.Perm(DataShards)
			missing := append([]int(nil), permutation[:missingCount]...)
			codingPermutation := random.Perm(CodingShards)
			coding := append([]int(nil), codingPermutation[:missingCount]...)
			available := availableFixture(original, missing, coding)
			presence, err := Presence(available)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := PrepareRecoverDataSubset(presence)
			if err != nil {
				t.Fatalf("missing=%d sample=%d: %v", missingCount, sample, err)
			}
			missingOrder := plan.MissingData()
			dst := make([][]byte, len(missingOrder))
			for index := range dst {
				dst[index] = make([]byte, len(original[0]))
			}
			if err := plan.Recover(available, dst); err != nil {
				t.Fatalf("missing=%d sample=%d: %v", missingCount, sample, err)
			}
			for output, dataIndex := range missingOrder {
				if !bytes.Equal(dst[output], original[dataIndex]) {
					t.Fatalf("missing=%d sample=%d data=%d: recovered bytes differ", missingCount, sample, dataIndex)
				}
			}

			oracle, err := reedsolomon.New(DataShards, CodingShards, reedsolomon.WithInversionCache(false))
			if err != nil {
				t.Fatal(err)
			}
			oracleShards := append([][]byte(nil), available...)
			// reedsolomon v1.14.0 documents a DataShards-length form but its
			// missing-required scan currently indexes through TotalShards when
			// parity is absent. Mithril's production path uses this safe full form.
			required := make([]bool, TotalShards)
			for _, dataIndex := range missingOrder {
				required[dataIndex] = true
			}
			if err := oracle.ReconstructSome(oracleShards, required); err != nil {
				t.Fatalf("oracle missing=%d sample=%d: %v", missingCount, sample, err)
			}
			for _, dataIndex := range missingOrder {
				if !bytes.Equal(oracleShards[dataIndex], original[dataIndex]) {
					t.Fatalf("oracle missing=%d sample=%d data=%d: recovered bytes differ", missingCount, sample, dataIndex)
				}
			}
		}
	}
}

func TestRecoverDataSubsetThresholdAndBufferFailures(t *testing.T) {
	original := encodedFixture(t, 64)
	missing := []int{0, 3, 9, 17}
	insufficient := availableFixture(original, missing, []int{0, 1, 2})
	presence, err := Presence(insufficient)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareRecoverDataSubset(presence); !errors.Is(err, ErrInvalidPattern) {
		t.Fatalf("insufficient pattern error = %v, want ErrInvalidPattern", err)
	}

	available := availableFixture(original, missing, []int{0, 1, 2, 3})
	presence, err = Presence(available)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := PrepareRecoverDataSubset(presence)
	if err != nil {
		t.Fatal(err)
	}
	dst := make([][]byte, len(missing))
	for index := range dst {
		dst[index] = bytes.Repeat([]byte{0x5d}, len(original[0]))
	}
	want := make([][]byte, len(dst))
	for index := range dst {
		want[index] = append([]byte(nil), dst[index]...)
	}
	dst[2] = dst[1]
	if err := plan.Recover(available, dst); !errors.Is(err, ErrInvalidBuffers) {
		t.Fatalf("aliased destination error = %v, want ErrInvalidBuffers", err)
	}
	for index := range want {
		if index == 2 {
			continue
		}
		if !bytes.Equal(dst[index], want[index]) {
			t.Fatalf("destination %d changed after validation error", index)
		}
	}
}

func TestRecoverAllDataFromCoding(t *testing.T) {
	original := encodedFixture(t, 987)
	available := availableFixture(original, makeRange(DataShards), makeRange(CodingShards))
	presence, err := Presence(available)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := PrepareRecoverAllDataFromCoding(presence)
	if err != nil {
		t.Fatal(err)
	}
	dst := make([][]byte, DataShards)
	for index := range dst {
		dst[index] = make([]byte, len(original[index]))
	}
	if err := plan.Recover(available, dst); err != nil {
		t.Fatal(err)
	}
	for index := range dst {
		if !bytes.Equal(dst[index], original[index]) {
			t.Fatalf("recovered data shard %d differs", index)
		}
	}

	changed := append([][]byte(nil), available...)
	changed[0] = original[0]
	if _, err := PrepareRecoverAllDataFromCoding(mustPresence(t, changed)); !errors.Is(err, ErrInvalidPattern) {
		t.Fatalf("non-coding-only pattern error = %v, want ErrInvalidPattern", err)
	}
}

func makeRange(count int) []int {
	result := make([]int, count)
	for index := range result {
		result[index] = index
	}
	return result
}

func mustPresence(t testing.TB, shards [][]byte) uint64 {
	t.Helper()
	presence, err := Presence(shards)
	if err != nil {
		t.Fatal(err)
	}
	return presence
}
