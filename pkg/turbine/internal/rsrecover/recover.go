package rsrecover

import (
	"errors"
	"fmt"
	"sync"

	"github.com/klauspost/reedsolomon"
)

const (
	DataShards   = 32
	CodingShards = 32
	TotalShards  = DataShards + CodingShards

	cauchyGamma = byte(0xa5)
)

var (
	ErrInvalidPattern = errors.New("invalid erasure pattern")
	ErrPatternChanged = errors.New("availability differs from prepared plan")
	ErrInvalidBuffers = errors.New("invalid recovery buffers")

	gfLog [256]byte
	gfExp [512]byte

	allCodingEncoderOnce sync.Once
	allCodingEncoder     reedsolomon.Encoder
	allCodingEncoderErr  error
)

func init() {
	value := uint16(1)
	for exponent := 0; exponent < 255; exponent++ {
		gfExp[exponent] = byte(value)
		gfLog[byte(value)] = byte(exponent)
		value <<= 1
		if value&0x100 != 0 {
			value ^= 0x11d
		}
	}
	for exponent := 255; exponent < len(gfExp); exponent++ {
		gfExp[exponent] = gfExp[exponent-255]
	}
}

// OneDataPlan recovers exactly one absent data shard from the other 31 data
// shards and one available coding shard. It avoids a general 32x32 decode
// matrix inversion. The plan is immutable and safe for concurrent execution
// when callers provide independent destinations.
type OneDataPlan struct {
	presence     uint64
	missing      uint8
	sources      [DataShards]uint8
	coefficients [DataShards]byte
}

// DataSubsetPlan recovers every absent data shard using the present data and
// the lowest-indexed coding rows required to reach the 32-shard threshold.
// Setup solves only the m x m system induced by the missing data columns.
type DataSubsetPlan struct {
	presence uint64
	missing  []uint8
	sources  [DataShards]uint8
	weights  [][]byte
}

// AllCodingPlan recovers all 32 data rows from all 32 coding rows. For the
// fixed Solana matrix C*C=I, so the package's optimized encoder can apply C a
// second time instead of constructing a decode matrix.
type AllCodingPlan struct {
	presence uint64
	encoder  reedsolomon.Encoder
}

// Presence reports which of the 64 input shards are non-empty. A zero-length
// shard is absent, matching reedsolomon.ReconstructSome.
func Presence(shards [][]byte) (uint64, error) {
	if len(shards) != TotalShards {
		return 0, fmt.Errorf("%w: got %d shards, want %d", ErrInvalidBuffers, len(shards), TotalShards)
	}
	var mask uint64
	for index, shard := range shards {
		if len(shard) != 0 {
			mask |= uint64(1) << index
		}
	}
	return mask, nil
}

// PrepareRecoverOneData constructs the direct coefficient row for a pattern
// with exactly one missing data shard. Additional coding shards may be present;
// the lowest-indexed one is selected deterministically.
func PrepareRecoverOneData(presence uint64, missingDataIndex int) (OneDataPlan, error) {
	if missingDataIndex < 0 || missingDataIndex >= DataShards {
		return OneDataPlan{}, fmt.Errorf("%w: missing data index %d", ErrInvalidPattern, missingDataIndex)
	}
	for index := 0; index < DataShards; index++ {
		present := presence&(uint64(1)<<index) != 0
		if index == missingDataIndex {
			if present {
				return OneDataPlan{}, fmt.Errorf("%w: requested data shard %d is present", ErrInvalidPattern, index)
			}
			continue
		}
		if !present {
			return OneDataPlan{}, fmt.Errorf("%w: data shard %d is also absent", ErrInvalidPattern, index)
		}
	}

	codingPosition := -1
	for position := 0; position < CodingShards; position++ {
		if presence&(uint64(1)<<uint(DataShards+position)) != 0 {
			codingPosition = position
			break
		}
	}
	if codingPosition < 0 {
		return OneDataPlan{}, fmt.Errorf("%w: no coding shard is available", ErrInvalidPattern)
	}

	coefficient := codingCoefficient(codingPosition, missingDataIndex)
	coefficientInv := reedsolomon.Inv(coefficient)
	if coefficient == 0 || coefficientInv == 0 {
		return OneDataPlan{}, fmt.Errorf("%w: coding coefficient is not invertible", ErrInvalidPattern)
	}

	plan := OneDataPlan{presence: presence, missing: uint8(missingDataIndex)}
	source := 0
	for dataIndex := 0; dataIndex < DataShards; dataIndex++ {
		if dataIndex == missingDataIndex {
			continue
		}
		plan.sources[source] = uint8(dataIndex)
		plan.coefficients[source] = gfMul(coefficientInv, codingCoefficient(codingPosition, dataIndex))
		source++
	}
	plan.sources[source] = uint8(DataShards + codingPosition)
	plan.coefficients[source] = coefficientInv

	if !verifyRequestedRows(plan.sources[:], [][]byte{plan.coefficients[:]}, []uint8{plan.missing}) {
		return OneDataPlan{}, fmt.Errorf("%w: direct coefficient proof gate failed", ErrInvalidPattern)
	}
	return plan, nil
}

// Recover writes only the requested data shard. Validation completes before
// dst is modified, so returned errors leave dst unchanged.
func (plan *OneDataPlan) Recover(shards [][]byte, dst []byte) error {
	shardSize, err := validateExecution(plan.presence, shards, [][]byte{dst})
	if err != nil {
		return err
	}
	if len(dst) != shardSize {
		return fmt.Errorf("%w: destination has %d bytes, want %d", ErrInvalidBuffers, len(dst), shardSize)
	}

	var lowLevel reedsolomon.LowLevel
	first := true
	for index, sourceIndex := range plan.sources {
		coefficient := plan.coefficients[index]
		if coefficient == 0 {
			continue
		}
		if first {
			lowLevel.GalMulSlice(coefficient, shards[sourceIndex], dst)
			first = false
		} else {
			lowLevel.GalMulSliceXor(coefficient, shards[sourceIndex], dst)
		}
	}
	if first {
		clear(dst)
	}
	return nil
}

// PrepareRecoverDataSubset constructs direct output rows for all absent data
// shards. It first inverts only the reduced m x m coding/data matrix, then
// expands those rows over exactly 32 selected input shards so byte execution
// can be compared fairly with a general decoder.
func PrepareRecoverDataSubset(presence uint64) (DataSubsetPlan, error) {
	plan := DataSubsetPlan{presence: presence}
	knownData := make([]uint8, 0, DataShards)
	for index := 0; index < DataShards; index++ {
		if presence&(uint64(1)<<index) == 0 {
			plan.missing = append(plan.missing, uint8(index))
		} else {
			knownData = append(knownData, uint8(index))
		}
	}
	if len(plan.missing) == 0 {
		return DataSubsetPlan{}, fmt.Errorf("%w: no data shards are missing", ErrInvalidPattern)
	}

	selectedCoding := make([]uint8, 0, len(plan.missing))
	for position := 0; position < CodingShards && len(selectedCoding) < len(plan.missing); position++ {
		if presence&(uint64(1)<<uint(DataShards+position)) != 0 {
			selectedCoding = append(selectedCoding, uint8(position))
		}
	}
	if len(selectedCoding) != len(plan.missing) {
		return DataSubsetPlan{}, fmt.Errorf("%w: have %d coding rows, need %d", ErrInvalidPattern, len(selectedCoding), len(plan.missing))
	}

	m := len(plan.missing)
	reduced := make([][]byte, m)
	for row, codingPosition := range selectedCoding {
		reduced[row] = make([]byte, m)
		for column, dataIndex := range plan.missing {
			reduced[row][column] = codingCoefficient(int(codingPosition), int(dataIndex))
		}
	}
	source := 0
	for _, dataIndex := range knownData {
		plan.sources[source] = dataIndex
		source++
	}
	for _, codingPosition := range selectedCoding {
		plan.sources[source] = uint8(DataShards) + codingPosition
		source++
	}

	// The standard 32+32 coding submatrix is Cauchy, so its inverse can be
	// derived in O(m^2). The requested-row proof below validates the complete
	// expanded result. A failure rebuilds with independent Gauss-Jordan setup
	// before returning an internal error.
	inverse := invertCauchy(selectedCoding, plan.missing)
	plan.weights = subsetWeights(knownData, selectedCoding, inverse)
	if !verifyRequestedRows(plan.sources[:], plan.weights, plan.missing) {
		inverse, err := invert(reduced)
		if err != nil {
			return DataSubsetPlan{}, fmt.Errorf("%w: reduced matrix: %v", ErrInvalidPattern, err)
		}
		plan.weights = subsetWeights(knownData, selectedCoding, inverse)
		if !verifyRequestedRows(plan.sources[:], plan.weights, plan.missing) {
			return DataSubsetPlan{}, fmt.Errorf("%w: reduced-system coefficient proof gate failed", ErrInvalidPattern)
		}
	}
	return plan, nil
}

// PrepareRecoverAllDataFromCoding accepts the single coding-only threshold
// pattern: no data rows and all coding rows. Encoder construction is shared
// process-wide because the 32+32 matrix is immutable.
func PrepareRecoverAllDataFromCoding(presence uint64) (AllCodingPlan, error) {
	want := uint64(0xffffffff) << DataShards
	if presence != want {
		return AllCodingPlan{}, fmt.Errorf("%w: all-coding recovery requires presence %#016x, got %#016x", ErrInvalidPattern, want, presence)
	}
	allCodingEncoderOnce.Do(func() {
		allCodingEncoder, allCodingEncoderErr = reedsolomon.New(DataShards, CodingShards)
	})
	if allCodingEncoderErr != nil {
		return AllCodingPlan{}, allCodingEncoderErr
	}
	return AllCodingPlan{presence: presence, encoder: allCodingEncoder}, nil
}

// Recover writes all 32 data destinations by applying the coding matrix to
// the 32 coding inputs. Validation completes before Encode writes any output.
func (plan *AllCodingPlan) Recover(shards, destinations [][]byte) error {
	shardSize, err := validateExecution(plan.presence, shards, destinations)
	if err != nil {
		return err
	}
	if len(destinations) != DataShards {
		return fmt.Errorf("%w: got %d destinations, want %d", ErrInvalidBuffers, len(destinations), DataShards)
	}
	var work [TotalShards][]byte
	for index := 0; index < DataShards; index++ {
		if len(destinations[index]) != shardSize {
			return fmt.Errorf("%w: destination %d has %d bytes, want %d", ErrInvalidBuffers, index, len(destinations[index]), shardSize)
		}
		work[index] = shards[DataShards+index]
		work[DataShards+index] = destinations[index]
	}
	if err := plan.encoder.Encode(work[:]); err != nil {
		return fmt.Errorf("recover all data from coding: %w", err)
	}
	return nil
}

func subsetWeights(knownData, selectedCoding []uint8, inverse [][]byte) [][]byte {
	weights := make([][]byte, len(inverse))
	for output := range inverse {
		row := make([]byte, DataShards)
		for knownIndex, dataIndex := range knownData {
			var coefficient byte
			for equation, codingPosition := range selectedCoding {
				coefficient ^= gfMul(inverse[output][equation], codingCoefficient(int(codingPosition), int(dataIndex)))
			}
			row[knownIndex] = coefficient
		}
		for equation := range selectedCoding {
			row[len(knownData)+equation] = inverse[output][equation]
		}
		weights[output] = row
	}
	return weights
}

// invertCauchy returns the inverse of
//
//	A[i,j] = gamma / (x[i] + y[j]),
//
// where x[i]=0x20 xor coding[i], y[j]=missing[j], and addition in
// GF(2^8) is XOR. Distinct coding and missing indices make every denominator
// nonzero. Products common to each row and column reduce setup to O(m^2).
func invertCauchy(coding, missing []uint8) [][]byte {
	m := len(missing)
	if m == 0 || len(coding) != m {
		return nil
	}
	x := make([]byte, m)
	y := make([]byte, m)
	for index := 0; index < m; index++ {
		x[index] = 0x20 ^ coding[index]
		y[index] = missing[index]
	}

	yFactor := make([]byte, m)
	for column := 0; column < m; column++ {
		numerator := byte(1)
		for row := 0; row < m; row++ {
			numerator = gfMul(numerator, x[row]^y[column])
		}
		denominator := byte(1)
		for other := 0; other < m; other++ {
			if other != column {
				denominator = gfMul(denominator, y[column]^y[other])
			}
		}
		yFactor[column] = gfMul(numerator, reedsolomon.Inv(denominator))
	}

	xFactor := make([]byte, m)
	for row := 0; row < m; row++ {
		numerator := byte(1)
		for column := 0; column < m; column++ {
			numerator = gfMul(numerator, x[row]^y[column])
		}
		denominator := byte(1)
		for other := 0; other < m; other++ {
			if other != row {
				denominator = gfMul(denominator, x[row]^x[other])
			}
		}
		xFactor[row] = gfMul(numerator, reedsolomon.Inv(denominator))
	}

	gammaInv := reedsolomon.Inv(cauchyGamma)
	result := make([][]byte, m)
	for output := 0; output < m; output++ {
		result[output] = make([]byte, m)
		for equation := 0; equation < m; equation++ {
			coefficient := gfMul(yFactor[output], xFactor[equation])
			coefficient = gfMul(coefficient, reedsolomon.Inv(x[equation]^y[output]))
			result[output][equation] = gfMul(gammaInv, coefficient)
		}
	}
	return result
}

// MissingData returns a copy of the data indices produced by the plan.
func (plan *DataSubsetPlan) MissingData() []uint8 {
	return append([]uint8(nil), plan.missing...)
}

// Recover writes one destination per MissingData entry. Validation completes
// before any destination is modified.
func (plan *DataSubsetPlan) Recover(shards [][]byte, destinations [][]byte) error {
	shardSize, err := validateExecution(plan.presence, shards, destinations)
	if err != nil {
		return err
	}
	if len(destinations) != len(plan.missing) {
		return fmt.Errorf("%w: got %d destinations, want %d", ErrInvalidBuffers, len(destinations), len(plan.missing))
	}
	for index, dst := range destinations {
		if len(dst) != shardSize {
			return fmt.Errorf("%w: destination %d has %d bytes, want %d", ErrInvalidBuffers, index, len(dst), shardSize)
		}
	}

	for _, dst := range destinations {
		clear(dst)
	}
	var lowLevel reedsolomon.LowLevel
	for sourcePosition, sourceIndex := range plan.sources {
		input := shards[sourceIndex]
		for output, dst := range destinations {
			coefficient := plan.weights[output][sourcePosition]
			if coefficient != 0 {
				lowLevel.GalMulSliceXor(coefficient, input, dst)
			}
		}
	}
	return nil
}

func validateExecution(wantPresence uint64, shards, destinations [][]byte) (int, error) {
	gotPresence, err := Presence(shards)
	if err != nil {
		return 0, err
	}
	if gotPresence != wantPresence {
		return 0, fmt.Errorf("%w: got %#016x, want %#016x", ErrPatternChanged, gotPresence, wantPresence)
	}
	shardSize := 0
	for _, shard := range shards {
		if len(shard) == 0 {
			continue
		}
		if shardSize == 0 {
			shardSize = len(shard)
		} else if len(shard) != shardSize {
			return 0, fmt.Errorf("%w: inconsistent shard sizes", ErrInvalidBuffers)
		}
	}
	if shardSize == 0 {
		return 0, fmt.Errorf("%w: all shards are empty", ErrInvalidBuffers)
	}
	for left := range destinations {
		for right := left + 1; right < len(destinations); right++ {
			if sameNonEmptyBuffer(destinations[left], destinations[right]) {
				return 0, fmt.Errorf("%w: destinations %d and %d alias", ErrInvalidBuffers, left, right)
			}
		}
	}
	return shardSize, nil
}

func sameNonEmptyBuffer(left, right []byte) bool {
	return len(left) != 0 && len(right) != 0 && &left[0] == &right[0]
}

func codingCoefficient(codingPosition, dataIndex int) byte {
	denominator := byte(0x20 ^ codingPosition ^ dataIndex)
	return gfMul(cauchyGamma, reedsolomon.Inv(denominator))
}

func generatorCoefficient(row, column int) byte {
	if row < DataShards {
		if row == column {
			return 1
		}
		return 0
	}
	return codingCoefficient(row-DataShards, column)
}

func verifyRequestedRows(sources []uint8, weights [][]byte, requested []uint8) bool {
	if len(sources) != DataShards || len(weights) != len(requested) {
		return false
	}
	for output, target := range requested {
		if len(weights[output]) != DataShards {
			return false
		}
		for column := 0; column < DataShards; column++ {
			var got byte
			for source, globalRow := range sources {
				got ^= gfMul(weights[output][source], generatorCoefficient(int(globalRow), column))
			}
			want := byte(0)
			if column == int(target) {
				want = 1
			}
			if got != want {
				return false
			}
		}
	}
	return true
}

func invert(input [][]byte) ([][]byte, error) {
	n := len(input)
	if n == 0 {
		return nil, errors.New("empty matrix")
	}
	augmented := make([][]byte, n)
	for row := 0; row < n; row++ {
		if len(input[row]) != n {
			return nil, errors.New("matrix is not square")
		}
		augmented[row] = make([]byte, 2*n)
		copy(augmented[row], input[row])
		augmented[row][n+row] = 1
	}
	for column := 0; column < n; column++ {
		pivot := column
		for pivot < n && augmented[pivot][column] == 0 {
			pivot++
		}
		if pivot == n {
			return nil, errors.New("singular matrix")
		}
		augmented[column], augmented[pivot] = augmented[pivot], augmented[column]
		pivotInv := reedsolomon.Inv(augmented[column][column])
		for index := range augmented[column] {
			augmented[column][index] = gfMul(augmented[column][index], pivotInv)
		}
		for row := 0; row < n; row++ {
			if row == column {
				continue
			}
			factor := augmented[row][column]
			if factor == 0 {
				continue
			}
			for index := range augmented[row] {
				augmented[row][index] ^= gfMul(factor, augmented[column][index])
			}
		}
	}
	result := make([][]byte, n)
	for row := range result {
		result[row] = append([]byte(nil), augmented[row][n:]...)
	}
	return result, nil
}

func gfMul(left, right byte) byte {
	if left == 0 || right == 0 {
		return 0
	}
	return gfExp[int(gfLog[left])+int(gfLog[right])]
}
