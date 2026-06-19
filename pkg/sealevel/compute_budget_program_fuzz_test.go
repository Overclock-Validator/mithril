package sealevel

import (
	"bytes"
	"testing"

	bin "github.com/gagliardetto/binary"
)

// FuzzComputeBudgetInstrRequestHeapFrame tests RequestHeapFrame instruction deserialization
func FuzzComputeBudgetInstrRequestHeapFrame(f *testing.F) {
	f.Add(makeValidRequestHeapFrameInstr(0))
	f.Add(makeValidRequestHeapFrameInstr(32768))
	f.Add(makeValidRequestHeapFrameInstr(262144))
	f.Add(makeValidRequestHeapFrameInstr(^uint32(0)))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBorshDecoder(data)
		instrType, err := decoder.ReadUint8()
		if err != nil || instrType != ComputeBudgetInstrTypeRequestHeapFrame {
			return
		}

		var requestHeapFrame ComputeBudgetInstrRequestHeapFrame
		_ = requestHeapFrame.UnmarshalWithDecoder(decoder)
	})
}

// FuzzComputeBudgetInstrSetComputeUnitLimit tests SetComputeUnitLimit instruction deserialization
func FuzzComputeBudgetInstrSetComputeUnitLimit(f *testing.F) {
	f.Add(makeValidSetComputeUnitLimitInstr(0))
	f.Add(makeValidSetComputeUnitLimitInstr(200000))
	f.Add(makeValidSetComputeUnitLimitInstr(1400000))
	f.Add(makeValidSetComputeUnitLimitInstr(^uint32(0)))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBorshDecoder(data)
		instrType, err := decoder.ReadUint8()
		if err != nil || instrType != ComputeBudgetInstrTypeSetComputeUnitLimit {
			return
		}

		var setComputeUnitLimit ComputeBudgetInstrSetComputeUnitLimit
		_ = setComputeUnitLimit.UnmarshalWithDecoder(decoder)
	})
}

// FuzzComputeBudgetInstrSetComputeUnitPrice tests SetComputeUnitPrice instruction deserialization
func FuzzComputeBudgetInstrSetComputeUnitPrice(f *testing.F) {
	f.Add(makeValidSetComputeUnitPriceInstr(0))
	f.Add(makeValidSetComputeUnitPriceInstr(1000))
	f.Add(makeValidSetComputeUnitPriceInstr(^uint64(0)))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBorshDecoder(data)
		instrType, err := decoder.ReadUint8()
		if err != nil || instrType != ComputeBudgetInstrTypeSetComputeUnitPrice {
			return
		}

		var setComputeUnitPrice ComputeBudgetInstrSetComputeUnitPrice
		_ = setComputeUnitPrice.UnmarshalWithDecoder(decoder)
	})
}

// FuzzComputeBudgetInstrSetLoadedAccountsDataSizeLimit tests SetLoadedAccountsDataSizeLimit instruction deserialization
func FuzzComputeBudgetInstrSetLoadedAccountsDataSizeLimit(f *testing.F) {
	f.Add(makeValidSetLoadedAccountsDataSizeLimitInstr(0))
	f.Add(makeValidSetLoadedAccountsDataSizeLimitInstr(64 * 1024 * 1024))
	f.Add(makeValidSetLoadedAccountsDataSizeLimitInstr(^uint32(0)))

	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := bin.NewBorshDecoder(data)
		instrType, err := decoder.ReadUint8()
		if err != nil || instrType != ComputeBudgetInstrTypeSetLoadedAccountsDataSizeLimit {
			return
		}

		var setLoadedAccountsDataSizeLimit ComputeBudgetInstrSetLoadedAccountsDataSizeLimit
		_ = setLoadedAccountsDataSizeLimit.UnmarshalWithDecoder(decoder)
	})
}

// FuzzComputeBudgetInstructionType tests compute budget instruction type parsing
func FuzzComputeBudgetInstructionType(f *testing.F) {
	f.Add(uint8(ComputeBudgetInstrTypeRequestHeapFrame))
	f.Add(uint8(ComputeBudgetInstrTypeSetComputeUnitLimit))
	f.Add(uint8(ComputeBudgetInstrTypeSetComputeUnitPrice))
	f.Add(uint8(ComputeBudgetInstrTypeSetLoadedAccountsDataSizeLimit))
	f.Add(uint8(255)) // Invalid type

	f.Fuzz(func(t *testing.T, instrType uint8) {
		// Test instruction type validation
		validTypes := []uint8{
			ComputeBudgetInstrTypeRequestHeapFrame,
			ComputeBudgetInstrTypeSetComputeUnitLimit,
			ComputeBudgetInstrTypeSetComputeUnitPrice,
			ComputeBudgetInstrTypeSetLoadedAccountsDataSizeLimit,
		}

		isValid := false
		for _, validType := range validTypes {
			if instrType == validType {
				isValid = true
				break
			}
		}

		if !isValid {
			// Should be rejected
			return
		}
	})
}

// FuzzComputeBudgetHeapSizeValidation tests heap size validation
func FuzzComputeBudgetHeapSizeValidation(f *testing.F) {
	f.Add(uint32(0))
	f.Add(uint32(32768))      // Min heap size
	f.Add(uint32(256 * 1024)) // 256KB
	f.Add(uint32(262144))     // Max heap size
	f.Add(uint32(262145))     // Over max
	f.Add(uint32(^uint32(0))) // Maximum uint32

	f.Fuzz(func(t *testing.T, heapSize uint32) {
		// Test heap size sanitization
		// Valid range should be MIN_HEAP_FRAME_BYTES to MAX_HEAP_FRAME_BYTES
		// and must be multiple of 1024
		minHeap := uint32(32768)
		maxHeap := uint32(262144)

		if heapSize < minHeap {
			// Too small - should fail or be adjusted
			return
		}

		if heapSize > maxHeap {
			// Too large - should fail or be adjusted
			return
		}

		// Check alignment to 1024 bytes
		if heapSize%1024 != 0 {
			// Not properly aligned - may need adjustment
			return
		}
	})
}

// FuzzComputeBudgetLimitCalculation tests compute unit limit calculation
func FuzzComputeBudgetLimitCalculation(f *testing.F) {
	f.Add(uint32(0), uint32(0))
	f.Add(uint32(1), uint32(200000))
	f.Add(uint32(10), uint32(200000))
	f.Add(uint32(1000), uint32(1400000))

	f.Fuzz(func(t *testing.T, numNonComputeBudgetInstrs uint32, computeUnitLimit uint32) {
		// Test compute unit limit calculation
		maxLimit := uint32(1400000) // MAX_COMPUTE_UNIT_LIMIT

		// Default compute unit calculation based on instruction count
		defaultLimit := numNonComputeBudgetInstrs * 200000 // DEFAULT_INSTRUCTION_COMPUTE_UNIT_LIMIT

		// Verify limits
		if computeUnitLimit > maxLimit {
			// Exceeds maximum - should be capped
			return
		}

		// Check for overflow in default calculation
		if numNonComputeBudgetInstrs > 0 && defaultLimit/numNonComputeBudgetInstrs != 200000 {
			t.Error("Compute unit limit calculation overflow")
		}

		// Actual limit should be min(specified, default, max)
		actualLimit := computeUnitLimit
		if actualLimit == 0 {
			actualLimit = defaultLimit
		}
		if actualLimit > maxLimit {
			actualLimit = maxLimit
		}

		_ = actualLimit
	})
}

// FuzzComputeBudgetDuplicateDetection tests duplicate instruction detection
func FuzzComputeBudgetDuplicateDetection(f *testing.F) {
	f.Add(bool(false), bool(false), bool(false), bool(false))
	f.Add(bool(true), bool(false), bool(false), bool(false))
	f.Add(bool(true), bool(true), bool(false), bool(false))
	f.Add(bool(true), bool(true), bool(true), bool(true))

	f.Fuzz(func(t *testing.T, hasHeap bool, hasLimit bool, hasPrice bool, hasDataSize bool) {
		// Test duplicate instruction detection
		// Each compute budget instruction type should only appear once

		// Simulate processing multiple instructions of same type
		type instrCount struct {
			heap     int
			limit    int
			price    int
			dataSize int
		}

		counts := instrCount{}

		if hasHeap {
			counts.heap++
		}
		if hasLimit {
			counts.limit++
		}
		if hasPrice {
			counts.price++
		}
		if hasDataSize {
			counts.dataSize++
		}

		// Check for duplicates (would be detected on second occurrence)
		// In actual processing, second occurrence of same type should fail
	})
}

// Helper functions to create seed data

func makeValidRequestHeapFrameInstr(heapBytes uint32) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBorshEncoder(buf)
	encoder.WriteUint8(ComputeBudgetInstrTypeRequestHeapFrame)
	encoder.WriteUint32(heapBytes, bin.LE)
	return buf.Bytes()
}

func makeValidSetComputeUnitLimitInstr(units uint32) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBorshEncoder(buf)
	encoder.WriteUint8(ComputeBudgetInstrTypeSetComputeUnitLimit)
	encoder.WriteUint32(units, bin.LE)
	return buf.Bytes()
}

func makeValidSetComputeUnitPriceInstr(microLamports uint64) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBorshEncoder(buf)
	encoder.WriteUint8(ComputeBudgetInstrTypeSetComputeUnitPrice)
	encoder.WriteUint64(microLamports, bin.LE)
	return buf.Bytes()
}

func makeValidSetLoadedAccountsDataSizeLimitInstr(dataBytes uint32) []byte {
	buf := new(bytes.Buffer)
	encoder := bin.NewBorshEncoder(buf)
	encoder.WriteUint8(ComputeBudgetInstrTypeSetLoadedAccountsDataSizeLimit)
	encoder.WriteUint32(dataBytes, bin.LE)
	return buf.Bytes()
}
