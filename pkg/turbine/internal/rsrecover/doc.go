// Package rsrecover contains fixed-shape Reed-Solomon recovery plans for
// Solana's 32 data + 32 coding shred FEC sets. Production dispatch uses only
// exactly-one-missing-data recovery. Reduced-subset and all-coding plans are
// reference/benchmark alternatives and are not selected by SlotAssembler.
package rsrecover
