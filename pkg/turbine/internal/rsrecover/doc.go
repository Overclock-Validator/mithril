// Package rsrecover contains experimental, fixed-shape Reed-Solomon recovery
// plans for Solana's 32 data + 32 coding shred FEC sets.
//
// Nothing in the production turbine assembler imports this package. It exists
// to measure two workload-specific questions before either policy is wired in:
// near-tip latency for one missing data shred, and catch-up throughput for a
// subset of missing data shreds.
package rsrecover
