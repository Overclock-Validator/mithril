//go:build !race

package sbpf

// raceDetector reports whether the tests were built with -race.
const raceDetector = false
