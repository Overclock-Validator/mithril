//go:build !amd64 || purego

package lthash

import "github.com/zeebo/blake3"

func readXOF(d *blake3.Digest, out *[2 * numElements]byte) { _, _ = d.Read(out[:]) }

// XOFFallbackReason is always empty: this build has no accelerated output path.
func XOFFallbackReason() string { return "" }
