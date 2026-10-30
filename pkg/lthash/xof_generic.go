//go:build !amd64 || purego

package lthash

import "github.com/zeebo/blake3"

func readXOF(d *blake3.Digest, out *[2 * numElements]byte) { _, _ = d.Read(out[:]) }
