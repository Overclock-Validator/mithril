# Go linker placement patch

This small patch modifies Go 1.27.2 `cmd/link` for Mithril's optional Zen 5 build.
The upstream Go code is copyright The Go Authors and licensed under the included
BSD license. The Mithril additions follow this repository's license.

`-mithril-interpreter-offset=32` places `sbpf.(*Interpreter).Run` 32 bytes after a
64-byte boundary. Offset 0 is a comparison control; the default -1 disables the
custom behavior. The same maximum padding is reserved after either placement,
including when the function's size changes, to keep following symbols fixed.
The Mithril build validates the final address after external linking.

The source hashes and Go version are pinned in `scripts/zen5-build.py`. Review and
retest the patch for each toolchain upgrade. See `docs/zen5-interpreter-build.md`.
