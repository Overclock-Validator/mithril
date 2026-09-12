These Votor message fixtures are pinned to AshwinSekar/solana tag
`v4.3.0-ag`, commit `3a6336ec5d39d62bf8659fc7541c24abd5759a14`.

They were generated with an isolated Rust program whose source-derived wire
types mirror `votor-messages/src/wire.rs`, using the versions pinned by that
commit:

- `wincode` 0.5.5
- `solana-bls-signatures` 3.3.0 (192-byte affine signature representation)

The v4.3 vote wire message intentionally excludes validator rank and stake;
the authenticated Votor transport identity supplies those values after decode.
The shred version is the final little-endian `u16`. Certificate bitmap vectors
use wincode's default bincode-compatible little-endian `u64` length.
