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

`agave_votor_certificate.der` is the 249-byte certificate constructed by
`anza-xyz/agave` commit `8fe3f1201abc5b0244540aed0c7bf8c6bcafb3f5`,
`tls-utils/src/tls_certificates.rs::new_dummy_x509_certificate`, with public
key bytes `00..1f` at offsets 100–131. It also matches Firedancer commit
`de039cd7fc9f4714782ec7e3d47db3728903abc6`,
`src/ballet/x509/fd_x509_mock.c::fd_x509_mock_pubkey_v2`. Its fixed dummy
X.509 signature is intentional; TLS CertificateVerify proves possession of
the identity key. This fixture contains no private key.
