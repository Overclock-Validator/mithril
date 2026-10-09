# agave-alpenglow-development-v1

Generated with `scripts/genesis-oracle/main.rs` at Agave commit
`7e51da963aee49622a395f562386a6bd8ba0e717`, in `profile` mode.
`alpenglow-v1.bin` contains the fixed GenesisConfig with creation time zero,
no user accounts, all pinned Alpenglow features, the genesis certificate,
stake config and epoch rewards. The native builder replaces the timestamp
and constructs funded, vote and stake accounts. See `docs/native-genesis.md`.

`features.json` records exact feature addresses and names. `reserved.json`
records Agave's reserved keys plus its Alpenglow clock PDA. These are pinned
data, not an instruction to enable features from the runtime's current list.
