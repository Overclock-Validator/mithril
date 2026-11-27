# Yellowstone gRPC protocol definitions

`geyser.proto` and `solana-storage.proto` are copied unmodified from
[rpcpool/yellowstone-grpc](https://github.com/rpcpool/yellowstone-grpc),
directory `yellowstone-grpc-proto/proto`, at commit
`bb3a44a2f723fb1885eb70a1d1efcab44e8ac32f` (2025-04-09). That directory is
licensed under Apache-2.0; its licence is in `LICENSE` here.

The Go code in `pkg/yellowstone/geyserpb` is generated from these files. It
replaces the previously imported `github.com/rpcpool/yellowstone-grpc/examples/golang/proto`
package, which carries the repository's default AGPL-3.0 licence.

To regenerate (pure Go tools; protoc is not needed):

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
go install github.com/bufbuild/buf/cmd/buf@v1.47.2
GP=github.com/Overclock-Validator/mithril/pkg/yellowstone/geyserpb
OPT="paths=source_relative,Mgeyser.proto=$GP;geyserpb,Msolana-storage.proto=$GP;geyserpb"
buf generate proto/yellowstone --template "{\"version\":\"v2\",\"plugins\":[
  {\"local\":\"protoc-gen-go\",\"out\":\"pkg/yellowstone/geyserpb\",\"opt\":\"$OPT\"},
  {\"local\":\"protoc-gen-go-grpc\",\"out\":\"pkg/yellowstone/geyserpb\",\"opt\":\"$OPT\"}]}"
```

Compared with the old generated package, the messages, fields and services
are the same, except that `SubscribeUpdateSlot.Status` is now its own
`SlotStatus` enum (the old generated code predates that change in the
protocol). The enum values and wire encoding are unchanged.
