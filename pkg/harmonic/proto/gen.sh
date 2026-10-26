#!/usr/bin/env bash
# Regenerates the Go code for the Harmonic validator protos vendored in
# upstream/. Requires protoc v3.21.12, protoc-gen-go v1.36.11 and
# protoc-gen-go-grpc v1.5.1 on PATH. The upstream files set no go_package, so
# each one is mapped to its Go package here.
set -euo pipefail
cd "$(dirname "$0")"

module=github.com/Overclock-Validator/mithril/pkg/harmonic/proto
declare -A packages=(
	[auth]=auth
	[block]=block
	[block_engine]=blockengine
	[bundle]=bundle
	[packet]=packet
	[relayer]=relayer
	[shared]=shared
)

go_opts=(--go_opt=paths=source_relative)
grpc_opts=(--go-grpc_opt=paths=source_relative)
for proto in "${!packages[@]}"; do
	go_opts+=("--go_opt=M${proto}.proto=${module}/${packages[$proto]}")
	grpc_opts+=("--go-grpc_opt=M${proto}.proto=${module}/${packages[$proto]}")
done

for proto in "${!packages[@]}"; do
	out=${packages[$proto]}
	mkdir -p "$out"
	protoc -I upstream \
		--go_out="$out" "${go_opts[@]}" \
		--go-grpc_out="$out" "${grpc_opts[@]}" \
		"upstream/${proto}.proto"
done
