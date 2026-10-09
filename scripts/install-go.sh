#!/usr/bin/env bash
set -euo pipefail

# Install exactly the toolchain tested by Mithril's CI, with official checksums.
# Usage: sudo ./scripts/install-go.sh [installation-directory]
# Use /var/tmp rather than Ubuntu 26.04's RAM-backed /tmp for the archive.
main() {
    local repo version os arch destination work staging backup filename checksum
    repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
    version=$(awk '$1 == "go" {print $2; exit}' "$repo/go.mod")
    [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "Invalid Go version in go.mod" >&2; return 1; }
    os=$(uname -s | tr '[:upper:]' '[:lower:]')
    case "$(uname -m)" in
        x86_64) arch=amd64 ;;
        aarch64|arm64) arch=arm64 ;;
        *) echo "Unsupported architecture" >&2; return 1 ;;
    esac
    case "$os" in linux|darwin) ;; *) echo "Unsupported OS: $os" >&2; return 1 ;; esac
    for command in curl python3 tar; do
        command -v "$command" >/dev/null || { echo "Install $command first" >&2; return 1; }
    done
    destination="${1:-/usr/local/go}"
    [[ "$destination" == /* && "$destination" != / && "$destination" != */ && ! -L "$destination" ]] || {
        echo "Choose an absolute, non-symlink Go installation directory" >&2; return 1;
    }
    if [[ -x "$destination/bin/go" ]] && [[ "$("$destination/bin/go" version)" == "go version go$version $os/$arch" ]]; then
        echo "Go $version already installed at $destination"
        return 0
    fi
    work=$(mktemp -d /var/tmp/mithril-go.XXXXXX)
    # A subshell trap cleans downloads and staging even when a check fails.
    (
        staging=""
        trap 'rm -rf "$work"; [[ -z "$staging" ]] || rm -rf "$staging"' EXIT
        curl --fail --silent --show-error --location --retry 3 \
            'https://go.dev/dl/?mode=json&include=all' -o "$work/releases.json"
        filename="go${version}.${os}-${arch}.tar.gz"
        checksum=$(python3 - "$work/releases.json" "$version" "$filename" <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    releases = json.load(f)
for release in releases:
    if release['version'] == 'go' + sys.argv[2] and release['stable']:
        for archive in release['files']:
            if archive['filename'] == sys.argv[3]:
                print(archive['sha256'])
                sys.exit(0)
sys.exit('No matching stable archive in official Go release metadata')
PY
        )
        [[ "$checksum" =~ ^[a-f0-9]{64}$ ]] || { echo "Invalid official checksum" >&2; exit 1; }
        curl --fail --silent --show-error --location --retry 3 \
            "https://go.dev/dl/$filename" -o "$work/$filename"
        python3 - "$work/$filename" "$checksum" <<'PY'
import hashlib, sys
h = hashlib.sha256()
with open(sys.argv[1], 'rb') as f:
    for chunk in iter(lambda: f.read(1024 * 1024), b''):
        h.update(chunk)
if h.hexdigest() != sys.argv[2]:
    sys.exit('Go archive checksum mismatch; current installation was not changed')
PY
        mkdir -p "$(dirname "$destination")"
        staging=$(mktemp -d "$(dirname "$destination")/.mithril-go.XXXXXX")
        tar -xzf "$work/$filename" -C "$staging"
        [[ "$("$staging/go/bin/go" version)" == "go version go$version $os/$arch" ]]
        backup="$staging/previous"
        [[ ! -e "$destination" ]] || mv "$destination" "$backup"
        if ! mv "$staging/go" "$destination"; then
            [[ ! -e "$backup" ]] || mv "$backup" "$destination"
            exit 1
        fi
        echo "Installed Go $version at $destination"
        echo "Add $destination/bin to PATH before building Mithril."
    )
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
