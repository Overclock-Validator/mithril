#!/usr/bin/env python3
"""Package locally built Mithril binaries with an approved viewer distribution."""

import argparse
from contextlib import contextmanager
import gzip
import hashlib
import io
import json
import os
import re
import stat
import subprocess
import tarfile
import tempfile
from pathlib import Path


BINARIES = ("mithril", "mithril-dashboard", "mithril-dashboard-collect")
RELEASE_FILES = (
    "LICENSE",
    "docs/dashboard-api.md",
    "docs/dashboard-api-reference.md",
    "cmd/mithril-dashboard-collect/README.md",
    "examples/dashboard/mithril-dashboard.nginx.conf",
    "examples/dashboard/mithril-dashboard.path",
    "examples/dashboard/mithril-dashboard.service",
    "examples/dashboard/mithril-dashboard-collect.service",
    "examples/dashboard/mithril-dashboard-collect.timer",
    "examples/dashboard/mithril-dashboard.service.d/20-standard-origin.conf",
)
ASSET = re.compile(
    r"(?:assets/[A-Za-z0-9][A-Za-z0-9._-]*\.(?:js|css)"
    r"|brand/[A-Za-z0-9][A-Za-z0-9._-]*\.svg"
    r"|fonts/[A-Za-z0-9][A-Za-z0-9._-]*\.(?:ttf|woff2?)"
    r"|licenses/[A-Za-z0-9][A-Za-z0-9._-]*\.txt)"
)
VIEWER_DIRS = {"assets", "brand", "fonts", "licenses", ".vite"}
BUILD_MANIFEST = ".vite/manifest.json"


@contextmanager
def regular_file(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as source:
        meta = os.fstat(source.fileno())
        if not stat.S_ISREG(meta.st_mode):
            raise ValueError(f"expected a regular file: {path}")
        yield source, meta.st_size


def sha256(source):
    digest = hashlib.sha256()
    for chunk in iter(lambda: source.read(1 << 20), b""):
        digest.update(chunk)
    return digest.hexdigest()


class HashingReader:
    def __init__(self, source):
        self.source = source
        self.hash = hashlib.sha256()

    def read(self, size):
        data = self.source.read(size)
        self.hash.update(data)
        return data


def check_linux_binary(path):
    with regular_file(path) as (source, _):
        header = source.read(20)
    # Linux AMD64 builds use little-endian ELF64 and machine EM_X86_64.
    if header[:6] != b"\x7fELF\x02\x01" or header[18:20] != b"\x3e\x00":
        raise ValueError(f"{path.name} is not a Linux AMD64 ELF binary")
    try:
        info = json.loads(subprocess.check_output(
            ["go", "version", "-m", "-json", str(path.absolute())],
            stderr=subprocess.DEVNULL, timeout=10,
        ))
    except (subprocess.SubprocessError, ValueError) as error:
        raise ValueError(f"could not check Go build settings for {path.name}") from error
    if not isinstance(info, dict):
        raise ValueError(f"could not check Go build settings for {path.name}")
    settings = {item["Key"]: item["Value"] for item in info.get("Settings") or []}
    expected = {"GOOS": "linux", "GOARCH": "amd64", "CGO_ENABLED": "1"}
    if info.get("Path") != f"github.com/Overclock-Validator/mithril/cmd/{path.name}" or any(
        settings.get(key) != value for key, value in expected.items()
    ):
        raise ValueError(f"{path.name} must be the matching Mithril command built for Linux AMD64 with CGO")


def viewer_files(dist, manifest):
    if dist.is_symlink() or not dist.is_dir():
        raise ValueError("viewer dist must be a real directory")
    with regular_file(manifest) as (source, _):
        expected = json.load(source)
    if not isinstance(expected, dict) or "index.html" not in expected:
        raise ValueError("viewer manifest must map relative paths to SHA-256 and include index.html")
    files = {}
    for path in dist.rglob("*"):
        name = path.relative_to(dist).as_posix()
        if path.is_symlink():
            raise ValueError(f"viewer symlink: {name}")
        if path.is_dir() and name in VIEWER_DIRS:
            continue
        if name not in ("index.html", BUILD_MANIFEST) and not ASSET.fullmatch(name):
            raise ValueError(f"unexpected viewer path: {name}")
        files[name] = path
    if files.keys() != expected.keys():
        raise ValueError("viewer files do not match the approved manifest")
    return files, expected


def make_bundle(binaries, dist, manifest, output, target):
    if binaries.is_symlink() or not binaries.is_dir():
        raise ValueError("binary input must be a real directory")
    if target == "linux-amd64" or "linux-amd64" in output.name.lower():
        for name in BINARIES:
            check_linux_binary(binaries / name)
    repo = Path(__file__).resolve().parents[1]
    files, expected = viewer_files(dist, manifest)
    with tempfile.TemporaryDirectory(prefix=".dashboard-bundle-", dir=output.parent) as work:
        archive = Path(work) / "bundle.tar.gz"
        sums = []
        with archive.open("xb") as out, gzip.GzipFile(
            filename="", mode="wb", fileobj=out, mtime=0,
        ) as compressed, tarfile.open(fileobj=compressed, mode="w|", format=tarfile.USTAR_FORMAT) as tar:
            def add(name, path, mode=0o644):
                with regular_file(path) as (source, size):
                    info = tarfile.TarInfo(name)
                    info.size = size
                    info.mode = mode
                    content = HashingReader(source)
                    tar.addfile(info, content)
                digest = content.hash.hexdigest()
                sums.append(f"{digest}  {name}\n")
                return digest

            for name, path in sorted(files.items()):
                if name == BUILD_MANIFEST:
                    with regular_file(path) as (source, _):
                        digest = sha256(source)
                else:
                    digest = add(f"dist/{name}", path)
                if digest != expected[name]:
                    raise ValueError(f"viewer checksum mismatch: {name}")
            for name in BINARIES:
                add(f"bin/{name}", binaries / name, 0o755)
            for name in RELEASE_FILES:
                add(name, repo / name)
            checksums = "".join(sums).encode()
            info = tarfile.TarInfo("SHA256SUMS")
            info.size = len(checksums)
            tar.addfile(info, io.BytesIO(checksums))

        with archive.open("rb") as source:
            digest = sha256(source)
        os.link(archive, output)
        print(f"{digest}  {output}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=("native", "linux-amd64"), default="native")
    parser.add_argument("binaries", type=Path)
    parser.add_argument("viewer_dist", type=Path)
    parser.add_argument("viewer_manifest", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    try:
        make_bundle(args.binaries, args.viewer_dist, args.viewer_manifest, args.output, args.target)
    except (OSError, ValueError, tarfile.TarError) as error:
        parser.exit(1, f"dashboard bundle: {error}\n")


if __name__ == "__main__":
    main()
