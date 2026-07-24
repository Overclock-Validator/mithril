#!/usr/bin/env python3
"""Hash every tracked and untracked nonignored Mithril source-tree entry."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import subprocess
import sys
from pathlib import Path


def git_source_paths(repo: Path) -> list[bytes]:
    completed = subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.decode("utf-8", "replace").strip())
    return sorted(set(path for path in completed.stdout.split(b"\0") if path))


def file_record(repo: bytes, path: bytes) -> tuple[str, int, int, str]:
    absolute = os.path.join(repo, path)
    try:
        metadata = os.lstat(absolute)
    except FileNotFoundError:
        return "missing", 0, 0, hashlib.sha256(b"").hexdigest()

    mode = stat.S_IMODE(metadata.st_mode)
    if stat.S_ISREG(metadata.st_mode):
        digest = hashlib.sha256()
        size = 0
        with open(absolute, "rb") as source:
            while chunk := source.read(1024 * 1024):
                digest.update(chunk)
                size += len(chunk)
        return "file", mode, size, digest.hexdigest()
    if stat.S_ISLNK(metadata.st_mode):
        target = os.readlink(absolute)
        if isinstance(target, str):
            target = os.fsencode(target)
        return "symlink", mode, len(target), hashlib.sha256(target).hexdigest()
    raise RuntimeError(
        f"unsupported source-tree entry {os.fsdecode(path)!r}: "
        f"mode {metadata.st_mode:o}"
    )


ManifestRecord = tuple[bytes, str, int, int, str]


def build_manifest(repo: Path) -> list[ManifestRecord]:
    repo_bytes = os.fsencode(str(repo))
    records: list[ManifestRecord] = []
    for path in git_source_paths(repo):
        kind, mode, size, digest = file_record(repo_bytes, path)
        records.append((path, kind, mode, size, digest))
    if not records:
        raise RuntimeError("source manifest is empty")
    return records


def tree_digest(records: list[ManifestRecord]) -> str:
    digest = hashlib.sha256()
    for path, kind, mode, size, content_digest in records:
        for field in (
            path,
            kind.encode("ascii"),
            f"{mode:o}".encode("ascii"),
            str(size).encode("ascii"),
            content_digest.encode("ascii"),
        ):
            digest.update(len(field).to_bytes(8, "big"))
            digest.update(field)
    return digest.hexdigest()


def write_manifest(repo: Path, output_path: Path) -> None:
    records = build_manifest(repo.resolve())
    digest = tree_digest(records)
    with output_path.open("w", encoding="utf-8") as output:
        output.write(f"source_tree_sha256={digest}\n")
        output.write(f"source_entry_count={len(records)}\n")
        output.write("path_json\ttype\tmode_octal\tsize_bytes\tcontent_sha256\n")
        for path, kind, mode, size, content_digest in records:
            rendered_path = json.dumps(os.fsdecode(path), ensure_ascii=True)
            output.write(
                f"{rendered_path}\t{kind}\t{mode:o}\t{size}\t{content_digest}\n"
            )


def self_test() -> None:
    fixtures: list[ManifestRecord] = [
        (b"a", "file", 0o644, 1, "0" * 64),
        (b"dir/b", "file", 0o755, 2, "1" * 64),
    ]
    first = tree_digest(fixtures)
    assert first == tree_digest(list(fixtures))
    changed = list(fixtures)
    changed[1] = (b"dir/b", "file", 0o755, 3, "1" * 64)
    assert first != tree_digest(changed)
    print("sigverify-unaffected-source-manifest: self-test passed")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--repo", default=".")
    parser.add_argument("--output")
    args = parser.parse_args()
    try:
        if args.self_test:
            self_test()
        elif not args.output:
            parser.error("--output is required")
        else:
            write_manifest(Path(args.repo), Path(args.output))
    except (OSError, RuntimeError) as error:
        print(f"sigverify-unaffected-source-manifest: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
