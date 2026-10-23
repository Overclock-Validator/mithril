#!/usr/bin/env python3
"""Build Linux/amd64 Mithril with a pinned, isolated interpreter-placement linker."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import struct
import subprocess
import sys
import tempfile

VERSION = "go1.27.2"
SYMBOL = "github.com/Overclock-Validator/mithril/pkg/sbpf.(*Interpreter).Run"
SOURCES = {
    "data.go": "2f51cea9197f1fc3dc4ccb62738f807400817a9759b960c5fbcf4403d7f5f08d",
    "main.go": "0fe8ad02e4b244bbaa82f14a86aaa005155e43c9ba7492c98c9e9650b86cd41e",
}
ROOT = Path(__file__).resolve().parent.parent
PATCH = ROOT / "scripts/toolchain/go1.27.2/interpreter-offset.patch"


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def run(args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def interpreter_address(nm, offset):
    matches = []
    for line in nm.splitlines():
        fields = line.split()
        if len(fields) == 4 and fields[3] == SYMBOL and fields[2] in ("T", "t"):
            matches.append(int(fields[0], 16))
    if len(matches) != 1:
        raise ValueError(f"expected one interpreter text symbol, found {len(matches)}")
    address = matches[0]
    if address % 64 != offset:
        raise ValueError(f"interpreter address 0x{address:x}: expected mod64={offset}, got {address % 64}")
    return address


def check_elf(path):
    with Path(path).open("rb") as f:
        header = f.read(20)
    if len(header) != 20 or header[:6] != b"\x7fELF\x02\x01" or struct.unpack_from("<H", header, 18)[0] != 62:
        raise ValueError("placement verification requires a Linux/amd64 ELF executable")


def make_linker(go, goroot, cache, patch_hash):
    """Copy only cmd/link; symlink the rest of GOROOT without modifying it."""
    cache.mkdir(parents=True, exist_ok=True)
    with (cache / "lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        target = cache / patch_hash
        manifest = target / "manifest.json"
        if manifest.exists():
            saved = json.loads(manifest.read_text())
            if saved["patch_sha256"] == patch_hash and digest(target / "link") == saved["linker_sha256"]:
                return target / "link", saved
            raise ValueError("cached linker does not match its provenance; remove this cache entry")
        if target.exists():
            raise ValueError("incomplete linker cache entry; remove it before retrying")
        with tempfile.TemporaryDirectory(prefix="prepare-", dir=cache) as tmp:
            stage = Path(tmp)
            tree = stage / "goroot"
            (tree / "src/cmd").mkdir(parents=True)
            for parent, destination, omit in ((goroot, tree, "src"), (goroot / "src", tree / "src", "cmd"), (goroot / "src/cmd", tree / "src/cmd", "link")):
                for entry in parent.iterdir():
                    if entry.name != omit:
                        (destination / entry.name).symlink_to(entry)
            shutil.copytree(goroot / "src/cmd/link", tree / "src/cmd/link")
            # Reject a stale patch before changing the private source tree.
            run(["patch", "--dry-run", "-p1", "-i", str(PATCH)], cwd=tree / "src")
            run(["patch", "-p1", "-i", str(PATCH)], cwd=tree / "src")
            env = dict(os.environ, GOROOT=str(tree), GOTOOLCHAIN="local", GOOS="linux", GOARCH="amd64")
            run([str(go), "build", "-pgo=off", "-trimpath", "-o", str(stage / "link"), "cmd/link"], cwd=ROOT, env=env, timeout=600)
            saved = {"go_version": VERSION, "patch_sha256": patch_hash, "source_sha256": SOURCES, "linker_sha256": digest(stage / "link")}
            (stage / "manifest.json").write_text(json.dumps(saved, indent=2) + "\n")
            # The private source tree is no longer needed after building the tool.
            shutil.rmtree(tree)
            stage.rename(target)
        return target / "link", saved


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go", default=os.environ.get("GO", "go"))
    parser.add_argument("--cache", type=Path, default=ROOT / ".build/zen5-linker")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--offset", type=int, choices=(0, 32), default=32, help="0 is a comparison control; 32 is the Zen 5 profile")
    parser.add_argument("command", nargs=argparse.REMAINDER, help="-- build ... or -- test -c ...")
    opts = parser.parse_args()
    args = opts.command
    if args[:1] == ["--"]:
        args = args[1:]
    if not args or args[0] not in ("build", "test") or (args[0] == "test" and "-c" not in args):
        parser.error("use -- build ... or -- test -c ...; tests run after artifact verification")
    for arg in args[1:]:
        if arg in ("-o", "-toolexec", "-pgo") or arg.startswith(("-o=", "-toolexec=", "-pgo=")):
            parser.error("output, toolexec and PGO are controlled by this build command")
    go_name = shutil.which(opts.go)
    if not go_name:
        parser.error(f"Go executable not found: {opts.go}")
    go = Path(go_name).resolve()
    env = dict(os.environ, GOTOOLCHAIN="local")
    version = run([str(go), "version"], env=env, capture_output=True, text=True).stdout.split()
    if len(version) < 3 or version[2] != VERSION:
        parser.error(f"requires {VERSION}; found {' '.join(version)}")
    info = json.loads(run([str(go), "env", "-json", "GOROOT", "GOHOSTOS", "GOHOSTARCH", "GOOS", "GOARCH"], env=env, capture_output=True, text=True).stdout)
    if any(info[k] != v for k, v in {"GOHOSTOS": "linux", "GOHOSTARCH": "amd64", "GOOS": "linux", "GOARCH": "amd64"}.items()):
        parser.error("this measured profile requires a Linux/amd64 host and target")
    if os.environ.get("GOEXPERIMENT", "") not in ("", "arenas") or os.environ.get("GOFLAGS", "").find("-toolexec") >= 0:
        parser.error("use default or arenas experiments and no inherited toolexec")
    goroot = Path(info["GOROOT"])
    for name, expected in SOURCES.items():
        if digest(goroot / "src/cmd/link/internal/ld" / name) != expected:
            raise ValueError(f"{VERSION} linker source differs from pinned {name}")
    patch_hash = digest(PATCH)
    linker, provenance = make_linker(go, goroot, opts.cache.resolve(), patch_hash)
    output = opts.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    # The final artifact is published only after its placement is verified.
    with tempfile.TemporaryDirectory(prefix="zen5-build-", dir=output.parent) as tmp:
        stage = Path(tmp)
        wrapper = stage / "toolexec.sh"
        wrapper.write_text("#!/bin/sh\nif [ \"${1##*/}\" = link ]; then\n  shift\n  exec " + shlex.quote(str(linker)) + f" -mithril-interpreter-offset={opts.offset} \"$@\"\nfi\nexec \"$@\"\n")
        wrapper.chmod(0o755)
        artifact = stage / "artifact"
        run([str(go), args[0], "-pgo=off", "-toolexec=" + str(wrapper), "-o", str(artifact), *args[1:]], cwd=ROOT, env=env, timeout=1200)
        check_elf(artifact)
        nm = run([str(go), "tool", "nm", "-size", str(artifact)], capture_output=True, text=True, env=env).stdout
        address = interpreter_address(nm, opts.offset)
        build_info = run([str(go), "version", "-m", str(artifact)], capture_output=True, text=True, env=env).stdout
        metadata = dict(provenance, profile="zen5" if opts.offset == 32 else "placement-control", interpreter_symbol=SYMBOL, interpreter_address=hex(address), interpreter_mod64=address % 64, binary_sha256=digest(artifact), mithril_source_sha256={name: digest(ROOT / name) for name in ("pkg/sbpf/interpreter.go", "pkg/sbpf/fastmem.go")}, build_info=build_info)
        sidecar = stage / "layout.json"
        sidecar.write_text(json.dumps(metadata, indent=2) + "\n")
        os.replace(artifact, output)
        os.replace(sidecar, Path(str(output) + ".layout.json"))
    print(f"verified {output}: {SYMBOL}=0x{address:x}, mod64={opts.offset}")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        sys.exit(str(error))
