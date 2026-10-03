#!/usr/bin/env python3
"""Compare this vendor tree to the checksum-pinned published crate, without extracting it.

Usage: python3 verify-source.py /path/to/openraft-0.9.25.crate [--diff]
Only the four inventoried upstream files may differ. New files are also inventoried.
"""

import difflib
import hashlib
import json
from pathlib import Path
import sys
import tarfile

ARCHIVE_SHA256 = "a97014fb78acb77be3a40ac2da305f6dd3a6b243f3a908ace87d29b3972eaafd"
REVISION = "8815cdba2826f74e848acef361ad03f93bb1c3f8"
PATCHED = {
    "src/core/raft_core.rs",
    "src/core/raft_msg/mod.rs",
    "src/raft/impl_raft_blocking_write.rs",
    "src/raft/mod.rs",
}
ADDED = {
    "LICENSE-MIT",
    "LICENSE-APACHE",
    "PROVENANCE.md",
    "verify-source.py",
    "src/raft/membership_admission_test.rs",
}
LICENSES = {
    "LICENSE-MIT": "23f18e03dc49df91622fe2a76176497404e46ced8a715d9d2b67a7446571cca3",
    "LICENSE-APACHE": "a60eea817514531668d7e00765731449fe14d059d3249e0bc93b36de45f759f2",
}


def main():
    root = Path(__file__).resolve().parent
    archive = Path(sys.argv[1])
    assert hashlib.sha256(archive.read_bytes()).hexdigest() == ARCHIVE_SHA256, "archive checksum mismatch"
    changed = set()
    originals = set()
    tree = hashlib.sha256()
    with tarfile.open(archive) as source:
        for member in sorted(source.getmembers(), key=lambda item: item.name):
            assert member.isfile(), f"unexpected archive entry: {member.name}"
            prefix, name = member.name.split("/", 1)
            assert prefix == "openraft-0.9.25"
            assert ".." not in Path(name).parts
            pristine = source.extractfile(member).read()
            originals.add(name)
            tree.update(name.encode() + b"\0" + hashlib.sha256(pristine).digest())
            current = (root / name).read_bytes()
            if current != pristine:
                changed.add(name)
                if "--diff" in sys.argv:
                    sys.stdout.writelines(difflib.unified_diff(
                        pristine.decode().splitlines(keepends=True),
                        current.decode().splitlines(keepends=True),
                        fromfile="pristine/" + name, tofile="vendor/" + name,
                    ))
    assert changed == PATCHED, f"unexpected changed files: {changed ^ PATCHED}"
    current_files = {
        str(path.relative_to(root)) for path in root.rglob("*")
        if path.is_file() and "target" not in path.relative_to(root).parts
    }
    assert current_files - originals == ADDED, f"unexpected additions: {(current_files - originals) ^ ADDED}"
    vcs = json.loads((root / ".cargo_vcs_info.json").read_text())
    assert vcs == {"git": {"sha1": REVISION}, "path_in_vcs": "openraft"}
    for name, checksum in LICENSES.items():
        assert hashlib.sha256((root / name).read_bytes()).hexdigest() == checksum, name
    print(f"Verified archive SHA-256: {ARCHIVE_SHA256}")
    print(f"Pristine tree SHA-256: {tree.hexdigest()} ({len(originals)} files)")
    print("Changed upstream files: " + ", ".join(sorted(changed)))
    print("All other published files, normalized manifest, VCS provenance, and upstream licenses verified.")


if __name__ == "__main__":
    main()
