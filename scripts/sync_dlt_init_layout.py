#!/usr/bin/env python3
"""Mirror paradox_dlt_sources/<name>/ → sources/<name>/ for dlt init discoverability.

The canonical home for source code is `paradox_dlt_sources/<name>/` (shipped on
PyPI). The `sources/<name>/` tree is a verbatim copy required because dlt's
`init` CLI hardcodes its verified-source lookup to `<repo_root>/sources/` and
rejects symlinks (FileStorage resolves realpath outside the storage root).

Run after editing any source. CI runs this with `--check` to fail on drift.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CANONICAL = REPO_ROOT / "paradox_dlt_sources"
SHIM = REPO_ROOT / "sources"
IGNORE = shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo")


def sources() -> list[str]:
    return sorted(
        p.name
        for p in CANONICAL.iterdir()
        if p.is_dir() and not p.name.startswith("_") and (p / "__init__.py").exists()
    )


def sync() -> None:
    SHIM.mkdir(exist_ok=True)
    for name in sources():
        dst = SHIM / name
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(CANONICAL / name, dst, ignore=IGNORE)
        print(f"synced sources/{name}/")


def check() -> int:
    result = subprocess.run(
        ["git", "diff", "--exit-code", "--", str(SHIM)],
        cwd=REPO_ROOT,
        check=False,
    )
    if result.returncode != 0:
        print(
            "\nsources/ is out of sync with paradox_dlt_sources/.\n"
            "Run: python scripts/sync_dlt_init_layout.py",
            file=sys.stderr,
        )
    return result.returncode


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Sync then fail if anything changed (for CI).",
    )
    args = parser.parse_args()
    sync()
    return check() if args.check else 0


if __name__ == "__main__":
    sys.exit(main())
