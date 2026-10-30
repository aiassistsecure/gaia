"""Shared fixtures for the gaia spine read tests.

Builds the ground-truth spine exactly once per test session: runs the
Rust `spine_writer` example binary (which records a full causally
chained lifecycle for one fixture model via hearth-store) into a temp
dir, parses its single JSON stdout line, and hands both to the tests.

A missing fixture binary fails loudly here — never a silent skip,
because a skipped read test would let a broken binding look green.
"""

from __future__ import annotations

import glob
import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

import gaia


def _find_writer_binary() -> Path:
    env_bin = os.environ.get("GAIA_PY_WRITER_BIN")
    if env_bin:
        path = Path(env_bin)
        assert path.is_file(), (
            f"GAIA_PY_WRITER_BIN={env_bin!r} does not point at a file; "
            "build the fixture first: cargo build -p gaia-py --example spine_writer"
        )
        return path

    tests_dir = Path(__file__).resolve().parent
    # tests -> python -> gaia-py (3 levels up from the tests dir);
    # the workspace target dir is <gaia-py>/../../target.
    gaia_py_dir = tests_dir.parent.parent
    target_dir = os.environ.get("CARGO_TARGET_DIR")
    if target_dir:
        search_root = Path(target_dir)
    else:
        search_root = gaia_py_dir.parent.parent / "target"

    pattern = str(search_root / "debug" / "examples" / "spine_writer*")
    candidates = [
        Path(p)
        for p in glob.glob(pattern)
        if Path(p).is_file()
        and Path(p).suffix not in {".d", ".rlib", ".rmeta", ".pdb", ".exp", ".lib"}
        and Path(p).stem == "spine_writer"
    ]
    assert candidates, (
        f"spine_writer fixture binary not found (globbed {pattern!r}); "
        "build it first: cargo build -p gaia-py --example spine_writer, "
        "or set GAIA_PY_WRITER_BIN / CARGO_TARGET_DIR"
    )
    # Prefer an exact `spine_writer` / `spine_writer.exe` name.
    candidates.sort(key=lambda p: (p.name not in {"spine_writer", "spine_writer.exe"}, str(p)))
    return candidates[0]


@pytest.fixture(scope="session")
def spine_root(tmp_path_factory: pytest.TempPathFactory) -> SimpleNamespace:
    """Session-scoped ground-truth spine: SimpleNamespace(root, truth).

    The writer's NEDB holds a single-process lock on the data dir
    while it runs, and the truth it prints is the state at ITS seq —
    so a second writer pass over the same dir is refused outright,
    and any re-record would move the global seq past the printed
    truth. The environment is therefore scrubbed of NEDB_SHARED_OPEN
    (a shared open would silently double-open instead of failing
    loudly) and the writer runs exactly once per output dir: if a
    re-run ever made the dir, it must be a fresh one, so languages
    and lifetimes never share a spine by accident.
    """
    binary = _find_writer_binary()
    root = tmp_path_factory.mktemp("spine") / "spine-root"
    env = {k: v for k, v in os.environ.items() if k != "NEDB_SHARED_OPEN"}
    result = subprocess.run(
        [str(binary), str(root)],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    assert lines, (
        f"spine_writer produced no stdout (stderr: {result.stderr!r}); "
        "expected exactly one JSON line"
    )
    try:
        truth = json.loads(lines[-1])
    except json.JSONDecodeError as exc:
        raise AssertionError(
            f"spine_writer stdout line is not JSON: {lines[-1]!r} ({exc})"
        ) from exc
    for key in ("root", "kinds", "hashes", "seqs", "resident_seq", "latest_hash", "spine_seq"):
        assert key in truth, f"spine_writer truth JSON is missing key {key!r}: {truth!r}"
    return SimpleNamespace(root=str(root), truth=truth)


@pytest.fixture(scope="session")
def spine(spine_root) -> gaia.Spine:
    """The session's one read-only holder of the fixture spine.

    Exactly one, on purpose: NEDB refuses a second opener while a
    holder lives (single-process by design), and pytest retains a
    failed test's frame — a per-test open would let one failure hold
    the lock and cascade lock errors through every later test. Read
    tests share this holder; reads are side-effect-free.
    """
    return gaia.Spine(spine_root.root)
