"""Read-only spine tests for the gaia Python bindings (G-7).

Ground truth comes from the `spine_root` fixture (conftest.py): the
Rust spine_writer binary records one chained lifecycle for MODEL via
hearth-store and reports the why() chain it derives from the spine.
These tests assert the Python `gaia.Spine` reader reconstructs exactly
that chain from disk — and exposes no write surface at all.

All read tests share the session's single `spine` holder (conftest):
NEDB is single-process and refuses a second live opener, so there is
exactly one holder per session by design, not by convenience.
"""

from __future__ import annotations

import pytest

import gaia
from gaia import Spine

MODEL = "gaia-fixture:latest"


def test_seq_and_latest(spine, spine_root):
    truth = spine_root.truth
    # The global clock is the writer's own spine.seq(), printed as
    # truth: NEDB's seq is global, not per-chain, and can sit above
    # the last event's seq (engine bookkeeping consumes seqs too).
    assert spine.seq() == truth["spine_seq"]
    assert spine.seq() >= max(truth["seqs"])
    latest = spine.latest(MODEL)
    assert latest is not None
    assert latest["kind"] == "lost"
    assert latest["hash"] == truth["latest_hash"]
    assert latest["model"] == MODEL


def test_why_chain_reconstruction(spine, spine_root):
    truth = spine_root.truth
    chain = spine.why(MODEL)
    assert [e["kind"] for e in chain] == truth["kinds"]
    assert [e["hash"] for e in chain] == truth["hashes"]
    seqs = [e["seq"] for e in chain]
    assert all(a > b for a, b in zip(seqs, seqs[1:])), f"seqs not strictly descending: {seqs}"
    by_kind = {e["kind"]: e for e in chain}
    assert by_kind["resident"]["facts"]["warmup_ms"] == 1200
    assert by_kind["pull_started"]["facts"]["source"] == "hf:fixture/gaia-fixture@Q4_K_M"
    assert by_kind["lost"]["facts"]["reason"] == "process_exited"


def test_state_as_of(spine, spine_root):
    truth = spine_root.truth
    then = spine.state_as_of(MODEL, truth["resident_seq"])
    assert then is not None
    assert then["kind"] == "resident"
    now = spine.state_as_of(MODEL, truth["seqs"][0])
    assert now is not None
    assert now["kind"] == "lost"


def test_provenance_and_latest_pull(spine):
    provenance = spine.provenance(MODEL)
    # provenance() is the causal walk from the latest pull, not a
    # pulls-only filter: the fixture's PullStarted cites the Declared
    # event as its cause, so the chain runs pull_completed ->
    # pull_started -> declared. (hearth-store's own provenance test
    # records its pull unlinked, so its chain stops at two — the walk
    # is the same; the fixture's links are longer.)
    assert [e["kind"] for e in provenance] == [
        "pull_completed", "pull_started", "declared",
    ]
    latest_pull = spine.latest_pull(MODEL)
    assert latest_pull is not None
    assert latest_pull["kind"] == "pull_completed"


def test_all_latest_and_head_and_verify(spine):
    matches = [e for e in spine.all_latest() if e["model"] == MODEL]
    assert len(matches) == 1, f"model should appear exactly once in all_latest: {matches}"
    assert matches[0]["kind"] == "lost"
    head = spine.head()
    assert isinstance(head, str) and head
    result = spine.verify()
    assert result["failures"] == []
    assert result["checked"] >= 6


def test_unknown_model_is_empty_not_an_error(spine):
    assert spine.latest("nope") is None
    assert spine.why("nope") == []
    assert spine.state_as_of("nope", 1) is None


def test_open_missing_root_raises(tmp_path):
    # Spine::open creates its dir via NEDB, so a merely-missing path is
    # not an error. A path whose parent is a FILE can never be created.
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory")
    with pytest.raises(ValueError):
        Spine(str(blocker / "spine"))


def test_second_open_while_held_is_refused(spine, spine_root):
    # NEDB is single-process: while the session holder holds the data
    # dir, a second open of the same dir is refused (ValueError)
    # rather than split-brain. This pins the engine's discipline —
    # and documents that audit tooling opens a spine when no other
    # opener holds it (or is the holder), never concurrently, unless
    # NEDB_SHARED_OPEN is set with its risk accepted. The fixtures
    # never set it.
    with pytest.raises(ValueError):
        Spine(spine_root.root)
    # The refused open did not disturb the holder.
    assert spine.latest(MODEL)["kind"] == "lost"


def test_read_only_surface(spine):
    forbidden_substrings = [
        "record", "write", "put", "delete", "remove", "insert",
        "update", "flush", "set", "open", "close", "mutate",
    ]
    for owner_name, owner in (("class", Spine), ("instance", spine)):
        for name in dir(owner):
            if name.startswith("_"):
                continue
            lowered = name.lower()
            for bad in forbidden_substrings:
                assert bad not in lowered, (
                    f"Spine {owner_name} exposes public attribute {name!r} "
                    f"containing forbidden substring {bad!r} — spine must be read-only"
                )
    for bad in ("record", "flush", "put", "delete"):
        assert not hasattr(gaia.Spine, bad), f"gaia.Spine must not have attribute {bad!r}"
    for method in (
        "seq", "latest", "latest_pull", "why", "provenance",
        "state_as_of", "all_latest", "head", "verify",
    ):
        assert callable(getattr(spine, method, None)), f"Spine is missing read method {method!r}"
