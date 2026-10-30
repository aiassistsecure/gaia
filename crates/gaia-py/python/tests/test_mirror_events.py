"""Tests for ``gaia.Mirror`` — the Python reader over the spine mirror.

The mirror is a cache of the spine, never a source of truth.
That is gaia-api's own framing in ``crates/gaia-api/src/mirror.rs``:
a poller watches the spine (``hearth-store``), appends newly
observed events to a plain JSONL file keyed by spine seq, and
readers replay and tail from that file's contents.  Every mirrored
entry records the spine ``hash`` of the event it mirrors, so the
mirror can be checked against spine truth at any time; if mirror
and spine ever disagree, the spine wins and the mirror is rebuilt.
Nothing here writes to the spine, and nothing here invents an
event the spine did not record.

Accordingly the Python ``Mirror`` is a reader only: it loads the
JSONL file written by gaia-api's ``MirrorStore``, rebuilds events
from the stored lines, and serves them in ascending seq order.
The writer is the Rust poller — appending from Python would
desync the mirror from spine truth.  There is likewise no
si-source read here by design: the mirror is a spine cache, so
every event it returns is a ``hearth``-source event, and the two
sequence spaces are never merged.

Each stored line has exactly the shape ``MirrorStore`` writes:
``{"seq", "model", "kind", "facts", "hash", "ts"}``.  A line that
cannot be rebuilt into an event — unparsable, or missing the
``hash`` / ``seq`` / ``model`` / ``kind`` it cannot be rebuilt
without — is counted and described in ``load_failures()`` (with
its 1-based line number, e.g. ``"line 3: ..."``, matching
``mirror.rs``), never silently dropped and never fatal.

These tests construct their mirror files themselves in tmp dirs;
there is no external fixture.
"""

import json

import pytest

from gaia import Mirror


def _stored_line(seq, kind, model="m", facts=None, hash_value=None, ts=None):
    """One mirror line in the stored shape written by MirrorStore."""
    return {
        "seq": seq,
        "model": model,
        "kind": kind,
        "facts": {"seq": seq} if facts is None else facts,
        "hash": f"hash-{seq}" if hash_value is None else hash_value,
        "ts": 1_700_000_000.0 + seq if ts is None else ts,
    }


def _write_mirror(tmp_path, lines):
    """Write dicts (serialised as JSON) and raw strings as JSONL."""
    path = tmp_path / "mirror.jsonl"
    rendered = [
        line if isinstance(line, str) else json.dumps(line) for line in lines
    ]
    text = "\n".join(rendered)
    if text:
        text += "\n"
    path.write_text(text, encoding="utf-8")
    return str(path)


def test_mirror_open_never_creates_the_file(tmp_path):
    # The Rust store's open creates an absent mirror — right for the
    # poller, its writer; wrong for this read-only binding, where a
    # mistyped path would conjure an empty mirror and read as "no
    # events". Opening a missing mirror raises and creates nothing.
    missing = tmp_path / "never-written.jsonl"
    with pytest.raises(ValueError):
        Mirror(str(missing))
    assert not missing.exists()


def _mirror(tmp_path, lines):
    return Mirror(_write_mirror(tmp_path, lines))


def test_events_since_cursor_and_order(tmp_path):
    kinds = ["declared", "pull_started", "pull_completed", "loading", "resident"]
    lines = [_stored_line(seq, kind) for seq, kind in enumerate(kinds, start=1)]
    mirror = _mirror(tmp_path, lines)

    all_events = mirror.events_since(0)
    assert [event["seq"] for event in all_events] == [1, 2, 3, 4, 5]
    assert [event["kind"] for event in all_events] == kinds

    assert [event["seq"] for event in mirror.events_since(2)] == [3, 4, 5]
    assert mirror.events_since(5) == []
    assert mirror.latest_seq() == 5


def test_filters(tmp_path):
    lines = [
        _stored_line(1, "declared"),
        _stored_line(2, "resident"),
        _stored_line(3, "lost"),
        _stored_line(4, "loading"),
        _stored_line(5, "resident"),
    ]
    mirror = _mirror(tmp_path, lines)

    by_kind = mirror.events_since(0, kinds=["resident", "lost"])
    assert [event["seq"] for event in by_kind] == [2, 3, 5]
    assert {event["kind"] for event in by_kind} == {"resident", "lost"}

    assert mirror.events_since(0, models=["other"]) == []

    # Combined kinds + models intersect (AND), they do not union.
    both = mirror.events_since(0, kinds=["resident"], models=["m"])
    assert [event["seq"] for event in both] == [2, 5]
    assert mirror.events_since(0, kinds=["resident"], models=["other"]) == []
    both = mirror.events_since(0, kinds=["resident", "lost"], models=["m"])
    assert [event["seq"] for event in both] == [2, 3, 5]


def test_event_shape(tmp_path):
    lines = [
        _stored_line(
            1,
            "declared",
            model="m",
            facts={"vram_bytes": 4_000_000_000, "admitted": True},
            hash_value="aaa111",
            ts=1.5,
        ),
        _stored_line(
            2,
            "resident",
            model="m",
            facts={"endpoint": "127.0.0.1:8080", "warmup_ms": 43},
            hash_value="bbb222",
            ts=2.5,
        ),
    ]
    mirror = _mirror(tmp_path, lines)

    events = mirror.events_since(0)
    assert len(events) == len(lines)
    for event, line in zip(events, lines):
        assert event["source"] == "hearth"
        assert event["collection"] == "spine"
        assert event["seq"] == line["seq"]
        assert event["model"] == line["model"]
        assert event["kind"] == line["kind"]
        assert event["facts"] == line["facts"]
        assert event["hash"] == line["hash"]
        assert event["ts"] == line["ts"]


def test_bad_lines_are_failures_not_silence(tmp_path):
    good_1 = _stored_line(1, "declared")
    good_2 = _stored_line(3, "loading")
    good_3 = _stored_line(5, "resident")
    missing_hash = _stored_line(4, "lost")
    del missing_hash["hash"]
    lines = [
        good_1,          # line 1
        "this is not json",  # line 2: unparsable
        good_2,          # line 3
        missing_hash,    # line 4: missing "hash"
        good_3,          # line 5
    ]
    mirror = _mirror(tmp_path, lines)

    entries = mirror.entries()
    assert [event["seq"] for event in entries] == [1, 3, 5]

    failures = mirror.load_failures()
    assert len(failures) == 2
    assert all(isinstance(failure, str) and failure for failure in failures)
    # Failure strings name their 1-based line, as in mirror.rs.
    assert any("line 2" in failure for failure in failures)
    assert any("line 4" in failure for failure in failures)

    # The good events are still readable despite the bad lines.
    assert [event["seq"] for event in mirror.events_since(0)] == [1, 3, 5]
    assert mirror.latest_seq() == 5


def test_empty_and_missing(tmp_path):
    empty = _mirror(tmp_path, [])
    assert empty.latest_seq() == 0
    assert empty.entries() == []
    assert empty.load_failures() == []
    assert empty.events_since(0) == []

    missing = tmp_path / "does-not-exist.jsonl"
    with pytest.raises(ValueError):
        Mirror(str(missing))


def test_read_only_surface(tmp_path):
    mirror = _mirror(tmp_path, [])
    public_names = {name for name in dir(mirror) if not name.startswith("_")}
    forbidden = {
        "append",
        "write",
        "record",
        "put",
        "delete",
        "remove",
        "rebuild",
        "flush",
    }
    assert public_names.isdisjoint(forbidden)
