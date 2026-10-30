"""gaia — read the runtime's memory from Python.

The Rust side hands JSON across the boundary and this module turns it into
dicts. Thin on purpose: every fact about residency history lives in
hearth-store's spine and is tested there once, in Rust. A binding that
reimplements any of it becomes a second source of truth, and second sources
of truth disagree with the first one eventually.

Read-only on purpose, too. The spine is the audit trail for everything the
runtime did; a Python consumer that could write to it could rewrite the
history it is supposed to be auditing. So ``Spine`` exposes reads — ``seq``,
``latest``, ``why``, ``state_as_of`` — and no way to record, and ``Mirror``
exposes reads over the mirror file the Rust poller maintains, and no way to
append to it. The absence of write methods is the feature.
"""

from __future__ import annotations

import json
from typing import Any, Iterable

from . import _gaia  # type: ignore[attr-defined]

__version__ = _gaia.__version__


def _one(raw: str | None) -> dict[str, Any] | None:
    return None if raw is None else json.loads(raw)


def _many(raw: str) -> list[dict[str, Any]]:
    return json.loads(raw)


class Spine:
    """A read-only handle on one on-disk spine.

    Opening takes the spine's root directory — the same directory a Rust
    supervisor opened with ``hearth_store::Spine::open``. NEDB is
    single-process: opening a spine a live process still holds is
    refused (``ValueError``), never split-brain — so the audit pattern
    is sequential (open it when the writer has finished, or be the
    process that holds it), not two live openers at once. The engine
    offers ``NEDB_SHARED_OPEN=1`` as an explicit opt-in with its own
    stated risks; these bindings never set it for you.
    """

    def __init__(self, root: str) -> None:
        self._s = _gaia.Spine(str(root))

    def seq(self) -> int:
        """The spine's current global sequence — the clock
        ``state_as_of`` reads against."""
        return self._s.seq()

    def latest(self, model: str) -> dict[str, Any] | None:
        """The model's current state — its latest residency event, or
        None if the spine has never seen it."""
        return _one(self._s.latest(model))

    def latest_pull(self, model: str) -> dict[str, Any] | None:
        """The model's latest pull event, if it was ever fetched. Kept
        separate from ``latest`` for the store's own reason: having the
        bytes is not having them loaded."""
        return _one(self._s.latest_pull(model))

    def why(self, model: str) -> list[dict[str, Any]]:
        """The causal chain that produced the model's current state,
        newest first: the answer to "why is this model (not) resident?"
        """
        return _many(self._s.why(model))

    def provenance(self, model: str) -> list[dict[str, Any]]:
        """Where the bytes came from, newest first: the pull chain."""
        return _many(self._s.provenance(model))

    def state_as_of(self, model: str, seq: int) -> dict[str, Any] | None:
        """What the model's state was at sequence ``seq``. Time travel."""
        return _one(self._s.state_as_of(model, int(seq)))

    def all_latest(self) -> list[dict[str, Any]]:
        """The latest residency event for every model the spine has
        ever seen."""
        return _many(self._s.all_latest())

    def head(self) -> str:
        """The Merkle head of the whole history."""
        return self._s.head()

    def verify(self) -> dict[str, Any]:
        """Check every node against its hash. Returns
        ``{"checked": int, "failures": [str, ...]}`` — cryptographic
        proof the history wasn't rewritten, or the list of places it
        was."""
        return json.loads(self._s.verify())


class Mirror:
    """A read-only handle on the spine mirror (G-4).

    The mirror is a gaia-owned JSONL cache of spine events that the
    Rust poller appends to; it is a cache of the spine, never a source
    of truth. This class reads it — replay and resume from a cursor —
    and cannot append: a Python-side writer would desync the file from
    the spine it mirrors.
    """

    def __init__(self, path: str) -> None:
        self._m = _gaia.Mirror(str(path))

    def events_since(self, after_seq: int = 0,
                     kinds: Iterable[str] | None = None,
                     models: Iterable[str] | None = None
                     ) -> list[dict[str, Any]]:
        """Mirrored events after ``after_seq``, in ascending seq order —
        the resume read. ``kinds`` and ``models`` optionally narrow the
        result; spine sequence space only (this mirror holds no other).
        """
        return _many(self._m.events_since(
            int(after_seq),
            list(kinds) if kinds is not None else None,
            list(models) if models is not None else None))

    def entries(self) -> list[dict[str, Any]]:
        """Every mirrored event, ascending by seq."""
        return _many(self._m.entries())

    def latest_seq(self) -> int:
        """The highest mirrored seq — the mirror's resume position.
        0 when the mirror is empty."""
        return self._m.latest_seq()

    def load_failures(self) -> list[str]:
        """Lines that failed to load, each naming its line and why.
        Empty means the file loaded clean. A line listed here was
        skipped, never silently dropped — a consumer can tell a gap
        from "no events"."""
        return self._m.load_failures()


__all__ = ["Spine", "Mirror", "__version__"]
