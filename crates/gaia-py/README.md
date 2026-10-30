# gaia-engine

**Read the runtime's memory from Python.** The spine remembers every
residency transition the runtime ever made — causally linked,
hash-verified, time-travelable. These bindings let Python ask it
questions.

Native bindings to [gaia](https://github.com/aiassistsecure/gaia)'s
spine (`hearth-store`), built with [PyO3](https://pyo3.rs) and
[maturin](https://maturin.rs). One [abi3](https://docs.python.org/3/c-api/stable.html)
wheel per platform covers Python 3.9+.

```python
from gaia import Spine, Mirror

spine = Spine("/var/lib/hearth/spine")

spine.seq()                          # the spine's global clock
spine.latest("muse-glimmer-30b")     # its current state, or None
spine.why("muse-glimmer-30b")        # the causal chain that produced it,
                                     # newest first — "why is this model
                                     # (not) resident?", answered from
                                     # record, not from grep
spine.state_as_of("muse-glimmer-30b", 412)   # what was true at seq 412
spine.verify()                       # {"checked": N, "failures": [...]}
```

## Read-only, on purpose

There is no `record`. There is no write method of any kind, and that is
the design, not a gap: the spine is the audit trail for everything the
runtime did, and a consumer who can write to it can rewrite the history
they are supposed to be auditing. Writers are Rust processes running
the supervisor; Python is where you *check their work*. The binding
surface is tested for the absence of mutating methods
(`tests/api_surface.rs`), not just documented as lacking them.

## The mirror

The spine has no event feed — no `events_since`, no subscription
(hearth-store's API is queries, by design). G-4's answer is the
**mirror**: a gaia-owned JSONL cache the Rust poller appends spine
events to, in seq order, each entry carrying its spine hash. `Mirror`
is its Python reader — the resume read, with the same load-failure
honesty as the Rust store (a bad line is a named failure, never a
silent gap in history):

```python
mirror = Mirror("/var/lib/gaia/mirror.jsonl")
mirror.latest_seq()                  # resume position
for event in mirror.events_since(after_seq=last_seen):
    ...                              # ascending, hash-carrying
mirror.load_failures()               # [] when the file loaded clean
```

The mirror is a cache of the spine, never a source of truth: if the
two ever disagree, the spine wins and the mirror is rebuilt (in Rust;
`Mirror` here cannot append or rebuild, for the same reason `Spine`
cannot record).

## The GIL

Every spine and mirror call runs under `allow_threads`: the GIL is
released around any call that can block, so a Python process can poll
`seq()` on one thread and keep serving on others. `hearth-py` has no
such pattern (§1.3 of the Gaia design); this crate is where it starts.

## Building

```bash
python3 -m venv ~/gaia-py-venv && source ~/gaia-py-venv/bin/activate
pip install maturin pytest
cd crates/gaia-py
maturin develop
python -m pytest python/tests
```

## Honest labels

- **One live opener per spine.** NEDB is single-process: a second
  open of a data directory another process (or the same process)
  still holds is refused — the engine calls the alternative
  split-brain, and its escape hatch is an explicit
  `NEDB_SHARED_OPEN=1` with stated risks, never set by these
  bindings. Python audits a spine sequentially: after the writer has
  finished, or as the sole holder. Plan audit tooling accordingly;
  do not design for a supervisor and a Python reader holding the
  same spine live at once.
- **Read-only** is total: if you need to write history, you want the
  Rust supervisor, not a bigger binding.
- **Performance is UNMEASURED.** Poll cadence, mirror latency, and
  binding overhead get numbers in slice G-8, not adjectives here.
- The mirror's cold-rebuild walk is causal, not exhaustive — it sees
  chain-reachable history (see `gaia-api`'s mirror docs). This reader
  inherits that limit and does not paper over it.
