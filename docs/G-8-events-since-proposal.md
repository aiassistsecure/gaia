# Proposal (upstream-first): additive `Spine::events_since` in `hearth-store`

| | |
|---|---|
| Status | **PROPOSAL ONLY.** Written for upstream-first review in the `aiassistsecure/hearth` process. This document is the deliverable; **no `hearth-store` code is changed by the G-8 PR**, and nothing here lands in upstream except through upstream's own review. |
| Origin | G-2 §4.3b ("a real `events_since` — later cleanup, additive") + §8 open question 1 |
| Settled by | G-8 source inspection of `nedb-engine` 10.30.93 (findings preserved with the G-8 backup materials as `FINDINGS-C.md`) and G-8 measurements (`docs/G-8-measurements.md` §§3, 5) |
| Ask | One additive method (plus one one-line readiness accessor) on `hearth_store::Spine`. No `nedb-engine` change. No change to any existing `Spine` behaviour. |

---

## 1. Why this is being proposed

The fork's SSE spine (G-4) reads residency history through a
mirror: a poller re-walks the spine's causal chains
(`collect_spine_events`) and appends what it finds to a gaia-owned
file. The mirror exists for exactly one reason — G-2 §1.3 found no
events-since read on the spine, and §4.3a designed the mirror to be
correct under that absence.

G-8 measured what that absence costs:

- **Every poll is O(chain-reachable history).** At 2,001 spine
  events a steady-state poll (zero new events) costs p50 ≈ 119 ms
  (≈ 60 µs per collected event, linear). The poll can never be
  cheaper than the whole history it re-walks, and at ≈ 8,000
  chain-reachable events one poll exceeds the 500 ms cadence it
  runs on (arithmetic from the measured rate).
- **Rebuild is bounded by chain reachability, not spine size.**
  `why()`/`provenance()` traces cap at 64 nodes per model, so over
  a 10,000-event spine the mirror can only ever rebuild the newest
  512 events (8 models × 64). The events exist; the read path
  cannot enumerate them.

Both costs vanish if the spine can be read *in seq order from a
cursor*. §2 shows it can — the engine already ships the primitive.

## 2. The engine already has the read

`nedb-engine` 10.30.93, `src/db.rs:1759` (verbatim):

```rust
pub fn since(&self, after_seq: u64, limit: usize) -> SinceBatch
```

- Returns writes in `(from_seq, to_seq]`, **ascending by seq**
  (`SinceBatch`, `db.rs:73-84`), paging by advancing the cursor to
  `to_seq` while `has_more`. The engine's own doc comment calls it
  a changefeed over the append-only log, O(page); its tests pin
  the exclusive-cursor contract (`db.rs:2437-2460`).
- `seq` is `Node.seq`, a monotonic global sequence assigned at
  write time (`src/store.rs:30-31`), matching the spine's own
  `seq()` notion.
- No engine change, no new index, no storage change is needed.
  Ordering comes from the integer walk itself.

### 2.1 Caveats that are part of the facts (not footnotes)

1. **The feed is global.** `since()` has no collection parameter;
   it resolves every seq in range, including engine-internal
   writes (e.g. `_nedb.collections` registry records). A spine
   wrapper must filter by `Node.coll` — the spine's events live in
   the `residency` and `pulls` collections
   (`crates/hearth-store/src/lib.rs:29-31`) — and must return the
   **unfiltered** `to_seq` as the cursor, or callers re-scan
   non-spine seqs forever.
2. **Warm boot is the trap.** The seq index is an in-memory
   `DashMap` (`db.rs:175-176`), populated by the current session's
   writes, the cold scan, or `repair()`/`rebuild_id_index()`. A
   warm boot (MANIFEST present) deliberately skips the scan
   (`db.rs:296-365`), so `since()` over history returns zero nodes
   with `has_more = true` — *not* a false "caught up", but only
   distinguishable via the readiness gate:
   `pub fn scan_status(&self) -> ScanStatus` (`db.rs:1794`) with
   `ScanStatus.seq_index_ready` (`db.rs:110`). Any wrapper must
   surface readiness, never let an empty page masquerade as an
   empty history.
3. **Deletes are not in the feed.** `delete()` seqs are never
   inserted into the seq index (`db.rs:1376-1412`), so `since()`
   is a feed of put-writes. The spine is append-only (events are
   recorded, never deleted), so this does not affect the proposed
   wrapper; it would matter to any future non-append-only consumer
   and is stated here so the limitation is on record.

## 3. The proposal

Additive to `crates/hearth-store/src/lib.rs`, next to `seq()`:

```rust
pub struct EventsPage {
    pub events: Vec<Event>,
    pub to_seq: u64,          // unfiltered cursor — advance to this
    pub head_seq: u64,
    pub has_more: bool,
    pub seq_index_ready: bool // from Db::scan_status()
}

pub fn events_since(&self, after_seq: u64, limit: usize) -> EventsPage
```

Semantics:

- Body is a thin wrapper: `let b = self.db.since(after_seq, limit);`
  filter `b.nodes` to `coll == RESIDENCY || coll == PULLS`, map
  through the existing `Event::of` (filter on `coll` *first* —
  registry payloads must never be parsed as events), return `b`'s
  `to_seq` / `head_seq` / `has_more` **unfiltered**, plus
  `seq_index_ready` from `self.db.scan_status()`.
- **Warm-boot policy is the caller's, surfaced honestly.** Option
  (a) — recommended here — the wrapper reports
  `seq_index_ready = false` and the consumer decides (the fork's
  composition would call `repair()` once at startup before
  serving history; a library default of silently scanning at
  `open` was rejected in the G-8 findings review because it hides
  a full-object-scan cost inside a constructor). Option (b), a
  `Spine::open` variant that repairs eagerly, can be added later
  if upstream prefers it; the page type does not change either
  way.
- Optionally, a one-line `pub fn seq_index_ready(&self) -> bool`
  on `Spine` so consumers can gate historical catch-up without
  paging first.
- **Nothing else changes:** no new index, no new collection, no
  `Transition` variant, no change to `record()`, `latest()`,
  `why()`, `state_as_of()`, or the mirror (the mirror keeps
  working; it becomes *deletable later*, §4).

Suggested tests (in `hearth-store`'s existing test module style):

1. Paging drains a multi-model history in ascending seq with no
   gaps or duplicates, and reconstructs the same event sequence
   the mirror's `collect_spine_events` returns for a
   chain-reachable history.
2. A page containing only a non-spine (registry) seq advances
   `to_seq` with zero events — the cursor never stalls.
3. On a warm-booted on-disk spine, `events_since` reports
   `seq_index_ready = false` (and after `repair()`, serves full
   history) — the caveat of §2.1.2 pinned as behaviour.

## 4. What the fork does with it (after upstream lands it)

Stated so upstream can judge the consumer's shape; none of this is
part of the ask:

1. `gaia-api`'s poller switches from the collect walk to
   `events_since` pages (cursor persisted as the mirror's max
   seq, exactly as today). Poll cost becomes O(new events).
2. `MirrorStore::rebuild` enumerates the full spine via paging —
   the 64-per-model chain cap stops bounding rebuilds.
3. The mirror itself is deleted only in a later slice, after the
   new path has its own measurements (G-8 discipline: replace
   numbers with numbers, not with enthusiasm). Until then the
   mirror remains the fallback and the audit cross-check
   (`verify_against` already compares the two).

## 5. What this proposal is not

- Not a patch: it arrives as a document first because upstream
  `hearth` is Mark's repo with its own review, and the G-2 spec
  (§4.3b) committed to an upstream-first conversation, never a
  drive-by change from the fork.
- Not a `nedb-engine` request: the engine needs nothing.
- Not a mirror-removal: §4 sequences that as separate, measured
  work.
- Not a claim about deployments: the fork's spine consumers run
  loopback, single-box (G-2 §2.5); `events_since` changes none of
  that.
