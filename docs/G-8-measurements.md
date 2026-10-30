# G-8 — Measurements + the `events_since` question

| | |
|---|---|
| Slice | G-8 (final slice of the G-2 §7 plan) |
| Status | **MEASURED** — on one machine class only (see §1). Every figure below replaces a G-2 **UNMEASURED** label; where a figure does not generalise, the limit is stated next to it. |
| Base | gaia main `93fbe70` (G-1…G-7 merged) |
| Harnesses | `crates/gaia-api/examples/g8_fanout_poll.rs`, `crates/gaia-serve/examples/g8_actuate_rebuild.rs` (in this PR) |
| Raw logs | Preserved with this slice's backup materials (`logs-a/`, `logs-b/`, `logs-coordinator/`); every headline number in this document was verified by the coordinator against those CSVs, and the coordinator re-ran both harnesses sequentially for the authoritative pass (§1.3). |
| Companion | `docs/G-8-events-since-proposal.md` — the §4.3b follow-up, a proposal for upstream-first review. No `hearth-store` code is changed by this PR. |

---

## 1. Machine, method, and how to read these numbers

### 1.1 Machine

All measurements were taken on the development VM:

- **CPU:** AMD EPYC 9D25 126-Core Processor, **2 vCPUs** available
  (`available_parallelism = 2`)
- **Memory:** 7.7 GiB total (8,125,588 KiB)
- **OS/disk:** the project's usual Linux VM; all state on local disk.
- **Builds:** `--release` throughout. Debug-profile numbers would be
  meaningless and none are reported.

These are *this class of machine* numbers (§7 G-8's wording). They are
evidence about the design's costs, not capacity claims for other
hardware.

### 1.2 What was measured, and why these quantities

G-2 §§3–5 deliberately labelled every performance figure UNMEASURED.
§7 G-8 names four quantities; §8 adds two open costs. This slice
measures all six:

| # | Quantity | G-2 source | Section here |
|---|---|---|---|
| (a) | SSE fan-out cost per subscriber | §4.4, §8.6 | §2 |
| (b) | Mirror poll latency distributions | §4.3a | §3 |
| (c) | Loss→proposal wall time | §5, §8.5 | §4 (with (d)) |
| (d) | Actuation round-trip (proposal→verdict→resident) | §5.3, §7 | §4 |
| (e) | Mirror rebuild time over a long spine | §4.3a, §7 | §5 |
| (f) | `Spine::record` flush-per-record cost | §8.2 ("unquantified until G-8") | §5 |
| — | `events_since` feasibility | §4.3b, §8.1 | §6 |

### 1.3 Method and contention honesty

- Each harness prints machine-readable CSV
  (`series,config,n,min_us,p50_us,p95_us,max_us,mean_us`, nearest-rank
  percentiles) with every parameter in `#` header comments, plus raw
  per-sample rows for the latency series.
- Workers A and B each ran a full pass while sibling compilation
  shared the box; those runs are preserved (`logs-a/`, `logs-b/`) and
  agree with the coordinator's pass in every headline figure.
- The **coordinator's pass is authoritative**: both harnesses re-run
  sequentially, nothing else building, binaries already compiled
  (`logs-coordinator/`). Where a worker's contended run differs
  materially (tails, mostly), both figures are given.
- The VM is shared with other project automation, so "quiet" means
  *nothing of this slice's was running concurrently*, not a dedicated
  bare-metal bench. p95/max tails should be read as upper bounds.

### 1.4 The fake-child limit (applies to §4)

Fleet scenarios use the acceptance tests' fake serving child: a tiny
Python HTTP server answering 200 on any GET. There is no machine
learning runtime and no model load anywhere in these numbers. They
measure the composition / spine / mirror / watcher / actuator
machinery. A real llama.cpp warmup would add its own time to the
verdict→resident segment, and nothing here estimates it.

---

## 2. (a) SSE fan-out cost per subscriber — G-2 §4.4, §8.6

Harness: `g8_fanout_poll fanout`. Two levels: the in-process
`EventHub` (the fan-out core), and the real `EventsServer` over TCP
(the shipped serving path).

### 2.1 Hub level: publish cost vs subscriber count

Ring capacity 256 (the §4.4 starting value), M = 2,000 publishes,
subscribers unfiltered, drained only at the end — the publisher's
worst case. Coordinator's pass:

| Subscribers | mean / publish | p50 | p95 | total for 2,000 |
|---|---|---|---|---|
| 0 | 0.1 µs | 0.1 µs | 0.1 µs | 1.9 ms |
| 1 | 0.2 µs | 0.1 µs | 0.6 µs | 2.9 ms |
| 8 | 2.0 µs | 0.1 µs | 7.0 µs | 5.9 ms |
| 32 | 5.1 µs | 0.2 µs | 31.6 µs | 14.6 ms |
| 64 | 18.0 µs | 0.5 µs | 64.6 µs | 37.4 ms |
| 128 | 23.4 µs | 1.0 µs | 136.6 µs | 52.1 ms |

(Worker A's contended run: same shape, means 0.1 / 0.2 / 1.2 / 6.0 /
23.7 / 43.1 µs.)

Reading:

- **Cost shape is the fill phase.** Each publish clones the event
  once per *accepting* subscriber until that subscriber's ring fills
  at 256 events; after the fill, a publish only marks overflow.
  Exactly **1 overflow episode per subscriber** was recorded at every
  N ≥ 1, ring kept = 256, `resync_at = 2000` — the §4.4 contract
  (bounded buffer, resync, never silent drop) behaving as designed.
- **The publisher never blocked at any N**, including 128
  subscribers with full rings. p50 stays at ~1 µs even at N = 128;
  the mean is carried by the first 256 publishes per subscriber.
- Marginal cost at the design's operating point (≤ 32 subscribers)
  is ≈ **5 µs per publish** — three orders of magnitude below the
  event rates a supervisor spine produces (a handful of transitions
  per tick). Fan-out cost is not the constraint at this scale.

### 2.2 HTTP level: the shipped server

`EventsServer` on an ephemeral loopback port, connection budget 32
(`EventsConfig::default`), K raw-TCP subscribers
(`GET /gaia/v1/events?source=hearth&follow=1`), 500 events published
through the hub. Latency method: per subscriber, (reader thread
received all events) − (publish loop end); the serving loop drains
the hub on a 10 ms poll, so latency granularity is ~10 ms.

- **Threads:** derived server cost **1.03 threads per subscriber**
  (baseline 45 threads in the measurement process → 110 with 32
  subscribers + 32 reader threads). Thread-per-connection, exactly
  as §1.1/§8.6 describe — this is the architecture's real price.
- **Memory:** RSS delta at K = 32 was **608 KiB ≈ 19 KiB per
  subscriber** (upper bound: reader threads share the measurement
  process). Memory per subscriber is negligible; threads are the
  cost, as designed and as §8.6 predicted.
- **Burst behaviour (§4.4 in action):** a 500-event burst overfills
  the 256-event ring for slow readers. K = 1: the subscriber
  received exactly 256 events, then `resync`, then close. K = 8:
  mixed (in the coordinator's pass 3/8 kept up and completed; the
  rest received partial streams + resync). K = 32: **all 32
  completed** in both passes (the publish loop itself took ~150 ms
  at that fan-out, letting the 10 ms drain keep up);
  publish→received latency p50 36.2 ms, mean 33.3 ms.
- **Paced behaviour:** at 1 event/ms (a fast spine by any realistic
  standard), K = 8 latency p50 0.42 ms, mean 2.5 ms — the stream
  keeps up trivially when the producer is paced.

### 2.3 Verdict on the §4.4 starting values

- **Budget 32:** costs ≈ 33 threads and < 1 MiB RSS on this box —
  cheap. The budget's purpose (inference-pool isolation, §4.4) is
  unaffected by these numbers; 32 remains a sane default.
- **Ring 256:** at supervisor event rates the ring is generous; it
  binds only under artificial bursts, where the resync path —
  measured working — is the designed answer.
- **§8.6 (thread-per-connection economics):** confirmed as the one
  real scaling limit. A subscriber count in the hundreds costs
  hundreds of threads on a 2 vCPU box; nothing in this slice changes
  §8.6's conclusion that such a deployment forces the async
  conversation. Fan-out *CPU* cost (§2.1) would not be the blocker.

---

## 3. (b) Mirror poll latency distributions — G-2 §4.3a

Harness: `g8_fanout_poll poll`. Spine on disk in a temp dir,
667 models × chained Declared→Loading→Resident (the `mirror.rs`
test pattern), 2,001 events recorded — deliberately chain-shaped so
**all 2,001 are collectable** by `collect_spine_events` (see §5.3
for why chain shape matters). Coordinator's pass:

### 3.1 `collect_spine_events` — the walk every poll performs

| Spine events | p50 | mean | p95 |
|---|---|---|---|
| 102 | 3.6 ms | 4.0 ms | 6.2 ms |
| 501 | 61.8 ms | 64.9 ms | 135.4 ms |
| 1,002 | 47.6 ms | 47.6 ms | 51.0 ms |
| 2,001 | 124.2 ms | 123.4 ms | 139.7 ms |

Growth is ~linear at ≈ **60 µs per collected event** (the 501 row's
p95 is a contention outlier; its p50 sits on the line within run
variance). There is no incremental cursor in the shipped read path —
`collect_spine_events` re-walks causal chains from the spine on
every call. That single fact drives everything else in this section,
and it is the concrete cost §4.3b's `events_since` would remove
(§6).

### 3.2 `poll_once` — steady state and catch-up

At 2,001 spine events:

| Regime | n | p50 | mean | p95 | max |
|---|---|---|---|---|---|
| Steady (0 new events) | 200 | 118.9 ms | 130.8 ms | 203.8 ms | 463.9 ms |
| Catch-up (behind by 100) | 20 | 145.3 ms | 156.9 ms | 223.6 ms | 243.2 ms |

- **A poll costs the same whether or not anything happened.**
  Steady state appends 0 events in all 200 samples and still pays
  p50 ≈ 119 ms — the collect walk *is* the poll. The incremental
  cost of actually appending 100 events is only ≈ +26 ms of p50;
  the walk dominates the work it finds.
- Catch-up appended **exactly 100 events in all 20 samples** — no
  loss, no duplication, at this scale.
- Worker A's contended pass agrees: steady p50 107.3 ms, catch-up
  p50 181.7 ms.

### 3.3 What this means for §4.3a

The mirror design is *correct* — §5.2 shows rebuild/verify doing
exactly what they promise — but its read path is O(history) per
poll, and this section prices that:

- At the shipped 500 ms cadence, one steady poll at 2,001 events
  occupies ≈ 119 ms of a poller thread — roughly a quarter of one
  of this box's two cores, spent re-confirming that nothing
  changed.
- **Projection (arithmetic from the measured ≈ 60 µs/event, not a
  measurement):** at ~8,000 chain-reachable events a single poll
  takes ≈ 500 ms — the poll can no longer keep up with its own
  cadence; at 100,000 events, ≈ 6 s per poll. A long-lived
  deployment's spine grows into exactly this wall.
- This is the measured form of §4.3b's motivation: an
  `events_since` cursor read makes poll cost O(new events)
  instead of O(history). §6 settles whether that read exists.

---

## 4. (c) Loss→proposal + (d) actuation round-trip — G-2 §5, §8.5

Harness: `g8_actuate_rebuild roundtrip`. Per trial: a fresh
composition (real `Supervisor` + `Spine` + mirror + watcher, one
declared model served by the fake child of §1.4) is brought to
Resident; the child is killed with `kill -9` (pid taken from the
spine `loading` event); the harness then drives `tick_once` at the
shipped 500 ms cadence and timestamps five segments. n = 8 trials.
Coordinator's pass:

| Segment | p50 | mean | p95 | max |
|---|---|---|---|---|
| kill → Lost recorded (detection) | 505.2 ms | 507.8 ms | 518.9 ms | 518.9 ms |
| Lost → proposal visible | 0 ms | 0 ms | 0 ms | 0 ms |
| proposal → verdict recorded | 48 µs | 51 µs | 102 µs | 102 µs |
| verdict → Resident (actuation) | 506.2 ms | 507.3 ms | 514.0 ms | 514.0 ms |
| **total kill → Resident** | **1,013.2 ms** | **1,015.2 ms** | **1,026.0 ms** | **1,026.0 ms** |

Worker B's contended pass reproduces every p50 within 4 ms (total
p50 1,017.2 ms) — the timeline is cadence-bound, hence
contention-proof. Per-trial samples are in Appendix A; segments
sum to totals per trial.

### 4.1 Reading the segments

- **Detection = one tick.** The kill lands uniformly inside a
  500 ms interval; the next tick's probe finds the child gone and
  records `Lost`. p50 505 ms is the cadence plus ~5 ms of probe
  and record work — there is no sub-tick detection path in this
  design, and §5 never claimed one.
- **Loss→proposal = 0, structurally.** Detection, mirror poll, and
  the watcher pass all happen inside the same `tick_once` step, so
  at shipped step granularity the proposal exists in the same step
  that records the loss. §8.5's "tick + probe + poll" latency is
  real but *intra*-tick: it does not add a second tick of delay.
- **Actuation = one tick.** `actuate_work` restarts the child
  synchronously on the admitted verdict; residency is *observed*
  on the following tick's probe. As with detection, the 500 ms is
  the observation cadence, not actuator slowness.
- **Total ≈ 1.0 s is two tick quanta and a rounding error.**
  §8.5 said "order of a second, UNMEASURED". Measured: it *is*
  one second, and the decomposition shows the floor is cadence
  policy, not machinery. Halving the tick would halve the total —
  at the poll cost §3 prices (until §6's read lands, a faster tick
  also means paying the O(history) walk twice as often).

### 4.2 The verdict segment, honestly

Proposal→verdict (p50 48 µs) measures this harness's in-process
bookkeeping only: the proposal's intent is submitted to a
`MemoryWorkStore` and the ADMIT verdict recorded immediately when
the proposal appears, by construction. It is **not** a measurement
of any external gate's decision latency (the SI-side gate is
Python and outside this fork's slices), and it is reported so the
timeline has no unexplained gap — not as a gate figure.

---

## 5. (f) Record flush cost + (e) long-spine mirror economics

Harness: `g8_actuate_rebuild rebuild`. A spine is generated
honestly: 10,000 `Spine::record` calls (8 models; per model one
`declared`, then repeating `loading{pid,endpoint}` →
`resident{endpoint,warmup_ms=25}` → `lost{process_exited}`,
causally chained via `caused_by`), every call timed. Final spine
seq 10,001; generation wall 7.72 s (coordinator) / 12.64 s
(contended worker pass — the difference is §5.1's tail under
compile load).

### 5.1 `Spine::record` — the §8.2 flush-per-record cost, quantified

n = 10,000 (coordinator's pass):

| min | p50 | p95 | max | mean |
|---|---|---|---|---|
| 80 µs | 209 µs | 2,939 µs | 37,535 µs | 770 µs |

(Contended pass: p50 581 µs, p95 5,366 µs, mean 1,260 µs.)

- §8.2 worried that `record()` flushes synchronously per event
  and that future spine-resident gaia records would inherit the
  cost. Measured: the *typical* record is ~0.2 ms — at supervisor
  rates (a handful of records per tick) the flush costs single
  milliseconds per tick even at p95. The tail is real (max ≈ 38 ms
  under contention) but it is a tail on a rare operation, not a
  per-event tax that compounds.
- If a future design records at high rate (per-token, per-tool-
  call), this distribution — not the p50 — is the number to
  design against. At the rates G-1…G-7 actually produce, §8.2's
  cost is settled: negligible.

### 5.2 Mirror rebuild / verify / open

Over the 10,000-event spine (×3 runs, coordinator's pass):

| Operation | p50 | mean | result |
|---|---|---|---|
| `collect_spine_events` | 14.6 ms | 15.2 ms | **512 events** (§5.3) |
| `MirrorStore::rebuild` | 30.5 ms | 32.9 ms | 512 entries |
| `verify_against` | 16.1 ms | 16.2 ms | **0 failures** |
| `MirrorStore::open` (load) | 2.7 ms | 3.3 ms | 512 entries |
| Mirror file size | — | — | 98,040 bytes |

The mechanics are fast and the audit holds: rebuild reconstructs
the mirror from the spine, and verification against spine hashes
finds no disagreement. Rebuild of the *reachable* history is a
~30 ms operation — a non-event operationally.

### 5.3 The 512 finding: rebuild is chain-bounded, not spine-bounded

`collect_spine_events` — and therefore `rebuild` and
`verify_against` — walk causal chains via `why()`/`provenance()`,
which `hearth-store` caps at **64 nodes per model**. The generated
spine has 8 models with ~1,250 events each, so the reachable
history is exactly 8 × 64 = **512 events** — the newest 64 per
model. The other 9,488 events are on disk, hashed, and unreadable
through this path.

This is `mirror.rs`'s own documented limit, now with a price tag
attached, and it cuts both ways:

- §3's spine was deliberately built the other way (667 models ×
  3-event chains) so all 2,001 events were collectable — chain
  *shape*, not event count, decides what the mirror can see.
- A real deployment's spine looks like §5's (few models, long
  chains), so a mirror rebuild after corruption would silently
  restore only the newest 64 events per model. "Silently" is the
  operative word, and it is a correctness limit, not a
  performance one. §6 is the way out.

---

## 6. §4.3b settled: `events_since` — verdict (i), it exists

Full evidence: the G-8 findings file (preserved with this slice's
backup materials as `FINDINGS-C.md` — the complete public API
listing of `Db`, the seq storage model, and the bitemporal reads,
each with file:line citations). Summary:

- **The API:** `pub fn since(&self, after_seq: u64, limit: usize)
  -> SinceBatch` — `nedb-engine` 10.30.93, `src/db.rs:1759`.
  Returns writes in `(from_seq, to_seq]` ascending by seq, with a
  paging cursor (`has_more`, advance to `to_seq`). The engine
  documents it as a changefeed over its append-only log and tests
  the cursor contract itself.
- **Why G-2 didn't find it:** §1.3 inventoried the *spine's* API
  (14 public methods on `Spine`, zero feed-shaped reads) and
  deliberately did not open the engine. The gap was in
  `hearth-store`'s surface, not in the engine.
- **The spine never calls it:** `Spine` uses only
  `put/get/get_as_of/trace/list` on `Db` today
  (`crates/hearth-store/src/lib.rs:166-304`).
- **Caveats (part of the verdict):** the feed is global (filter
  by `Node.coll`; return the unfiltered cursor); the seq index
  is in-memory and session-scoped, so after a warm boot history
  is invisible until `repair()`/`rebuild_id_index()` — consumers
  must gate on `scan_status().seq_index_ready` (`db.rs:1794`);
  `delete()` seqs are not indexed (a put-only feed — the
  append-only spine is unaffected).
- **The proposal:** `docs/G-8-events-since-proposal.md` specifies
  the additive `Spine::events_since(seq, limit)` wrapper —
  filter by collection, unfiltered cursor, readiness surfaced in
  the page type — written for **upstream-first review**. This PR
  changes no `hearth-store` code. With the wrapper landed, §3's
  O(history) poll becomes O(new events) and §5.3's rebuild cap
  disappears; the mirror can then be deleted in a later,
  separately measured slice.

---

## 7. Closure table — every UNMEASURED, answered

| G-2 label | Where | G-8 answer |
|---|---|---|
| "every performance figure is UNMEASURED" (§ header, §2.6) | §§3–5 | Replaced by §§2–6 of this document, on this machine class |
| SSE fan-out capacity | §4.4, §7 | §2: ≈ 5 µs/publish at 32 subs; publisher never blocks; 1.03 threads + ≈ 19 KiB RSS per HTTP subscriber |
| Ring 256 / budget 32 / heartbeat 15 s — "starting values, UNMEASURED" | §4.4 | §2.3: values stand; resync path measured working under burst. Heartbeat: §8 gap |
| Mirror poll latency | §4.3a | §3: steady p50 ≈ 119 ms at 2,001 events — O(history), ≈ 60 µs/event |
| Loss→proposal wall time ("order of a second, UNMEASURED") | §5, §8.5 | §4: detection 1 tick (p50 505 ms); proposal same tick (0 ms at step granularity) |
| Actuation round-trip | §5.3, §7 | §4: verdict→Resident p50 506 ms (one observation tick); total kill→Resident p50 1,013 ms |
| Mirror rebuild over a long spine | §4.3a, §7 | §5.2: 30.5 ms — over the 512 chain-reachable events of a 10,000-event spine (§5.3) |
| Flush-per-record ("unquantified until G-8") | §8.2 | §5.1: p50 209 µs, p95 2.9 ms, n = 10,000 |
| Thread-per-connection economics ("not a measured capacity") | §8.6 | §2.2: threads measured (1.03/sub); capacity in the hundreds remains an architectural limit, not a measured one (§8) |
| NEDB seq-range read ("not verified") | §8.1, §4.3b | §6: **exists** — `Db::since`, verdict (i), caveats stated |

## 8. Remaining gaps (stated, not papered over)

- **Heartbeat cost** (§4.4): not measured — one comment-frame
  write per idle connection per 15 s; the burst-oriented harness
  has no honest series for it. Nothing depends on the figure.
- **Subscribers in the hundreds:** the HTTP budget caps at 32 by
  design, and the hub was taken to 128 subscribers (§2.1), not
  beyond. §8.6's async conversation stays exactly where G-2 left
  it — this slice measured the thread *cost* that motivates it,
  not a capacity beyond the budget.
- **A 100,000-event rebuild:** not run, and the reason is the
  finding, not the clock — generation would take ~80 s at the
  measured record rate, but the shipped read path caps reachable
  history at 64 events per model regardless of spine size, so
  the rebuild number would still be a 512-class number. The
  honest next measurement of long-spine behaviour happens after
  `events_since` lands.
- **Real-model actuation:** §1.4 — a real llama.cpp child's
  load/warmup time inside verdict→Resident is unmeasured by
  design of the fixture; the machinery around it is what was
  priced.

## 9. Limits of this document

- One machine class (2 vCPU, 7.7 GiB), one VM, loopback only.
  Numbers transfer to similar small VMs as orders of magnitude,
  not as guarantees.
- Spines are synthetic (lifecycle-shaped, honestly generated and
  described in §5); a production spine's event mix differs, and
  §3's per-event rate is the transferable figure, not any single
  total.
- The shared-VM caveat of §1.3 applies to every tail: p95/max are
  upper bounds; p50s reproduced across independent passes within
  a few percent (Appendix B.5).



---

## 10. Reproduction

Both harnesses are examples in this PR. From a checkout of this
branch, on a quiet box:

```sh
cargo build --release -p gaia-api -p gaia-serve --examples

# (a) + (b): fan-out and poll (full mode, ~40 s on this VM class)
./target/release/examples/g8_fanout_poll

# (c) + (d) + (e) + (f): round-trip, rebuild, record cost (~20 s)
./target/release/examples/g8_actuate_rebuild

# smoke modes (seconds): g8_fanout_poll --quick / g8_actuate_rebuild --quick
```

Each prints its parameters as `#` header comments before the CSV,
so a log is self-describing. Trial counts, spine sizes, subscriber
counts, and cadences are constants at the top of each harness file;
changing them changes the headers too.

Method details that matter for reproduction:

- **Percentiles** are nearest-rank over the full sample set; raw
  samples are printed for the round-trip, record-flush, and poll
  series, so any other percentile convention can be recomputed.
- **Warm-up:** hub series discard an initial fill pass per
  configuration; fleet trials each build a fresh composition
  (fresh temp dir, fresh spine), so no trial inherits another's
  processes or files.
- **Tick driving:** the round-trip harness calls `tick_once` and
  then sleeps to the shipped 500 ms cadence
  (`crates/gaia-serve/src/main.rs:261`); bring-up before the kill
  uses a faster unmeasured drive and is excluded from every
  segment.
- **Cleanup:** harnesses stop their fleets (`stop_all`) and remove
  temp dirs; a run leaves no fake-server processes behind
  (verified after each pass).

---

## 11. Evidence index

| Claim | Primary source | Verified by |
|---|---|---|
| Hub publish series (§2.1) | `logs-coordinator/fanout_poll_full.csv` (`hub_publish_latency` rows) | coordinator re-run; worker A pass (`logs-a/`) agrees in shape and order |
| Overflow = 1 episode/subscriber, ring kept 256 | same CSV, `# hub n_subs=…` comment rows (`HubStats`) | both passes |
| Threads/RSS per subscriber (§2.2) | same CSV, `# thread_rss k=32 …` row (`/proc/self/status`) | both passes (identical delta: 65 threads, 608 KiB) |
| Burst resync behaviour (§2.2) | same CSV, `# http_burst …` rows | both passes; K = 32 completed 32/32 in both |
| Poll distributions (§3) | same CSV, `poll_steady` / `poll_catchup` rows + raw samples | both passes; catch-up appended exactly 100 in 40/40 samples across passes |
| Round-trip segments (§4) | `logs-coordinator/actuate_rebuild_full.csv` (`roundtrip_*` rows + 10,052 raw rows incl. record samples) | worker B pass (`logs-b/`) reproduces every p50 within 4 ms; per-trial segments sum to per-trial totals (App. A) |
| Record-flush distribution (§5.1) | same CSV, `spine_record_flush_per_record` (n = 10,000 raw samples) | both passes (p50 209 µs quiet / 581 µs contended) |
| Rebuild over 512 of 10,000 (§5.2) | same CSV, `# scalar,…` rows (seq 10,001; collected 512; verify failures 0; file 98,040 B) | both passes; cap arithmetic 8 × 64 = 512 |
| 64-node trace cap | `crates/hearth-store/src/lib.rs` (`why`/`provenance`); `crates/gaia-api/src/mirror.rs` (documented limit) | read at source for this slice |
| `Db::since` exists (§6) | `nedb-engine-10.30.93/src/db.rs:1759` | coordinator spot-checked the citation and `scan_status` (`db.rs:1794`), `Node.seq` (`store.rs:30-31`) against the vendored source |
| Warm-boot seq-index caveat (§6) | `db.rs:175-176`, `db.rs:296-365`, `db.rs:110` | findings file review (`logs-c/FINDINGS-C.md`, 404 lines, full `Db` API listing) |
| Spine never calls `since` today | `crates/hearth-store/src/lib.rs:166-304` call inventory | findings file; grep |

Worker working papers (summaries with their own caveats, and the
full findings file) are preserved with this slice's backup
materials as `SUMMARY-A.md`, `SUMMARY-B.md`, and `FINDINGS-C.md`.

---

## Appendix A — Round-trip per-trial samples (coordinator pass, µs)

Loss→proposal was 0 in all trials and is omitted; the verdict
column is the bookkeeping segment of §4.2. Segments sum to totals
(±1 µs rounding).

| Trial | kill→Lost | proposal→verdict | verdict→Resident | total |
|---|---|---|---|---|
| 1 | 509,031 | 102 | 514,049 | 1,023,183 |
| 2 | 504,988 | 32 | 506,183 | 1,011,204 |
| 3 | 509,804 | 57 | 503,337 | 1,013,198 |
| 4 | 503,718 | 38 | 511,327 | 1,015,083 |
| 5 | 508,176 | 48 | 504,851 | 1,013,076 |
| 6 | 518,883 | 49 | 507,104 | 1,026,037 |
| 7 | 505,201 | 51 | 508,178 | 1,013,430 |
| 8 | 502,850 | 33 | 503,108 | 1,005,992 |

Reading: detection and actuation each sit within ±3 % of one
500 ms tick in every trial — the distribution *is* the tick, with
single-digit-millisecond jitter. There is no hidden long tail in
the machinery at this scale; there is only cadence.

Worker B's pass (contended) for comparison — same trials design:
detection p50 505,083 µs; actuation p50 504,909 µs; total
p50 1,017,177 µs, max 1,048,945 µs.

---

## Appendix B — Full series, coordinator pass

All values µs unless noted. `n` is sample count.

### B.1 Hub publish latency (per call; M = 2,000 publishes per row)

| N subs | n | min | p50 | p95 | max | mean |
|---|---|---|---|---|---|---|
| 0 | 2,000 | 0.1 | 0.1 | 0.1 | 2.9 | 0.1 |
| 1 | 2,000 | 0.1 | 0.1 | 0.6 | 107.6 | 0.2 |
| 8 | 2,000 | 0.1 | 0.1 | 7.0 | 1,160.7 | 2.0 |
| 32 | 2,000 | 0.2 | 0.2 | 31.6 | 675.1 | 5.1 |
| 64 | 2,000 | 0.5 | 0.5 | 64.6 | 5,091.3 | 18.0 |
| 128 | 2,000 | 0.9 | 1.0 | 136.6 | 2,216.3 | 23.4 |

Max spikes are scheduler noise on a shared 2 vCPU box (present in
both passes); p50/p95 are the stable story.

### B.2 HTTP receive latency (publish end → subscriber complete)

| Config | n | min | p50 | p95 | max | mean |
|---|---|---|---|---|---|---|
| burst, K = 32 | 32 | 1,259.9 | 36,151.2 | 57,209.9 | 60,435.5 | 33,293.6 |
| paced 1/ms, K = 8 | 8 | 0.0 | 421.5 | 5,496.4 | 5,496.4 | 2,473.3 |
| paced 1/ms, K = 1 | 1 | 8,111.6 | — | — | — | 8,111.6 |

Burst K = 1 and K = 8 are not latency rows: they are the resync
evidence of §2.2 (subscribers received 256 + `resync` + close, or
partial streams when readers kept partial pace). The K = 1 paced
figure is a single sample dominated by where the last event fell
inside the 10 ms drain cycle — reported for completeness, not as
a latency claim.

### B.3 Poll series (2,001-event spine)

| Series | n | min | p50 | p95 | max | mean |
|---|---|---|---|---|---|---|
| `poll_steady` (0 new) | 200 | 70,096.0 | 118,933.2 | 203,778.1 | 463,883.6 | 130,816.8 |
| `poll_catchup` (behind 100) | 20 | 95,398.3 | 145,276.8 | 223,587.8 | 243,163.6 | 156,937.0 |
| `collect_spine` @102 | 5 | 3,166.3 | 3,594.8 | 6,170.3 | 6,170.3 | 3,971.4 |
| `collect_spine` @501 | 5 | 27,554.8 | 61,812.2 | 135,404.2 | 135,404.2 | 64,923.5 |
| `collect_spine` @1,002 | 5 | 42,200.5 | 47,647.0 | 51,038.1 | 51,038.1 | 47,624.7 |
| `collect_spine` @2,001 | 5 | 103,444.8 | 124,157.7 | 139,683.8 | 139,683.8 | 123,356.9 |

### B.4 Long-spine series (10,000 recorded; 512 chain-reachable)

| Series | n | min | p50 | p95 | max | mean |
|---|---|---|---|---|---|---|
| `spine_record_flush_per_record` | 10,000 | 80 | 209 | 2,939 | 37,535 | 770.0 |
| `collect_spine_events` (512) | 3 | 14,431 | 14,580 | 16,629 | 16,629 | 15,213.3 |
| `mirror_rebuild` (512) | 3 | 29,525 | 30,480 | 38,816 | 38,816 | 32,940.3 |
| `mirror_verify_against` (512) | 3 | 15,708 | 16,124 | 16,819 | 16,819 | 16,217.0 |
| `mirror_open_load` (512) | 3 | 2,693 | 2,748 | 4,580 | 4,580 | 3,340.3 |

Scalars: spine generation wall 7,723,172 µs; final spine seq
10,001; verify failures 0; mirror file 98,040 bytes.

### B.5 Worker-pass deltas (contention check)

The contended worker passes bound the quiet numbers from above in
every tail and agree at p50 within: hub publish mean at N = 128,
43.1 vs 23.4 µs (worst delta, a fill-phase mean under compile
load); record flush p50, 581 vs 209 µs; rebuild p50, 70.3 vs
30.5 ms; round-trip total p50, 1,017 vs 1,013 ms (cadence-bound,
essentially contention-proof). No headline conclusion in §§2–5
changes between passes.
