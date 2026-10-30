# G-2 — Gaia agency layer on the Hearth fork: design spec

| | |
|---|---|
| Status | **DESIGN ONLY.** Nothing in this document is built. Every behaviour below is a proposal against the code as read; every performance figure is **UNMEASURED**. |
| Repo / base | `aiassistsecure/gaia` (fork of `aiassistsecure/hearth`), based on main `2840e2b` |
| Licence | This repo is Business Source License 1.1 (licensor Interchained LLC). This spec proposes **no** relicensing and no licence change of any kind. |
| Method | Crate analysis was delegated to three parallel workers with disjoint scopes (`hearth-api`; `hearth-serve` + `hearth-core`; `hearth-store` + `hearth-py`); the coordinator spot-checked every load-bearing claim against source before writing (§10). |
| Baseline | G-1 (fork baseline + identity) is being measured in a parallel PR. Its results are **pending** and are deliberately **not cited** anywhere in this spec. |
| Companion system | SentinelIntelligence (SI) — the Python mind. Its `si/gaia/` subpackage already implements tool intents, the gate, the NEDB journal, the executor, the task loop, the service adapter (SI-42…SI-46) and the SSE event semantics (SI-47/48/49). This spec designs the **fork-side Rust layer** those Python pieces talk to. It changes nothing in the SI repo. |

**Vocabulary.** *Hearth* = the upstream runtime manager (model residency
above llama.cpp). *Gaia* = this fork: Hearth's runtime layer carrying an
agentic harness/loop and an SSE event spine. *SI* = the mind (Python).
*Spine* = Hearth's NEDB-backed residency journal (`hearth-store`).
*SI journal* = SI's own NEDB journal, where gated work is recorded before
it runs. These are two different journals; §4 keeps them distinct on
purpose.

---

## 1. Ground truth: what the fork contains today

Everything in this section was verified against source at `2840e2b`.
Citations are `path :: symbol`.

### 1.1 The gateway (`hearth-api`) is a pure decision function over `std::net`

- No web framework, no async runtime. The crate is three files:
  `crates/hearth-api/src/lib.rs`, `src/gateway.rs`, `src/http.rs`.
  Dependencies are only `hearth-core` + `serde_json`
  (`crates/hearth-api/Cargo.toml`).
- `gateway::classify(path)` is exact-match on the path only (query and
  trailing slash stripped). `gateway::decide(path, body, &Fleet, now)`
  receives **no HTTP method and no headers** — routing is method-blind.
- `http::Server` wraps `std::net::TcpListener`; the accept loop, one OS
  thread per connection, and the inflight cap (`--max-inflight` /
  `HEARTH_MAX_INFLIGHT`, default **64**, over-cap ⇒ 503 `server_busy`)
  live in the caller, `crates/hearth-serve/src/bin/hearth.rs`, which
  binds `127.0.0.1:{port}` (default port 11434). Loopback is the entire
  security model: **no authentication exists anywhere in the crate**
  (grep-verified); the `authorization` header is forwarded, never checked.
- Existing routes: inference proxying (`/v1/chat/completions`,
  `/v1/completions`, `/v1/embeddings`, `/api/chat`, `/api/generate`),
  model lists (`/v1/models`, `/api/tags`), residency (`/residency`,
  `/v1/residency`), health (`/health`, `/healthz`, `/v1/health`), and a
  clean OpenAI-shaped 404 for everything else. `/gaia/*` collides with
  nothing and currently 404s.
- Residency diagnosis is the gateway's reason to exist, and it is
  precise. `gateway::route_to_decision` maps `hearth_core::fleet::Route`
  to HTTP: Ready ⇒ proxy; Warming ⇒ 503 + `Retry-After: 5`; **Lost ⇒
  503 + `Retry-After: 30`, `error.type: model_lost`, `reason` =
  Debug-name lower-cased (`gpudetached`, `processexited`, `evicted`,
  `unhealthy`), `operator_fault` carried from core**; Failed ⇒ 503,
  `retryable: false`, no `Retry-After`; **NotAdmitted ⇒ 409
  deliberately (not 503), no `Retry-After`, with exact `short_bytes`**;
  NotDeclared ⇒ 404; Unknown ⇒ 503 + `Retry-After: 2`. The human
  message is keyed off the *reason*, not the fault boolean — the code
  comments call a wrong diagnosis worse than a vague one. Behaviour is
  pinned by 26 in-module tests (`gateway.rs` 17, `http.rs` 9).
- Generated responses are one shape only: `http::render_response`
  emits `application/json`, a fixed `content-length`, and
  `connection: close`. `Decision` is one-shot: `Proxy` or
  `Answer(Response)`. There is no 202/job handle, no subscription, no
  generated streaming. Upstream SSE from llama.cpp passes *through*
  `http::proxy` untouched (16 KiB chunks, flush per chunk, no upstream
  read timeout), but the gateway originates no `text/event-stream` of
  its own (grep-verified). `Request::wants_stream()` exists and is dead
  code outside its own tests.
- `decide` runs under the supervisor mutex, briefly; the proxy runs
  after the lock is dropped. "Decide under lock, act outside it" is
  the established discipline any new long-lived response must keep.

### 1.2 Residency and supervision (`hearth-core`, `hearth-serve`)

- The state machine is `crates/hearth-core/src/residency.rs`:
  `Residency::{Unknown, Loading, Resident, Lost{reason}, Failed,
  Stopped}` with `LostReason::{Evicted, GpuDetached, ProcessExited,
  Unhealthy}`; `LostReason::is_operator_fault()` is false only for
  `GpuDetached`; `worth_retrying_here()` is true only for
  `GpuDetached | ProcessExited`. One pure transition function,
  `Residency::observe(&self, &Observation, now)`, total and
  deterministic. Two subtleties the hook design (§5) depends on:
  a failed probe against a previously-`Resident` model yields
  `Evicted` (GPU present) or `GpuDetached` (GPU absent) — **not**
  `Unhealthy`; and `Loading` never times out.
- A `Fleet`'s roster is immutable for its lifetime:
  `Fleet::declare(budget, Vec<Declared>)` (`crates/hearth-core/src/
  fleet.rs`) is the only constructor; there is **no add / remove /
  undeclare / redeclare** (verified by full read of `fleet.rs`). The
  only mutators are `Fleet::observe` and `Fleet::set_endpoint`.
  Declarations are built in the CLI from `--model` flags; the standing
  form is `$HEARTH_HOME/fleet.conf`, parsed by `scripts/start.sh`,
  not by Rust.
- `Supervisor` (`crates/hearth-serve/src/lib.rs`) spawns llama.cpp /
  vLLM children (`server::ServerChild`, SIGKILL stop, Drop kills),
  probes `/health` every tick (500 ms ticker in `cmd_up`; 750 ms probe
  timeout, hard-coded), and records **changes only** to the spine.
  A fleet `Failed` state is coarsened to spine `Lost{ProcessExited}`;
  `Observation::LoadFailed` is never constructed by the supervisor,
  so `Failed` is unreachable via its own probes.
- **Bounded auto-remediation already exists** — this corrects an
  assumption the G-2 brief started from. `Supervisor::restart_lost()`
  restarts a managed model when its state is
  `Lost{ProcessExited | Unhealthy}`, its child handle is already
  gone, the original start is > 60 s old, and `Fleet::next_to_load()`
  names it. It never restarts `Evicted` or `GpuDetached`; it has no
  restart counter, backoff, or cap; and it is wired only in `cmd_up`
  (`cmd_serve` ticks without it). It is **unpinned by any test**
  (verified by grep of serve tests). Core's `worth_retrying_here()`
  and serve's `restart_lost()` disagree about `GpuDetached` — the
  predicate blesses it, the policy excludes it.
- There is **no event bus, watch channel, callback, or tracing** in
  either crate (grep-verified). Observation today is: poll `&Fleet`
  in-process behind `Arc<Mutex<Supervisor>>`; poll the spine
  (`latest` / `why` / `state_as_of`, structured `LostReason`
  preserved, at tick + record latency); poll `GET /residency` (state
  rendered as the human `explain()` string, **not** the enum — only
  the derived `operator_fault` / `try_elsewhere` booleans are
  structured); or read `eprintln!` logs. Nothing fires at the moment
  a transition happens.
- Everything is synchronous. The CLI supplies the only concurrency:
  ticker thread, warmer thread, one thread per gateway connection,
  all sharing the one supervisor mutex.

### 1.3 The spine (`hearth-store`) and the Python bindings (`hearth-py`)

- `Spine` (`crates/hearth-store/src/lib.rs`, the crate's only source
  file) stores per-model versioned JSON documents `{kind, facts}` in
  an embedded, in-process `nedb-engine` database (`Db::open` — no
  subprocess, no pgwire, no FFI in this crate). Two collections:
  `residency` and `pulls`. Content is enumerated by
  `enum Transition::{Declared, PullStarted, PullCompleted, Loading,
  Resident, Lost{reason}, Unloaded}`; each `record()` is a new
  document version hash-linked to caller-supplied `caused_by`
  event refs, and **flushes synchronously on every call** (deliberate
  event-granularity durability).
- Read-back `Event{model, kind, facts, seq: u64, hash, ts}` takes its
  `seq` from NEDB's `Node.seq`: a single global sequence exists.
  `Spine::seq()` reads it ("the clock `state_as_of` reads against").
  Replay today is by query only: `state_as_of(model, seq)`,
  `why(model)` / `provenance(model)` (causal trace from latest,
  **capped at 64 nodes**), `all_latest()`, `head()` (Merkle head),
  `verify()`. The complete public API is 14 methods (listed in §10).
- **There is no event feed.** No `events_since`, no range iterator,
  no subscription, watch, callback, or notification of any kind
  (verified by full read of the crate's single source file). A
  consumer that wants new events must poll `seq()` / `latest()`
  itself. A resume token exists only as values a caller assembles
  itself (`seq()` + an `EventRef` returned by `record()`), and no
  read method accepts one.
- `hearth-py` (PyPI `hearth-engine`, PyO3 0.22 + maturin, abi3-py39)
  depends on and exposes **only `hearth-core`**: `plan()` and an
  in-memory `Fleet` class (declare-as-data, `observe`, `route`,
  `report`, byte counters) — the decision procedure, not the
  runtime, as its own README says. Python **cannot** reach the
  spine, serve, pull, or convert through this package today. No
  GIL-release bridging pattern exists in the crate to copy.
- `hearth convert` (safetensors → GGUF) lives in the `hearth`
  binary: `cmd_convert` / `ensure_converter` in
  `crates/hearth-serve/src/bin/hearth.rs`, subprocess-driving
  upstream llama.cpp's `convert_hf_to_gguf.py` and `llama-quantize`.
  It is not in `hearth-store` or `hearth-py`, and this spec never
  treats it as part of the agency layer.

### 1.4 What this ground truth forces

1. The gateway cannot grow methods, headers, streams, or job handles
   without changing the `decide` contract that upstream tests pin —
   so the agency surface must **not** be patched into `hearth-api`
   (§3, §6).
2. The spine has sequence numbers and causal history but no feed —
   so SSE serving needs exactly one new read capability, and §4
   designs it without touching upstream crates first.
3. Supervision already contains an unsupervised restart policy —
   so §5 must draw the trust boundary around composition (which
   loops a binary runs), not around new code alone.
4. Rosters are immutable and Python is spine-blind — so
   "re-declare" and Python-side spine reads are designed as
   proposals and new binding crates, honestly labelled as the
   limited things they are (§5.4, slice G-7).

---

## 2. Design principles

1. **Rust transports and actuates; Python decides.** The gate, the
   SI journal, task policy, and all judgement stay in SI (Python).
   The fork provides facts (residency, spine history), transport
   (HTTP submission, SSE), and actuation primitives that execute
   admitted intents verbatim. No Rust in this design evaluates a
   tool intent, loosens a policy, or originates work.
2. **Journal before execution, on both sides.** SI-side work is
   journaled in the SI journal before it runs (existing SI rule).
   Fork-side actuation is recorded in the spine as part of the
   operation, mirroring `Supervisor`'s existing write-before-act
   pattern for `Declared` / `Loading`.
3. **Additive or absent.** Gaia additions live in new `gaia-*`
   crates (§6). Upstream crates are patched only if a later slice
   proves a change cannot live outside them, and then only
   additively (new public items, no signature or behaviour change),
   as its own reviewed slice.
4. **The residency diagnosis is untouchable.** Nothing in the
   agency surface may reuse, shadow, or dilute the §1.1 mapping.
   Model-not-ready answers come only from `route_to_decision`;
   agency answers are a separate route family with separate error
   types (§3.2).
5. **Loopback honesty.** The fork inherits upstream's security
   model — loopback bind, no auth — and says so wherever the
   agency surface is described. Designing auth/TLS is a non-goal
   (§9); anyone exposing `/gaia/*` beyond the box is outside this
   design's safety claims, exactly as upstream's README already
   warns for the gateway.
6. **Performance silence.** No claim in this spec about latency,
   fan-out capacity, or overhead is measured. Slice G-8 exists to
   replace adjectives with numbers; until it lands, the honest
   word is UNMEASURED.

---

## 3. Agency endpoints in the gateway surface

### 3.1 Placement: a sibling crate, composed — not a patch

New crate **`gaia-api`**: a method-aware decision layer in the
image of `hearth-api`, but separate from it. `gaia-api` depends on
`hearth-core`, `hearth-store`, and `hearth-api` (for `http::Server`
and the response envelope helpers' *shapes*, re-implemented where
the fixed-length renderer cannot be reused — see §3.4). It exposes
its own `classify(method, path)` and `decide(request, &GaiaState)`,
where `GaiaState` is a handle the composition binary owns
(§3.5). `hearth-api` is not modified: `classify`/`decide`, the
`Endpoint` enum, and all 26 pinning tests stay exactly as upstream
wrote them.

Composition happens in a new binary crate **`gaia-serve`** (§6),
which constructs `Supervisor` from `hearth-serve`'s public library
API (`Supervisor::new`, `start`, `stop`, `tick`, `fleet`,
`spine` — all public today) and serves both route families. The
existing `hearth` binary is untouched and keeps upstream
behaviour, including `restart_lost` (§5.2).

### 3.2 Routes

All routes are new, versioned under `/gaia/v1/`, and method-aware
(unlike `gateway::decide`, `gaia-api` receives the full request:
method, headers, path, body). Proposed shapes:

| Method & path | Purpose | Response |
|---|---|---|
| `POST /gaia/v1/work` | Submit a work order: an SI tool-intent envelope (the SI-41/42 format: `{type:"tool", tool, args, reason}` plus task id and the SI journal sequence at which the intent was journaled) | `202` with `{work_id, state:"accepted", journal_seq}` once the submission is recorded (§3.3); `400` malformed; `409` if the intent's class is one the fork is configured never to carry (e.g. `external` while unconfigured) |
| `GET /gaia/v1/work/{id}` | Status and, when finished, result | `200` `{work_id, state, result?, provenance: {si_journal_seqs, spine_event_refs}}`; `404` unknown id |
| `POST /gaia/v1/work/{id}/verdict` | Deliver the gate's verdict for a previously submitted intent that policy escalated (the return path that makes the fork a proposer, not a decider — used by §5) | `200` on recorded verdict; `409` if the work already has a verdict or has executed |
| `GET /gaia/v1/events` | The SSE stream (§4) | `200 text/event-stream`, held open |
| `GET /gaia/v1/residency` | Structured residency for policy consumers | `200` — per-model `{model, state (enum string), lost_reason?, operator_fault, try_elsewhere}` read from the spine/fleet, fixing the §1.2 gap where `/residency` exposes only the human `explain()` string |
| `GET /gaia/v1/health` | Liveness of the agency surface itself | `200 {"status":"ok","service":"gaia"}` |

Rules that keep §1.1's semantics intact:

- **No agency route proxies inference**, and no inference route
  learns about work. A model that is not ready still answers via
  `route_to_decision` and nothing else.
- Agency errors reuse the OpenAI *envelope* (`{error:{message,
  type, param, code}}`) for client consistency but with distinct
  `gaia_*` types (`gaia_work_unknown`, `gaia_verdict_conflict`,
  `gaia_class_not_carried`, …). A `model_lost` can never be
  produced by a `/gaia/*` route, and a `gaia_*` error can never be
  produced by an inference route. The two vocabularies stay
  disjoint, so no client can mistake a work problem for a
  residency diagnosis or vice versa.
- Submission is **not** execution. `POST /gaia/v1/work` records
  and queues; actuation happens only per §3.3.

### 3.3 The work lifecycle

1. **Submit.** SI (or an operator client) posts an intent that SI
   has *already journaled*. The fork records the submission in the
   spine as a new `Transition` kind proposed for `gaia-store`
   (§6 — implemented as a gaia-side record type, not an upstream
   enum patch; see §6.2 for the mechanism) with `caused_by`
   pointing at the latest spine event, and answers 202.
2. **Gate.** Two configurations, chosen by SI policy, both honest:
   (a) *pre-admitted* — the intent arrives with its ADMIT verdict
   and SI journal seq attached; the fork verifies the envelope's
   internal consistency (seq present, class carriable) and may
   act. (b) *escalated* — the intent arrives verdict-less; the
   fork holds it in `awaiting_verdict`, surfaces it on the event
   stream (§4), and waits for `POST …/verdict`. The fork never
   manufactures a verdict in either configuration.
3. **Act.** The composition executes exactly the journaled args
   through the executor mapping of §5.3 (for residency work) or
   returns the work order to SI's own executor over the status
   endpoint (for machine work SI executes itself — the fork is
   then transport + provenance only). Which executor applies is a
   property of the tool class, declared in the submission, never
   inferred by the fork.
4. **Record.** Outcome (success value, error, or refusal) is
   recorded in the spine, causally linked to the submission
   record, and the result + provenance trail (SI journal seqs +
   spine `EventRef{hash, seq}` pairs) becomes the
   `GET /gaia/v1/work/{id}` answer. This is SI-41 §12 use case (a)
   — agentic work as a service, result returned **with** its
   gate-journaled provenance trail — given a transport.

### 3.4 The streaming/response gap, named

`http::render_response` cannot emit an SSE response (fixed
`content-length`, `connection: close`, JSON only). `gaia-api`
therefore includes its own small response writer for
`text/event-stream` (chunked, flush per event), modelled on
`http::proxy`'s relay loop, which already demonstrates the
crate's tolerated pattern for long-lived writes (no read timeout,
flush per chunk). Ordinary agency answers (202/200/4xx) reuse
the one-shot JSON shape. The writer is new code in `gaia-api`,
not a change to `http.rs`.

### 3.5 What stays Python, what becomes Rust

| Concern | Owner | Why |
|---|---|---|
| Tool registry, intent format, gate checks, verdicts | SI (Python) — exists (SI-42…44) | Judgement stays with the mind |
| SI journal (work provenance) | SI (Python) — exists | Source of truth for work |
| Task loop, budgets, leash policy | SI (Python) — exists (SI-44/50 design) | Policy, not transport |
| HTTP submission/status/verdict transport | **Rust, `gaia-api`** (new) | The runtime is the always-on process; Python services come and go |
| SSE serving of the unified event stream | **Rust, `gaia-api`** (new, §4) | Long-lived connections belong with the runtime's thread budget, not a Python client of it |
| Residency facts, structured | **Rust** (spine reads; `/gaia/v1/residency`) | The spine is in-process to the runtime; exporting facts beats exporting the database |
| Actuation primitives (start/stop/restart/pull) | **Rust, `gaia-serve`** composition over public `Supervisor` / `hearth_pull` APIs | Only the runtime may touch its children |
| Spine access from Python | Later: new `gaia-py` bindings (slice G-7) | `hearth-py` is upstream and exposes no spine (§1.3); a sibling binding crate keeps hygiene |


---

## 4. SSE serving from the runtime

### 4.1 Semantics imported, unchanged

The stream's semantics are SI's, already implemented on the SI
side (SI-47 spine, SI-48 filtered subscriptions, SI-49 resilient
follower) and are **not** redesigned here:

- Every event has an `id` equal to its journal sequence number.
- A reconnecting consumer sends `Last-Event-ID: <seq>` and
  resumes exactly after it — reconnect is replay, not a new
  subscription.
- Subscriptions are filterable (by source, kind, model/task).
- Duplicate or out-of-order delivery is a loud protocol error on
  the consumer side, never silently deduplicated (SI-49).

What the fork adds is a **server** for these semantics inside the
runtime process, plus a second event source (below). SI's Python
follower remains the reference consumer and the conformance
check: if SI-49's follower cannot follow the fork's stream, the
fork is wrong.

### 4.2 Two journals, two sequence spaces — kept separate

The fork serves events from two sources, and this spec refuses
to merge their sequences:

| Source tag | Journal | Sequence | Events |
|---|---|---|---|
| `hearth` | The spine (`hearth-store`) | `Event.seq` (NEDB global seq, §1.3) | `declared`, `loading`, `resident`, `lost{reason}`, `unloaded`, pull events, and (from §3) gaia work records |
| `si` | SI's NEDB journal | SI journal seq | Loop transitions (perceive/propose/gate/journal/execute/result) as forwarded by SI's service adapter (SI-46/47) |

Wire shape per event:

```
id: <seq>
event: <source>:<kind>
data: {"source": "...", "seq": <u64>, "model"|"task": "...", "facts": {...}}
```

`Last-Event-ID` resume is **per source**: a consumer following
both sources tracks two cursors, and the subscription request
declares them (`?source=hearth&since=1234&source=si&since=987`
or per-source headers on reconnect). A single merged counter
would be a third sequence space owned by nobody's journal —
exactly the kind of quiet invention this design exists to avoid.
How SI-side events reach the fork (SI posts them, or the fork
polls SI's service) is a deployment choice of the SI adapter;
the fork's contract begins at "events with SI seqs arrive in
order".

### 4.3 The one missing read: events-since on the spine

Serving `hearth`-source events requires reading the spine *in
seq order, from a cursor* — the capability §1.3 shows does not
exist. Two candidate implementations, in the order this spec
recommends building them:

**(a) Mirror index (recommended first; zero upstream change).**
`gaia-serve` runs a poller at the supervisor's own cadence
(500 ms) that reads `Spine::seq()` and `all_latest()` /
per-model `latest()`, and appends newly observed spine events
to a **gaia-owned append-only mirror** (a plain file or a
`gaia-store` collection) keyed by spine seq. The mirror is a
*cache of the spine, never a source of truth*: it is rebuildable
from the spine at any time, it records the spine `hash` of every
mirrored event so a consumer (or `verify()`-style audit) can
check mirror entries against spine truth, and if mirror and
spine ever disagree the spine wins and the mirror is rebuilt.
Cost, stated honestly: event latency inherits tick (500 ms) +
probe (≤ 750 ms per model, sequential) + poll cadence — the
same latency every spine consumer already lives with (§1.2).
Change-only recording in `tick()` means the mirror sees exactly
the transitions the spine sees, no more.

**(b) A real `events_since` (later cleanup, additive).** If a
later slice verifies that `nedb-engine`'s `Db` supports an
ordered range read by seq (its internals were **not** opened by
the G-2 analysis — this is an open question, §8), an additive
`Spine::events_since(seq, limit)` in `hearth-store` would let
the mirror be deleted. That is the one upstream-crate change
this spec can currently imagine wanting; it is additive-only
and is proposed as its own slice (G-8 territory) with an
upstream-first conversation, never as a drive-by patch. The
design does not depend on it.

For the `si` source no new read is needed: SI's journal already
supports the follower semantics SI-47…49 built against.

### 4.4 Backpressure and slow consumers

The stream is read-only fan-out: **writers never block on
consumers.** Concretely:

- Each SSE connection gets a bounded ring buffer of **256
  events** (a policy constant, tunable, stated here as the
  design's starting value — UNMEASURED). The serving thread
  offers events non-blockingly.
- On overflow the server does not drop silently and does not
  queue unboundedly: it emits a final `event: resync` frame
  carrying the newest seq, then closes the connection. The
  consumer resumes with `Last-Event-ID` (§4.1). Slow consumers
  cost the server one buffer, never memory growth, never
  writer latency.
- Heartbeat: an SSE comment (`: hb`) every 15 s of silence, so
  intermediaries and the SI-49 follower can distinguish "quiet"
  from "dead" without inventing events. Heartbeats carry no
  `id` and never advance a cursor.
- **Connection budget is separate from inference.** Worker A's
  central conflict (§1.1): gateway connections share one
  `max_inflight` pool (default 64) of thread-per-connection
  slots; an SSE client would hold one for its whole life.
  Therefore `/gaia/v1/events` is served on the `gaia-serve`
  composition's **own listener with its own budget** (starting
  value 32 connections, tunable, UNMEASURED), never out of the
  inference pool. A fork deployment can thus be flooded with
  subscribers and inference keeps its full budget — the failure
  modes are independent by construction.
- Filters are applied server-side before buffering (a consumer
  subscribed to one model's events does not fill its ring with
  another's), matching SI-48's filtered-subscription semantics.

---

## 5. Residency self-maintenance hooks — "Gaia's first job is itself"

### 5.1 The shape: observe in Rust, propose to Python, act on verdict

```
spine ──(poll, §4.3a)──▶ gaia-serve watcher
                            │  Lost{reason} observed for model M
                            ▼
                     proposal intent ──▶ SI gate + SI journal
                     {type:"tool",        (Python decides;
                      tool:"hearth.restart"|"hearth.redeclare"|   fork waits)
                      "hearth.repull"|"hearth.notify",
                      args:{model:M, spine_event:<EventRef>},
                      reason:"observed lost{process_exited} at seq N"}
                            │  ADMIT (journaled in SI)
                            ▼
                     verdict ──▶ gaia-serve actuation (§5.3)
                            │
                            ▼
                     outcome recorded in spine (caused_by the
                     observed Lost event) + returned to SI with
                     provenance (§3.3 step 4)
```

The watcher is a *proposer*. It holds no policy beyond the
reason→candidate mapping table in §5.2, and that table proposes
**at most one** candidate per observed loss. Whether the
candidate runs is the SI gate's decision, journaled in SI
before any actuation — the same discipline SI applies to every
other tool.

### 5.2 Reason → candidate mapping (grounded in §1.2 semantics)

| Observed spine event | Candidate proposal | Notes from the code |
|---|---|---|
| `lost{process_exited}` | `hearth.restart` (stored `ServerSpec`, via `Supervisor::start`) | The child is gone; restart is the actuation `restart_lost` performs today, moved behind the gate (§5.2.1) |
| `lost{unhealthy}` | `hearth.restart`, but only after a confirming re-probe | Reached mainly for non-`Resident` prior states (§1.2); a previously-`Resident` model that stops answering records `evicted`, so this reason is rarer than intuition suggests |
| `lost{evicted}` | `hearth.notify` first; `hearth.restart` only if the fleet has free VRAM at proposal time | `Evicted` is ambiguous by construction (§1.2): planner pressure *or* a resident's failed probe. The proposal's `reason` must carry the spine causal chain (`why()`) so the gate sees which. Never auto-proposed twice for the same loss event |
| `lost{gpu_detached}` | `hearth.notify` only | The card is physically gone. Core's `worth_retrying_here()` blesses this reason but serve's policy excludes it (§1.2) — the design sides with serve: no restart proposal against absent hardware; re-declaration targets a different host, which is an operator/SI decision, not a watcher proposal |
| `pull` failures / stale weights suspicion | `hearth.repull` (`hearth_pull::pull`, as `cmd_pull` calls it) | Only ever proposed by SI or an operator, never by the watcher — the watcher cannot smell stale weights |

#### 5.2.1 The `restart_lost` boundary — stated precisely

Upstream `hearth up` already restarts `ProcessExited` /
`Unhealthy` losses without asking anyone (§1.2). That behaviour
is **upstream's**, it stays in the upstream `hearth` binary
untouched, and this design does not pretend otherwise. The
trust boundary is at **composition**:

- A deployment running the upstream `hearth` binary gets
  upstream behaviour, including `restart_lost`. It is not
  running Gaia agency, and no Gaia claim may be made for it.
- A deployment running **`gaia-serve`** gets the gated
  behaviour: the composition calls `tick()` and **never calls
  `restart_lost()`**. Every restart it performs arrived as an
  admitted, SI-journaled intent (§5.1). This is achievable with
  zero upstream patches because `tick` and `restart_lost` are
  separate public methods and the loop wiring lives in the
  binary, not the library (§1.2).
- `gaia-serve` additionally checks the **leash** before every
  actuation: if SI's journal holds an un-resumed stop for the
  target (surfaced via the verdict path / SI adapter), the
  actuation refuses and records the refusal. A stopped mind's
  runtime does not restart its models behind its back.

### 5.3 Actuation primitives (all public today)

| Proposal | Actuation in `gaia-serve` | Evidence it exists |
|---|---|---|
| `hearth.restart` | `Supervisor::start(stored_spec)` — the same call `restart_lost` makes internally | `crates/hearth-serve/src/lib.rs :: Supervisor::start` |
| `hearth.stop` | `Supervisor::stop(model)` | same |
| `hearth.repull` | `hearth_pull::pull(reference, &PullConfig, &Spine)` | as called by `cmd_pull`, `crates/hearth-serve/src/bin/hearth.rs` |
| `hearth.notify` | A spine record + an `si`-visible event (§4); no process action | §3.3 recording path |
| `hearth.redeclare` | **Does not exist as a primitive** — §5.4 | `Fleet` roster is immutable (§1.2) |

Actuation executes the admitted args verbatim (model name +
the spine `EventRef` it answers). If the fleet state has moved
on since the proposal (model already `Resident` again, or
`Stopped`), actuation refuses, records the refusal in the
spine, and reports it — it does not improvise a substitute
action. Proposals are answered, not reinterpreted.

### 5.4 The re-declare limitation, honestly

"Re-declare" appears in Gaia's founding use cases (a mind
perceives a loss and re-declares its model). Against today's
code, a roster change means constructing a new `Fleet` — in
practice, editing `fleet.conf` and restarting the runtime
process. This spec therefore scopes `hearth.redeclare` as:
the gate-admitted intent is recorded, the *standing declaration
file* is updated by the actuation layer (a file write SI could
have gated as an ordinary tool intent anyway), and the actual
roster change takes effect at the next runtime start, which
the actuation reports as `pending_restart`, not as done.
Claiming more would be faking a capability the code does not
have. A future slice may explore a `gaia-core` extension type
wrapping `Fleet` with roster versioning; it is **not** in the
slice plan (§7) because no honest need has been measured yet,
and patching `Fleet` itself is exactly the upstream conflict
§6 exists to avoid.

### 5.5 What the watcher never does

- Never restarts, stops, pulls, or edits anything on its own
  authority — including "obvious" cases. One observed loss ⇒
  at most one proposal ⇒ the gate decides.
- Never proposes for a loss already answered (the spine causal
  chain is the dedup: a proposal record `caused_by` the loss
  event; a second watcher pass sees its own proposal in the
  chain and stays silent).
- Never touches a model whose declaration marks it out of Gaia
  scope — scope is a declaration-time property carried in the
  work envelope, defaulting to *out* (opt-in, not opt-out).


---

## 6. Upstream hygiene: how Gaia stays mergeable

### 6.1 The rule: new crates, not patches

| Addition | Home | Upstream files touched |
|---|---|---|
| Agency decision layer (routes, work lifecycle, verdicts) | **new crate `gaia-api`** | none |
| SSE writer + event fan-out | `gaia-api` | none |
| Composition binary (supervisor loop, watcher, actuation) | **new crate `gaia-serve`** (binary `gaia`) | none |
| Spine mirror index (§4.3a) | `gaia-serve` (later `gaia-store` if it outgrows the binary) | none |
| Python spine bindings (§3.5) | **new crate `gaia-py`** (later slice) | none |
| Workspace membership for the above | root `Cargo.toml` `[workspace] members` | **one list, kept sorted** — the single expected recurring conflict point, accepted knowingly |

Rejected alternative: patching `hearth-api` with a `Gaia`
endpoint variant and a widened `decide` signature. It would put
every upstream gateway change in conflict with Gaia's surface,
force upstream's 26 pinning tests to adjudicate Gaia behaviour,
and couple the agency release cadence to the gateway's. The
sibling-crate shape costs one extra crate and buys: upstream
merges that touch only hearth-* files merge clean; Gaia changes
never appear in upstream's diff at all; and the `hearth` binary
remains byte-for-byte upstream behaviour for anyone who runs it.

### 6.2 Records without upstream enum patches

§3.3 wants gaia work records in the spine, but `Transition` is
an upstream enum in `hearth-store`. Patching it is exactly what
§6.1 forbids in early slices. The interim mechanism: gaia
records live in the **mirror store** (§4.3a) and in SI's
journal (which already records the intent and verdict); the
spine receives only the residency-visible consequences (a
restart produces real `Loading`/`Resident` spine events through
`Supervisor` itself). Provenance then spans two stores — SI
journal seqs + spine refs — which §3.3's result shape already
assumes. If operating experience shows spine-resident work
records are worth an upstream conversation, that is an
additive-enum-variant proposal to make deliberately, later,
with evidence — not a founding patch.

### 6.3 Naming

- Crates: `gaia-api`, `gaia-serve`, `gaia-store`, `gaia-py` —
  the `gaia-*` prefix, parallel to `hearth-*`.
- Routes: `/gaia/v1/*`. Errors: `gaia_*` types (§3.2).
- Env/config: `GAIA_*` (e.g. `GAIA_PORT`, `GAIA_MAX_EVENTS`),
  following upstream's `HEARTH_*` tunable pattern.
- Binary: `gaia`. Nothing Gaia ships is named `hearth`
  anything; nothing upstream is renamed.

### 6.4 Sync discipline (proposal, for the fork's runbook)

Syncs from upstream land as PRs (merge upstream/main), never
as direct pushes to this repo's main. Gaia-side conflict
resolution favours upstream in `hearth-*` crates (take theirs,
re-apply nothing — there should be nothing to re-apply) and
Gaia in `gaia-*` crates. Every sync PR must re-run the full
workspace tests plus the §7 acceptance tests of any landed
Gaia slice before merge. Sync before each new Gaia feature
slice, and at least weekly while the fork is active — cadence
is a proposal, to be ratified with the G-1 runbook.

---

## 7. Slice plan (all UNBUILT; each independently mergeable)

Slices are ordered so each is true on its own: nothing below
requires a later slice to be honest. Every slice is one PR,
built by a builder who does not merge it, reviewed and merged
under the fork's standing review discipline (the G-2 pattern:
independent suite run before merge; docs-only slices use the
fast lane). Acceptance criteria are test-shaped on purpose.

### G-3 — `gaia-api` skeleton + work submission/status
New crate `gaia-api`: method-aware classify/decide for
`POST /gaia/v1/work`, `GET /gaia/v1/work/{id}`,
`POST /gaia/v1/work/{id}/verdict`, `GET /gaia/v1/health`;
in-memory work store behind a trait (mirror persistence is
G-4's concern); error envelope with `gaia_*` types.
**Acceptance:** (1) all 26 upstream `hearth-api` tests pass
unmodified and `hearth-api` shows zero diff; (2) a submitted
pre-admitted intent returns 202 + a stable `work_id` and its
status round-trips; (3) an escalated intent cannot reach the
actuation trait without a recorded verdict — proven by a test
that attempts it; (4) verdict conflicts (double verdict,
verdict after execution) return 409; (5) no route produces a
`model_*` error type and no inference route produces a
`gaia_*` type (route-table test).

### G-4 — SSE serving
`GET /gaia/v1/events` on its own listener/budget (§4.4); the
spine mirror poller (§4.3a) with hash-checked entries; filters;
`Last-Event-ID` per-source resume; resync-on-overflow;
heartbeats.
**Acceptance:** (1) SI's SI-49 reference follower consumes the
fork's stream through a real drop-and-reconnect and resumes
with zero missed and zero duplicated events; (2) a consumer
slower than the event rate receives `resync` + close, and
writers are never delayed (test with a blocked consumer);
(3) mirror entries verify against spine hashes, and a
corrupted mirror is detected and rebuilt (fault-injection
test); (4) 40 concurrent SSE subscribers leave the inference
`max_inflight` pool untouched (subscribers served from the
Gaia budget only).

### G-5 — Residency watcher + proposals + structured residency
The §5.1 watcher over the mirror; §5.2 proposal mapping with
causal dedup; `GET /gaia/v1/residency` (structured enum +
`lost_reason`); the `gaia-serve` composition binary running
`tick()` **without** `restart_lost`.
**Acceptance:** (1) killing a child's process in a test fleet
produces exactly one proposal, `caused_by`-linked to the spine
`lost` event, and **no restart occurs** in the `gaia-serve`
composition; (2) the same scenario under the upstream `hearth`
binary keeps upstream behaviour (composition-boundary test,
documenting §5.2.1 rather than discovering it); (3)
`lost{gpu_detached}` yields a notify proposal and never a
restart proposal; (4) a second watcher pass over the same
history proposes nothing new.

### G-6 — Gated actuation
Verdict-driven execution of `hearth.restart` / `hearth.stop` /
`hearth.repull` through §5.3, spine outcome records,
stale-state refusal, leash check before actuation.
**Acceptance:** (1) an ADMIT-ted restart intent restarts the
model and the spine shows the outcome causally linked to the
observed loss; (2) a REJECTED or never-verdicted intent
actuates nothing (process table + spine both checked);
(3) an intent whose target recovered on its own is refused,
recorded, and reported — not executed; (4) with a journaled
leash stop in force, an otherwise-admitted restart refuses;
(5) re-pull actuation records `PullStarted`/`PullCompleted`
through the existing spine path.

### G-7 — `gaia-py` spine bindings
New crate `gaia-py` (PyO3, mirroring `hearth-py`'s packaging):
read-only spine access for Python — `seq`, `latest`, `why`,
`state_as_of`, mirror `events_since` — with explicit
GIL-release (`allow_threads`) around any call that can block,
a pattern §1.3 notes `hearth-py` does not provide.
**Acceptance:** (1) Python reads a spine written by a Rust
supervisor and reconstructs the same `why()` chain;
(2) bindings expose no mutating method (API surface test);
(3) `hearth-py` is unmodified.

### G-8 — Measurement + the `events_since` question
In the SI-40 tradition: measure, on this class of machine,
SSE fan-out cost per subscriber, mirror poll latency
distributions (loss→proposal wall time), actuation round-trip
(proposal→verdict→resident), and mirror rebuild time over a
long spine; **and** settle §4.3b by inspecting `nedb-engine`'s
`Db` for an ordered seq-range read. Deliverable is a
measurements document and, if (b) is real, an additive
`hearth-store` proposal written for upstream-first review.
**Acceptance:** numbers exist, with method stated, for every
quantity §§3–5 currently label UNMEASURED; the `events_since`
question is answered from the `nedb-engine` source, not
inferred.

---

## 8. Open questions and risks (unresolved, stated)

1. **NEDB seq-range read** — whether `nedb-engine 10.30.93`
   supports ordered enumeration by `seq` was not verified; the
   G-2 analysis deliberately did not open the external crate.
   §4.3 is designed to be correct under either answer. (G-8.)
2. **Flush-per-record under mirroring** — `Spine::record`
   flushes synchronously per event (§1.3). The mirror reads
   rather than writes the spine, so G-4 does not multiply that
   cost; but any future spine-resident gaia records (§6.2)
   inherit it. Unquantified until G-8.
3. **`restart_lost` divergence** — core's `worth_retrying_here`
   vs serve's exclusions (§1.2) is an upstream inconsistency
   this design routes around by composition. If upstream
   changes either side, §5.2's table must be re-derived, not
   assumed.
4. **Probe-semantics ambiguity** — `Evicted` conflates two
   causes (§5.2). The `why()` chain mitigates; it does not
   fully disambiguate a resident that failed a probe while the
   planner was also under pressure. Policy in SI must treat
   `Evicted` proposals as evidence-bearing, not self-evident.
5. **The watcher is a poller** — loss→proposal latency is
   tick + probe + poll (order of a second, UNMEASURED). Any
   future claim of "real-time self-healing" without G-8
   numbers is overclaiming.
6. **Thread-per-connection economics** — the events listener
   inherits upstream's blocking model (§1.1). 32 budgeted
   threads is a starting policy, not a measured capacity;
   a subscriber count in the hundreds would force the async
   conversation upstream has so far declined on purpose.

## 9. Non-goals

- No relicensing; BUSL 1.1 stands (§ header).
- No changes to the SentinelIntelligence repo, its journal, or
  its gate. SI consumes this layer; it is not modified by it.
- No async runtime adoption in Gaia crates; the fork follows
  upstream's synchronous discipline until a measured need
  (§8.6) says otherwise.
- No auth/TLS design (§2.5). No multi-host federation, no
  hosted-service shape — the licence's Additional Use Grant
  and the loopback model both point the other way.
- No dynamic roster mutation (§5.4) and no claim that
  "re-declare" is more than it is today.
- No replacement of SI's Python SSE implementation; the fork's
  server is validated *against* it (§4.1).

## 10. Evidence index and verification

Analysis was delegated in three disjoint scopes; full worker
notes are preserved with this slice's backup materials.

| Claim area | Primary source read | Coordinator spot-check |
|---|---|---|
| Routes, `decide` contract, Lost mapping, 409 | `crates/hearth-api/src/gateway.rs` (`classify`:102, `decide`:134, `route_to_decision`:168, `retry_in(30)`:234, `409`:249) | grep-verified line/symbol level |
| Response shape (`content-length`, `connection: close`) | `crates/hearth-api/src/http.rs` (`render_response`) | grep-verified |
| States, `LostReason`, fault/retry predicates | `crates/hearth-core/src/residency.rs` (`LostReason`:41, `is_operator_fault`:62, `worth_retrying_here`:69, `Residency`:77) | grep-verified |
| Roster immutability | `crates/hearth-core/src/fleet.rs` (`declare`:117 is the only constructor; mutators `set_endpoint`:161, `observe`:170) | full public-API listing |
| `restart_lost` existence + conditions | `crates/hearth-serve/src/lib.rs` (`restart_lost`:242; reasons:251/254; `> 60_000`:260) | grep-verified |
| Spine API completeness; no feed | `crates/hearth-store/src/lib.rs` (14 public methods, `seq`:286; zero hits for `events_since`/`subscribe`/`watch`/`notify`) | full public-API listing + grep |
| Python surface = core only | `crates/hearth-py/Cargo.toml` (sole path dep `hearth-core`:17), `src/lib.rs` module registration (`plan` + `PyFleet`:139–141) | grep-verified |
| Convert location | `crates/hearth-serve/src/bin/hearth.rs` (`cmd_convert`/`ensure_converter`) | worker-read; consistent with serve `Cargo.toml` `[[bin]]` |
| Licence | `LICENSE` (BUSL 1.1, Interchained LLC); workspace `Cargo.toml` (`license = "BUSL-1.1"`, version 0.4.7, 8 member crates) | read directly |

**Findings accepted vs rejected.** Accepted: all three
workers' factual findings, each spot-checked as above.
Rejected — three framings the evidence corrected, recorded
because they shaped this design: (1) the brief's starting
assumption that the supervisor performs *no* auto-remediation
— false; `restart_lost` exists, and §5.2.1 exists because of
it. (2) Any reading of the spine as already providing an event
feed — false; §4.3 designs the missing read instead of citing
one. (3) The assumption that conversion lives near the store
or bindings — it lives in the serve binary (§1.3), and the
agency layer leaves it alone.

*End of G-2 design spec. Design only — unbuilt, unmeasured.*
