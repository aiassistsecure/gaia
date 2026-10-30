# G-9 — The SI↔Gaia seam: proposals, verdicts, leash, outcomes over real transports

Status: DESIGN ONLY. Nothing in this document is built unless a section
explicitly says "as-built" and cites the code. Quantities labelled
UNMEASURED stay UNMEASURED; capabilities labelled UNBUILT stay UNBUILT.
Base: gaia main `62b13be` (G-1..G-8 merged). SI inventory taken against
SentinelIntelligence main `214a564` (SI-51 / PR #56 merged).

## Contents

- [1. Purpose and scope](#1-purpose-and-scope)
- [2. Invariants the seam must not break](#2-invariants-the-seam-must-not-break)
- [3. Vocabulary mapping](#3-vocabulary-mapping)
- [4. As-built inventory: the fork side](#4-as-built-inventory-the-fork-side)
- [5. As-built inventory: the SI side](#5-as-built-inventory-the-si-side)
- [6. The seam today: boundary crossings and the three missing ones](#6-the-seam-today-boundary-crossings-and-the-three-missing-ones)
- [7. Design principles](#7-design-principles)
- [8. Flow 1: proposals, fork to SI](#8-flow-1-proposals-fork-to-si)
- [9. Flow 2: verdicts and work submission, SI to fork](#9-flow-2-verdicts-and-work-submission-si-to-fork)
- [10. The leash crossing](#10-the-leash-crossing)
- [11. Flow 3: outcomes, fork to SI, as provenance](#11-flow-3-outcomes-fork-to-si-as-provenance)
- [12. Consolidated message shapes](#12-consolidated-message-shapes)
- [13. Failure modes and dispositions](#13-failure-modes-and-dispositions)
- [14. Security posture](#14-security-posture)
- [15. Implementation slices](#15-implementation-slices)
- [16. Open questions and risks (unresolved, stated)](#16-open-questions-and-risks-unresolved-stated)
- [17. Non-goals](#17-non-goals)
- [18. Evidence index](#18-evidence-index)

---

## 1. Purpose and scope

Gaia exists as two halves that meet only inside one process at a time:

- The **fork** (this repo, Rust): Hearth's runtime manager plus the
  `gaia-api` / `gaia-serve` agency layer. It watches model residency,
  proposes remediation (`hearth.restart`, `hearth.notify`,
  `hearth.stop`, `hearth.repull`), holds work orders and verdicts,
  and — since G-6 — actuates, but only through its in-process stores.
- **SI** (SentinelIntelligence, Python): the mind side. It holds the
  gate (`si/gate.py`), the NEDB journal (`si/journal.py`), the queue
  discipline (SI-50/SI-51), the agentic-work service (B-5), and the
  SSE machinery (SI-47/48/49).

The G-6 PR stated the gap plainly: "verdicts/leash stops arrive
in-process (SI-adapter transport across that seam is not wired)".
This document designs that transport. It covers exactly three flows
plus one deferred stream:

1. **Proposals** — fork → SI (§8).
2. **Verdicts + leash state** — SI → fork (§9, §10).
3. **Actuation outcomes** — fork → SI, as provenance (§11).
4. **SI journal events into the fork's SSE `source=si`** — the pipe
   exists on the fork side and is empty in production; §15 slice S-6
   wires it. It is not one of the three control flows and no control
   decision may ever ride it (§7, principle 6).

Out of scope: any change to the gate's checks, the queue discipline,
the watcher's proposal mapping, or upstream Hearth crates. The seam
moves records that already exist; it does not redesign either side.

## 2. Invariants the seam must not break

These hold on each side today (citations in §4/§5) and must hold
end-to-end once the flows cross processes:

1. **Journal before execution.** No actuation without a prior,
   durable record of the intent and its ADMIT verdict. SI side: the
   gate's self-edit (intent + verdict) is journaled before any claim
   (`drain.py:admit_tool_intent:378`, `Drainer.claim:589`). Fork
   side: `claim_for_actuation` + leash/stale checks precede the
   primitive (`work.rs:358`, `actuate.rs:269`). The seam preserves
   this by ordering: SI journals *before* it submits work (§9.2);
   the fork actuates only journaled-at-SI intents.
2. **Two sequence spaces, never merged.** SI journal `seq` and Rust
   spine `seq` are different clocks. Cross-references are always
   explicit pairs — `(journal_seq, spine hash+seq)` — never a fused
   sequence, never one space's number presented in the other's field.
   As-built precedents: `Intent.journal_seq` ("NOT a spine seq",
   `work.rs:71`), `LeashRecord.journal_seq` (`leash.rs:66`),
   `status_body.provenance {si_journal_seqs, spine_event_refs}`
   (`work.rs:133`), `StreamEvent.seq` per-`Source` (`events.rs:56`).
3. **The executor runs exactly the journaled args.** Fork side: the
   actuator reads args verbatim from the work record
   (`actuate.rs:539`). SI side: the executor re-reads the intent
   from the journal row and re-prices it (`executor.py:230`).
   Across the seam: what SI journals is what SI submits; the fork
   executes the submitted record; any mismatch is a refusal, not a
   repair.
4. **Exactly-once verdict.** A work record accepts one verdict, from
   `AwaitingVerdict` only; a second delivery is a 409 conflict, and
   the first verdict wins (`work.rs:337`). SI's gate likewise
   journals one verdict per intent.
5. **The leash preempts.** A stop in force refuses actuation before
   any primitive (`actuate.rs:269`), and stale leash state must
   refuse too (§10.3 defines staleness). A resume is always an
   explicit journaled act, never a clock expiry (§10.4).
6. **Outcome-unknown is terminal, never replayed.** SI-50 §13: an
   executed-but-unreported actuation is indistinguishable from
   died-before/died-during; write/execute class goes
   `outcome-unknown` pending operator disposition
   (`accept-unknown` / `compensate` / `re-propose` as a fresh
   intent); there is deliberately no "mark-executed" (§13.3).
7. **Observations are data, never instructions.** Everything that
   crosses the seam is a record to be gated, journaled, or folded —
   including residency observations and outcome details
   (`hearth.py` states this for its leg; the seam inherits it).

## 3. Vocabulary mapping

The two codebases name the same lifecycle differently. The seam
translates at the boundary; neither side renames its internals.

### 3.1 Verdicts

| SI gate (`gate.py:GateReport:331`) | Fork (`work.rs:Verdict:33`) | Seam meaning |
|---|---|---|
| `ADMIT` | `admit` (attached at submit, or via verdict route) | Fork may actuate after claim + leash/stale checks |
| `REJECT` | `reject` | Terminal; never actuates (`claim_for_actuation` → 409 forever) |
| `ESCALATE` | *(no value)* | SI-side terminal for v1: the intent is not submitted (§9.4). If a work record was already submitted undecided, it rests in `AwaitingVerdict` and receives no verdict until an operator disposition exists — which is UNBUILT (§16) |

### 3.2 Intent / work lifecycle states

| SI queue state (`queue.py:ALL_STATES:74`) | Fork `WorkState` (`work.rs:83`) | Notes |
|---|---|---|
| `proposed` | *(no record)* | Proposal known to SI, not yet gated/submitted |
| `queued` (ADMIT, unclaimed) | `Admitted` (pre-claim) | Fork's 202 spelling `"accepted"` is a response literal only; no record rests in `Accepted` (`work.rs:83`) |
| `claimed` | *(no state)* | Fork claim is a transient step inside `actuate_work` (`gaia-serve/lib.rs:308`), not a resting state |
| `executed` | `Executed` | SI derives from the journaled outcome; fork from `complete` (`work.rs:382`) |
| `refused-at-claim` | `Executed` with `result.outcome="refused"` | Fork has no separate state; the refusal kind (`leash`/`stale_state`/`no_stored_spec`) is in the result and the `OutcomeRecord` (`outcomes.rs:71`) |
| `held` (leash in force) | *(no equivalent)* | Fork expresses the same fact at actuation time as a `leash` refusal, or pre-submission via the leash feed (§10) |
| `escalated` | `AwaitingVerdict` *(only if submitted undecided)* | See §3.1 |
| `rejected` | `Rejected` | Terminal both sides |
| `skipped` | *(no equivalent)* | SI fold-only in Q-1 (`queue_skip` has no writer — §5.4) |
| `outcome-unknown` | *(no equivalent)* | SI-side journal state only; the fork contributes evidence, never this state (§11.4) |
| `compensated` | *(no equivalent)* | Annotation (`compensated_by`), never a state value, SI side |

### 3.3 Leash scope

| | SI | Fork |
|---|---|---|
| Scope | per-**task** (`queue_stop` names a `task_seq`) | per-**model** (`LeashRecord.model`, `leash.rs:66`) |
| Store | journal fold (`fold_queue:511`) over `queue_stop`/`queue_resume` rows | JSONL `leash.jsonl`, latest record per model wins (`stop_in_force:254`) |
| Seam mapping | §10.2: SI keeps **one seam task per model**, so per-task stop ↔ per-model stop is 1:1 | unchanged |

### 3.4 Residency states

Fork `GET /gaia/v1/residency` emits `unknown | loading | resident |
lost | stopped` plus `lost_reason` (`residency.rs`). SI's Hearth
vocabulary is `Resident, Loading, Lost{Evicted}, Lost{GpuDetached},
Failed, Stopped, NotAdmitted` (`hearth.py:RESIDENCY_STATES:81`) —
capitalised, includes `NotAdmitted`, has no `Unknown`, and spells
loss as `Lost{…}` where the fork splits state/reason. The seam does
not reconcile these vocabularies in v1: residency is a perception
surface, not a control flow, and each side keeps its spelling at its
own surface. Any future consumer must map explicitly; silent
coercion is how `failed` already disappears on the fork side
(coarsened to `lost{process_exited}` upstream of `residency.rs`).

### 3.5 Identity

| Concept | SI | Fork | Seam key |
|---|---|---|---|
| Intent | self-edit row: `edit_id` (`se_NNNN`), `intent_seq` = its journal `seq` | work record: `work_id` (`work-NNNNNN`) | the pair `(journal_seq, work_id)`, recorded on both sides (§9.3) |
| Proposal cause | — (fork-originated) | `caused_by = (spine hash, spine seq)` (`proposals.rs:34`) | `caused_by` pair, carried verbatim end-to-end (§8.2) |
| Task | `task_seq`, `task_ref = "task@<seq>"` | `task_id` string (required at submit, `gateway.rs:170`) | `task_id := "task@<task_seq>"` (§9.2) |
| Outcome | `tool_result` experience row, `self_edit_id` = the intent's edit | `OutcomeRecord` in `outcomes.jsonl`, keyed by `work_id` | `work_id`, journaled once by SI (§11.2) |

---

## 4. As-built inventory: the fork side

All citations are this repo at `62b13be`. Full worker inventory:
§18, evidence A.

### 4.1 Work lifecycle — `crates/gaia-api/src/gateway.rs`, `work.rs`

- Routes (method-aware classify, `gateway.rs:55`; the only transport
  adapter is `gateway::decide(req, &mut dyn WorkStore)`,
  `gateway.rs:87`):
  - `POST /gaia/v1/work` → submit; success `202 {"work_id",
    "state":"accepted", "journal_seq"}`.
  - `GET /gaia/v1/work/{id}` → `WorkRecord::status_body()` (`work.rs:133`):
    `{"work_id","state","provenance":{"si_journal_seqs":[journal_seq],
    "spine_event_refs":[]},"result"?}`. **`spine_event_refs` is
    hard-coded `[]`** — its comment promises "spine persistence is
    G-4"; G-4 shipped SSE/mirror instead, and the field never landed.
  - `POST /gaia/v1/work/{id}/verdict` → body exactly
    `{"verdict":"admit"|"reject"}` (`parse_verdict`, `gateway.rs:263`);
    no journal_seq, no reason, no actor, no leash field.
  - `GET /gaia/v1/health` → `200 {"status":"ok","service":"gaia"}`.
  - Fifth route outside the gateway: `GET /gaia/v1/residency`
    (`residency.rs:handle`), checked before `decide` in the binary.
- Submit body (`parse_intent`, `gateway.rs:170`) — required: `type`
  (must be `"tool"`), `tool` (non-empty), `reason`, `task_id`
  (string), `journal_seq` (u64; provenance, explicitly NOT
  inventable). Optional: `args` (default null), `class` (default
  `"execute"`), `verdict` (`"admit"|"reject"` or absent = escalated).
  Violations → 400 `gaia_malformed_request`.
- State machine (`work.rs:83`, transitions in `MemoryWorkStore`):
  submit → `Admitted` (attached admit), `Rejected` (attached
  reject), or `AwaitingVerdict` (no verdict). `record_verdict`
  (`work.rs:337`): only from `AwaitingVerdict`; anything else → 409
  `gaia_verdict_conflict`; unknown id → 404. `claim_for_actuation`
  (`work.rs:358`): `Admitted` → claim; `AwaitingVerdict` → 409
  `gaia_verdict_required` (the actuator is never invoked — the
  escalation block is exactly this check); `Rejected`/`Executed` →
  409. `complete` (`work.rs:382`): only `Admitted` → `Executed`.
- Store: `MemoryWorkStore` (`work.rs:257`) — **process memory only**,
  `Vec` + linear scan, ids `work-{:06}` from `work-000001`, spent
  only on recorded submissions. Carried classes default
  `{read, write, execute}`; `external` → 409 `gaia_class_not_carried`
  unless configured. **No dedup**: `submit` always mints a fresh id;
  there is no lookup by `journal_seq` or `task_id` anywhere.
- Two claim→actuate→complete implementations exist:
  `gaia_api::run_actuation` (`work.rs:413`) is **tests-only**;
  production is `Composition::actuate_work` (`gaia-serve/lib.rs:308`).
  The seam names the latter as the real path.

### 4.2 Proposals — `crates/gaia-serve/src/proposals.rs`, `watcher.rs`

- `Proposal` (`proposals.rs:34`): `{model, tool, args, reason,
  caused_by: EventRef{hash, seq}}`. On disk (JSONL):
  `{"model","tool","args","reason","caused_by":{"hash","seq"}}`.
- `ProposalStore` (`proposals.rs:100`): append+flush before the
  in-memory copy updates; malformed lines are named in
  `load_failures`, never fatal. Dedup is `answered(&EventRef)`
  (`proposals.rs:175`) keyed on **hash and seq together** — same
  seq/other hash and same hash/other seq are both "not answered".
- Producer: `Watcher::pass` (`watcher.rs:135`) over mirrored `lost`
  events for in-scope models; mapping `candidate(reason, fleet_room)`
  (`watcher.rs:68`): `process_exited`/`unhealthy` → `hearth.restart`;
  `evicted` → restart iff fleet room else `hearth.notify`;
  `gpu_detached` → `hearth.notify` always. Args are exactly
  `{"model","spine_event":{"hash","seq"}}`; reason text carries the
  `spine.why` causal chain.
- `Proposal::intent_json()` (`proposals.rs:49`) builds the SI-41
  envelope `{"type":"tool","tool","args","reason"}` — and has **no
  production caller** (one watcher test). The envelope also lacks
  `task_id`/`journal_seq`, so the fork's own work endpoint would
  400 it. **Proposals terminate at `proposals.jsonl`.** Nothing
  submits them anywhere; the binary only logs them to stderr.

### 4.3 Leash — `crates/gaia-serve/src/leash.rs`

- `LeashRecord` (`leash.rs:66`): `{model, kind: "stop"|"resume",
  journal_seq (SI journal seq, NOT spine), reason}`; JSONL file
  `leash.jsonl`. Semantics: latest record per model speaks —
  `stop_in_force(model)` (`leash.rs:254`).
- `record_stop`/`record_resume` (`leash.rs:185,201`) have **zero
  production callers** (verified by grep: tests only). The module
  doc's claim that stops "arrive here because SI journaled them
  first and the verdict path surfaced them" describes no implemented
  path: the verdict body cannot express a stop. **In the shipped
  binary the leash can only ever be empty** (or hand-edited into the
  file before start). §10 designs the missing ingress.

### 4.4 Actuation and outcomes — `actuate.rs`, `outcomes.rs`, `lib.rs`, `main.rs`

- `FleetActuator` (`actuate.rs:73`) is constructed per actuation
  from borrowed composition fields. `actuate` (`actuate.rs:539`)
  dispatches on `intent.tool`: `hearth.restart`, `hearth.stop`,
  `hearth.repull`, `hearth.notify`. Args are read verbatim from the
  work record: restart/stop/notify require `spine_event:{hash,seq}`;
  repull requires `reference`, `spine_event` optional.
- Check order for runtime-touching tools: **1) leash**
  (`leash_refusal`, `actuate.rs:269` — refusal kind `"leash"`,
  detail quotes the stop's SI journal_seq + reason), **2)
  stale-state** (restart: latest spine event must be exactly the
  answered lost event, else `"stale_state"`; no stored spec →
  `"no_stored_spec"`), **3) primitive** (`Supervisor::start/stop`,
  `hearth_pull::pull`). `hearth.notify` takes **no** leash check and
  **no** stale check — it actuates nothing; the outcome record is
  the actuation. Repull takes the leash check but **no stale
  check**.
- `finish` (`actuate.rs:195`) builds the result JSON
  `{"outcome","tool","model","detail","caused_by","spine_event_refs",
  "refusal"?}` and the identical `OutcomeRecord`
  (`outcomes.rs:71`), and records the outcome **before** returning.
  Honest window, documented in code: if outcome recording fails
  after the primitive ran, the error propagates and the work stays
  `Admitted` — an operator-visible discrepancy, not silence
  (§13.3 builds on this).
- `OutcomeStore` (`outcomes.rs:163`): JSONL `outcomes.jsonl`,
  append+flush first, `for_work(work_id)` returns a list ("honest
  if a defect ever files two"). No reader exists outside the
  process: outcomes are visible externally only as a completed work
  record's `result`, and only while the process lives.
- Composition/binary: one process (`main.rs`) holds work store,
  composition (supervisor + spine + mirror + watcher + proposals +
  leash + outcomes) and the SSE hub behind mutexes. The **500 ms
  ticker** (`main.rs:256`) runs `tick_once` (`lib.rs:179`:
  supervisor tick → synchronous mirror `poll_once` → watcher pass),
  then drains: snapshot `Admitted` ids, per id
  `Composition::actuate_work`. File-backed under `$GAIA_HOME`:
  `spine/` (NEDB), `mirror.jsonl`, `proposals.jsonl`,
  `outcomes.jsonl`, `leash.jsonl`, `blobs/`. **The work store is
  the only state with no file** — a restart loses every work record,
  verdict, and result (`main.rs:524` constructs it fresh).
- Actuation is serial and ticker-bound, holding the composition
  lock during the primitive (a long repull blocks ticks, watcher,
  and residency reads). Stated here because §13 prices it; fixing
  it is not a seam slice.

### 4.5 SSE and mirror — `events.rs`, `sse.rs`, `mirror.rs`

- `GET /gaia/v1/events` on its **own listener and connection
  budget** (32, `EventsConfig`, `sse.rs:321` — design starting
  values, UNMEASURED per G-2 §4.4; G-8 measured fan-out/poll costs
  for the shipped constants). Sources `hearth|si` (`events.rs:30`);
  `StreamEvent` (`events.rs:56`): `{source, seq (source's own
  space), kind, model?, task?, facts, collection, row, row_kind?,
  hash? (hearth only), ts?}`; wire `id: <seq>`, `event:
  <source>:<kind>`.
- Resume: `Last-Event-ID` header beats `?since=`/`?after=`; absent
  ⇒ 0; unparseable ⇒ 400 `gaia_event_resume_invalid` (fail closed).
  Subscribe-before-replay, tail deduped by `seq <= last_sent`
  (`serve_stream`, `sse.rs:702`). Slow consumer ⇒ final `resync`
  frame (**no `id:` line** — it must not advance a cursor) + close.
  Hub ring 256 (policy constant, UNMEASURED, `sse.rs:118`).
- Mirror: `MirrorStore` (`mirror.rs:117`) polls the spine
  (`poll_once`, `mirror.rs:371`) into `mirror.jsonl`; append is
  idempotent by seq; `verify_against`/`rebuild` exist but have **no
  production caller** — mirror corruption is never checked at
  runtime in the binary. History is causal-walk-derived
  (`collect_spine_events`, `mirror.rs:345`), depth-capped, not an
  exhaustive scan.
- SI source: `SiEventBuffer::push` (`sse.rs:255`) requires strictly
  increasing SI seqs; a duplicate or out-of-order push is a loud
  `Err`, never reordered or silently deduped. **The binary never
  calls `push` in production** (`main.rs:512` creates the buffer
  empty) — `source=si` serves an empty history forever today.
  §15 S-6 wires real ingest.

### 4.6 gaia-py — `crates/gaia-py`

- Read-only by construction (G-7): `Spine` (seq, latest,
  latest_pull, why, provenance, state_as_of, all_latest, head,
  verify) and `Mirror` (events_since, entries, latest_seq,
  load_failures); absence of writers is enforced by an API-surface
  test. No bindings exist for work, proposals, leash, or outcomes,
  and there is no write path at all.
- NEDB is single-process: a second live open of a held spine is
  refused (`ValueError`), never split-brain; `NEDB_SHARED_OPEN=1`
  exists but the bindings never set it. While `gaia-serve` runs, SI
  **cannot** open the fork's spine even to read. The mirror file is
  the lock-free read path. §7 principle 5 builds on this.

---

## 5. As-built inventory: the SI side

All citations are SentinelIntelligence at `214a564`. Full worker
inventory: §18, evidence B.

### 5.1 The service — `si/gaia/service.py` (B-5 / SI-46)

- The only HTTP surface in `si/gaia/`: stdlib `ThreadingHTTPServer`,
  `make_service_server(bundle_dir, host="127.0.0.1", port=8000)`
  (`service.py:551`). Routes: `GET /` (description), `GET /health`,
  `POST /tasks`, `GET /tasks/<seq>` (provenance trail), `GET
  /events` (SI-47/48 SSE). Everything else → 404 JSON. Body cap
  1 MiB (`MAX_BODY_BYTES`, `service.py:146`).
- `POST /tasks` is **synchronous**: it blocks in `run_task` and
  returns `{status:"task-returned", …, result, provenance_trail}`;
  refusal is `{status:"refused", problems, journaled:False}` with
  nothing journaled (`submit_task`, `service.py:422`). There is no
  job queue, no async handle, no per-intent endpoint, no
  gate-evaluation endpoint, no verdict or leash route.
- Submission shape: `{submitter, instruction, script, work_root?,
  cycles?}`; the script is a list of intent dicts (a nested list is
  one SI-51 batch). **The scripted script is the only proposer**:
  "no trained mind proposes tool intents yet" (`service.py:46-52`).
- Provenance trail (`build_provenance_trail`, `service.py:279`):
  `{task_seq, task_ref, submitter, submitted_via,
  submitter_identity_label, task_entry, steps:[{seq, edit_id, kind,
  intent, reason, gate_verdict, gate_report, result,
  provenance:[…], outcomes:[…]}], queue:[…], task_stop,
  journal_chain_verified, trail_label}`. Steps are chained by the
  §10.2 rule (a self-edit belongs iff its `intent.source_seqs`
  intersect known seqs or `task@<seq>` ∈ `intent.parents`).
- Honesty labels in code: `NO_AUTH_LABEL` (`service.py:137`) —
  "no authentication: submitter identity is asserted and journaled
  as provenance, never verified; localhost by default; no security
  claim". `GATEWAY_EXPOSURE_LABEL` (`service.py:130`) — exposure
  through a Hearth gateway is proposal H-2, not built.

### 5.2 The gate — `si/gate.py` (not `si/gaia/`)

- `Gate` (`gate.py:481`); verdicts are exactly `ADMIT | REJECT |
  ESCALATE` (`GateReport`, `gate.py:331`). Intent record
  (`Intent`, `gate.py:203`): `{kind, reason, target_modules, rank,
  alpha, parents, source_seqs, …, tool, args, ordering_key,
  depends_on, barrier}` (tool-only fields marked in source).
- Tool path `_evaluate_tool` (`gate.py:786`): five checks —
  `constitution`, `safety_bounds` (subchecks incl. `tool_registered,
  allowed_tools, args_schema, effective_class, execution_scope,
  class_policy, command_bounds, ordering_key, depends_on,
  budgets`), `identity_continuity` and `capability_floor` (recorded
  not-applicable for tools), `provenance_completeness` (parents +
  source_seqs non-empty; with a task context, source_seqs must
  include the task seq and parents must include `task@<seq>`).
  Decisive = 1/2/5; any decisive failure → REJECT. Every tool
  report carries the queue pricing block (`GateReport.queue`:
  ordering_key, depends_on, barrier, batch_seq, effective_class) —
  the fold reads it, never recomputes.
- Registry: 12 tools (`registry.py`), classes `read | write |
  execute | external`, policy via `policy.py:resolve_tool_policy`
  (fail-closed defaults; `external` escalates even under
  `external:admit` in the live path). Live execution sets are the
  B-2/B-3 miniature tool sets.
- **ESCALATE has no handler.** No escalation queue, no operator
  notification, no resume-of-escalated-intent writer. In the task
  loop ESCALATE halts proposing and stops the task `escalated`; in
  the queue fold it is a terminal state. Resolution is out-of-band
  operator judgement. §9.4 and §16 carry this honestly into the
  seam.

### 5.3 The journal — `si/journal.py`, `si/gaia/queue.py`, `drain.py`, `executor.py`

- Five collections (`journal.py:37`): `self_edits, deltas,
  checkpoints, provenance, experiences`. Every write passes
  `Journal._append` (`journal.py:288`): one global monotonic `seq`
  across all collections, `prev_hash`/`entry_hash` sha256 chain;
  `verify_chain` recomputes the whole chain. All Gaia task/queue/
  outcome entries ride inside `experiences` rows' `record`.
- Entry kinds (writer → kind): `task` (`task.py:record_task_entry:163`),
  `task_stop` (`task.py:record_task_stop:204`), `tool_result`
  (`executor.py:record_execution_outcome:160` live;
  `record_non_execution_outcome:177` non-execution; shadow variant
  `shadow.py:record_shadow_outcome:90`), `queue_batch`
  (`queue.py:126`), `queue_claim` (`queue.py:146`),
  `queue_recovery_reclaim` (`queue.py:174`), `queue_refusal`
  (`queue.py:201` — defined, **no caller** in drain/task/service),
  `queue_stop`/`queue_resume` (`queue.py:220,237`), `hearth_residency`
  (`hearth.py:593`). `queue_skip` is fold-handled with **no writer**
  in the tree. Tool intents + verdicts are `self_edits` rows via
  `record_self_edit` (`journal.py:325`, `edit_id` = `se_NNNN`).
- Journal-before-execution, three layers: (1) admission
  (`drain.py:admit_tool_intent:378`) — gate → journal self-edit →
  provenance edges → ADMIT rests `queued`; (2) claim-before-execute
  (`Drainer.claim:589` → `record_queue_claim`, lease default 300 s)
  before `execute_claim`; (3) executor ticket
  (`executor.py:execute_journaled:230`) — intent re-read **from the
  journal row**, re-priced; refusal (journaled `tool_result`,
  status `refused`) on claim/intent mismatch, verdict ≠ ADMIT,
  args failing re-validation, effective class changed between GATE
  and EXECUTE (TOCTOU), or path outside work root.
- Fold discipline (`queue.py:fold_queue:511`): state is derived
  from the journal, never stored; any prefix folds to state as of
  that prefix. `outcome-unknown` is **fold-derived only**, scoped
  to queue-managed intents: an outcome with no prior claim, or a
  second outcome for one intent, marks `outcome-unknown` + an
  integrity fault. It is in `TERMINAL_STATES` (`queue.py:91`) —
  "the fold itself never moves an intent out of it" — and **no
  disposition writer/API exists in this tree** (§11.4, §16).
- Recovery (`Drainer.recover`, `drain.py:652`): claimed + no
  outcome → read-class may be re-claimed (journaled, naming the
  original claim); write/execute are `deferred`, left claimed,
  never blindly re-run. Foreign claims are left alone. Q-1
  concurrency is fixed at 1 (`Q1_CONCURRENCY`, `drain.py:89`).
- Identity is bundle-local and seq-based: `task_seq`, `intent_seq`,
  claim seq, `edit_id`. **No globally unique or bundle-qualified
  IDs, no idempotency keys, no request IDs** exist on any SI
  surface. The seam's dedup keys (§8–§11) are built from the pairs
  in §3.5, not from new ID machinery.

### 5.4 The leash, SI side — `queue.py`, `drain.py`

- The leash proper (SI-50 §11) is a **per-task journal fold**:
  `record_queue_stop` (`queue.py:220`) journals `{kind:"queue_stop",
  source:"operator", operator, task_seq, cause, detail}`;
  `record_queue_resume` (`queue.py:237`) the matching resume.
  `fold_queue` derives per-task `{stop_in_force, stop_seq,
  resume_seq}`; still-queued intents of a stopped task derive to
  `held`; claimed/terminal are untouched. "A restart never clears
  a stop — the fold is all there is."
- Observation: `drain.py:_task_stop_in_force:214`;
  `Drainer.claimable:542` refuses under stop; `drain_task` breaks
  with `sync_reason="stop-in-force"`; `run_task` maps it to
  `stop_reason="operator-stop"`.
- **No HTTP or CLI caller exists for either writer** in this tree —
  the leash is a journal-entry + fold mechanism with no shipped
  operator surface. Both sides of the seam therefore lack leash
  ingress today; §10 designs both halves. (Task stop rules §10.4 —
  `budget-exhausted`, `escalated`, … — are task lifecycle, not the
  leash; do not conflate them.)

### 5.5 SSE — `si/gaia/events.py`, `events_client.py` (SI-47/48/49)

- One journal row = one event; **event `id` IS the journal seq**.
  Wire: `id: <seq>\n event: <name>\n data: {"collection","row"}`
  (`format_sse`, `events.py:464`). Names derive from the row
  (`event_name_for_row`, `events.py:418`): `self_edit.<kind>`,
  `experience.<k>`, `task_open`, `tool_result`, `checkpoint`, …
- Resume: `Last-Event-ID` header beats `?after=`; absent ⇒ 0;
  invalid ⇒ `ValueError` → 400, never silently 0
  (`parse_resume_seq`, `events.py:489`). Replay is exactly
  `seq > resume_seq`, ascending. The tail is poll-the-journal at
  0.25 s (`stream_events`, `events.py:519`) — labelled, no push
  channel. Filters (SI-48) are a projection: ids are never
  renumbered; gaps are filtered-out seqs.
- Reference follower (SI-49, `events_client.py:follow_events:410`):
  reconnects with resume = last seq **actually delivered**; any
  `id <= last delivered` raises `FollowError` immediately —
  duplicate/out-of-order is a loud protocol failure, **never
  silently deduped**; only transport errors are retried (bounded
  deterministic backoff); HTTP 4xx fails closed.
  `verify_events_against_journal` (`events_client.py:243`) checks a
  received window row-for-row against the journal. §7 principle 7
  generalises this discipline to every seam consumer.

### 5.6 Hearth interface — `si/gaia/hearth.py` (B-4 / SI-45)

- Three contact points: a bundle **declaration** record
  (`build_declaration`, `hearth.py:256` — built, validated, and
  **never sent**; the fleet side is proposal H-1, not built);
  **residency as perception** (`HearthResidencyAdapter.poll:525` —
  failure IS the observation, journaled as `hearth_residency`);
  and a gateway client (`list_models`, `chat_completion` —
  inference leg runs only when the caller asserts a real base is
  Resident).
- Evidence is replay/stub only: `HEARTH_PATH_UNMEASURED`
  (`hearth.py:104`) — "no real Hearth node was reachable in this
  environment". The real-Hearth path stays UNMEASURED after this
  design; no seam slice claims otherwise. The gate never calls
  Hearth, and this module creates no intents (§12.4 discipline) —
  the seam keeps that separation: the fork is reached through the
  seam client (§8–§11), not through `hearth.py`.

---

## 6. The seam today: boundary crossings and the three missing ones

Fork-side seam points (full table: §18, evidence A §6). The ones
the design builds on:

| # | Crossing | Mechanism today | Status |
|---|---|---|---|
| S1 | SI → fork, work submission | HTTP `POST /gaia/v1/work` → `gateway::decide` | exists; no dedup (§4.1) |
| S2 | SI → fork, verdict | HTTP `POST /gaia/v1/work/{id}/verdict` | exists; exactly-once by state machine |
| S3 | SI ← fork, work status/result | HTTP `GET /gaia/v1/work/{id}` | exists; dies with the process |
| S4 | SI ← fork, residency | HTTP `GET /gaia/v1/residency` | exists; perception only |
| S5 | consumer ← fork, events | SSE, own listener/budget | exists; `source=si` empty (S6 below) |
| S6 | SI → fork, SI journal events | in-process `SiEventBuffer::push` only | **no transport** → S-6 |
| S9 | gate decision → actuation | in-process ticker drain → `Composition::actuate_work` | exists in-process |
| S10 | leash → actuator | in-process read at actuation time | exists in-process |
| S11 | leash population | — | **no crossing exists** → §10 |
| S12 | proposal → SI / work store | — | **no crossing exists** → §8 |
| S14 | actuator → outcome record | in-process + `outcomes.jsonl` | exists; no external reader → §11 |

SI-side facts that shape the design (§5): the gate and journal are
in-process Python; the only HTTP surface is the synchronous B-5
service; the leash has no operator surface; outcomes are written
only by the in-process executor; identity is bundle-local seqs.

**The three missing crossings are exactly the three flows.** Each
is missing for a different reason, which is why they get different
transports:

- Proposals have no egress because the watcher was built to
  journal-and-stop: `proposals.jsonl` is a terminus (§4.2).
- Verdicts have a route but no *client discipline* and no
  submission idempotency: the route is exactly-once, the submit
  path is not (§4.1).
- The leash has no ingress on either side (§4.3, §5.4).
- Outcomes have no reader: the durable record (`outcomes.jsonl`)
  exists but nothing outside the fork process consumes it (§4.4).

## 7. Design principles

1. **Journal first, ACK second.** A side acknowledges a seam
   message only after the record it implies is durable on its own
   side: SI gates and journals a proposal *before* its ACK (§8.3);
   the fork records an outcome *before* completing the work record
   (already true, `actuate.rs:195`); SI journals an outcome
   *before* advancing its consumption cursor (§11.3). An
   ACK-then-journal crash window manufactures phantom state on the
   other side; there are none in this design.
2. **The sender retries; the receiver dedups.** Every seam send is
   retried freely on transport failure, because every receive is
   idempotent under a named key: proposals by `caused_by` (§8.4),
   submissions by `journal_seq` (§9.3 — new fork behaviour, S-1),
   verdicts by the state machine itself (§9.5), leash records by
   `(model, journal_seq)` watermark (§10.3), outcomes by `work_id`
   (§11.2). Where today's code lacks the dedup, the slice that
   needs it builds it — the gap is named, never assumed away.
3. **One writer per store.** The fork owns `proposals.jsonl`,
   `outcomes.jsonl`, `leash.jsonl`, the work store, the spine. SI
   owns its journal. Cross-seam writes go through the owner's API
   (HTTP route or the owner's own process), never by appending to
   another owner's file: an external append is invisible to the
   running owner's in-memory state, risks interleaved lines (no
   file locks exist in any store), and corrupts dedup memory.
   Read-only tailing of a sender-owned file by the receiver is
   legitimate (§11.3) — reading is not writing.
4. **Fail closed, loudly.** Malformed cursors are 400s, duplicate
   SSE seqs are errors, integrity mismatches freeze drains (SI
   as-built). The seam adds no silent path: every refusal is a
   recorded record (an outcome, a journal fault, a load failure
   line), never a dropped message.
5. **No shared database.** NEDB is single-process on both sides
   (§4.6, §5.3). Neither side opens the other's store. The mirror
   pattern — a derived, rebuildable, read-only projection — is the
   only sanctioned way to read across, and control flows never ride
   a projection.
6. **Control flows ride request/response; streams carry
   observations.** Verdicts, leash records, and submissions are
   HTTP request/response with explicit ACKs, because each needs a
   durable, disambiguable answer. SSE carries observations
   (residency, journal events) where loss is detectable and
   replayable. Rejected alternative, recorded once here: deriving
   proposals or verdicts from the other side's SSE stream requires
   re-implementing the watcher's mapping or mining the whole
   journal for one field, couples control to a poll-cadence tail,
   and — for outcomes — silently drops refusals, which emit no
   spine event (§18, evidence C §B). Streams are for seeing;
   request/response is for deciding.
7. **Cursor after journal, never before** (the SI-49 rule,
   generalised): a consumer advances its durable cursor only after
   the consumed record is journaled/recorded on its own side.
   Cursor-before-journal is exactly how lost verdicts and lost
   outcomes are manufactured.
8. **Smallest new surface.** New routes: one SI ingest endpoint
   (§8), one fork leash route + heartbeat (§10), one fork events
   ingest (§15 S-6). Everything else reuses as-built routes,
   stores, and record shapes. Where a shape must grow a field, the
   field is added to the existing record, not wrapped in a new
   envelope.

## 8. Flow 1: proposals, fork to SI

### 8.1 Shape of the flow

```
watcher ──record──▶ proposals.jsonl ──▶ seam egress (fork)
                                            │  POST /gaia/proposals
                                            ▼
                              SI ingest: validate → seam task →
                              gate → journal self-edit (verdict)
                                            │  200 {task_seq, journal_seq, verdict}
                                            ▼
                              fork marks proposal answered(caused_by)
                              + records the ACK pair locally
```

### 8.2 Message

Fork → SI, `POST /gaia/proposals` (new SI route on the B-5
service, §15 S-2). Body (§12.1): the proposal verbatim plus its
cause:

```json
{"model":"…","tool":"hearth.restart","args":{…},"reason":"…",
 "caused_by":{"hash":"…","seq":123}}
```

`caused_by` is the dedup key and the provenance root. The pair is
the key — neither half alone (§4.2: `answered` already keys on
both). `args.spine_event{hash,seq}` duplicates `caused_by` in
watcher proposals by construction; the ingest checks they agree
and refuses (400) if they do not — a disagreement is a defect, not
a variant.

### 8.3 SI ingest semantics

1. Validate shape; refuse malformed with 400, journaling nothing
   (the B-5 service's existing discipline for refused submissions).
2. **Dedup check**: scan the journal for a prior seam self-edit
   citing this `caused_by` (the ingest writes `caused_by` into the
   intent's journaled record — §12.1 note). Found → return the
   original ACK unchanged (same `task_seq`, `journal_seq`,
   `verdict`). This makes fork retries free (§7.2).
3. Attach to the model's **seam task** (§10.2): create the task
   entry on first proposal for the model, reuse it after. The
   intent's `parents`/`source_seqs` cite the seam task, satisfying
   the gate's provenance check (`gate.py:1078`) by construction.
4. Build the `Intent` (`kind="tool"`, tool/args/reason from the
   proposal, `caused_by` recorded in the journaled record) and run
   the gate. **Journal the self-edit with its full gate report —
   for every verdict, including REJECT and ESCALATE.** This is the
   journal-before-execution anchor for the whole seam: after this
   step, anything the fork does traces to a journaled ADMIT.
5. ACK **after** journaling (§7.1):
   `200 {"task_seq":N,"journal_seq":M,"verdict":"ADMIT"|"REJECT"|"ESCALATE"}`
   where `journal_seq` is the self-edit row's seq — the intent's
   identity for every later flow.

Transport failure at any point: the fork retries the POST (§8.5);
SI's dedup makes the retry safe. A 4xx is a terminal answer
(shape defect), surfaced on the fork as a recorded egress failure
— never retried blindly, never dropped silently (§7.4).

### 8.4 What the fork records

On ACK, the fork records the pair `(caused_by → journal_seq,
verdict)` in a sender-side egress log (new JSONL store following
the existing stores' append+flush discipline; §15 S-2). Purposes:
restart-safe retry state (an un-ACKed proposal is re-sent; an
ACKed one is not), and the join key for §11 reconciliation. A
proposal whose verdict was REJECT is answered — the watcher's
`answered(caused_by)` already suppresses re-proposal of the same
loss; the egress log adds the *disposition* (rejected at SI), so
operators can distinguish "gated and refused" from "never sent".

### 8.5 Ordering and timing claims

Claimed: per-`caused_by` exactly-once gating (dedup §8.3.2);
proposals for distinct losses are independent and may be gated in
any order. NOT claimed: FIFO across proposals; any latency bound
(egress cadence is a policy constant of the fork's ticker class —
UNMEASURED); that a proposal is ever sent while the fork's watcher
is disabled for that model (scope is the watcher's, unchanged).

### 8.6 Rejected alternatives

- **SI tails `proposals.jsonl`** (§18 C §C.3): legitimate transport
  on one host, zero fork changes — rejected as the primary because
  it has no ACK: the fork cannot distinguish "SI gated it" from
  "SI is down", so answered-marking and retry state smear across
  both sides. It remains the documented fallback if the ingest
  endpoint is unavailable, and the egress log (§8.4) keeps the two
  reconcilable.
- **SSE `source=hearth` + re-derive proposals in SI**: rejected
  (§7.6) — duplicates `watcher.rs:candidate` in Python and loses
  `args`/`reason` fidelity.
- **Fork submits proposals directly as work** (`POST /gaia/v1/work`
  from the watcher): rejected outright — it bypasses the gate,
  breaking invariant 1. The work endpoint's required `journal_seq`
  is precisely the proof-of-journal the watcher does not have.

## 9. Flow 2: verdicts and work submission, SI to fork

### 9.1 Shape of the flow

The verdict is decided at ingest (§8.3.4), before any fork work
exists. Flow 2 is therefore mostly **submission with the verdict
attached**, using as-built routes unchanged:

```
SI (journal_seq, verdict=ADMIT) ──POST /gaia/v1/work──▶ fork
                                     202 {work_id, …}
SI journals (journal_seq ↔ work_id)          │
fork ticker: claim → leash → stale → primitive → outcome (§4.4)
```

### 9.2 Submission

Body = the as-built intent shape (§12.2), with seam values:

- `task_id` := `"task@<seam task_seq>"` (§3.5).
- `journal_seq` := the self-edit seq from the §8.3 ACK. Required
  and un-inventable by design (`gateway.rs:170`) — the fork
  receives proof of journaling with the intent itself.
- `args` := the proposal's args **verbatim** (invariant 3). SI
  does not edit watcher args; if SI's gate priced a different
  intent, that intent is a different proposal's business.
- `verdict` := `"admit"` for ADMIT. REJECT and ESCALATE intents
  are **not submitted** (§9.4) — a rejected work record on the
  fork would be a tombstone with no reader.
- `class`: the watcher's tools are fork-side runtime operations;
  SI's registry classes (`read/write/execute/external`) describe
  SI's 12 tools, not `hearth.*`. The seam submits the as-built
  default (`"execute"`) and records the mapping as a known
  vocabulary seam (§3.2 note): class-based fork policy is not
  claimed for `hearth.*` tools in v1.

### 9.3 The submission dedup (new fork behaviour, §15 S-1)

As-built, a retried POST mints a second `work_id` for the same
`journal_seq`, and the ticker would try to actuate both —
restart's stale check saves it *by accident*; `hearth.notify` and
`hearth.repull` have no such check (repull explicitly none,
`actuate.rs:477`). This is the seam's biggest idempotency hole,
and it is closed fork-side, before any seam slice depends on it:

- `WorkStore` gains a `journal_seq → work_id` index. `submit`
  with an already-recorded `journal_seq` returns the **existing**
  record (202 with the original `work_id`), recording nothing new.
- The index lives in the store impl, not the gateway: `decide`
  stays a pure adapter (§4.1), and every store consumer inherits
  the behaviour.
- Scope honesty: the index is in-memory like the store. A fork
  restart still loses work records (§13.2 prices this; the
  reconciler in §11.3 is the recovery path, and work-store
  persistence remains G-4b, §16).

SI records `(journal_seq ↔ work_id, task_seq)` in its journal on
submission (new experience kind `gaia_work`, §12.5) — after the
202 returns, before any outcome can arrive (§7.1 applied to SI's
own bookkeeping).

### 9.4 REJECT and ESCALATE dispositions

- REJECT: terminal at ingest. The egress log (§8.4) carries the
  disposition; nothing crosses further. SI's journal is the record.
- ESCALATE: terminal at ingest **for v1**. No fork work is
  submitted; no operator surface exists on either side to resolve
  an escalation (§5.2). The design does not invent one: §16 names
  escalation resolution UNBUILT. What the seam guarantees is that
  an escalated proposal is durably journaled, ACKed as
  `ESCALATE`, and distinguishable from a lost one (§8.4) — the
  precondition any future resolution path needs.
- Deferred-decision submissions (submit with no verdict →
  `AwaitingVerdict`, decide later via `POST /work/{id}/verdict`)
  remain available as-built for SI-internal use, but the seam's
  watcher flow does not use them: the gate has already decided
  before submission, and submitting undecided work would park
  fork state behind a decision SI has no mechanism to revisit.

### 9.5 Verdict-retry protocol (for the as-built verdict route)

Where the verdict route *is* used (deferred decisions, §9.4), its
retry semantics are already idempotent; the client discipline is:

1. POST the verdict. 200 → delivered.
2. Transport failure or timeout → the verdict may or may not have
   landed. Re-POST the same verdict: 200 = it hadn't; 409
   `gaia_verdict_conflict` = it had (or the record moved on).
3. Disambiguate by `GET /work/{id}`: state `Admitted`/`Rejected`
   matching the sent verdict = delivered, treat the 409 as
   success. Still `AwaitingVerdict` after a 409 = contradiction —
   surface as an SI-side defect, do not retry it away.
4. A 409 where the landed verdict is the *opposite* value means
   two deciders raced one intent — first verdict wins (fork
   as-built), and SI must journal a fault. The seam has one
   decider (the gate) by construction, so this is a bug alarm,
   not an expected case.

### 9.6 Ordering claims

Claimed: for one intent, journal (§8) precedes submission (§9)
precedes actuation precedes outcome (§11) — enforced by the
ACK/record dependencies, not by clocks. NOT claimed: ordering
across intents (the fork drains serially per its ticker, SI gates
per arrival; two proposals for the same model may actuate in
either order subject to the fork's stale check, which is the
designed guard), and no latency bound anywhere (UNMEASURED).

---

## 10. The leash crossing

### 10.1 Where the leash lives

**The leash lives in SI's journal.** The operator's stop/resume
are journal acts (`record_queue_stop`/`record_queue_resume`,
`queue.py:220,237`); the fold is the state; "a restart never
clears a stop — the fold is all there is". The fork's `Leash`
store is a **derived replica** of the SI state for the models the
fork serves — a cache with exactly one writer path (this flow),
rebuildable from SI at any time, never authoritative. If the two
disagree, SI's journal wins and the replica is re-synced (§10.5);
the replica's only job is to be *fresh enough at actuation time*
(§10.3) on the fork's side of the wire.

### 10.2 The scope mapping: one seam task per model

SI's leash is per-task; the fork's is per-model (§3.3). The seam
removes the mismatch by construction: SI's ingest (§8.3.3)
maintains **one seam task per model**, created on that model's
first proposal. A `queue_stop` on a model's seam task *is* the
model's stop; there is never a second seam task for the same
model to disagree. Non-seam SI tasks keep their own leashes, which
never cross (they govern SI-side work, not fork actuation).

### 10.3 How state crosses; staleness defined

Carrier: HTTP on the verdict channel (new fork routes, §15 S-4).

1. **Event records.** When the fold for a seam task changes
   stop-state, SI POSTs the record to the fork:
   `POST /gaia/v1/leash` (§12.3):
   `{"model","kind":"stop"|"resume","journal_seq","reason"}` —
   field-for-field the fork's `LeashRecord` (`leash.rs:66`) and
   SI's journal record. The fork applies it through the existing
   `record_stop`/`record_resume` (`leash.rs:185,201`) — the first
   production callers those functions will have.
2. **Watermark.** The fork keeps `last_leash_seq[model]` = the
   highest leash `journal_seq` applied for that model. A record
   with `journal_seq <= last_leash_seq[model]` is a duplicate or
   out-of-order arrival: ACKed (200, so senders stop retrying) but
   **not applied**. This kills the one dangerous case: a delayed
   duplicate `resume` landing after a newer `stop` would, under
   latest-arrival-wins, silently lift the stop. Under the
   watermark, latest-*journaled* wins, arrival order is
   irrelevant.
3. **Watermark piggyback.** Every Flow-1 ACK and every Flow-2
   message context carries SI's current leash tip for the model
   (the `journal_seq` of its latest leash record, or the seam
   task's creation seq if none). If the fork's
   `last_leash_seq[model]` is *behind* a tip it has been told
   about, the fork knows records are missing — it does not know
   their content, and §10.3.4 applies until the feed catches up.
4. **Heartbeat and staleness.** While the seam is active for a
   model, SI sends `POST /gaia/v1/leash/heartbeat` (§12.3)
   carrying its leash tip, at a policy interval (starting value
   10 s — UNMEASURED policy constant, priced in §16). The fork
   stamps each heartbeat with its **local monotonic** receipt
   time. **Leash state for a model is STALE, and actuation for
   that model must refuse, when any of:**
   - no heartbeat has been received within the TTL (starting
     value 30 s, UNMEASURED policy constant), or none ever; or
   - `last_leash_seq[model]` is behind a piggybacked tip (§10.3.3)
     by any amount; or
   - the leash feed has reported a transport fault it has not
     since recovered from.
   Refusals are recorded with the existing outcome shape:
   `outcome:"refused", refusal:"leash"`, detail naming the
   staleness cause and the watermark (§12.4). Staleness refusal is
   per-model, matching the leash's own scope; other models with
   fresh state actuate normally.
5. **Exemption, inherited:** `hearth.notify` actuates nothing and
   takes no leash check as-built (`actuate.rs:311`). The seam
   keeps this: a notify may complete under stale leash state,
   because refusing it would record a refusal *about* an operator
   notification *to* the operator — the outcome record itself is
   the notification channel. Restart/stop/repull refuse.

### 10.4 What never happens

- **No stop expiry.** A stop does not lapse on a clock (rejected
  in analysis, §18 C §E.4): fail-open expiry silently un-stops a
  model whose stop may still be intended — the exact failure the
  leash exists to prevent. Heartbeat TTL gates *actuation
  freshness*, never the stop's validity: a stale feed refuses
  everything; it never resumes anything.
- **No fork-originated leash records.** The fork applies records;
  it does not mint them. An operator at the fork host edits
  nothing; stops are SI journal acts or they are not stops.
- **No actuation on unknown state.** A model the fork has never
  received leash state for is treated as stale (§10.3.4, "none
  ever") once the seam is active for it. Before S-4, the as-built
  behaviour (empty leash, actuation proceeds) is unchanged — the
  fail-closed rule arrives with the feed that makes it
  satisfiable, not before.

### 10.5 Re-sync

On fork start, and whenever §10.3.4 fires persistently, SI
re-sends the **current folded state** per seam model as a single
snapshot record: the latest stop/resume per model with its
`journal_seq`, applied under the same watermark rule (a snapshot
older than applied state is ignored; a newer one replaces). The
snapshot is a convenience for recovery, not a second state
machine: it is derived from the same fold, and its records are
indistinguishable in the fork store from event records.

### 10.6 Rejected alternatives

- **Fork follows SI's SSE `queue_stop`/`queue_resume` projection**
  (best ordering story — the follower's last-delivered seq *is*
  the watermark, disconnect *is* the staleness signal): deferred,
  not rejected forever. It requires a Rust SSE follower (the
  largest single build in this design) to move two small record
  types. If S-6's ingest work later justifies a general fork-side
  SI client, revisiting this is §16's first item.
- **Pull-at-claim** (fork queries SI synchronously before each
  actuation): strongest freshness, worst coupling — actuation
  latency plus a hard SI-up dependency at the worst moment, and a
  query path that can disagree with the pushed replica. Rejected.
- **SI writes `leash.jsonl` directly**: rejected by §7.3 (invisible
  to the running fork; corrupts the store's single-writer
  contract).

## 11. Flow 3: outcomes, fork to SI, as provenance

### 11.1 Shape of the flow

```
fork actuation ──▶ OutcomeStore.record (before return, as-built)
               └─▶ work record complete(result) ──▶ Executed
SI poller: GET /gaia/v1/work/{id} ──▶ terminal? ──▶ journal outcome
                                                     (keyed by work_id)
SI reconciler: tail outcomes.jsonl (read-only) ──▶ cross-check,
                                                     recover post-restart
```

### 11.2 The primary path: SI pulls status

SI knows every `(journal_seq ↔ work_id)` pair it submitted (§9.3).
For each open pair, SI polls `GET /gaia/v1/work/{id}` until the
record is `Executed` (poll interval is a policy constant —
UNMEASURED; the as-built result is worth waiting for because it is
the *same JSON value* the actuator stored in the `OutcomeRecord`,
`actuate.rs:195`). On terminal state, SI journals the outcome
**once, keyed by `work_id`**:

- as an `experiences` row of kind `tool_result` whose record is
  the SI-side outcome envelope (§12.5): the fork result verbatim
  (`outcome, tool, model, detail, caused_by, spine_event_refs,
  refusal?`) plus the seam pair `(work_id, journal_seq)`, with
  the row's `self_edit_id` set to the intent's edit — the exact
  key `build_provenance_trail` already collects outcomes by
  (§5.1), so seam outcomes appear in provenance trails without
  trail changes;
- and the queue fold learns it as the intent's outcome through
  the same `_apply_outcome` path SI's own executor outcomes take
  (claim discipline §11.4 governs the fold's reaction).

A second poll finding the same `work_id` already journaled is a
no-op (§7.2). A *distinct second outcome* for one `work_id`
(fork-side defect — `for_work` returns a list precisely to expose
it, `outcomes.rs:284`) is journaled as an integrity fault, never
silently picked between.

Known gap, carried openly: the status body's
`provenance.spine_event_refs` is always `[]` (§4.1), but the
*result* JSON carries the real `spine_event_refs` — so the pull
path loses nothing today. Populating the provenance block from
the outcome store is folded into S-1 as a small honesty fix
alongside dedup.

### 11.3 The reconciler: tailing `outcomes.jsonl`

The pull path dies with the fork's memory (§4.4). The durable
record is `outcomes.jsonl` — sender-owned, append+flush, torn
lines named not fatal (§4.4). SI tails it **read-only** (§7.3):

- Byte-offset cursor per file, advanced **only after** the
  records it covers are journaled (§7.7). A torn final line is
  skipped and counted, retried next poll — the stores' own
  tolerance, mirrored.
- Every tailed record is matched by `work_id` against SI's
  journal: already-journaled → cross-check the result values
  agree (disagreement = integrity fault, journaled); not
  journaled → journal it (this is the post-restart recovery path:
  the fork lost the work record, SI lost nothing it cannot
  rebuild, because the outcome file survived).
- The reconciler is **mandatory, not a garnish**: it is the only
  path that survives a fork restart, and the evidence source for
  §11.4. S-5 ships pull + reconciler together or not at all.

### 11.4 Outcome-unknown across the seam

The dangerous case: the fork actuated (or may have), and SI has
no outcome. Three indistinguishable causes (SI-50 §13.2): died
before, during, or after the primitive, before the outcome was
recorded/reported. Discipline, applied exactly as SI-50
specifies for its own queue:

- **Never re-execute, never resubmit the same intent.** A
  resubmission of the same `caused_by` is blocked while an
  outcome-unknown citing it is undisposed (§8's dedup makes the
  block natural: the proposal is already answered; re-proposal
  requires a *fresh* intent through operator disposition).
- On ambiguity (e.g. work record stuck `Admitted` past a bound,
  fork restarted with no matching outcome in `outcomes.jsonl`,
  or an outcome arrived with no prior seam claim), SI journals
  the intent `outcome-unknown` with the explicit pair
  `(work_id, journal_seq, caused_by)` and the evidence consulted.
  Reconciliation sources, in order: `GET /work/{id}`;
  `outcomes.jsonl` tail; spine refs via gaia-py `Mirror`
  (after-the-fact, sequential — §4.6's lock discipline).
- Dispositions are SI-50 §13.4's, unchanged: `accept-unknown`,
  `compensate`, `re-propose` (a fresh intent, new journal entry —
  never a replay). As-built SI has **no disposition writer**
  (§5.3); S-5 includes the minimal seam-scoped disposition
  journaling (operator-called, in-process) or explicitly leaves
  dispositions manual — the slice brief must say which (§15 S-5
  acceptance pins it).
- Read-class note: none of the current `hearth.*` tools are
  read-class in SI's sense (restart/stop/repull mutate; notify is
  effect-free but already outcome-recorded by construction), so
  SI-50's read-class re-claim shortcut has no seam application in
  v1. Stated to prevent a future reader importing it by analogy.

### 11.5 Ordering claims

Claimed: at most one journaled outcome per `work_id` (§11.2);
every fork outcome is eventually journaled or faulted (§11.3);
outcome journaling never precedes the fork's own outcome record
(it is derived from it). NOT claimed: outcome latency (poll-based,
UNMEASURED); cross-work ordering (outcomes journal in discovery
order; the journal seq SI assigns at ingest time is SI-side
bookkeeping, and the fork's `caused_by`/spine refs — not arrival
order — carry the causal story).

## 12. Consolidated message shapes

Fields marked † are new in this design; everything else is
as-built (§4/§5). JSON field order is not significant anywhere.

### 12.1 Proposal ingest — `POST /gaia/proposals` (new, SI side)

Request: `{"model", "tool", "args", "reason",
"caused_by":{"hash","seq"}}` — the `Proposal` record verbatim
(`proposals.rs:60`). The SI ingest additionally records
`caused_by`† inside the journaled intent record (the dedup index
of §8.3.2 — a journal field, not a wire change after first
delivery).
Response 200: `{"task_seq"†, "journal_seq", "verdict"}` (verdict
spelled SI-style: `ADMIT|REJECT|ESCALATE`). Errors: 400 malformed
(nothing journaled); 5xx/transport = retry (§8.3).

### 12.2 Work submission — `POST /gaia/v1/work` (as-built)

`{"type":"tool", "tool", "args", "reason", "task_id",
"journal_seq", "class"?, "verdict"?}` with seam values per §9.2.
Response 202: `{"work_id", "state":"accepted", "journal_seq"}` —
and, after S-1, the same body for a deduplicated retry, naming
the **original** `work_id`.

### 12.3 Leash — `POST /gaia/v1/leash`, `POST /gaia/v1/leash/heartbeat` (new, fork side)

Record: `{"model", "kind":"stop"|"resume", "journal_seq",
"reason"}` — `LeashRecord` verbatim (`leash.rs:75`).
Heartbeat: `{"model", "leash_tip"† (journal_seq), "journal_tip"†
(journal seq of SI's latest leash-relevant entry, or its journal
tip)}`. Responses: 200 `{"applied":bool, "last_leash_seq"}` —
`applied:false` is the watermark no-op ACK (§10.3.2), not an
error. Snapshot (§10.5) is the record shape sent once per model;
no separate snapshot type.

### 12.4 Outcome refusal under staleness (as-built shape)

`{"outcome":"refused","tool","model","detail":"leash state stale:
<cause>; watermark <n>","caused_by":{…},"spine_event_refs":[],
"refusal":"leash"}` — the `finish` result shape (`actuate.rs:195`)
with a staleness detail string; no new fields.

### 12.5 SI journal records (new kinds, SI side)

- `gaia_work`† (experiences): `{kind:"gaia_work",
  source:"gaia-seam", task_seq, journal_seq (the intent's),
  work_id, caused_by:{hash,seq}}` — journaled on submission
  (§9.3).
- Seam outcome: kind `tool_result` (existing), record
  `{kind:"tool_result", source:"gaia-seam", intent_id:edit_id,
  work_id†, journal_seq†, gate_verdict:"ADMIT",
  execution:{…fork result verbatim…}}`, row `self_edit_id` =
  the intent's edit (§11.2).
- Egress log (fork side, new JSONL store): `{caused_by:{hash,seq},
  model, tool, journal_seq, verdict, acked_at_local_seq}`† —
  sender bookkeeping (§8.4), following the existing stores'
  append+flush + `load_failures` discipline.

---

## 13. Failure modes and dispositions

### 13.1 Matrix

| Failure | Flow 1 (proposals) | Flow 2 (verdicts/work) | Flow 3 (outcomes) |
|---|---|---|---|
| Fork down | Proposals accumulate in `proposals.jsonl` (durable); egress retries on recovery; `caused_by` dedup makes replay free. Nothing was gated meanwhile — the watcher keeps journaling losses locally | SI's POST fails (transport): the intent stays journaled at SI, unsubmitted; retry later. If the fork **restarted**, its work store is empty — see §13.2 | Poll fails (transport): retry later. Durable fallback is the `outcomes.jsonl` tail (§11.3) |
| SI down | Fork POST fails: proposal stays un-ACKed in the egress log's pending set; retry with backoff. The fork must not actuate — it cannot: no ADMIT exists, and `claim_for_actuation` refuses verdict-less work by construction | Fork holds any `AwaitingVerdict` records indefinitely (as-built: a verdict-less intent "waits… however long that takes"). Safe, but in-memory growth is unpriced (§16) | Outcomes sit in the fork's `OutcomeStore` + completed work records; SI ingests on recovery, dedup by `work_id`. True loss requires fork disk loss |
| Verdict lost in transit | n/a (ACK loss = fork retries the proposal POST; SI dedup returns the original ACK — §8.3.2) | The §9.5 protocol: re-POST → 409-or-200, disambiguate by GET. Submission loss: re-POST → S-1 dedup returns the original `work_id` | n/a |
| Duplicate verdict | n/a | Second delivery → 409, first verdict wins, never a second transition, never double actuation (state machine + `complete` terminal). Opposite-value duplicate → journaled fault at SI (§9.5.4) | n/a |
| Duplicate outcome delivery | n/a | n/a | SI no-ops on journaled `work_id`; a distinct second outcome for one `work_id` → integrity fault journaled, never silently resolved (§11.2) |
| Actuation executed, outcome lost | n/a | n/a | §11.4 outcome-unknown procedure. Never re-execute, never resubmit |
| Leash feed silent | n/a | §10.3.4: staleness refuses restart/stop/repull per model once TTL lapses; notify exempt | Refusals are outcomes and flow back normally — SI sees the refusal and its staleness detail |
| One side's clock wrong | Seam never compares wall clocks across the wire. Staleness uses fork-local monotonic receipt times (§10.3.4); ordering uses seqs, not timestamps | same | same |

### 13.2 Fork restart semantics (priced, not hidden)

A fork restart loses: all work records (verdicts, results), the
submission-dedup index, hub state. It keeps: spine, mirror,
proposals, outcomes, leash (all file-backed). Seam behaviour:

- SI learns of the restart when a poll/verdict returns 404 or a
  transport reset. For an intent SI believes is `Admitted` but
  the fork no longer holds: SI does **not** blindly resubmit —
  whether the original actuated is exactly the §11.4 question.
  Reconciliation order: `outcomes.jsonl` tail (an outcome ⇒ it
  actuated; journal it), spine refs via the mirror (a `loading`
  after the loss ⇒ it actuated), else outcome-unknown procedure.
- Proposals un-ACKed at restart are re-sent from the egress log;
  ACKed ones are not (§8.4) — SI's journal persists independently.
- Leash replica is re-synced by snapshot (§10.5); until the first
  heartbeat after restart, the model is stale by §10.3.4 ("none
  ever" applies per boot) and refuses. This is deliberate: a
  restarted fork actuates nothing until SI has spoken.

### 13.3 The fork's own honest window

`FleetActuator::finish` records the outcome before returning; if
that record fails after the primitive ran, the error propagates
and the work stays `Admitted` (§4.4). Across the seam this
presents as: work stuck `Admitted`, no outcome anywhere. SI's
disposition is §11.4 with the fork's stderr/outcome-store state
as evidence — the window is operator-visible by design on both
sides, and the seam adds no code that could paper over it (in
particular, the poller never "helpfully" completes a work record
by inference from spine state; spine evidence feeds the
outcome-unknown *procedure*, not a fabricated outcome).

## 14. Security posture

Stated plainly, because both codebases state it plainly:

- **There is no authentication on either side of the seam in
  v1.** SI's service carries `NO_AUTH_LABEL` in code: submitter
  identity is "asserted and journaled as provenance, never
  verified" (`service.py:137`). The fork's listeners have no auth
  at all (`sse.rs`, `main.rs` module docs). The seam inherits
  this and adds none.
- **The seam is same-host, loopback-only.** Both listeners bind
  `127.0.0.1` as-built; every new route in §15 is bound the same
  way. On one host, "can reach the verdict route" already implies
  "can run code as a local user", so the v1 threat model is:
  trusted local processes, untrusted *content* (§2.7 — records are
  data, gated or folded, never executed as instructions; args are
  validated against schemas at the gate and re-validated at
  execution, both as-built).
- **What this does NOT cover, explicitly UNBUILT:** any
  non-loopback deployment; TLS; shared secrets or tokens;
  per-submitter authorisation (e.g. "only the watcher may
  propose restarts"); rate limiting beyond the as-built body cap
  (SI, 1 MiB) and SSE connection budget (fork). The moment the
  seam crosses a host boundary, an auth design is a prerequisite
  slice, not an enhancement. Neither codebase's docs may claim
  otherwise, and this document does not.
- Provenance is the v1 integrity story: every seam record carries
  its causal pair (§3.5), SI's journal is hash-chained
  (`journal.py:288`), the fork's outcomes cite spine hashes. A
  forged local record is detectable after the fact by chain
  verification and pair mismatch — detection, not prevention,
  stated as such.

## 15. Implementation slices

Six slices, each one PR, each independently provable, ordered so
no slice depends on an unbuilt later one. Builders do not merge;
review is per the fork's standing discipline (independent suite
run; docs fast lane where applicable). SI-side slices land in the
SentinelIntelligence repo under its own review discipline; the
slice states its repo. "UNBUILT after S-n" is cumulative honesty,
not a apology list.

### S-1 — Fork: submission dedup + honest provenance block (fork repo)

Scope: `journal_seq → work_id` index in `MemoryWorkStore`
(§9.3); deduplicated submit returns the existing record;
`status_body.provenance.spine_event_refs` populated from the
`OutcomeStore` for completed work (§11.2).
Acceptance (test-shaped):
1. Two POSTs with the same `journal_seq` return the same
   `work_id`; the store holds exactly one record; the ticker
   actuates it exactly once — including for `hearth.notify`
   (the no-stale-check tool).
2. A POST with a different `journal_seq` mints a new id.
3. After a completed actuation, `GET /work/{id}` shows the real
   `spine_event_refs` from the outcome record; before completion
   it shows `[]`.
4. All G-3 acceptance tests pass unmodified.
UNBUILT after S-1: everything else in this document; work-store
persistence (G-4b) still absent.

### S-2 — Flow 1: proposal ingest + fork egress (SI repo + fork repo, two PRs, SI first)

Scope: SI `POST /gaia/proposals` on the B-5 service with §8.3
semantics (seam task per model, gate, journal, `caused_by`
dedup, ACK-after-journal); fork egress client + egress log
(§8.4) driven from the ticker, sending un-ACKed proposals.
Acceptance:
1. A watcher proposal for a killed miniature-fleet child is
   gated and journaled at SI; the ACK's `journal_seq` names the
   self-edit row; the gate report is in the journal.
2. Killing the SI endpoint between POST and ACK, then retrying,
   yields exactly one self-edit and the identical ACK (dedup by
   `caused_by`).
3. A REJECT-verdict proposal produces no fork work submission
   and an egress-log disposition of `rejected`.
4. Malformed proposal → 400, zero journal growth.
5. Fork restart with an un-ACKed proposal re-sends it; with an
   ACKed one does not.
UNBUILT after S-2: work submission (proposals gate but nothing
actuates through the seam yet — the fork's in-process path is
unchanged), leash crossing, outcome return, events ingest.

### S-3 — Flow 2: seam work submission (SI repo)

Scope: SI submits ADMIT seam intents per §9.2 (attached verdict,
verbatim args, seam `task_id`), journals `gaia_work` on the 202
(§12.5); retry discipline per §9.3/§9.5.
Acceptance:
1. **End-to-end first proof**: a miniature-fleet child dies →
   watcher proposal → SI gate ADMIT → seam submission → fork
   actuation restarts the child; every step's record exists on
   both sides, joined by `(journal_seq, work_id, caused_by)`.
2. A lost 202 (submit landed, response dropped) retried by SI
   produces one work record and one actuation (S-1 dedup).
3. An ESCALATE verdict produces no submission (fork store
   unchanged) and the proposal's egress disposition reads
   `escalated`.
4. Args in the fork's work record are byte-identical to the
   journaled intent's args.
UNBUILT after S-3: leash crossing (fork leash still empty —
actuation relies on SI-side gating + fork stale checks only,
stated in the S-3 PR body), outcome return, events ingest.

### S-4 — Flow 2b: leash crossing (fork repo + SI repo, two PRs, fork first)

Scope: fork `POST /gaia/v1/leash` + heartbeat (§12.3), watermark
application into the existing `Leash` (§10.3.2), staleness
refusal in `FleetActuator` (§10.3.4/§12.4), snapshot re-sync
(§10.5); SI seam client sending leash records on seam-task fold
changes + heartbeats while seam tasks are active.
Acceptance:
1. An operator `queue_stop` journaled at SI refuses an
   otherwise-admitted restart across the seam; the outcome
   records `refusal:"leash"` with the stop's journal_seq.
2. A `queue_resume` re-enables actuation.
3. A duplicate `resume` delivered *after* a newer `stop` does
   not lift the stop (watermark test — records delivered out of
   order on purpose).
4. Heartbeats stopped (SI killed): after TTL, restart refuses
   with a staleness detail; `hearth.notify` still completes.
5. Fork restart: no actuation for a seam model until snapshot +
   first heartbeat arrive.
UNBUILT after S-4: outcome return, events ingest, escalation
resolution, auth (all §16).

### S-5 — Flow 3: outcome return + reconciliation (SI repo)

Scope: SI outcome poller (§11.2) journaling `tool_result` seam
outcomes keyed by `work_id`; `outcomes.jsonl` read-only
reconciler (§11.3); outcome-unknown journaling per §11.4. The
slice brief must pin the disposition question: either the
minimal operator disposition journaling (accept-unknown /
compensate / re-propose as journal acts) is in this slice, or
dispositions remain manual journal acts by an operator with
journal access and the PR body says so. No third option.
Acceptance:
1. The S-3 end-to-end scenario completes the loop: the restart's
   outcome is journaled at SI exactly once, appears in the
   intent's provenance trail (`build_provenance_trail` unmodified),
   and carries the fork's `spine_event_refs`.
2. Fork killed after actuation, before SI polls: on restart,
   the reconciler journals the outcome from `outcomes.jsonl`;
   the work-record loss is a non-event.
3. Outcome destroyed on both paths (work record gone, outcome
   line removed in a fault-injection test): the intent goes
   `outcome-unknown`; re-proposal of the same `caused_by` is
   refused; disposition `accept-unknown` unblocks new proposals
   for the model.
4. A torn final line in `outcomes.jsonl` is skipped, counted,
   and recovered on the next poll — never fatal, never silent.
UNBUILT after S-5: events ingest, escalation resolution, auth,
work-store persistence, measurements (below).

### S-6 — SI journal events into fork SSE `source=si` (fork repo + SI repo, two PRs)

Scope: fork `POST /gaia/v1/events/ingest` (loopback, agency
listener) feeding `SiEventBuffer::push` with SI journal events in
the `StreamEvent::from_si` shape (§4.5); SI forwarder following
its own journal (the SI-49 discipline: cursor = last journaled
seq, advanced only after the fork's 200) and pushing in order.
Acceptance:
1. `GET /gaia/v1/events?source=si&follow=1` on the fork replays
   SI journal events with exact seq fidelity; SI's own
   `events_client` follower consumes the fork stream through a
   drop-and-reconnect with zero missed and zero duplicated
   events (the G-4 acceptance pattern, reversed direction).
2. A duplicate push (same seq) is refused loudly by the fork
   (`SiEventBuffer` as-built) and the forwarder treats it as a
   protocol failure, not a retry.
3. Events pushed while no subscriber exists are replayable from
   the buffer afterwards (history is the buffer, not the hub).
UNBUILT after S-6: escalation resolution, auth, non-loopback
anything, work-store persistence (G-4b), combined multi-source
subscriptions (G-4b), and **seam measurements** — every latency
and throughput number in this document is UNMEASURED; a G-8-style
measurement slice for the seam (proposal→ACK, verdict→resident
across the wire, outcome latency under the poller) is the
natural S-7 and is proposed, not designed, here.

## 16. Open questions and risks (unresolved, stated)

1. **Escalation resolution has no surface on either side.**
   ESCALATE is journal-terminal in SI (§5.2) and the seam keeps
   it so (§9.4). A real operator path (who is asked, through
   what surface, with what journal act) is a design of its own;
   until it exists, seam automations that can escalate need a
   human watching the egress dispositions.
2. **In-memory work store bounds the seam's memory of itself.**
   §13.2 prices a restart; a long-running fork also accumulates
   work records without bound (no eviction exists). G-4b
   (work-store persistence) would retire both; it is fork-internal
   and deliberately not a seam slice, but S-5's reconciler is
   designed to survive its landing unchanged.
3. **Policy constants are starting values, all UNMEASURED:**
   heartbeat interval 10 s, leash TTL 30 s, outcome poll
   interval, egress retry backoff, the fork's 500 ms ticker and
   32-connection SSE budget (G-8 measured the shipped constants'
   costs, not the seam's). S-7 measurement may move any of them;
   none may be quoted as measured before then.
4. **Class vocabulary for `hearth.*` tools is unmapped**
   (§9.2): SI's gate classes describe SI tools. If fork-side
   class policy is ever wanted for runtime operations, the
   mapping is a policy decision for Mark's programme, not a
   seam detail to invent.
5. **One seam task per model concentrates provenance.** All of a
   model's watcher proposals share one task chain; the §10.2
   provenance trails will be long. The chain rules (§5.1) handle
   this mechanically, but trail readability at fleet scale is
   untested.
6. **The stale check is the cross-intent guard.** Two proposals
   for one model in flight can interleave (§9.6); correctness
   rests on the fork's stale-state check comparing against the
   latest spine event. That check is as-built and tested, but
   the seam's S-3 acceptance exercises only the single-proposal
   path; a two-proposals-one-model scenario test is owed by
   whichever slice first makes concurrent proposals likely
   (named here so it is not lost).
7. **NEDB single-opener discipline constrains operations.**
   Sequential-audit-only spine reads (§4.6) mean seam debugging
   that needs a live spine read must go through the mirror or
   the residency route, not gaia-py. Operational, not blocking.

## 17. Non-goals

- No changes to upstream `hearth-*` crates; no `events_since`
  spine patch (G-8's proposal route stands as the upstream-first
  path, and the mirror design makes it unnecessary here).
- No new proposer: the seam carries the watcher's proposals. A
  thinking proposer (a real model under Hearth) is a different
  programme frontier, untouched by this design.
- No auth, no non-loopback transport, no TLS (§14).
- No merging of the sequence spaces, under any convenience
  pressure, ever (§2.2).
- No redesign of the gate, the queue, the watcher mapping, or
  the actuator's check order. Where this document found those
  wanting (stale README in `gaia-api`, the tests-only
  `run_actuation`, the unlanded G-4 provenance promise), it says
  so in the inventories and designs around the code as it is;
  fixing them is ordinary fork work, not seam work.
- No performance claims. Every number not cited from G-8 is
  UNMEASURED.

## 18. Evidence index

Worker inventories (full, with per-symbol citations) are kept
with this slice's backups and summarised in §4–§6:

- **Evidence A — fork-side as-built inventory** (worker A):
  work/SSE/mirror/proposals/leash/outcomes/actuate/composition/
  binary/gaia-py, literal wire and JSONL field lists, the 16-row
  seam-point table, gaps and surprises (leash ingress absent,
  proposals dead-ended, `source=si` empty, README staleness).
- **Evidence B — SI-side as-built inventory** (worker B):
  service/gate/journal/queue/leash/SSE/Hearth inventories,
  literal record shapes, journal entry kinds with writers,
  vocabulary tables, gaps (no proposal ingress, no leash surface,
  no outcome ingress, no disposition writer).
- **Evidence C — transport options analysis** (worker C):
  per-flow × per-transport matrix (HTTP / SSE / gaia-py /
  file-mediated), dedup-key table, ordering claims, failure
  dispositions, leash-staleness mechanisms 1–5 with the
  stop-expiry option rejected, ranked recommendations per flow.
  This document adopts C's first-ranked option for every flow,
  with §10.6 recording the deferred SSE-leash variant.

Coordinator spot-checks performed against source before
synthesis (2026-10-30): `parse_intent` requires `task_id`
(`gateway.rs`); `record_stop`/`record_resume` have no production
callers outside tests; `intent_json`'s only caller is a watcher
test; SI's `Gate` is `si/gate.py:481`; the B-5 service routes are
exactly `/`, `/health`, `/tasks`, `/events`; `record_queue_stop`
is `si/gaia/queue.py:220` with no in-tree caller. All confirmed
the workers' claims.

---

*Design only — unbuilt, unmeasured. The seam exists today as
exactly three missing crossings (§6); S-1 through S-6 build them
in order, and nothing in this document should be read as claiming
otherwise.*
