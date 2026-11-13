# SI-115 — Why the fork does not consume SI journal resolutions

**Diagnosis only. No code, no node change, nothing actuated.**
Question: on 2026-11-13 the SI-114 exercise resolved a live
escalation in the SI seam journal — `se_0012`, intent seq 64
(`hearth.restart`, imagine-v13), admit-after-review journaled
at seq 69, fold `escalated → queued`, tip 70, chain verified —
and in the following 10 minutes nothing on the fork side
consumed seqs 69/70: the inference child (pid 2100) never
restarted, no outcome returned, `gaia.log` / `egress.jsonl`
frozen at 21:08:34Z, mirror untouched. Why?

**Answer, in one paragraph.** There is nothing on either side
of the seam that *can* consume a resolution. G-9 designed
ESCALATE as terminal at ingest — no work submission, and
escalation resolution explicitly UNBUILT (G-9 §9.4, §16.1).
The fork's egress records the `escalated` ACK as a **terminal**
disposition and never looks at that proposal again; the fork
has no SI-journal reader of any kind (its only SI-bound call
is the Flow 1 proposal POST). SI's S-3 submission path exists
only as an operator-invoked library function, and its
eligibility gate refuses any self-edit whose journaled gate
verdict is not exactly `ADMIT` — a resolved escalation's row
still reads `ESCALATE`, so even a manual invocation would
refuse `se_0012`. The one consumer of the fold's `queued`
state, `execute_resolved_escalation`, drives SI's own
Executor, which refuses the `external` class by construction.
The SI-111/SI-113 exercise plan assumed a Flow 2 lifecycle
("verdict-less submission; the verdict route carries the
resolution's Admit") that G-9 never designed for escalations:
Flow 2 submits at ingest with the verdict attached, and the
verdict route actuates only a work record that a submission
first created. No submission for `se_0012` ever existed. The
trail did not go cold at a broken intake; it ends at a
crossing that was designed as a terminus and never rebuilt
as a bridge.

Labels are stated per leg as **designed / built / running** —
three different facts, kept separate throughout.

---

## 1. Provenance: what the running binary is

| Fact | Evidence |
| --- | --- |
| Running process | pid 2019, started Fri 2026-11-13 15:07:18Z, argv `~/workspace/gaia-node/bin/gaia … --si-addr 127.0.0.1:11437 --binary llama-server-v13.sh …` (`ps`, read at diagnosis time; child llama-server pid 2100, port 41583) |
| Binary file | `~/workspace/gaia-node/bin/gaia`, 3,090,944 B, mtime 2026-11-01 18:45:47Z, sha256 `f30dd549…` — byte-identical to `~/workspace/gaia-kvfix-work/repo/target/release/gaia` (same hash), built 2026-10-31 23:40Z in that worktree |
| Source commit | kvfix worktree HEAD `6f1621c0bbb28f9ca3e80b0128adece9115a4470` ("KV fix: bounded child specs + KV-aware admission", 2026-10-31 23:32:59Z) |
| Tree identity | `6f1621c^{tree}` = `a5b04fcb57c6f6b142249bc8a3397b0880594503` = `e06f33c^{tree}` (the PR #15 factory commit) = `0625595^{tree}` (current gaia main). **The running binary's source tree is identical to current gaia main** `0625595686fd54c0eff0178e7ecd54e54d60e1f4` (verified by fresh clone for this slice) |
| NODE.md | "Binary: `~/workspace/gaia-node/bin/gaia` (the merged PR #15 build)" — matches the hash evidence |

**Verdict.** The binary does **not** predate any merged Flow 2
code: gaia main has not moved since PR #15 merged 2026-11-01,
and the binary was built from that same tree. Every leg
audited in §2 that is *built* is also *running*. The gap is
not a stale deploy. There is no commit — in the running
binary, in gaia main, or in any merged PR — that contains a
resolution-consumption path.

## 2. Design vs code, leg by leg

The expected lifecycle (SI-113 doc §1, SI-111 §4.3 step 5) has
four legs: (a) the fork learns of the resolution, (b) a work
record carrying the Admit reaches the fork's store, (c) the
fork claims and actuates, (d) the outcome returns to SI.

### Leg (a) — SI journal resolution → fork awareness

- **Designed: NO.** G-9 §9.4: "ESCALATE: terminal at ingest
  **for v1**. No fork work is submitted; no operator surface
  exists on either side to resolve an escalation (§5.2)." §16.1:
  "Escalation resolution has no surface on either side…
  a design of its own; until it exists, seam automations that
  can escalate need a human watching the egress dispositions."
  §3.1: ESCALATE has **no fork verdict value**. SI-92's
  resolution machinery (PR #99) was later built SI-side, but no
  seam slice ever designed its crossing.
- **Built: NO.** The fork's entire SI-bound surface is
  `egress_client.rs` — one raw-`TcpStream` client whose only
  request is `POST /gaia/proposals` (egress_client.rs:268).
  No GET of SI state, no journal follower, no poller, no
  subscription exists in `gaia-serve` or `gaia-api` (grep for
  SI-journal reads returns only `journal_seq` provenance
  fields and doc comments). On the egress side, an ACK with
  verdict ESCALATE becomes `Disposition::Escalated`, and
  `Disposition::is_terminal()` (egress.rs:98) means
  `drive_egress` (egress.rs:431) skips that proposal forever:
  the fork's seam story for `se_0012` ended, by design, at
  ingest — recorded in `~/workspace/gaia-node/home/egress.jsonl`
  line 10: `caused_by {…, seq 565} → escalated, journal_seq 64,
  task_seq 43`.
- **Running: NO** — same code as built (§1).

### Leg (b) — work submission + verdict route (the fork's intake)

- **Designed: YES, for a different case.** G-9 §9.2 designs
  submission *at ingest* with the gate's verdict attached
  (`ADMIT` only; REJECT/ESCALATE not submitted). §9.5 designs
  the verdict route for *deferred decisions* on records
  submitted undecided — and §9.4 states the watcher flow "does
  not use them". The seam in `crates/gaia-api/src/gateway.rs`
  (`classify`:55, `decide`:87, `parse_intent`:170,
  `parse_verdict`:263) and `work.rs` (`record_verdict`:487,
  `claim_for_actuation`:508, `complete`:532) is exactly this
  design: a verdict can only land on a `work_id` a prior
  submission minted (S-1 dedup keys retries by `journal_seq`).
- **Built: YES** (G-3, S-1, G-4b `FileWorkStore`).
- **Running: YES but never exercised across the seam.** The
  routes are live on 127.0.0.1:11435 in the running binary.
  Yet `FileWorkStore::open(home/work.jsonl)` (main.rs:670) has
  never persisted a record: **`~/workspace/gaia-node/home/
  work.jsonl` does not exist**. Zero work submissions have
  occurred in this node's life. Nothing submitted `se_0012`,
  so there is no record for a verdict to attach to — the
  verdict route is a door into an empty room.
- **SI-side caller: built as a library, never wired, and
  eligibility excludes this case.** `si/gaia/seam_submit.py`
  (S-3/SI-56) is reachable only via `service.py:667
  submit_work` — "No new HTTP route is added in this slice —
  submission is an outbound, operator-invoked act" — and the
  running `si-seam-service` invokes nothing of the kind (its
  routes are `/`, `/health`, `/tasks`, `/events`,
  `/gaia/proposals`; no CLI wiring exists). Worse for this
  case: `_eligibility_problems` refuses any edit whose
  journaled `gate_verdict != "ADMIT"` ("REJECT and ESCALATE
  never submit"). A resolution journals a *separate* entry
  (seq 69) and moves the *fold*; the self-edit row's verdict
  stays `ESCALATE`. So even an operator running the S-3 path
  by hand against `se_0012` today gets a refusal, journaling
  nothing. There is also a shape gap: S-3's
  `build_work_payload` emits SI's own record design ("not
  claimed to be a fork-published schema"), not
  `gateway.rs:parse_intent`'s required shape
  (`type/tool/args/reason/task_id/journal_seq[/class/verdict]`)
  — the two halves of Flow 2 were each built against their own
  side's contract and have never met on the wire for any
  intent, escalated or not (live interop UNMEASURED, as S-3's
  own header states).

### Leg (c) — claim → actuation for `hearth.*`

- **Designed: YES** (G-6/G-9 §4.4). **Built: YES**
  (`actuate.rs`: `actuate_restart`:370 dispatch,
  `staleness_refusal`:311, `leash_refusal`:278; ticker drain
  main.rs:289 scans the work store for `Admitted` records and
  calls `Composition::actuate_work`, lib.rs:560).
  **Running: YES** — the drain runs every 500 ms in the live
  ticker. It is unreachable for `se_0012` solely because leg
  (b) never produced a record.
- **What actuation would do today (priced, not guessed).**
  `actuate_restart` step 3 (actuate.rs:395): restart actuates
  only if the model's *latest* spine event **is** the lost
  event the intent answers (kind `lost`, same hash, same
  seq). `se_0012` answers spine seq 565 (`lost{evicted}`,
  hash `2fc5d2e7…`, verbatim in proposals.jsonl's last line).
  The node's latest spine event for imagine-v13 is seq 593
  `resident` (mirror.jsonl tail: 591 declared → 592 loading,
  pid 2100 → 593 resident — the 15:07:19Z reload that
  self-healed the loss). The leash checks pass (feed installed
  main.rs:577, but `LeashFeed::staleness` returns `None` for a
  model with no feed state, leash_feed.rs:583; the leash store
  is empty). So a submission today yields, deterministically:
  **`stale_state` refusal** — outcome recorded in
  `outcomes.jsonl`, work record `Executed` with
  `result.outcome="refused"`, child pid 2100 untouched. The
  designed restart effect cannot occur against current state,
  because the loss it answers no longer exists. Any plan that
  promises §4.3 step 5's "node restarts its inference child"
  for `se_0012` is promising something the fork's own guard
  correctly forbids.

### Leg (d) — outcome return (Flow 3 / S-5)

- **Designed: YES** (G-9 §11: SI polls `GET /gaia/v1/work/{id}`;
  mandatory `outcomes.jsonl` reconciler). **Built: SI-side,
  as operator-invoked library only** (`si/gaia/seam_outcome.py`
  poller + `seam_reconcile.py`, wrapped by `service.py
  poll_outcomes` — no route, no daemon, no caller in the
  running service). Fork-side writer built and running
  (`OutcomeStore` records before completion, actuate.rs
  `finish`) — it writes only when an actuation happens.
  **Running: NO for this case, twice over:** the poller keys
  on journaled `gaia_work` submissions (`work_id`s) — none
  exist (§2b) — and nobody invokes it. `outcomes.jsonl` has
  never been created on this node.
- The SI-internal alternative is not a substitute:
  `resolution.py:222 execute_resolved_escalation` consumes
  fold-`queued` intents through SI's `Executor`, whose live
  sets exclude the `external` class by construction (SI-113
  §1) — it would journal a claim and then a refusal, spending
  the shot without the fork ever moving (exactly why SI-114
  declined to run negative control (b) through it).

### Summary table

| Leg | Designed | Built | Running |
| --- | --- | --- | --- |
| (a) resolution → fork awareness | **No** (G-9 §9.4/§16.1: UNBUILT) | No | No |
| (b) submission + verdict intake | Yes (ingest-time ADMIT; §9.5 deferred) | Yes (fork); SI caller library-only, ESCALATE-ineligible, wrong wire shape | Routes live; **zero submissions ever** |
| (c) claim → actuation | Yes | Yes | Yes (drain live; unreachable without (b)) |
| (d) outcome return | Yes (§11) | SI poller library-only; fork writer yes | No (no work_id exists; no caller) |

**The exact seam where consumption should live** per the
design as it stands: G-9 §9.2's submission step, executed by
SI — the side that owns the journal and the resolution —
extended to spend an admit-after-review. The fork needs no
new awareness: its as-built intake (submit with attached
verdict → ticker claim → leash/stale checks → primitive →
outcome) already consumes exactly that shape. §4 builds
precisely that extension and nothing else.

## 3. The freeze at 21:08:34Z

**Cause: by-design quiet after the backlog flush completed.
No thread died; nothing crashed; the loop did not stop.**

Evidence, in order:

1. **Every fork write surface is write-on-event.** The ticker
   (main.rs:289, 500 ms) logs a tick line only when
   `transitions > 0 || !proposals.is_empty()`, an egress line
   only when `pass.attempted > 0` (main.rs:341), and a work
   line only per actuation. `egress.jsonl`, `mirror.jsonl`,
   `proposals.jsonl`, `outcomes.jsonl` are append-only stores
   written only by their events.
2. **Connection-refused never harmed the loop.** While the
   seam was down, each pass attempted the 5 pending proposals
   and counted them `still pending` (transport failure =
   `SendOutcome::Retryable`, egress.rs:431) — gaia.log holds
   **210,374 egress lines**, overwhelmingly
   `5 attempted, 0 acked, …, 5 still pending`, and **zero**
   `gaia: egress failed` or `gaia: tick failed` lines in the
   entire log: the pass-level `Err` path (main.rs, "the loop
   keeps going") never fired even once.
3. **The flush is the last line.** `si-seam-service` started
   21:08:30Z (STATE.md); the next ticker pass flushed the
   backlog — gaia.log's final line and egress.jsonl's final
   write share mtime 21:08:34Z: `5 attempted, 0 acked,
   0 rejected, 5 escalated, 0 failed, 0 still pending`,
   recording journal_seqs 44/49/54/59/64 for spine causes
   509/538/547/556/565. The loop was demonstrably alive at
   21:08:34Z — it *produced* the freeze timestamp.
4. **After the flush, no input channel had an event.**
   10/10 proposals terminal in the egress store → every later
   pass attempts 0 → logs nothing. The watcher saw no new
   loss: mirror's last append is 15:07:37Z (spine 593
   resident, seconds after node start), and the child has
   been resident since. The work store was and is empty.
   And the 21:37Z resolution arrived on **no fork input
   channel at all** (§2a) — a perfectly healthy fork produces
   exactly this silence in response to it. The silence is
   the consumption gap's signature, not a second failure.
5. The node (pid 2019) and child (pid 2100) were alive at
   diagnosis time; both systemd services active; SI-114's
   residency polls answered throughout 21:37–21:47Z.

Correction to the incident's framing, for the record:
"egress froze at 21:08:34Z" inverts the fact — 21:08:34Z is
when egress last had work and completed it. The mirror's
15:07:37Z freeze is the same phenomenon (no spine events
since the reload), a full six hours earlier and equally
by design.

## 4. The plan — smallest build that closes the gap

No fork code change is required. The consumer is one SI-side
slice plus one lead-run operational completion. The fork's
intake, drain, actuator, outcome store, and S-1 dedup are
all as-built and running (§1, §2).

### P-1 — SI repo: resolution-spend bridge (the consumer)

- **Files:** extend `si/gaia/seam_submit.py` (or sibling
  `si/gaia/seam_resolve_submit.py`) + a `submit_resolved`
  wrapper beside `service.py:667 submit_work` + CLI wiring in
  `si/cli.py` next to `gaia-escalation` (explicit operator
  act; see D-115-1). No service route, no daemon.
- **Behaviour:** given an intent seq, fold-verify: state
  `queued`, resolution `admit-after-review`, `resolution_seq`
  present (the `resolve_escalation` record is the ticket —
  reuse its checks, invent none). Build the payload in the
  **fork's as-built shape** (`gateway.rs:170 parse_intent`):
  `{"type":"tool","tool":"hearth.restart","args":<journaled
  args verbatim>,"reason":<journaled reason>,"task_id":
  "task@43","journal_seq":64,"verdict":"admit"}` — note
  `journal_seq` is the intent's self-edit seq (G-9 §9.2),
  never the resolution seq. POST to the fork agency address
  (operator-supplied URL, S-3's discipline; the live value is
  `http://127.0.0.1:11435/gaia/v1/work`). On 202 + `work_id`:
  journal the `gaia_work` row exactly as S-3 does (carrying
  `resolution_seq` in the record), re-anchor. Dedup twice
  over: local `gaia_work` scan (S-3 as-built) and fork S-1
  (`journal_seq → work_id`) — a repeat invocation returns the
  original submission and actuates nothing twice.
- **Tests:** eligibility table (unresolved `escalated`
  refused; `reject`-resolved refused; already-submitted
  deduped; non-seam tool refused); payload field-for-field
  against `parse_intent`'s required set; loopback fork-double
  end-to-end (submit → double claims/actuates → S-5 poll
  journals the outcome once, keyed by `work_id`); a
  stale-state-shaped double outcome journals as the intent's
  refused outcome, not an error.
- **Deploy:** SI repo PR only. The bridge runs from the
  standing `sentinel-main` checkout like the SI-113 CLI did —
  **no node rebuild, no node restart, no seam-service
  restart** (invoked out-of-process; if the lead later wires
  it into `si-seam-service`, that deploy is a
  sentinel-main advance + service restart, separately).

### P-2 — Operational: complete the exercise against se_0012

Lead-run, one sitting, SI-111 §4.3 steps 5–7 adapted to the
diagnosed reality (D-115-2):

1. Preconditions per §4.3 (Resident v13, seam active on
   current main, chain verified, tip recorded — expect 70).
2. Invoke P-1 for seq 64. Expect: 202, first `work.jsonl`
   record in the node's life; ticker claim; **outcome
   `refused`, kind `stale_state`** (latest `resident`@593 vs
   answered `lost`@565) — the fork's guard answering a loss
   that already healed. Child pid 2100 unchanged **by
   design**, and the observation record says why.
3. S-5 return: invoke `poll_outcomes` → the refusal journals
   at SI as `se_0012`'s `tool_result` outcome (once, keyed
   by `work_id`); fold reaches its terminal state carrying
   `resolution_seq` 69 provenance.
4. **Negative control (b), now runnable:** re-invoke P-1 →
   deduped, same `work_id`, no second actuation; and
   `execute_resolved_escalation` → refuses (no longer
   `queued`). Both refusals recorded — §4.3 step 7 complete.
5. Record: docs-only SI PR amending the SI-113 record with
   seqs/hashes/tips and the exact §18.3 scope the lead rules
   this earns (§4.4 discipline: the record states its scope,
   no broader).

**What happens to the loaded intent:** P-2 spends
`se_0012`'s single shot on the designed refusal. That is
not a failed execution — it is the only outcome the current
world state permits, and it converts a silently-queued
loaded intent into a fully-journaled, chain-verified,
terminal record on both sides. Until P-2, **se_0012 stays
exactly as it is: queued, shot intact.** One warning the
plan must carry: from the moment any consumer exists, the
intent is live ammunition — nobody runs P-1's code path
against the real journal outside the P-2 sitting.

### P-3 — Operational follow-on: the restart effect, on a fresh loss

§4.3 step 5's real effect (child restart + Resident return)
is observable only when the answered loss is still the
latest spine event. In a separate lead-directed sitting:
induce one controlled loss of the current resident, walk
Flow 1 → ESCALATE → CLI resolve → P-1 submit within the
loss window, and observe claim → restart → `loading` →
`resident`, the executed outcome journaled via S-5, and
negative controls per step 7. This is a new exercise subject
by necessity (the old loss healed), sequenced after P-2, and
its record claims its own scope.

### Decisions for the lead

- **D-115-1 — Trigger shape for the spend.** (a) Explicit
  CLI act after `resolve` (recommended: preserves §4.2's
  "no policy key makes a resolution automatic" — the
  resolution and its spend stay two named operator acts);
  (b) `si-seam-service` journal watcher that auto-submits on
  `admit-after-review` (rejected by this diagnosis: makes
  resolution effectively auto-executing and adds a daemon
  surface); (c) fork-side journal follower (rejected:
  violates G-9 §7.5 no-shared-database; the fork has no SI
  journal reader and building one is the largest option).
- **D-115-2 — `se_0012` completion semantics.** Accept the
  designed `stale_state` refusal (P-2) as `se_0012`'s
  completion, with the restart effect earned separately in
  P-3 (recommended) — versus retiring `se_0012` undisposed
  and letting P-3 carry the whole exercise (leaves a queued
  loaded intent in the live journal indefinitely; not
  recommended).
- **D-115-3 — Submission schema authority.** Adopt the
  fork's as-built `parse_intent` shape as the seam's
  submission schema for the resolution path (recommended;
  zero fork change) — versus teaching the fork SI-56's
  payload shape (fork PR + binary rebuild + node restart;
  not the smallest build).

## 5. What this diagnosis did not do

No code was written or changed; the node, the seam service,
and the journal were only read. `se_0012` is untouched:
fold `queued`, resolution_seq 69, shot intact, tip 70.
§18.3 remains **NOT earned**. Fork measurements remain as
G-9 left them (policy constants UNMEASURED); nothing here
prices latency. Evidence:
`~/workspace/si-dev-loop/si115-build-evidence.md`.

— SI-115 coordinator (fork lane), 2026-11-13. Diagnosis only;
the lead scopes the build.
