# gaia-serve

The Gaia composition for the hearth fork: a supervisor loop, a
residency watcher, and the agency surface, in one binary —
`gaia`.

Upstream hearth's `up` composition restarts children that die
unexpectedly, on its own authority. That behaviour is
upstream's, it stays in the upstream `hearth` binary untouched,
and this crate does not pretend otherwise. The trust boundary
is at **composition** (G-2 §5.2.1): `gaia` wires the same
public supervisor API into a loop that calls `tick()` and
never calls the restart method. What a loss produces here is
not a restart — it is a **proposal**.

## What one tick does

1. **Tick.** The supervisor probes its children and records
   every transition in the spine — the same facts upstream
   records, through the same public API.
2. **Mirror.** Newly recorded spine events are appended to
   the G-4 mirror and published to the event hub, so SSE
   subscribers and the watcher read one history.
3. **Propose.** The watcher scans the mirrored history for
   `lost` events and applies the §5.2 reason → candidate
   mapping — at most one proposal per loss, recorded in a
   gaia-owned append-only proposal store:
   - `lost{process_exited}` / `lost{unhealthy}` →
     `hearth.restart`
   - `lost{evicted}` → `hearth.notify`, or `hearth.restart`
     when the fleet has free VRAM at proposal time
   - `lost{gpu_detached}` → `hearth.notify`, never a restart

## Invariants

- **The watcher actuates nothing.** A proposal is an SI tool
  intent (`{type:"tool", tool, args, reason}`) plus its causal
  provenance — the spine `EventRef` of the loss it answers.
  Whether it runs is the SI gate's decision, journaled in SI
  before any actuation.
- **Dedup is causal and durable.** A loss that already has a
  proposal recorded against its event ref is answered; a
  second watcher pass — or a restarted process over the same
  store — proposes nothing new.
- **Scope is opt-in.** The watcher only proposes for models
  declared in scope (`--watch NAME`, or `--watch-all`); the
  default is to watch nothing.
- **A superseded loss is not proposed for.** If the model has
  already recovered, the old loss is history, not a work order.

## Serving

- `POST /gaia/v1/work`, `GET /gaia/v1/work/{id}`,
  `POST /gaia/v1/work/{id}/verdict`, `GET /gaia/v1/health`
  (from `gaia-api`) and `GET /gaia/v1/residency` — structured
  residency read from the spine — on `--port`
  (default `GAIA_PORT` or 11435).
- `GET /gaia/v1/events` (SSE) on its own listener,
  `--events-port` (default agency port + 1), with its own
  connection budget — never the inference pool. One connection
  may follow both sources (`?source=hearth&source=si`): wire ids
  are then source-qualified (`id: hearth:<seq>`, `id: si:<seq>`)
  and resume carries both cursors (`Last-Event-ID:
  hearth:<seq>,si:<seq>`); single-source streams keep the bare
  id shape they have always had.

The work store is file-backed (G-4b): `work.jsonl`, beside
`proposals.jsonl` / `outcomes.jsonl` / `egress.jsonl` under the
same home. Submissions, verdicts, results, and the submission
dedup index survive a restart — a retried submission after a
restart returns the existing record instead of minting a second
work order. The in-memory store remains available to tests and
embedders behind the same trait; the binary does not use it.

## Running

```
gaia --home ~/.gaia \
     --model muse=/path/to/muse.gguf \
     --watch muse \
     --binary /path/to/llama-server
```

Declarations account weights only: upstream `hearth up`
additionally estimates KV-cache bytes from context and
parallelism, and this composition does not replicate that
estimate (it is stated here rather than hidden). The binary
supervises llama.cpp/GGUF models; vLLM declarations are
upstream's composition for now.

## Actuation (G-6)

The ticker drains the work store after every step: each
record in `Admitted` state — an intent SI's gate has
admitted, and no other kind — is handed to
`Composition::actuate_work`, which executes its journaled
args verbatim through the §5.3 primitives:

- `hearth.restart` → `Supervisor::start(stored spec)` — the
  spec this composition started the model with, remembered
  via `note_spec`. No stored spec, no restart: a refusal.
- `hearth.stop` → `Supervisor::stop(model)`.
- `hearth.repull` → `hearth_pull::pull`, which records
  `PullStarted`/`PullCompleted` in the spine itself.
- `hearth.notify` → an outcome record; no process action.

Checks stand in front of every process actuation, in
this order: the **leash-feed staleness gate** (S-4, below —
only for models the seam feed is active for), the
**leash** (a stop SI journaled and has not resumed, held
in `leash.jsonl` — §5.2.1), and the **stale-state check**
(the fleet has moved on since the proposal: the model
recovered, or the intent answers a loss that is no longer
the latest event). A failed check is a
*refusal*: recorded in `outcomes.jsonl`, returned as the
work's result, never executed. Every outcome — executed,
refused, failed — is recorded the same way, with the spine
refs it produced and the loss it answers.

What this is not: the gate. Verdicts are SI's; this crate
carries and honours them, it does not make them. A rejected
or never-verdicted intent actuates nothing — the claim
refuses it before an actuator exists. The evidence base is
miniature fleets (a python stand-in child in the acceptance
tests), not a production fleet. Performance is unmeasured
(G-8).

## Seam egress (S-2b)

Proposals used to terminate at `proposals.jsonl`. With
`--si-addr HOST:PORT` (or `GAIA_SI_ADDR`) set, the ticker
also runs an egress pass after every step: each proposal
SI has not yet answered is POSTed to SI's proposal ingest
(`POST /gaia/proposals`, G-9 §8 — the proposal record
verbatim), and SI's answer is recorded in `egress.jsonl`,
a JSONL store beside the other stores. A proposal's
disposition is `pending` until a terminal answer is
recorded: `acked` (the gate admitted it), `rejected`,
`escalated` — each carrying the ACK's `journal_seq`, the
self-edit row in SI's journal the proposal became — or
`failed` (SI answered 4xx: a terminal shape refusal,
recorded, never retried blindly). Transport failures and
5xx answers record nothing; the proposal stays pending
and is re-sent on a later pass, and SI's dedup by
`caused_by` returns the identical ACK, so retries are
free and a restart re-sends exactly the un-ACKed set.

What this is not: the other half of the seam. The ingest
endpoint itself is SI-side work (S-2a); the acceptance
tests here prove the fork half against a stub that
implements the §8.3 contract — dedup, ACK-after-journal,
verdict spellings — and claim nothing about the real SI
service. Nothing actuates through the seam yet: an ACKed
proposal is provenance, not a work submission (that is
S-3). There is no authentication anywhere — loopback is
the entire security model, as everywhere in this binary
(G-9 §14). Performance is unmeasured.

## Leash crossing (S-4, fork half)

The leash used to arrive only in-process. Now the fork
takes it over the wire, the way G-9 §10 designs it: SI's
journal stays the leash's home, and this composition's
`leash.jsonl` becomes a **derived replica** with exactly
one writer path — `POST /gaia/v1/leash` and
`POST /gaia/v1/leash/heartbeat`, answered by the agency
listener from the composition. A `LeashFeed`
(`leash-feed.json`, beside the leash) keeps the replica
honest:

- **Watermark.** Records apply only in SI journal order:
  a record at or below the model's applied `journal_seq`
  is ACKed (`applied: false`) and not applied, so a
  delayed duplicate `resume` can never lift a newer
  `stop`. A snapshot re-sync is the same record shape
  under the same rule (§10.5) — no second state machine.
- **Staleness, fail-closed.** Once the feed is active for
  a model, actuation refuses (`refusal: "leash"`, detail
  naming the cause and the watermark) when no heartbeat
  has arrived within the TTL (30 s — an UNMEASURED policy
  constant), when the watermark is behind a tip the fork
  has been told about, or when the feed carries an
  unrecovered transport fault. A fresh heartbeat plus a
  current watermark restores actuation. `hearth.notify`
  is exempt, as it has always been: it actuates nothing.
- **No stop expiry, ever.** The TTL gates actuation
  freshness, never a stop's validity — a stale feed
  refuses everything and resumes nothing. Only an
  explicit resume record lifts a stop.
- **Restart.** Watermarks and activation persist;
  heartbeat stamps do not. After a restart, a seam model
  is stale until the snapshot and the first heartbeat
  arrive.

What this is not: the SI half. Nothing here sends leash
records — SI's seam client (the S-4 SI PR) will POST fold
changes and heartbeats when it lands; until then records
arrive from tests and operator tooling, and this half
claims fork-side proof only.
