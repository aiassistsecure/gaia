# Gaia Console

The operator console for the Gaia runtime: resident mind status, chat with
the resident mind, the SI seam journal, the escalation queue, and the
agent's full command history — one screen, designed as a
luxury-automobile instrument face (piano black, chrome, ambient light)
with Aceternity-style polish. The header carries the operator controls:
an **Autoapprove** indicator and a **Kill** killswitch.

## Run it

```sh
python3 console/server.py
# → http://127.0.0.1:11440
```

`server.py` is a zero-dependency Python stdlib server and binds
**127.0.0.1 only**. Port: `GAIA_CONSOLE_PORT` (default 11440). It serves
`index.html` and exposes:

| Endpoint | What it does |
| --- | --- |
| `GET /api/journal` | Seam journal tail (last 8 entries, full fields), read live from `si-seam.si` |
| `GET /api/escalations` | The folded open escalations, with their full gate reports |
| `GET /api/commands` | **Every tool intent in the journal** — the agent's command history: each intent folded with its gate verdict, its execution outcome (from its `tool_result` experiences), and its resolution if any |
| `GET /api/spine` | Spine totals, head seq, last event, and residency — from the node's live events/agency endpoints |
| `GET /api/config` | Console config state: autoapprove env/active, kill dry-run, killed state + last kill report |
| `POST /api/chat` | `{message, history?}` → the resident model's reply |
| `POST /api/resolve` | `{edit_id, resolution}` → journal an operator resolution (see below) |
| `POST /api/kill` | `{}` → the killswitch (see below) |
| `POST /api/resume` | `{}` → remove the kill marker and start the units again |

On load the page fetches the APIs and renders **live** data; the header
tag reads `LIVE · wired to this node`. If the APIs don't answer, the page
falls back to the baked **2026-11-21 snapshot** and labels itself
`SNAPSHOT` — the snapshot is a fallback, never a disguise. Chat goes
through `/api/chat` to the resident model's llama-server endpoint, which
the server discovers live (the spine's last resident event, falling back
to `gaia.log`; pin it with `GAIA_MODEL_ENDPOINT`). The model runs on CPU
here, so replies can take a while.

## Reads, and the one write path

- Reads are read-only. The seam journal is read through the SI package's
  own read path (`si_read.py`: open the bundle, verify the hash chain,
  fold) in a subprocess — the same path as `si gaia-escalation list`.
  The spine is read through the node's events endpoint (11436); the
  server never touches the spine store.
- **Resolutions are the one write path.** The escalation modal's
  Approve / Refuse buttons (two clicks: arm, then confirm — the button
  names the exact resolution it will journal) call `POST /api/resolve`,
  and `si_act.py` performs the SI-92 verifying act through
  `si.gaia.escalation_cli.resolve` — the same path as the
  `si gaia-escalation resolve` CLI: bundle verification, fold, verify
  the intent is escalated and unresolved, journal the
  `escalation_resolution`, re-anchor the checkpoint. The vocabulary is
  closed (`admit-after-review` / `reject`); a refusal journals nothing
  and the SI layer's message is returned verbatim (HTTP 409). A
  resolution **never rewrites a gate verdict** — it is a separate
  journaled operator act, recorded under operator
  `Mark (Gaia console)`.
- Chat goes **only** to the model inference endpoint — never to the
  seam egress (11437), never to the agency work API.

## Autoapprove (`GAIA_AUTOAPPROVE=1`, default off)

A resolution **policy**, never a gate bypass. The gate still escalates
exactly what it escalates. While enabled, the server polls the pending
escalations every `GAIA_AUTOAPPROVE_INTERVAL` seconds (default 3) and
answers each with `admit-after-review`, credited to operator
`autoapprove (env GAIA_AUTOAPPROVE)` with the basis *"autoapprove
enabled in console server env (GAIA_AUTOAPPROVE=1); gate verdict
preserved, resolution journaled as a separate operator act"*. The
journal shows both halves: the untouched ESCALATE verdict and the
separate autoapprove resolution. The console header shows a lit
**Autoapprove** indicator while it is active (state via
`GET /api/config`). With the env unset, nothing automatic happens,
ever. The killswitch turns autoapprove off for the life of the server
process, whatever the env says.

## Killswitch (`POST /api/kill`)

The header's **Kill** button (two clicks: arm, then confirm) calls
`POST /api/kill`, which:

1. disables autoapprove in this process immediately, and
2. stops the node's systemd units (`GAIA_NODE_UNITS`, default
   `gaia-node.service si-seam-service.service`) with `systemctl stop`,
   then verifies and reports the post-state: each unit's state and
   whether each port in `GAIA_NODE_PORTS` (default 11435, 11436,
   11437) still listens.

**Durability:** since 2026-11-21 the watchdog honors a
deliberate-stop marker at `~/workspace/gaia-node/console-kill.marker`
(`GAIA_KILL_MARKER`). `POST /api/kill` writes the marker *before*
stopping the units, so the watchdog skips recovery and the halt
sticks. `POST /api/resume` removes the marker and starts the units
again; the header shows a **Resume** button while the node is
killed. Both endpoints return the verified post-state.

`GAIA_KILL_DRY_RUN=1` makes `POST /api/kill` report what *would* be
stopped — current unit states, port states, autoapprove state —
changing nothing. That is how the kill path is tested.

## Configuration

| Env | Default | Meaning |
| --- | --- | --- |
| `GAIA_CONSOLE_PORT` | `11440` | Listen port (127.0.0.1) |
| `GAIA_SEAM_BUNDLE` | `~/workspace/gaia-node/si-seam.si` | Seam bundle to read / resolve against |
| `GAIA_SI_PATH` | `~/workspace/sentinel-main` | SI package checkout (PYTHONPATH for `si_read.py` / `si_act.py`) |
| `GAIA_SI_PYTHON` | seam service venv python | Interpreter for `si_read.py` / `si_act.py` |
| `GAIA_NODE_EVENTS` | `http://127.0.0.1:11436/gaia/v1/events` | Node events endpoint |
| `GAIA_NODE_AGENCY` | `http://127.0.0.1:11435` | Node agency base |
| `GAIA_MODEL_ENDPOINT` | *(discovered)* | Pin the model endpoint, e.g. `http://127.0.0.1:44117` |
| `GAIA_NODE_HOME` | `~/workspace/gaia-node` | Node home (gaia.log fallback) |
| `GAIA_AUTOAPPROVE` | *(off)* | `1` enables the autoapprove resolution policy |
| `GAIA_AUTOAPPROVE_INTERVAL` | `3` | Autoapprove poll interval, seconds |
| `GAIA_KILL_DRY_RUN` | *(off)* | `1` makes `POST /api/kill` a no-op report |
| `GAIA_NODE_UNITS` | `gaia-node.service si-seam-service.service` | Units the killswitch stops |
| `GAIA_NODE_PORTS` | `11435,11436,11437` | Ports the killswitch verifies after stopping |

## Snapshot provenance

The baked fallback data was extracted verbatim from the real journals on
2026-11-21 (`si-seam.si`, chain verified, tip seq 74; node spine head
seq 965; open escalations se_0007–se_0011) — every entry, hash, and gate
report in the detail modals traces to those records.
