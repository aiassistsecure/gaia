# Gaia Console

The operator console for the Gaia runtime: resident mind status, chat with
the resident mind, the SI seam journal, and the escalation queue — one
screen, designed as a luxury-automobile instrument face (piano black,
chrome, ambient light) with Aceternity-style polish.

## Run it

```sh
python3 console/server.py
# → http://127.0.0.1:11440
```

`server.py` is a zero-dependency Python stdlib server and binds
**127.0.0.1 only**. Port: `GAIA_CONSOLE_PORT` (default 11440). It serves
`index.html` and exposes:

| Endpoint | What it returns |
| --- | --- |
| `GET /api/journal` | Seam journal tail (last 8 entries, full fields), read live from `si-seam.si` |
| `GET /api/escalations` | The folded open escalations, with their full gate reports |
| `GET /api/spine` | Spine totals, head seq, last event, and residency — from the node's live events/agency endpoints |
| `POST /api/chat` | `{message, history?}` → the resident model's reply |

On load the page fetches the APIs and renders **live** data; the header
tag reads `LIVE · wired to this node`. If the APIs don't answer, the page
falls back to the baked **2026-11-21 snapshot** and labels itself
`SNAPSHOT` — the snapshot is a fallback, never a disguise. Chat goes
through `/api/chat` to the resident model's llama-server endpoint, which
the server discovers live (the spine's last resident event, falling back
to `gaia.log`; pin it with `GAIA_MODEL_ENDPOINT`). The model runs on CPU
here, so replies can take a while.

## Read-only guarantees

- The seam journal is read through the SI package's own read path
  (`si_read.py`: open the bundle, verify the hash chain, fold) in a
  subprocess — the same path as `si gaia-escalation list`. Nothing
  writes to the journal: no appends, no resolutions, no checkpoints.
- The spine is read through the node's events endpoint (11436); the
  server never touches the spine store.
- Chat is the only outbound write, and it goes **only** to the model
  inference endpoint — never to the seam egress (11437), never to the
  agency work API. Approving or refusing an escalation remains an
  operator act at the SI CLI; the console's Refuse/Approve buttons are
  inert concept controls.

## Configuration

| Env | Default | Meaning |
| --- | --- | --- |
| `GAIA_CONSOLE_PORT` | `11440` | Listen port (127.0.0.1) |
| `GAIA_SEAM_BUNDLE` | `~/workspace/gaia-node/si-seam.si` | Seam bundle to read |
| `GAIA_SI_PATH` | `~/workspace/sentinel-main` | SI package checkout (PYTHONPATH for `si_read.py`) |
| `GAIA_SI_PYTHON` | seam service venv python | Interpreter for `si_read.py` |
| `GAIA_NODE_EVENTS` | `http://127.0.0.1:11436/gaia/v1/events` | Node events endpoint |
| `GAIA_NODE_AGENCY` | `http://127.0.0.1:11435` | Node agency base |
| `GAIA_MODEL_ENDPOINT` | *(discovered)* | Pin the model endpoint, e.g. `http://127.0.0.1:44117` |
| `GAIA_NODE_HOME` | `~/workspace/gaia-node` | Node home (gaia.log fallback) |

## Snapshot provenance

The baked fallback data was extracted verbatim from the real journals on
2026-11-21 (`si-seam.si`, chain verified, tip seq 74; node spine head
seq 965; open escalations se_0007–se_0011) — every entry, hash, and gate
report in the detail modals traces to those records.
