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
  before any actuation. Actuation itself is slice G-6.
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
  connection budget — never the inference pool.

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

Nothing here executes a tool intent. Performance is
unmeasured (G-8).
