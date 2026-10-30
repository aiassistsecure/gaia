# gaia-api

**The agency surface of the gaia fork — one port for work, where submission is not execution.**

[![crates.io](https://img.shields.io/crates/v/gaia-api.svg)](https://crates.io/crates/gaia-api)
[![docs.rs](https://img.shields.io/docsrs/gaia-api)](https://docs.rs/gaia-api)

`hearth-api` serves models. This crate serves *work*: a tool intent that
SI — the mind — has already journaled, handed to the runtime that will
carry it, with the gate's verdict travelling on its own route. The two
surfaces sit side by side in the composition binary and share nothing
but the response envelope's shape. No agency route proxies inference,
and no inference route learns about work.

```
  POST /gaia/v1/work                 submit a journaled tool intent
  GET  /gaia/v1/work/{id}            status, and the result when finished
  POST /gaia/v1/work/{id}/verdict    deliver the gate's verdict
  POST /gaia/v1/leash                deliver a leash record (S-4)
  POST /gaia/v1/leash/heartbeat      leash feed heartbeat (S-4)
  GET  /gaia/v1/health               the agency surface itself
```

The leash routes are the wire shape of the leash crossing
(G-9 §12.3): they are answered by a `LeashSink` the
composition implements, not by the work store — SI's
journal is the leash's home, and what crosses here is a
replica feed. A record the fork's watermark has already
applied is a 200 with `applied: false`, not an error.

## Submitting work is not executing it

```bash
$ curl -s -X POST localhost:11434/gaia/v1/work -d '{
    "type": "tool",
    "tool": "filesystem.read",
    "args": {"path": "/etc/hostname"},
    "reason": "checking which box this is",
    "task_id": "task-1",
    "journal_seq": 42
  }'
```
```json
{"work_id": "…", "state": "accepted", "journal_seq": 42}
```

`202` means *recorded*, nothing more. An intent that arrives with its
ADMIT verdict attached may be acted on by the composition; one that
arrives verdict-less is held in `awaiting_verdict` until a verdict
arrives on its own route:

```bash
$ curl -s -X POST localhost:11434/gaia/v1/work/{id}/verdict \
    -d '{"verdict": "admit"}'
```

The fork **never manufactures a verdict**. It is a proposer and a
carrier, not a decider — judgement, the tool registry, and the journal
stay with SI. Which executor applies to a piece of work is a property
of the tool's class, declared in the submission, never inferred here.

## Two error vocabularies, kept disjoint

Agency errors reuse the OpenAI envelope (`{error:{message, type,
param, code}}`) so existing clients surface the message instead of
choking on it — but the types are `gaia_*`, never hearth's residency
types:

| situation | status | `type` |
|---|---|---|
| no such route | 404 | `gaia_route_not_found` |
| unknown work id | 404 | `gaia_work_unknown` |
| verdict already recorded, or work already executed | 409 | `gaia_verdict_conflict` |
| actuation attempted without a verdict | 409 | `gaia_verdict_required` |
| intent's class is one the fork does not carry | 409 | `gaia_class_not_carried` |
| malformed envelope or verdict body | 400 | `gaia_malformed_request` |

A `model_lost` can never be produced by a `/gaia/*` route, and a
`gaia_*` error can never be produced by an inference route, so no
client can mistake a work problem for a residency diagnosis or vice
versa.

Status answers carry the provenance the work arrived with:

```json
{
  "work_id": "…",
  "state": "awaiting_verdict",
  "provenance": {"si_journal_seqs": [42], "spine_event_refs": []}
}
```

## What is NOT here yet

This is the skeleton slice (G-3), and it says so plainly:

- **In-memory only.** The store behind these routes lives and dies
  with the process. Persistence on the spine is a later slice (G-4).
- **No SSE.** `GET /gaia/v1/events` — the journal-derived event
  stream — is designed in G-2 §4 and not served by this crate yet.
- **No actuation execution.** The verdict state machine is real, and
  an actuator seam exists in `work`, but no tool is executed by this
  crate. Wiring actuation to the runtime is a later slice (G-6).
- **No `/gaia/v1/residency` yet.** Structured residency for policy
  consumers is likewise a later slice.

## Design

**Method-aware, unlike the inference gateway.** `hearth-api` routes
by path alone because inference clients name their model in the body.
Work has verbs: the same path submitted, read, and verdicted is three
different operations, so `classify` here takes the method as well as
the path.

**No HTTP framework, no new dependencies.** Same call as the rest of
this workspace. This crate is a pure decision layer over
`hearth-api`'s `Request`/`Response` — read a request, answer it — and
`hearth-api` itself is not modified by its existence.

**Loopback by default, no auth.** Same warning as the gateway: a
surface on `0.0.0.0` hands anyone on the network an unauthenticated
way to submit work. This design makes no safety claim for
`/gaia/*` exposed beyond the box. Put it behind something that
authenticates if it ever needs to leave.

## The rest of the workspace

| crate | what it does |
|---|---|
| [`hearth-core`](https://crates.io/crates/hearth-core) | the decision procedure — states, budget, routing |
| [`hearth-api`](https://crates.io/crates/hearth-api) | the inference gateway this crate sits beside |
| **gaia-api** | this crate — the agency surface |

---

Built by **Vex × Interchained**

© Interchained LLC · BUSL-1.1 (converts to Apache-2.0 on 2030-08-27)
