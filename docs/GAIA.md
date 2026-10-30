# Gaia

## What Gaia Is

Gaia is a fork of Hearth, created 2026-10-30 at `aiassistsecure/gaia`, forked from `aiassistsecure/hearth`.

Hearth is a Rust model-residency runtime manager. It sits above llama.cpp, declares models, keeps them resident, and serves them through one OpenAI-compatible / Ollama-style gateway. It is explicitly "not an inference engine" (its own README).

Workspace crates in Hearth (root `Cargo.toml`, version 0.4.7, edition 2021, license BUSL-1.1, authors Interchained LLC):

- `hearth-core` — residency state machine + VRAM planner; states Unknown / Loading / Resident / Lost{Evicted, GpuDetached, ProcessExited, Unhealthy} / Failed / Stopped
- `hearth-serve` — supervisor over llama-server children + the `hearth` CLI
- `hearth-store` — NEDB spine
- `hearth-api` — OpenAI-compatible gateway; HTTP status carries the residency diagnosis; `/residency` endpoint
- `hearth-pull` — digest-verified fetches
- `hearth-resolve`
- `hearth-py` — Python bindings, PyPI `hearth-engine`
- `hearth-node` — npm `@interchained/hearth`

Gaia is the fork that carries the agentic harness/loop + an SSE event spine on Hearth's runtime-manager layer — in Mark's words (2026-10-29): "Hearth becomes Gaia".

Gaia is the acting layer on computer systems: files, processes, services, repos, and infrastructure. It is not a chatbot; chat is at most a control surface.

SI (the SentinelIntelligence project, repo `aiassistsecure/SentinelIntelligence`) is the model/mind; Gaia serves and houses it. SI runs as a model under the runtime's governance, kept resident and served through the gateway.

Gaia is self-servicing: Gaia maintains its own stack.

## Upstream Relationship

Upstream is `aiassistsecure/hearth`.

Merges flow in from upstream into this fork. This fork never pushes changes upstream unprompted.

Licence: the repo remains Business Source License 1.1 (BUSL-1.1), licensor Interchained LLC — unchanged from upstream unless Mark decides otherwise.

For contrast: the SentinelIntelligence repo itself is Apache-2.0; that licence does not extend to this fork.

## Upstream Sync Runbook

This runbook assumes a clone of `aiassistsecure/gaia` with an `upstream` remote pointing at `https://github.com/aiassistsecure/hearth.git`.

1. Fetch upstream:

   ```sh
   git fetch upstream
   ```

2. Create a sync branch off gaia's main:

   ```sh
   git checkout main && git checkout -b sync/upstream-YYYYMMDD
   ```

3. Merge upstream:

   ```sh
   git merge upstream/main
   ```

   Never direct-push or direct-merge into gaia's main; the sync lands via a PR ("sync PR") against gaia main.

4. Conflict policy:

   - For `crates/hearth-*` and other upstream-owned files, favour upstream (`git checkout --theirs` on those paths during a merge from upstream) unless a Gaia change there is deliberate and documented in the sync PR.
   - For Gaia-added files (`docs/GAIA.md`, future `gaia-*` additions), favour the GAIA side.

5. Verify before merge:

   ```sh
   cargo build --workspace --all-targets
   cargo test --workspace --all-targets
   ```

   Both must pass on the sync branch; note results in the sync PR body.

6. Open the sync PR with the gh wrapper or web UI, title format `sync: upstream hearth @ <short-sha>`. The PR is reviewed and merged by the Gaia maintainer agent (Mark granted review+merge autonomy on this repo 2026-10-30); upstream hearth itself is never touched.

7. Cadence — PROPOSAL: sync before each new Gaia feature slice, and at least weekly while the fork is active.

## Roadmap Pointer

The agency currently lives in SentinelIntelligence's Python subpackage `si/gaia/`, built under SI-41..SI-50:

- tool-intent format + 12-tool registry
- gate in the self-write loop
- NEDB journal before execution
- executor + filesystem tools
- task loop
- Hearth interface (replay/stub; real Hearth path UNMEASURED)
- work-as-a-service adapter
- SSE event spine + filtered subscriptions + resilient follower
- queue-drain spec SI-50

Fork-side Rust slices (numbered G-2 onward) are planned, not built. No item in this list exists in this repo yet:

- PLANNED, NOT BUILT: agency endpoints in the gateway crate (`hearth-api`)
- PLANNED, NOT BUILT: SSE serving from the runtime
- PLANNED, NOT BUILT: residency self-maintenance hooks — Gaia watching its own mind's residency and acting: re-declare / restart / re-pull, all journaled on the SI side

## Baseline (G-1, 2026-10-30)

Measured on the pristine fork, before any Gaia change, at `main` =
`2840e2b08f6462a09041de34768e86ca8117d2a7` (identical to upstream
`aiassistsecure/hearth` main at fork creation). Machine: this VM, 2 vCPU,
7.7 GiB RAM. Toolchain: cargo / rustc 1.99.1 (stable, rustup, user-local).
Check set is the one upstream CI runs (`.github/workflows/ci.yml`); durations
are single cold-target runs on this box — indicative, not benchmarks.

| Check | Result | Detail |
|---|---|---|
| `cargo build --workspace --all-targets` | PASS | 140 crates compiled, 0 warnings, 437 s cold |
| `cargo test --workspace --all-targets` | PASS | 267 passed, 0 failed, 1 ignored, across 16 test binaries |
| `cargo check --workspace --all-targets` | PASS | 138 s |
| `cargo clippy --workspace --all-targets -- -D warnings` | PASS | 20 s, no warnings |
| `cargo run -p hearth-core --example a6000` | PASS | exit 0; planner output as documented in the README (2 of 5 admitted) |
| `cargo fmt --all -- --check` | FAIL (upstream code) | diffs in exactly one file: `crates/hearth-serve/src/bin/hearth.rs`, lines 657 and 716, both inside `cmd_convert` (added by upstream PR #14) |

Notes:

- The 1 ignored test is the live-network Imagine resolver smoke test, which
  upstream CI itself runs only on manual dispatch.
- The fmt failure is recorded as an upstream finding and is deliberately NOT
  fixed in G-1 (this slice changes no `hearth-*` source). It was measured
  with rustfmt from Rust 1.99.1; whether it also fails under the exact
  toolchain upstream CI used is UNMEASURED.
- `Cargo.lock` was not modified by any of the runs above (identical hash
  before and after), so this slice carries no lockfile change.
- Everything else in the table above is a real, local, single-run result;
  CI on this fork's own infrastructure has not run yet (UNMEASURED there).
