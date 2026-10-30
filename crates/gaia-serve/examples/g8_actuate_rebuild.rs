//! G-8 measurement example (Worker B): actuation round-trip and
//! mirror-rebuild economics. Measurement only — this binary
//! proposes nothing new, changes no policy, and records only
//! what the shipped composition / spine / mirror already do.
//!
//! Two subcommands (no subcommand = both):
//!
//!   * `roundtrip` — the full self-maintenance timeline of
//!     G-2 §5/§7 G-8/§8.5 over a real `Composition`: one
//!     declared fake-server model (the python stand-in pattern
//!     copied from the composition / actuation acceptance
//!     tests — examples cannot import tests), started resident,
//!     killed out from under the supervisor, then driven with
//!     `tick_once` at the shipped 500 ms cadence
//!     (`crates/gaia-serve/src/main.rs`, ticker loop) while the
//!     segments kill -> lost, lost -> proposal,
//!     proposal -> verdict, verdict -> resident are timestamped.
//!     The ADMIT verdict is recorded by this driver *immediately*
//!     when the proposal appears: the proposal -> verdict
//!     segment therefore measures in-process bookkeeping only
//!     (submit + record_verdict), ~0 by construction, and says
//!     nothing about an external gate's latency.
//!   * `rebuild` — long-spine mirror economics + the §8.2
//!     flush cost: a spine is generated honestly on disk with
//!     `Spine::record` (which flushes synchronously per record),
//!     timing every `record()` call; then `MirrorStore::rebuild`,
//!     `verify_against`, `MirrorStore::open`, and
//!     `collect_spine_events` are timed over that spine.
//!
//! Output: header comments (`# ...`) stating the parameters,
//! then CSV rows
//! `series,config,n,min_us,p50_us,p95_us,max_us,mean_us`, then
//! per-sample raw rows (`raw:<series>`, n=1) where feasible,
//! plus `# scalar,...` comment lines for single quantities
//! (file sizes, counts, wall times).
//!
//! Run in --release; debug numbers are meaningless:
//!   cargo run -p gaia-serve --example g8_actuate_rebuild --release -- --quick
//!   cargo run -p gaia-serve --example g8_actuate_rebuild --release -- roundtrip
//!   cargo run -p gaia-serve --example g8_actuate_rebuild --release -- rebuild

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use gaia_api::mirror::collect_spine_events;
use gaia_api::{EventHub, Intent, MemoryWorkStore, MirrorStore, Verdict, WorkStore};
use gaia_serve::{Composition, ProposalStore, Watcher};
use hearth_core::{Budget, Declared, LostReason, GIB};
use hearth_serve::server::{free_port, ServerSpec};
use hearth_serve::Supervisor;
use hearth_store::{EventRef, Spine, Transition};

// ------------------------------------------------------------------
// Small stats helper
// ------------------------------------------------------------------

fn percentile(sorted: &[u64], p: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    // Nearest-rank.
    let rank = (p * sorted.len()).div_ceil(100).max(1);
    sorted[rank - 1]
}

fn print_stats(series: &str, config: &str, samples: &[u64]) {
    if samples.is_empty() {
        println!("{series},{config},0,0,0,0,0,0.0");
        return;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let mean = sorted.iter().sum::<u64>() as f64 / sorted.len() as f64;
    println!(
        "{series},{config},{},{},{},{},{},{mean:.1}",
        sorted.len(),
        sorted[0],
        percentile(&sorted, 50),
        percentile(&sorted, 95),
        sorted[sorted.len() - 1],
    );
}

fn print_raw(series: &str, config: &str, samples: &[u64]) {
    for v in samples {
        println!("raw:{series},{config},1,{v},{v},{v},{v},{v}.0");
    }
}

fn print_scalar(name: &str, value: impl std::fmt::Display) {
    println!("# scalar,{name},{value}");
}

fn us(d: Duration) -> u64 {
    d.as_micros() as u64
}

// ------------------------------------------------------------------
// Temp dirs (std-only; the example stays within gaia-serve's
// existing dependency set and invents no cleanup debt: every
// dir made here is removed before the trial / section ends,
// and composition supervisors are stop_all()-ed first so no
// fake server is orphaned)
// ------------------------------------------------------------------

static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn make_temp_dir(tag: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!(
        "gaia-g8-{tag}-{}-{n}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

// ------------------------------------------------------------------
// roundtrip
// ------------------------------------------------------------------

/// The fake serving child, copied from the acceptance tests:
/// a tiny HTTP server answering 200 `{}` on any GET, including
/// the supervisor's `/health` probe, parsing `--port` from argv.
const FAKE_SERVER: &str = r#"#!/usr/bin/env python3
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

port = int(sys.argv[sys.argv.index("--port") + 1])

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = b"{}"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass

HTTPServer(("127.0.0.1", port), Handler).serve_forever()
"#;

fn build_composition(dir: &std::path::Path) -> Composition {
    let spine = Spine::open(&dir.join("spine")).expect("open spine");
    let mirror = Arc::new(Mutex::new(
        MirrorStore::open(&dir.join("mirror.jsonl")).expect("open mirror"),
    ));
    let proposals = ProposalStore::open(&dir.join("proposals.jsonl")).expect("open proposals");
    let watcher = Watcher::new(["fake".to_string()]);
    let hub = EventHub::new(256);
    let supervisor = Supervisor::new(
        spine,
        Budget {
            total_bytes: 24 * GIB,
            reserve_bytes: 2 * GIB,
        },
        vec![Declared {
            model: "fake".to_string(),
            weights_bytes: GIB,
            kv_bytes: 0,
        }],
    );
    Composition::new(supervisor, mirror, watcher, proposals, hub)
}

fn fake_spec(dir: &std::path::Path) -> ServerSpec {
    let script = dir.join("fake_server.py");
    std::fs::write(&script, FAKE_SERVER).expect("write fake server");
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))
            .expect("chmod fake server");
    }
    let gguf = dir.join("fake.gguf");
    std::fs::write(&gguf, b"not a real gguf").expect("write dummy gguf");
    let mut spec = ServerSpec::new("fake", &gguf, free_port().expect("free port"));
    spec.binary = script;
    spec.log_dir = dir.to_path_buf();
    spec
}

/// The most recent `loading` pid recorded for `fake`, if any.
fn latest_loading_pid(comp: &Composition) -> Option<u64> {
    comp.spine()
        .why("fake")
        .into_iter()
        .filter(|e| e.kind == "loading")
        .filter_map(|e| e.facts["pid"].as_u64())
        .next()
}

struct TrialSegments {
    detection: u64,
    loss_to_proposal: u64,
    proposal_to_verdict: u64,
    verdict_to_resident: u64,
    total: u64,
}

fn one_trial(trial: u64) -> TrialSegments {
    let dir = make_temp_dir("roundtrip");
    let mut comp = build_composition(&dir);
    let spec = fake_spec(&dir);
    comp.note_spec(spec.clone());
    comp.supervisor()
        .start(spec)
        .expect("start fake through the supervisor");

    // Bring-up (not a measured segment): tick fast, as the
    // acceptance tests do, until the spine calls it resident.
    let mut resident = false;
    for _ in 0..300 {
        comp.tick_once().expect("tick during bring-up");
        if comp.spine().latest("fake").is_some_and(|e| e.kind == "resident") {
            resident = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(resident, "trial {trial}: fake never became resident");
    let first_pid = latest_loading_pid(&comp).expect("resident model has a loading pid");
    assert!(comp.proposals().is_empty(), "no proposal before the kill");

    // -- Kill, then drive the shipped 500 ms cadence -----------
    let t_kill = Instant::now();
    let status = std::process::Command::new("kill")
        .args(["-9", &first_pid.to_string()])
        .status()
        .expect("spawn kill");
    assert!(status.success(), "trial {trial}: kill -9 {first_pid} failed");

    let cadence = Duration::from_millis(500);
    let mut t_loss: Option<Instant> = None;
    let mut t_proposal: Option<Instant> = None;
    for _ in 0..60 {
        std::thread::sleep(cadence);
        comp.tick_once().expect("tick after kill");
        let now = Instant::now();
        // tick_once is one composition step (supervisor tick +
        // mirror poll + watcher pass): a loss and its proposal
        // first observed in the same step share that step's
        // timestamp, so loss->proposal is honestly 0 at the
        // shipped step granularity when they land together.
        if t_loss.is_none()
            && comp.spine().latest("fake").is_some_and(|e| e.kind == "lost")
        {
            t_loss = Some(now);
        }
        if !comp.proposals().is_empty() {
            t_proposal = Some(now);
            break;
        }
    }
    let t_loss = t_loss.unwrap_or_else(|| panic!("trial {trial}: loss never recorded"));
    let t_proposal =
        t_proposal.unwrap_or_else(|| panic!("trial {trial}: no proposal was recorded"));
    let proposal = comp.proposals()[0].clone();
    assert_eq!(
        proposal.tool, "hearth.restart",
        "trial {trial}: proposal is not a restart proposal"
    );

    // -- Verdict: recorded immediately, by this driver ----------
    // This segment is bookkeeping by construction (submit a
    // verdict-less intent for the proposal, then record the
    // ADMIT verdict) — it is NOT an external gate's latency.
    let mut store = MemoryWorkStore::new();
    let submitted = store
        .submit(Intent {
            tool: proposal.tool.clone(),
            args: proposal.args.clone(),
            reason: proposal.reason.clone(),
            task_id: format!("g8-roundtrip-{trial}"),
            journal_seq: 9000 + trial,
            class: "execute".to_string(),
            verdict: None,
        })
        .expect("submit proposal intent");
    store
        .record_verdict(&submitted.id, Verdict::Admit)
        .expect("record ADMIT verdict");
    let t_verdict = Instant::now();

    // -- Actuation: act on the admitted work, then cadence-tick
    //    until the spine shows Resident again with a new pid ---
    let done = comp
        .actuate_work(&mut store, &submitted.id)
        .unwrap_or_else(|e| panic!("trial {trial}: actuate_work failed: {e}"));
    let outcome = done
        .result
        .as_ref()
        .and_then(|r| r["outcome"].as_str())
        .unwrap_or("")
        .to_string();
    assert_eq!(
        outcome, "executed",
        "trial {trial}: actuation did not execute: {:?}",
        done.result
    );

    let mut t_resident: Option<Instant> = None;
    for _ in 0..60 {
        std::thread::sleep(cadence);
        comp.tick_once().expect("tick after actuation");
        let now = Instant::now();
        let back = comp.spine().latest("fake").is_some_and(|e| e.kind == "resident")
            && latest_loading_pid(&comp).is_some_and(|pid| pid != first_pid);
        if back {
            t_resident = Some(now);
            break;
        }
    }
    let t_resident =
        t_resident.unwrap_or_else(|| panic!("trial {trial}: never resident again"));

    let segs = TrialSegments {
        detection: us(t_loss.duration_since(t_kill)),
        loss_to_proposal: us(t_proposal.duration_since(t_loss)),
        proposal_to_verdict: us(t_verdict.duration_since(t_proposal)),
        verdict_to_resident: us(t_resident.duration_since(t_verdict)),
        total: us(t_resident.duration_since(t_kill)),
    };

    // Cleanup: stop children (records the unload, kills the
    // restarted fake) before the temp dir goes away.
    comp.supervisor().stop_all();
    drop(comp);
    let _ = std::fs::remove_dir_all(&dir);
    segs
}

fn run_roundtrip(config: &str, trials: u64) {
    let mut detection = Vec::new();
    let mut loss_to_proposal = Vec::new();
    let mut proposal_to_verdict = Vec::new();
    let mut verdict_to_resident = Vec::new();
    let mut total = Vec::new();
    for trial in 0..trials {
        let s = one_trial(trial);
        detection.push(s.detection);
        loss_to_proposal.push(s.loss_to_proposal);
        proposal_to_verdict.push(s.proposal_to_verdict);
        verdict_to_resident.push(s.verdict_to_resident);
        total.push(s.total);
    }
    print_stats("roundtrip_detection_kill_to_lost", config, &detection);
    print_stats("roundtrip_loss_to_proposal", config, &loss_to_proposal);
    print_stats(
        "roundtrip_proposal_to_verdict_bookkeeping",
        config,
        &proposal_to_verdict,
    );
    print_stats(
        "roundtrip_actuation_verdict_to_resident",
        config,
        &verdict_to_resident,
    );
    print_stats("roundtrip_total_kill_to_resident", config, &total);
    print_raw("roundtrip_detection_kill_to_lost", config, &detection);
    print_raw("roundtrip_loss_to_proposal", config, &loss_to_proposal);
    print_raw(
        "roundtrip_proposal_to_verdict_bookkeeping",
        config,
        &proposal_to_verdict,
    );
    print_raw(
        "roundtrip_actuation_verdict_to_resident",
        config,
        &verdict_to_resident,
    );
    print_raw("roundtrip_total_kill_to_resident", config, &total);
}

// ------------------------------------------------------------------
// rebuild
// ------------------------------------------------------------------

fn run_rebuild(config: &str, total_events: usize, models: usize) {
    let dir = make_temp_dir("rebuild");
    let spine = Spine::open(&dir.join("spine")).expect("open spine on disk");

    // Generate the long spine honestly: per model, a causal
    // chain of synthetic lifecycle events — declared once,
    // then loading -> resident -> lost{process_exited}
    // repeating — each record caused_by the model's previous
    // event. Event shapes used are exactly the Transition
    // variants named here; nothing is written to the spine by
    // any path other than Spine::record.
    let mut prev: HashMap<String, EventRef> = HashMap::new();
    let mut per_model_count: HashMap<String, u64> = HashMap::new();
    let mut record_us: Vec<u64> = Vec::with_capacity(total_events);
    let gen_start = Instant::now();
    for idx in 0..total_events {
        let model = format!("model-{}", idx % models);
        let count = {
            let c = per_model_count.entry(model.clone()).or_insert(0);
            *c += 1;
            *c
        };
        let transition = if count == 1 {
            Transition::Declared {
                vram_bytes: GIB,
                admitted: true,
            }
        } else {
            match count % 3 {
                2 => Transition::Loading {
                    pid: 2000 + (idx as u32),
                    endpoint: "127.0.0.1:9100".to_string(),
                },
                0 => Transition::Resident {
                    endpoint: "127.0.0.1:9100".to_string(),
                    warmup_ms: 25,
                },
                _ => Transition::Lost {
                    reason: LostReason::ProcessExited,
                },
            }
        };
        let caused: Vec<EventRef> = prev.get(&model).cloned().into_iter().collect();
        let t0 = Instant::now();
        let ev = spine
            .record(&model, &transition, &caused)
            .expect("record spine event");
        record_us.push(us(t0.elapsed()));
        prev.insert(model, ev);
    }
    let gen_wall = gen_start.elapsed();
    print_scalar("spine_generation_wall_us", us(gen_wall));
    print_scalar("spine_final_seq", spine.seq());
    print_stats("spine_record_flush_per_record", config, &record_us);
    print_raw("spine_record_flush_per_record", config, &record_us);

    // collect_spine_events over the long spine (x3).
    let mut collect_us = Vec::new();
    let mut collected = 0usize;
    for _ in 0..3 {
        let t0 = Instant::now();
        let events = collect_spine_events(&spine);
        collect_us.push(us(t0.elapsed()));
        collected = events.len();
    }
    print_scalar("collect_spine_events_count", collected);
    print_stats("collect_spine_events", config, &collect_us);
    print_raw("collect_spine_events", config, &collect_us);

    // Mirror rebuild (x3), verify, and open/load.
    let mirror_path = dir.join("mirror.jsonl");
    let mut mirror = MirrorStore::open(&mirror_path).expect("open empty mirror");
    let mut rebuild_us = Vec::new();
    let mut rebuild_count = 0usize;
    for _ in 0..3 {
        let t0 = Instant::now();
        rebuild_count = mirror.rebuild(&spine).expect("mirror rebuild");
        rebuild_us.push(us(t0.elapsed()));
    }
    print_scalar("mirror_rebuild_entry_count", rebuild_count);
    print_scalar("mirror_entries_after_rebuild", mirror.entries().len());
    print_stats("mirror_rebuild", config, &rebuild_us);
    print_raw("mirror_rebuild", config, &rebuild_us);

    let mut verify_us = Vec::new();
    let mut verify_failures = 0usize;
    for _ in 0..3 {
        let t0 = Instant::now();
        let failures = mirror.verify_against(&spine);
        verify_us.push(us(t0.elapsed()));
        verify_failures = failures.len();
    }
    print_scalar("mirror_verify_failures", verify_failures);
    print_stats("mirror_verify_against", config, &verify_us);
    print_raw("mirror_verify_against", config, &verify_us);

    let size = std::fs::metadata(&mirror_path).map(|m| m.len()).unwrap_or(0);
    print_scalar("mirror_file_bytes", size);
    drop(mirror);

    let mut open_us = Vec::new();
    let mut open_entries = 0usize;
    for _ in 0..3 {
        let t0 = Instant::now();
        let reopened = MirrorStore::open(&mirror_path).expect("reopen mirror");
        open_us.push(us(t0.elapsed()));
        open_entries = reopened.entries().len();
        drop(reopened);
    }
    print_scalar("mirror_open_entry_count", open_entries);
    print_stats("mirror_open_load", config, &open_us);
    print_raw("mirror_open_load", config, &open_us);

    drop(spine);
    let _ = std::fs::remove_dir_all(&dir);
}

// ------------------------------------------------------------------
// main
// ------------------------------------------------------------------

fn cpu_model() -> String {
    std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|text| {
            text.lines()
                .find(|l| l.starts_with("model name"))
                .and_then(|l| l.split_once(':'))
                .map(|(_, v)| v.trim().replace(',', " "))
        })
        .unwrap_or_else(|| "unknown".to_string())
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let quick = args.iter().any(|a| a == "--quick");
    let subcommand = args
        .iter()
        .find(|a| !a.starts_with('-'))
        .map(|s| s.as_str())
        .unwrap_or("all");
    if !["all", "roundtrip", "rebuild"].contains(&subcommand) {
        eprintln!("usage: g8_actuate_rebuild [roundtrip|rebuild] [--quick]");
        std::process::exit(2);
    }
    let (config, trials, spine_events): (&str, u64, usize) = if quick {
        ("quick", 2, 500)
    } else {
        ("full", 8, 10_000)
    };
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    let parallelism = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(0);

    println!("# g8_actuate_rebuild (G-8, Worker B: actuation round-trip + mirror rebuild)");
    println!("# config={config} profile={profile} subcommand={subcommand}");
    println!("# cpu={} vcpus_available_parallelism={parallelism}", cpu_model());
    println!("# roundtrip: trials={trials} models=1(fake python http server) tick_cadence_ms=500 (shipped gaia-serve ticker) bringup_tick_ms=20 (unmeasured segment)");
    println!("# roundtrip_verdict_note: ADMIT is recorded by this driver immediately when the proposal appears; proposal_to_verdict measures submit+record_verdict bookkeeping only, ~0 by construction, not external gate latency");
    println!("# rebuild: spine_events={spine_events} models=8 shapes=declared,loading,resident,lost{{process_exited}} chained causally per model via Spine::record on disk (synchronous flush per record, G-2 section 8.2)");
    println!("# rebuild_note: collect/rebuild see chain-reachable history only; hearth-store why()/provenance traces are capped at 64 nodes per model, so collected counts below the recorded total are the documented limit, not missing data invented away");
    println!("series,config,n,min_us,p50_us,p95_us,max_us,mean_us");

    if subcommand == "all" || subcommand == "roundtrip" {
        run_roundtrip(config, trials);
    }
    if subcommand == "all" || subcommand == "rebuild" {
        run_rebuild(config, spine_events, 8);
    }
}
