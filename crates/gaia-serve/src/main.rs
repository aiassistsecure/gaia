//! The `gaia` binary — the composition, running.
//!
//! ```text
//! gaia --model NAME=PATH [--model NAME=PATH …]
//!      [--home DIR] [--port N] [--events-port N]
//!      [--binary PATH] [--total-gib N]
//!      [--watch NAME …] [--watch-all]
//! ```
//!
//! This binary is where the G-2 §5.2.1 trust boundary stops
//! being a sentence in a design document and becomes a wiring
//! fact. Upstream's `hearth` binary composes the same public
//! supervisor API into a loop that, on its own authority,
//! restarts children that died unexpectedly — that behaviour is
//! upstream's, it stays in the upstream binary untouched, and
//! a deployment running `hearth` gets it, honestly labelled as
//! upstream behaviour.
//!
//! A deployment running `gaia` gets the gated behaviour
//! instead, and the difference is exactly one loop: the ticker
//! thread in this file calls [`gaia_serve::Composition::tick_once`]
//! and nothing else supervisory. It never calls the supervisor's
//! unsupervised restart method — the method the prose here
//! names as restart_lost, bare, on purpose: the composition's
//! own acceptance test scans this crate's sources for the call
//! shape, and this file must not trip its own scan by quoting
//! the thing it refuses to do. Losses observed here become
//! proposals recorded by the watcher; whether a proposal is
//! ever acted on is the SI gate's decision, journaled in SI —
//! and since G-6, the ticker also drains the work store after
//! each step, handing every ADMIT-ted record to
//! [`gaia_serve::Composition::actuate_work`]. Since S-2b it
//! also runs the seam egress pass after each step, POSTing
//! every proposal SI has not yet ACKed to SI's proposal
//! ingest (`--si-addr`) and recording the dispositions in
//! `egress.jsonl`. The gate's
//! decision is the only path from observation to action, and
//! this loop is where that path is walked.
//!
//! Three listeners/loops, three separate concerns:
//!
//!   * The **ticker** (500 ms) steps the composition: probe,
//!     mirror, propose, then act on admitted work. It also
//!     owns shutdown, because it is the loop that can stop
//!     children and record it cleanly.
//!   * The **agency listener** (`--port`) answers the gaia
//!     work routes and the structured residency route. It holds
//!     no inference proxying of its own in this composition —
//!     requests that are not residency go to
//!     `gaia_api::gateway::decide` over the work store.
//!   * The **events listener** (`--events-port`) is the G-4
//!     SSE server on its own budget, fed by the shared mirror.
//!     It is not the agency listener and does not draw on any
//!     shared connection pool.
//!
//! Loopback is the entire security model, exactly as upstream:
//! both listeners bind `127.0.0.1`, and there is no
//! authentication anywhere in this binary.

use std::io::Write;
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use gaia_serve::{
    Composition, EgressStore, HttpProposalSink, Leash, LeashFeed, OutcomeStore, ProposalStore,
    SharedMirror, Watcher,
};
use hearth_core::{Budget, Declared};
use hearth_pull::PullConfig;
use hearth_serve::server::{free_port, ServerSpec};
use hearth_serve::Supervisor;
use hearth_store::Spine;

/// SIGINT/SIGTERM latch. The handler only flips a bool — all
/// real work (stopping children, recording `unloaded`, flushing
/// the spine) happens on the ticker thread, because a signal
/// handler that touches a database is a signal handler that
/// corrupts one. This mirrors upstream's latch for the same
/// reason upstream has one.
static SHUTDOWN: AtomicBool = AtomicBool::new(false);

extern "C" fn on_signal(_sig: i32) {
    SHUTDOWN.store(true, Ordering::SeqCst);
}

fn install_signal_handlers() {
    // SAFETY: on_signal is async-signal-safe — it writes one
    // atomic bool and does nothing else.
    unsafe {
        libc::signal(libc::SIGINT, on_signal as *const () as libc::sighandler_t);
        libc::signal(libc::SIGTERM, on_signal as *const () as libc::sighandler_t);
    }
}

/// Print the usage message on stderr and exit 2 — the exit
/// code upstream uses for a invocation that cannot be parsed,
/// kept so scripts driving either binary read the same signal.
fn usage_exit() -> ! {
    eprintln!(
        "usage: gaia --model NAME=PATH [--model NAME=PATH …] \
         [--home DIR] [--port N] [--events-port N] [--binary PATH] \
         [--total-gib N] [--watch NAME …] [--watch-all] \
         [--si-addr HOST:PORT]"
    );
    std::process::exit(2);
}

/// A setup failure that is not a usage problem: say what failed
/// on stderr and exit 1.
fn fail(message: String) -> ! {
    eprintln!("gaia: {message}");
    std::process::exit(1);
}

/// Parsed command line. Defaults that come from the
/// environment are resolved in [`parse_args`] before the flags
/// are read, so a flag always beats its env var — an operator
/// typing a flag is expressing intent now, while an env var is
/// standing configuration (upstream's rule, kept).
struct Args {
    home: PathBuf,
    models: Vec<(String, PathBuf)>,
    port: u16,
    events_port: Option<u16>,
    binary: PathBuf,
    total_gib: u64,
    watch: Vec<String>,
    watch_all: bool,
    /// Where SI's proposal ingest listens (`HOST:PORT`), if
    /// the operator wired the seam. `None` is a legitimate
    /// deployment: proposals accumulate un-ACKed in the egress
    /// log's pending set (G-9 §13.1), they are not dropped.
    si_addr: Option<String>,
}

/// Take a flag's value: the inline `--flag=value` form if one
/// was given, otherwise the next argv entry. A missing or empty
/// value is malformed, not a default.
fn take_value(argv: &[String], i: &mut usize, inline: Option<String>) -> String {
    match inline {
        Some(v) => {
            if v.is_empty() {
                usage_exit();
            }
            v
        }
        None => {
            *i += 1;
            match argv.get(*i) {
                Some(v) if !v.is_empty() => v.clone(),
                _ => usage_exit(),
            }
        }
    }
}

/// Parse a numeric flag value; anything that does not parse is
/// malformed usage, exit 2 — never a silent fallback to the
/// default, which would start a fleet on a port or a budget
/// nobody asked for.
fn parse_num<T: std::str::FromStr>(raw: &str) -> T {
    match raw.parse::<T>() {
        Ok(n) => n,
        Err(_) => usage_exit(),
    }
}

fn parse_args() -> Args {
    let mut args = Args {
        home: match std::env::var("GAIA_HOME") {
            Ok(h) if !h.is_empty() => PathBuf::from(h),
            _ => {
                let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
                PathBuf::from(home).join(".gaia")
            }
        },
        models: Vec::new(),
        port: match std::env::var("GAIA_PORT") {
            Ok(v) if !v.is_empty() => parse_num(&v),
            _ => 11435,
        },
        events_port: None,
        binary: PathBuf::from("llama-server"),
        total_gib: 24,
        watch: Vec::new(),
        watch_all: false,
        si_addr: match std::env::var("GAIA_SI_ADDR") {
            Ok(v) if !v.is_empty() => Some(v),
            _ => None,
        },
    };

    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < argv.len() {
        let arg = &argv[i];
        // Both spellings, as upstream's flag helpers accept:
        // `--port 11435` and `--port=11435`. Splitting on the
        // FIRST `=` is what leaves a `--model` value's own
        // `NAME=PATH` equals sign intact.
        let (flag, inline) = match arg.split_once('=') {
            Some((f, v)) => (f, Some(v.to_string())),
            None => (arg.as_str(), None),
        };
        match flag {
            "--home" => args.home = PathBuf::from(take_value(&argv, &mut i, inline)),
            "--model" => {
                let raw = take_value(&argv, &mut i, inline);
                match raw.split_once('=') {
                    Some((name, path)) if !name.is_empty() && !path.is_empty() => {
                        args.models.push((name.to_string(), PathBuf::from(path)));
                    }
                    _ => usage_exit(),
                }
            }
            "--port" => args.port = parse_num(&take_value(&argv, &mut i, inline)),
            "--events-port" => {
                args.events_port = Some(parse_num(&take_value(&argv, &mut i, inline)))
            }
            "--binary" => args.binary = PathBuf::from(take_value(&argv, &mut i, inline)),
            "--total-gib" => args.total_gib = parse_num(&take_value(&argv, &mut i, inline)),
            "--watch" => {
                let name = take_value(&argv, &mut i, inline);
                args.watch.push(name);
            }
            "--watch-all" => {
                if inline.is_some() {
                    usage_exit();
                }
                args.watch_all = true;
            }
            "--si-addr" => args.si_addr = Some(take_value(&argv, &mut i, inline)),
            _ => usage_exit(),
        }
        i += 1;
    }

    // At least one model is required: a composition with an
    // empty roster has nothing to supervise, nothing to watch,
    // and nothing honest to serve.
    if args.models.is_empty() {
        usage_exit();
    }
    args
}

/// The ticker: step the composition every 500 ms, report what
/// each step actually did, drain admitted work, and own
/// shutdown.
///
/// The counts logged are the [`gaia_serve::TickOutcome`]'s own
/// counts, read off the struct — a line that says "2
/// transition(s), 1 proposal(s)" is a measurement of one step,
/// not narration. Quiet steps log nothing, following upstream's
/// ticker; a step that produced proposals logs one line per
/// proposal, because a proposal is a thing the gate will be
/// asked about and its reason belongs in the operator's log.
///
/// The boundary, again, in the code that embodies it: this
/// loop calls `tick_once` only. See the module docs.
///
/// The drain (G-6): after each step, the work store's records
/// are scanned for `Admitted` ones and each is handed to
/// [`Composition::actuate_work`]. A record the gate has not
/// admitted is never in the scan's output — the claim inside
/// `actuate_work` re-checks regardless, so the scan is a
/// finding aid, not the gate. Lock order is composition →
/// work store wherever both are held; the agency handler
/// takes the two locks in sequence, never nested, so no
/// cycle exists.
fn ticker_loop(
    comp: Arc<Mutex<Composition>>,
    work_store: Arc<Mutex<gaia_api::MemoryWorkStore>>,
) {
    loop {
        std::thread::sleep(Duration::from_millis(500));
        if SHUTDOWN.load(Ordering::SeqCst) {
            // process::exit skips destructors, which is why
            // stop_all is called explicitly first: it is the
            // version of stopping that records `unloaded` and
            // flushes the spine, not just the Drop backstop.
            let mut guard = comp.lock().unwrap_or_else(|p| p.into_inner());
            guard.supervisor().stop_all();
            std::process::exit(0);
        }
        let outcome = {
            let mut guard = comp.lock().unwrap_or_else(|p| p.into_inner());
            guard.tick_once()
        };
        match outcome {
            Ok(outcome) => {
                if outcome.transitions > 0 || !outcome.proposals.is_empty() {
                    eprintln!(
                        "gaia: {} transition(s), {} proposal(s)",
                        outcome.transitions,
                        outcome.proposals.len()
                    );
                }
                for proposal in &outcome.proposals {
                    eprintln!(
                        "gaia: proposal {} for {} — {}",
                        proposal.tool, proposal.model, proposal.reason
                    );
                }
            }
            // A failed step is reported and the loop keeps
            // going: one bad poll must not end supervision of
            // every other model on the box.
            Err(e) => eprintln!("gaia: tick failed: {e}"),
        }

        // The seam egress (S-2b): offer every un-ACKed
        // proposal to SI and record the answers. A pass that
        // attempted nothing logs nothing (the common case);
        // a failed pass — an ACK that could not be recorded —
        // is reported and the loop keeps going, because the
        // next pass re-sends and SI's dedup makes that free.
        let egress = {
            let mut guard = comp.lock().unwrap_or_else(|p| p.into_inner());
            guard.egress_pass()
        };
        match egress {
            Ok(pass) if pass.attempted > 0 => eprintln!(
                "gaia: egress — {} attempted, {} acked, {} rejected, {} escalated, \
                 {} failed, {} still pending",
                pass.attempted,
                pass.acked,
                pass.rejected,
                pass.escalated,
                pass.failed,
                pass.still_pending
            ),
            Ok(_) => {}
            Err(e) => eprintln!("gaia: egress failed: {e}"),
        }

        // The drain. The scan is taken and released before any
        // actuation: actuation can be slow (a re-pull moves
        // bytes), and the agency listener must not wait on the
        // store lock for its duration.
        let admitted: Vec<String> = {
            let store = work_store.lock().unwrap_or_else(|p| p.into_inner());
            store
                .records()
                .into_iter()
                .filter(|r| r.state == gaia_api::WorkState::Admitted)
                .map(|r| r.id)
                .collect()
        };
        for id in admitted {
            let acted = {
                let mut guard = comp.lock().unwrap_or_else(|p| p.into_inner());
                let mut store = work_store.lock().unwrap_or_else(|p| p.into_inner());
                guard.actuate_work(&mut *store, &id)
            };
            match acted {
                Ok(rec) => {
                    let outcome = rec
                        .result
                        .as_ref()
                        .and_then(|r| r["outcome"].as_str())
                        .unwrap_or("unknown");
                    eprintln!("gaia: work {} acted on — outcome {outcome}", rec.id);
                }
                // A claim that races a verdict or an actuator
                // that cannot read its args: reported, and the
                // loop keeps going — one bad work order must
                // not end supervision either.
                Err(e) => eprintln!("gaia: work {id} not acted on: {e}"),
            }
        }
    }
}

/// Serve one agency connection: read the request, answer the
/// structured residency route from the spine if that is what
/// it is, answer the leash routes from the composition's
/// replica if that is what it is, and hand everything else
/// to the gaia work gateway over the shared work store. The
/// response is written back in upstream's rendered shape —
/// the same `render_response` upstream's gateway loop uses.
fn handle_agency(
    mut stream: TcpStream,
    comp: Arc<Mutex<Composition>>,
    work_store: Arc<Mutex<gaia_api::MemoryWorkStore>>,
) {
    let req = match hearth_api::http::read_request(&stream) {
        Ok(r) => r,
        Err(e) => {
            let body = serde_json::json!({ "error": { "message": e } }).to_string();
            let _ =
                stream.write_all(hearth_api::http::render_response(400, &body, None).as_bytes());
            return;
        }
    };

    // Residency first: that route needs the spine, which the
    // work gateway deliberately does not hold (the serving
    // split in gaia-api's residency module). The composition
    // lock is held only for the duration of the call — the
    // response is owned, so the lock is released before any
    // socket write.
    let resp = {
        let guard = comp.lock().unwrap_or_else(|p| p.into_inner());
        gaia_api::residency::handle(&req, guard.spine())
    };
    let resp = match resp {
        Some(r) => r,
        None => {
            // Leash routes next: they write the replica the
            // composition holds, so — like residency — they
            // are answered from the composition, not the work
            // gateway. The response is owned, so the lock is
            // released before any socket write.
            let leash_resp = {
                let mut guard = comp.lock().unwrap_or_else(|p| p.into_inner());
                let mut sink = guard.leash_sink();
                gaia_api::leash::handle(&req, &mut sink)
            };
            match leash_resp {
                Some(r) => r,
                None => {
                    let mut store = work_store.lock().unwrap_or_else(|p| p.into_inner());
                    gaia_api::gateway::decide(&req, &mut *store)
                }
            }
        }
    };

    let _ = stream.write_all(
        hearth_api::http::render_response(resp.status, &resp.body, resp.retry_after).as_bytes(),
    );
}

fn main() {
    let args = parse_args();
    // The events port defaults to the agency port + 1, computed
    // only after both flags are known; an agency port with no
    // room above it is malformed usage, not a wrap to port 0.
    let events_port = match args.events_port {
        Some(p) => p,
        None => match args.port.checked_add(1) {
            Some(p) => p,
            None => usage_exit(),
        },
    };
    install_signal_handlers();

    let home = args.home.clone();
    if let Err(e) = std::fs::create_dir_all(&home) {
        fail(format!("creating home {}: {e}", home.display()));
    }
    let log_dir = home.join("logs");
    if let Err(e) = std::fs::create_dir_all(&log_dir) {
        fail(format!("creating log dir {}: {e}", log_dir.display()));
    }

    let spine =
        Spine::open(&home.join("spine")).unwrap_or_else(|e| fail(format!("opening spine: {e}")));
    let mirror = Arc::new(Mutex::new(
        gaia_api::MirrorStore::open(&home.join("mirror.jsonl"))
            .unwrap_or_else(|e| fail(format!("opening mirror: {e}"))),
    ));
    let proposals = ProposalStore::open(&home.join("proposals.jsonl"))
        .unwrap_or_else(|e| fail(format!("opening proposal store: {e}")));
    let hub = gaia_api::EventHub::new(256);

    // Watcher scope is opt-in per G-2 §5.5: a model is watched
    // only if the operator named it with --watch, or named
    // everything with --watch-all. The default is out — a model
    // nobody opted in is invisible to the watcher, however it
    // dies.
    let scope: Vec<String> = if args.watch_all {
        args.models.iter().map(|(name, _)| name.clone()).collect()
    } else {
        args.watch.clone()
    };
    let watcher = Watcher::new(scope);

    // Declarations: the weights are the file's actual size.
    // kv_bytes is 0, honestly: upstream's `cmd_up` estimates KV
    // bytes from the configured context and parallelism, and
    // this composition does not replicate that estimate — so
    // admission here accounts weights only, and a reader of
    // this code (or of a budget refusal) should know the KV
    // term is absent rather than estimated badly.
    let mut declared = Vec::with_capacity(args.models.len());
    for (name, path) in &args.models {
        let weights_bytes = match std::fs::metadata(path) {
            Ok(m) => m.len(),
            Err(e) => fail(format!(
                "cannot read model {name} at {}: {e}",
                path.display()
            )),
        };
        declared.push(Declared {
            model: name.clone(),
            weights_bytes,
            kv_bytes: 0,
        });
    }
    let budget = Budget {
        total_bytes: args.total_gib * hearth_core::GIB,
        reserve_bytes: 2 * hearth_core::GIB,
    };

    let supervisor = Supervisor::new(spine, budget, declared);
    let mut comp = Composition::new(supervisor, mirror, watcher, proposals, hub);

    // The G-6 stores, file-backed under the same home as the
    // spine and the proposals: actuation outcomes and the
    // operator leash are records a restart must not lose —
    // the leash most of all, since a stop SI journaled must
    // still be in force after this process restarts, or the
    // restart it forbids happens behind the operator's back.
    comp.set_outcome_store(
        OutcomeStore::open(&home.join("outcomes.jsonl"))
            .unwrap_or_else(|e| fail(format!("opening outcome store: {e}"))),
    );
    let leash = Leash::open(&home.join("leash.jsonl"))
        .unwrap_or_else(|e| fail(format!("opening leash: {e}")));
    // The leash feed (S-4, G-9 §10.3): the replica's
    // watermark and activation state, seeded from the leash
    // it feeds and persisted beside it. Heartbeat stamps
    // are per-process by design, so a restarted binary's
    // seam models are stale until SI's feed speaks again —
    // fail-closed across restarts, as §15 S-4 requires.
    comp.set_leash_feed(
        LeashFeed::open(&home.join("leash-feed.json"), &leash)
            .unwrap_or_else(|e| fail(format!("opening leash feed: {e}"))),
    );
    comp.set_leash(leash);
    // Re-pulls land in <home>/blobs, upstream's `cmd_pull`
    // default; progress off because this binary's stderr is a
    // log, not a terminal.
    comp.set_pull_config(PullConfig {
        blobs_dir: home.join("blobs"),
        progress: false,
        verify_existing: false,
    });

    // The seam egress store (S-2b), file-backed under the
    // same home: which proposals SI answered is retry state,
    // and a restart that forgot it would re-send answered
    // proposals — SI's dedup would absorb them, but the
    // egress log exists so the fork does not have to lean on
    // that. The sink exists only if the operator named SI's
    // ingest address; without one the ticker's egress pass
    // sends nothing and says so once, here, not every 500 ms.
    comp.set_egress_store(
        EgressStore::open(&home.join("egress.jsonl"))
            .unwrap_or_else(|e| fail(format!("opening egress store: {e}"))),
    );
    match &args.si_addr {
        Some(addr) => {
            comp.set_egress_sink(Box::new(HttpProposalSink::new(addr)));
            eprintln!("gaia: seam egress to SI at {addr} (POST /gaia/proposals)");
        }
        None => eprintln!(
            "gaia: seam egress not configured (no --si-addr / GAIA_SI_ADDR); \
             proposals will accumulate un-ACKed"
        ),
    }

    // Start each declared model. A refusal is not a crash —
    // mirror upstream's `cmd_up`: say it and keep going, because
    // the rest of the fleet is still worth serving, and the
    // refused model stays declared and visible in residency.
    for (name, path) in &args.models {
        let port = match free_port() {
            Ok(p) => p,
            Err(e) => {
                eprintln!("gaia: {name} not started — no free port: {e}");
                continue;
            }
        };
        let mut spec = ServerSpec::new(name, path, port);
        spec.binary = args.binary.clone();
        spec.log_dir = log_dir.clone();
        // The composition remembers the spec it started the
        // model with: an admitted restart (G-6) re-runs
        // exactly this spec, and one it never noted cannot be
        // restart-actuated at all.
        comp.note_spec(spec.clone());
        match comp.supervisor().start(spec) {
            Ok(()) => eprintln!("gaia: {name} loading on 127.0.0.1:{port}"),
            Err(e) => eprintln!("gaia: {name} not started — {e}"),
        }
    }

    // The events listener, on its own port and its own budget.
    // The si source is a buffer this composition fills from
    // nothing: per G-2 §4.2, how SI-side events reach the fork
    // is a deployment choice of the SI adapter, and this binary
    // wires no SI ingest transport — the si source therefore
    // serves what is pushed in-process, which in this binary is
    // nothing yet. The buffer is attached to the hub so that if
    // an ingest is wired later, replay and live tail are already
    // the same history.
    let si_buffer = gaia_api::SiEventBuffer::new();
    si_buffer.attach_hub(comp.hub());
    let _events_server = gaia_api::EventsServer::bind(
        &format!("127.0.0.1:{events_port}"),
        Arc::new(SharedMirror(comp.mirror())),
        Arc::new(si_buffer),
        comp.hub(),
        gaia_api::EventsConfig::default(),
    )
    .unwrap_or_else(|e| fail(format!("binding events listener on {events_port}: {e}")));

    let comp = Arc::new(Mutex::new(comp));
    let work_store = Arc::new(Mutex::new(gaia_api::MemoryWorkStore::new()));

    // The ticker thread: the composition loop, the actuation
    // drain, and the owner of shutdown (see ticker_loop).
    {
        let comp = Arc::clone(&comp);
        let work_store = Arc::clone(&work_store);
        std::thread::spawn(move || ticker_loop(comp, work_store));
    }

    eprintln!(
        "gaia: agency on 127.0.0.1:{}, events on 127.0.0.1:{events_port}",
        args.port
    );

    // The agency listener: one thread per accepted connection,
    // following upstream's gateway loop shape.
    let server = hearth_api::Server::bind(&format!("127.0.0.1:{}", args.port))
        .unwrap_or_else(|e| fail(format!("binding agency listener: {e}")));
    for conn in server.incoming() {
        let stream = match conn {
            Ok(s) => s,
            Err(e) => {
                eprintln!("gaia: accept failed: {e}");
                continue;
            }
        };
        let comp = Arc::clone(&comp);
        let work_store = Arc::clone(&work_store);
        std::thread::spawn(move || handle_agency(stream, comp, work_store));
    }
}
