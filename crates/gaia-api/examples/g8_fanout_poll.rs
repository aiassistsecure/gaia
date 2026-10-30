//! G-8 measurement example: SSE fan-out cost and mirror poll latency.
//!
//! This binary measures the quantities G-2 §§4.3/4.4 label
//! UNMEASURED, on the `gaia-api` implementations as they stand:
//!
//! - `fanout` — [`EventHub`] publish cost as subscriber count
//!   grows (in-process), and [`EventsServer`] cost over real
//!   loopback TCP: publish→last-byte latency per subscriber,
//!   plus the server's thread and RSS cost per subscriber read
//!   from `/proc/self/status`.
//! - `poll` — [`poll_once`] latency in steady state (mirror
//!   caught up) and in catch-up (mirror behind by a fixed number
//!   of events), and [`collect_spine_events`] total time as the
//!   spine grows.
//!
//! Method, stated once here and echoed in the output header:
//!
//! - Every latency series is reported as
//!   `series,config,n,min_us,p50_us,p95_us,max_us,mean_us`
//!   (microseconds, one decimal; percentiles are nearest-rank).
//! - Warm-up work is done and discarded before every series.
//! - Hub publish timings are per-call `Instant` deltas around
//!   `hub.publish`. Consumers do not drain during the hub-level
//!   run: with a 256-event ring and 2000 publishes, overflow /
//!   resync accounting (via `HubStats`) is part of the result,
//!   and the point stands — the publisher never blocks.
//! - HTTP latency is approximate by construction: for each
//!   subscriber, (the instant its reader thread had counted all
//!   published `id:` blocks) − (the instant the publish loop
//!   ended). The serving loop drains the hub on a 10 ms poll,
//!   so figures carry that granularity. If a burst publish
//!   overfills the 256-event rings before the serving loop
//!   drains them (resync + close is the designed behaviour,
//!   §4.4), the burst outcome is reported as a comment and the
//!   latency series is taken from a paced run (1 event / ms)
//!   against a fresh server, labelled as such.
//! - Thread/RSS deltas are process-wide. Reader threads run in
//!   this same process, so the raw delta includes one reader
//!   thread per subscriber; the derived server-side thread cost
//!   subtracts those. RSS is an upper bound for the same reason.
//! - The poll spine is built with many models × short causal
//!   chains (Declared → Loading → Resident), because
//!   `collect_spine_events` walks causal chains (depth-capped):
//!   short chains keep every recorded event collectable, and the
//!   collected count is printed next to the recorded count so
//!   any gap is visible rather than assumed away.
//!
//! Usage:
//!   cargo run -p gaia-api --example g8_fanout_poll --release -- [--quick] [fanout|poll]
//!
//! No subcommand runs both. `--quick` shrinks every parameter
//! set for a smoke run; default (full) mode is the bounded set
//! the coordinator re-runs for final numbers. Build in
//! `--release`: debug timings would be meaningless.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::json;

use gaia_api::mirror::{collect_spine_events, poll_once};
use gaia_api::sse::EventsConfig;
use gaia_api::{EventFilter, EventHub, EventLog, EventsServer, MirrorStore, Source, StreamEvent};

use hearth_store::{Spine, Transition};

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

/// One synthetic `hearth`-source event, shaped like a spine
/// `resident` transition.
fn hearth_ev(seq: u64) -> StreamEvent {
    StreamEvent {
        source: Source::Hearth,
        seq,
        kind: "resident".to_string(),
        model: Some("g8-model".to_string()),
        task: None,
        facts: json!({ "endpoint": "127.0.0.1:8080", "warmup_ms": 1 }),
        collection: "spine".to_string(),
        row: json!({ "kind": "resident", "seq": seq }),
        row_kind: Some("resident".to_string()),
        hash: Some(format!("g8hash{seq}")),
        ts: Some(0.0),
    }
}

/// Emit one stats line for a series of nanosecond samples.
fn stats_line(series: &str, config: &str, samples_ns: &[u64]) {
    if samples_ns.is_empty() {
        println!("# {series},{config}: NO SAMPLES");
        return;
    }
    let mut s = samples_ns.to_vec();
    s.sort_unstable();
    let n = s.len();
    let pct = |p: f64| -> u64 {
        let idx = ((p / 100.0) * n as f64).ceil() as usize;
        s[idx.saturating_sub(1).min(n - 1)]
    };
    let mean = s.iter().map(|&v| v as f64).sum::<f64>() / n as f64;
    println!(
        "{},{},{},{:.1},{:.1},{:.1},{:.1},{:.1}",
        series,
        config,
        n,
        s[0] as f64 / 1000.0,
        pct(50.0) as f64 / 1000.0,
        pct(95.0) as f64 / 1000.0,
        s[n - 1] as f64 / 1000.0,
        mean / 1000.0
    );
}

/// Emit raw samples for the smaller series, one line each.
fn raw_lines(series: &str, config: &str, samples_ns: &[u64]) {
    for (i, v) in samples_ns.iter().enumerate() {
        println!("raw,{},{},{},{:.1}", series, config, i, *v as f64 / 1000.0);
    }
}

/// (thread count, VmRSS in KiB) of this process.
fn proc_stats() -> (usize, u64) {
    let text = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let mut threads = 0usize;
    let mut rss_kb = 0u64;
    for line in text.lines() {
        if let Some(v) = line.strip_prefix("Threads:") {
            threads = v.trim().parse().unwrap_or(0);
        }
        if let Some(v) = line.strip_prefix("VmRSS:") {
            rss_kb = v.trim().trim_end_matches("kB").trim().parse().unwrap_or(0);
        }
    }
    (threads, rss_kb)
}

fn mem_total_kb() -> u64 {
    let text = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
    for line in text.lines() {
        if let Some(v) = line.strip_prefix("MemTotal:") {
            return v.trim().trim_end_matches("kB").trim().parse().unwrap_or(0);
        }
    }
    0
}

// ---------------------------------------------------------------------------
// (a1) Hub-level fan-out
// ---------------------------------------------------------------------------

fn hub_fanout(quick: bool) {
    let ns: &[usize] = if quick {
        &[0, 1, 8]
    } else {
        &[0, 1, 8, 32, 64, 128]
    };
    let m: u64 = if quick { 200 } else { 2000 };
    println!(
        "# fanout_hub: ring_capacity=256 m_events={} subscriber_counts={:?}",
        m, ns
    );

    // Warm-up: discarded.
    {
        let hub = EventHub::new(256);
        let sub = hub.subscribe(Source::Hearth, EventFilter::default());
        for seq in 1..=200 {
            hub.publish(&hearth_ev(seq));
        }
        let _ = hub.drain(sub);
    }

    for &n in ns {
        let hub = EventHub::new(256);
        let mut ids = Vec::new();
        for _ in 0..n {
            ids.push(hub.subscribe(Source::Hearth, EventFilter::default()));
        }
        let mut samples: Vec<u64> = Vec::with_capacity(m as usize);
        let t0 = Instant::now();
        for seq in 1..=m {
            let ev = hearth_ev(seq);
            let s = Instant::now();
            hub.publish(&ev);
            samples.push(s.elapsed().as_nanos() as u64);
        }
        let total = t0.elapsed();
        let config = format!("n_subs={}_m={}", n, m);
        stats_line("hub_publish_latency", &config, &samples);
        stats_line("hub_publish_total", &config, &[total.as_nanos() as u64]);
        let stats = hub.stats();
        // Drain one subscriber to show ring behaviour at the end.
        let (ring_kept, resync_at) = match ids.first() {
            Some(&id) => {
                let d = hub.drain(id);
                (d.events.len(), d.resync_at)
            }
            None => (0, None),
        };
        println!(
            "# hub n_subs={} published_total={} overflow_episodes={} first_sub_ring_kept={} first_sub_resync_at={:?}",
            n, stats.published_total, stats.overflowed_total, ring_kept, resync_at
        );
    }
}

// ---------------------------------------------------------------------------
// (a2) HTTP-level fan-out
// ---------------------------------------------------------------------------

/// An empty history: HTTP subscribers here follow the live tail
/// only, so replay has nothing to say.
struct EmptyLog;

impl EventLog for EmptyLog {
    fn events_since(
        &self,
        _source: Source,
        _after_seq: u64,
        _filter: &EventFilter,
    ) -> Vec<StreamEvent> {
        Vec::new()
    }
    fn latest_seq(&self, _source: Source) -> u64 {
        0
    }
}

struct ReaderOutcome {
    received: usize,
    resync_seen: bool,
    /// When the reader had counted all expected events.
    completed_at: Option<Instant>,
}

/// One raw-TCP SSE subscriber: sends the follow request, reads
/// until it has counted `expect` event blocks, the peer closes,
/// or the read times out.
fn reader(addr: String, expect: u64) -> ReaderOutcome {
    let mut stream = match TcpStream::connect(addr.as_str()) {
        Ok(s) => s,
        Err(_) => {
            return ReaderOutcome {
                received: 0,
                resync_seen: false,
                completed_at: None,
            }
        }
    };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
    if stream
        .write_all(
            b"GET /gaia/v1/events?source=hearth&follow=1 HTTP/1.1\r\nhost: 127.0.0.1\r\n\r\n",
        )
        .is_err()
    {
        return ReaderOutcome {
            received: 0,
            resync_seen: false,
            completed_at: None,
        };
    }
    let _ = stream.flush();
    let mut text = String::new();
    let mut buf = [0u8; 8192];
    let mut received = 0usize;
    loop {
        if received as u64 >= expect {
            return ReaderOutcome {
                received,
                resync_seen: text.contains("event: resync"),
                completed_at: Some(Instant::now()),
            };
        }
        match stream.read(&mut buf) {
            Ok(0) => break, // peer closed (e.g. after resync)
            Ok(n) => {
                text.push_str(&String::from_utf8_lossy(&buf[..n]));
                received = text.matches("id: ").count();
            }
            Err(_) => break, // timeout or error
        }
    }
    ReaderOutcome {
        received,
        resync_seen: text.contains("event: resync"),
        completed_at: None,
    }
}

/// Run one HTTP fan-out configuration against a fresh server.
/// Returns per-subscriber completion latencies (ns since the
/// publish loop ended) for subscribers that received everything.
fn http_run(k: usize, events: u64, paced: bool) -> (Vec<u64>, Vec<ReaderOutcome>, u64) {
    let hub = EventHub::new(256);
    let server = EventsServer::bind(
        "127.0.0.1:0",
        Arc::new(EmptyLog),
        Arc::new(EmptyLog),
        hub.clone(),
        EventsConfig::default(),
    )
    .expect("bind events server");
    let addr = server.local_addr();

    let handles: Vec<_> = (0..k)
        .map(|_| {
            let addr = addr.clone();
            std::thread::spawn(move || reader(addr, events))
        })
        .collect();

    // Wait until the server counts every subscriber (bounded).
    let deadline = Instant::now() + Duration::from_secs(10);
    while server.active_connections() < k {
        assert!(
            Instant::now() < deadline,
            "only {}/{} subscribers connected",
            server.active_connections(),
            k
        );
        std::thread::sleep(Duration::from_millis(2));
    }
    // Let connection threads settle into their serving loops.
    std::thread::sleep(Duration::from_millis(50));
    let (threads1, rss1) = proc_stats();
    println!(
        "# http k={} paced={} threads_with_subs={} rss_kib_with_subs={}",
        k, paced, threads1, rss1
    );

    let p0 = Instant::now();
    for seq in 1..=events {
        hub.publish(&hearth_ev(seq));
        if paced {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    let publish_end = Instant::now();
    let publish_ns = p0.elapsed().as_nanos() as u64;

    let outcomes: Vec<ReaderOutcome> = handles
        .into_iter()
        .map(|h| h.join().expect("reader thread"))
        .collect();
    let latencies: Vec<u64> = outcomes
        .iter()
        .filter_map(|o| {
            o.completed_at
                .map(|t| t.duration_since(publish_end).as_nanos() as u64)
        })
        .collect();
    drop(server);
    // Give detached connection threads a moment to notice the
    // closed sockets before the next configuration baselines.
    std::thread::sleep(Duration::from_millis(300));
    (latencies, outcomes, publish_ns)
}

fn http_fanout(quick: bool) {
    let ks: &[usize] = if quick { &[1, 8] } else { &[1, 8, 32] };
    let events: u64 = if quick { 100 } else { 500 };
    println!(
        "# fanout_http: budget=32 (EventsConfig::default) events_per_run={} subscriber_counts={:?}",
        events, ks
    );
    println!(
        "# fanout_http method: latency = reader_thread_all_events_received - publish_loop_end; \
         serving loop drains hub every 10 ms, so granularity is ~10 ms"
    );

    for &k in ks {
        let (threads0, rss0) = proc_stats();
        let (latencies, outcomes, publish_ns) = http_run(k, events, false);
        let complete = outcomes.iter().filter(|o| o.completed_at.is_some()).count();
        let received: Vec<usize> = outcomes.iter().map(|o| o.received).collect();
        let resyncs = outcomes.iter().filter(|o| o.resync_seen).count();
        println!(
            "# http_burst k={} complete={}/{} received_per_sub={:?} resync_seen_by={} publish_loop_us={:.1}",
            k,
            complete,
            k,
            received,
            resyncs,
            publish_ns as f64 / 1000.0
        );
        if complete == k {
            stats_line(
                "http_recv_latency_burst",
                &format!("k={}_events={}", k, events),
                &latencies,
            );
        } else {
            // The burst overfilled the 256-event rings before the
            // serving loop's 10 ms drain could move them — the
            // designed §4.4 outcome. Take the latency series from
            // a paced publish against a fresh server instead.
            println!(
                "# http k={}: burst incomplete (ring overflow / resync by design); \
                 measuring latency with paced publish (1 event/ms)",
                k
            );
            let (lat2, out2, pub2) = http_run(k, events, true);
            let complete2 = out2.iter().filter(|o| o.completed_at.is_some()).count();
            println!(
                "# http_paced k={} complete={}/{} publish_loop_us={:.1}",
                k,
                complete2,
                k,
                pub2 as f64 / 1000.0
            );
            if complete2 == k {
                stats_line(
                    "http_recv_latency_paced",
                    &format!("k={}_events={}", k, events),
                    &lat2,
                );
            } else {
                println!(
                    "# http k={}: paced run also incomplete — no latency series claimed",
                    k
                );
            }
        }
        // Thread/RSS accounting for this K. The baseline was read
        // before this run's server existed; `http_run` printed
        // the with-subscribers figures. The server-side thread
        // derivation (delta − k reader threads) is stated in the
        // file header.
        let _ = (threads0, rss0);
    }

    // A dedicated thread/RSS pass at the largest K, baselined
    // immediately before its own server starts, so the delta is
    // attributable to that server + its subscribers alone.
    let k = *ks.last().unwrap();
    let (threads0, rss0) = proc_stats();
    let hub = EventHub::new(256);
    let server = EventsServer::bind(
        "127.0.0.1:0",
        Arc::new(EmptyLog),
        Arc::new(EmptyLog),
        hub.clone(),
        EventsConfig::default(),
    )
    .expect("bind events server for thread/rss pass");
    let addr = server.local_addr();
    let (threads_srv, rss_srv) = proc_stats();
    let handles: Vec<_> = (0..k)
        .map(|_| {
            let addr = addr.clone();
            std::thread::spawn(move || reader(addr, 1))
        })
        .collect();
    let deadline = Instant::now() + Duration::from_secs(10);
    while server.active_connections() < k {
        assert!(Instant::now() < deadline, "thread/rss pass: subs never all connected");
        std::thread::sleep(Duration::from_millis(2));
    }
    std::thread::sleep(Duration::from_millis(50));
    let (threads1, rss1) = proc_stats();
    println!(
        "# thread_rss k={} baseline_threads={} server_only_threads={} with_subs_threads={} \
         delta_threads_total={} reader_threads={} derived_server_threads_per_sub={:.2} \
         baseline_rss_kib={} server_only_rss_kib={} with_subs_rss_kib={} \
         rss_delta_kib={} rss_per_sub_kib={:.1} (upper bound: readers share this process)",
        k,
        threads0,
        threads_srv,
        threads1,
        threads1 as i64 - threads0 as i64,
        k,
        (threads1 as i64 - threads0 as i64 - k as i64).max(0) as f64 / k as f64,
        rss0,
        rss_srv,
        rss1,
        rss1 as i64 - rss0 as i64,
        (rss1 as i64 - rss0 as i64).max(0) as f64 / k as f64
    );
    // Release the readers by publishing their one event.
    hub.publish(&hearth_ev(1));
    for h in handles {
        let _ = h.join();
    }
    drop(server);
    std::thread::sleep(Duration::from_millis(300));
}

// ---------------------------------------------------------------------------
// (b) Mirror poll latency
// ---------------------------------------------------------------------------

/// Record a Declared → Loading → Resident chain for one model,
/// each event caused by the previous (the mirror.rs test pattern).
fn record_chain(spine: &Spine, model: &str) {
    let declared = spine
        .record(
            model,
            &Transition::Declared {
                vram_bytes: 4_000_000_000,
                admitted: true,
            },
            &[],
        )
        .expect("record declared");
    let loading = spine
        .record(
            model,
            &Transition::Loading {
                pid: 4242,
                endpoint: "127.0.0.1:8080".into(),
            },
            std::slice::from_ref(&declared),
        )
        .expect("record loading");
    spine
        .record(
            model,
            &Transition::Resident {
                endpoint: "127.0.0.1:8080".into(),
                warmup_ms: 43,
            },
            std::slice::from_ref(&loading),
        )
        .expect("record resident");
}

fn poll_measure(quick: bool) {
    let models_total: u64 = if quick { 100 } else { 667 };
    let total_events = models_total * 3;
    let collect_targets: &[u64] = if quick { &[100, 300] } else { &[100, 500, 1000, 2000] };
    let collect_reps = if quick { 2 } else { 5 };
    let steady_n = if quick { 20 } else { 200 };
    let catchup_n = if quick { 5 } else { 20 };
    let behind: usize = if quick { 30 } else { 100 };
    println!(
        "# poll: models={} events_per_model=3 total_events={} collect_targets={:?} \
         collect_reps={} steady_samples={} catchup_samples={} catchup_behind={}",
        models_total, total_events, collect_targets, collect_reps, steady_n, catchup_n, behind
    );

    let dir = std::env::temp_dir().join(format!("gaia-g8-poll-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let spine = Spine::open(&dir.join("spine")).expect("open spine in temp dir");
    let mirror_path = dir.join("mirror.jsonl");

    // Growth phase, with collect_spine_events timed at each
    // target size.
    let mut recorded: u64 = 0;
    let mut next_target = 0usize;
    for idx in 0..models_total {
        record_chain(&spine, &format!("g8-m{:04}", idx));
        recorded += 3;
        while next_target < collect_targets.len() && recorded >= collect_targets[next_target] {
            let target = collect_targets[next_target];
            let _ = collect_spine_events(&spine); // warm-up, discarded
            let mut samples = Vec::new();
            let mut collected = 0usize;
            for _ in 0..collect_reps {
                let s = Instant::now();
                let evs = collect_spine_events(&spine);
                samples.push(s.elapsed().as_nanos() as u64);
                collected = evs.len();
            }
            stats_line(
                "collect_spine",
                &format!("target={}_recorded={}_collected={}", target, recorded, collected),
                &samples,
            );
            next_target += 1;
        }
    }
    println!(
        "# poll growth done: recorded={} spine_seq={}",
        recorded,
        spine.seq()
    );

    let all = collect_spine_events(&spine);
    println!(
        "# poll collectable_events={} (of {} recorded)",
        all.len(),
        recorded
    );

    // Steady state: mirror fully caught up, poll finds nothing.
    let mut store = MirrorStore::open(&mirror_path).expect("open mirror");
    let first_appended = poll_once(&mut store, &spine);
    println!(
        "# poll initial_catchup_appended={} mirror_max_seq={}",
        first_appended.len(),
        store.max_seq()
    );
    for _ in 0..5 {
        let _ = poll_once(&mut store, &spine); // warm-up, discarded
    }
    let mut steady_samples = Vec::new();
    let mut steady_appended_total = 0usize;
    for _ in 0..steady_n {
        let s = Instant::now();
        let appended = poll_once(&mut store, &spine);
        steady_samples.push(s.elapsed().as_nanos() as u64);
        steady_appended_total += appended.len();
    }
    let steady_config = format!("behind=0_spine_events={}", recorded);
    stats_line("poll_steady", &steady_config, &steady_samples);
    raw_lines("poll_steady", &steady_config, &steady_samples);
    println!(
        "# poll_steady appended_total_across_samples={} (expect 0)",
        steady_appended_total
    );
    drop(store);

    // Catch-up: rebuild the mirror `behind` events short of the
    // spine, then time the poll that closes the gap. Spine size
    // stays fixed across samples.
    assert!(
        all.len() > behind,
        "spine too small for catch-up behind={}",
        behind
    );
    let mut catchup_samples = Vec::new();
    let mut appended_counts = Vec::new();
    for _ in 0..catchup_n {
        let _ = std::fs::remove_file(&mirror_path);
        let mut st = MirrorStore::open(&mirror_path).expect("reopen mirror");
        for ev in &all[..all.len() - behind] {
            st.append(ev).expect("seed mirror");
        }
        let s = Instant::now();
        let appended = poll_once(&mut st, &spine);
        catchup_samples.push(s.elapsed().as_nanos() as u64);
        appended_counts.push(appended.len());
    }
    let catchup_config = format!("behind={}_spine_events={}", behind, recorded);
    stats_line("poll_catchup", &catchup_config, &catchup_samples);
    raw_lines("poll_catchup", &catchup_config, &catchup_samples);
    println!(
        "# poll_catchup appended_per_sample={:?} (expect all {})",
        appended_counts, behind
    );

    // Drop the spine before removing its directory, so its
    // shutdown flush does not write into a deleted tree.
    drop(spine);
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let quick = args.iter().any(|a| a == "--quick");
    let sub = args.iter().find(|a| !a.starts_with("--")).cloned();
    let mode = if quick { "quick" } else { "full" };

    println!("# g8_fanout_poll mode={} build=release (run via --release)", mode);
    println!(
        "# machine: available_parallelism={} mem_total_kib={} (brief: 2 vCPU AMD EPYC 9D25, 7.7 GiB VM)",
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(0),
        mem_total_kb()
    );
    println!("# design starting values under test (G-2 §4.4): ring=256 events, budget=32 connections, heartbeat=15s");
    println!("# csv: series,config,n,min_us,p50_us,p95_us,max_us,mean_us");

    match sub.as_deref() {
        Some("fanout") => {
            hub_fanout(quick);
            http_fanout(quick);
        }
        Some("poll") => poll_measure(quick),
        Some(other) => {
            eprintln!("unknown subcommand {other:?} (expected fanout|poll)");
            std::process::exit(2);
        }
        None => {
            hub_fanout(quick);
            http_fanout(quick);
            poll_measure(quick);
        }
    }
    println!("# done");
}
