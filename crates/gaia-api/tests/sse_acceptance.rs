//! Acceptance tests for G-4 SSE serving (G-2 §7, the G-4 entry).
//!
//! These map one-to-one onto the four G-4 acceptance criteria,
//! in criterion order in the test names even though the tests
//! themselves run in any order and share nothing:
//!
//!   (1) SI's SI-49 reference follower consumes the fork's
//!       stream through a real drop-and-reconnect and resumes
//!       with zero missed and zero duplicated events — driven
//!       through `follow_si49.py`, a killable-proxy harness
//!       around SI's own `collect_followed`;
//!   (2) a consumer slower than the event rate receives
//!       `resync` + close, and writers are never delayed —
//!       proven with a consumer that stops reading entirely;
//!   (3) mirror entries verify against spine hashes, and a
//!       corrupted mirror is detected and rebuilt — fault
//!       injection on the mirror file, plus the same mirror
//!       served through `EventsServer` so the hashes are also
//!       checked on the wire;
//!   (4) 40 concurrent SSE subscribers are served from the
//!       gaia budget only, and that budget caps at its own
//!       number (48 here) with a 503 — the events listener and
//!       its counter belong to this server alone, so no
//!       inference pool is involved by construction.
//!
//! Everything here runs over real sockets against a real
//! `EventsServer`. Timeouts are generous but bounded: each one
//! is a failure bound, not an expectation.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use gaia_api::events::{EventFilter, EventLog, Source, StreamEvent};
use gaia_api::mirror::{collect_spine_events, poll_once, MirrorStore};
use gaia_api::sse::{EventHub, EventsConfig, EventsServer, SiEventBuffer};
use hearth_store::{Spine, Transition};
use serde_json::{json, Value};

// -- shared helpers ----------------------------------------------------------

/// An `EventLog` with nothing in it, for the source a test does
/// not exercise. The two sources keep separate logs because
/// their sequence spaces are separate; a test that follows one
/// still has to hand the server something honest for the other.
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

/// A pid-suffixed temp directory and the mirror path inside it.
/// (No `tempfile` dependency in this crate; each test removes
/// its own directory afterwards.)
fn temp_mirror(test: &str) -> (PathBuf, PathBuf) {
    let dir =
        std::env::temp_dir().join(format!("gaia-sse-acceptance-{}-{test}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("mirror.jsonl");
    let _ = std::fs::remove_file(&path);
    (dir, path)
}

/// Read a stream one byte at a time until the SSE banner has
/// fully arrived, and return everything read (headers + banner).
///
/// Byte-at-a-time is deliberate: a chunked read could pull event
/// bytes that arrive with the banner into a userspace buffer the
/// rest of the test then cannot see, and several tests need to
/// know exactly what has — and has not — been read so far.
fn read_through_banner(stream: &mut TcpStream) -> String {
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut acc: Vec<u8> = Vec::new();
    let mut byte = [0u8; 1];
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let text = String::from_utf8_lossy(&acc);
        if text.contains(": gaia events") && text.ends_with("\n\n") {
            return text.into_owned();
        }
        assert!(
            Instant::now() < deadline,
            "banner never fully arrived: {text}"
        );
        match stream.read(&mut byte) {
            Ok(0) => panic!("connection closed before its banner: {text}"),
            Ok(_) => acc.extend_from_slice(&byte),
            Err(e) => panic!("reading the banner failed ({e}): {text}"),
        }
    }
}

/// Open one following connection and read it through its banner.
fn follow_connection(addr: &str, source: &str) -> (TcpStream, String) {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .write_all(
            format!(
                "GET /gaia/v1/events?source={source}&follow=1 HTTP/1.1\r\nhost: 127.0.0.1\r\n\r\n"
            )
            .as_bytes(),
        )
        .unwrap();
    stream.flush().unwrap();
    let banner = read_through_banner(&mut stream);
    (stream, banner)
}

/// A plain replay request, read to EOF. Returning at all means
/// the server closed the connection after the replay; a read
/// timeout panics instead, so "closed" is never assumed.
fn http_get_to_eof(addr: &str, target: &str) -> String {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .write_all(format!("GET {target} HTTP/1.1\r\nhost: 127.0.0.1\r\n\r\n").as_bytes())
        .unwrap();
    stream.flush().unwrap();
    let mut out: Vec<u8> = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => out.extend_from_slice(&buf[..n]),
            Err(e) => panic!("replay read failed before EOF ({e})"),
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Wait until the server's own connection counter reads `want`.
fn wait_for_active(server: &EventsServer, want: usize) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while server.active_connections() != want {
        assert!(
            Instant::now() < deadline,
            "server never counted {want} active connections (at {})",
            server.active_connections()
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Read from `stream` until `needle` appears in the accumulated
/// bytes, the stream ends, or the deadline passes.
fn read_until_contains(stream: &mut TcpStream, needle: &[u8], timeout: Duration) -> Vec<u8> {
    stream.set_read_timeout(Some(timeout)).unwrap();
    let deadline = Instant::now() + timeout;
    let mut acc: Vec<u8> = Vec::new();
    let mut buf = [0u8; 8192];
    while !acc.windows(needle.len()).any(|w| w == needle) {
        assert!(
            Instant::now() < deadline,
            "never received {needle:?} in {} bytes",
            acc.len()
        );
        match stream.read(&mut buf) {
            Ok(0) => panic!(
                "stream closed before {needle:?} arrived ({} bytes read)",
                acc.len()
            ),
            Ok(n) => acc.extend_from_slice(&buf[..n]),
            Err(e) => panic!("read failed while waiting for {needle:?} ({e})"),
        }
    }
    acc
}

fn si_task_event(seq: u64) -> StreamEvent {
    StreamEvent::from_si(
        seq,
        "task_open",
        "experiences",
        json!({"kind": "task", "seq": seq}),
    )
}

// -- criterion (3): mirror fault ----------------------------------------------

/// Record a declared -> pull -> loading -> resident chain: the
/// pull events ride the same causal links as the residency
/// events, so both of the spine's causal walks (`why` and
/// `provenance`) reach them, and `collect_spine_events` sees
/// the pull chain whole.
fn record_pull_chain(spine: &Spine, model: &str) {
    let declared = spine
        .record(
            model,
            &Transition::Declared {
                vram_bytes: 4_000_000_000,
                admitted: true,
            },
            &[],
        )
        .unwrap();
    let pull_started = spine
        .record(
            model,
            &Transition::PullStarted {
                source: "hf:owner/muse@Q4_K_M".into(),
            },
            std::slice::from_ref(&declared),
        )
        .unwrap();
    let pull_completed = spine
        .record(
            model,
            &Transition::PullCompleted {
                path: "/blobs/sha256-abc".into(),
                size_bytes: 4_000_000_000,
            },
            std::slice::from_ref(&pull_started),
        )
        .unwrap();
    let loading = spine
        .record(
            model,
            &Transition::Loading {
                pid: 4242,
                endpoint: "127.0.0.1:8080".into(),
            },
            std::slice::from_ref(&pull_completed),
        )
        .unwrap();
    spine
        .record(
            model,
            &Transition::Resident {
                endpoint: "127.0.0.1:8080".into(),
                warmup_ms: 43,
            },
            std::slice::from_ref(&loading),
        )
        .unwrap();
}

/// Record a plain declared -> loading -> resident chain.
fn record_simple_chain(spine: &Spine, model: &str) {
    let declared = spine
        .record(
            model,
            &Transition::Declared {
                vram_bytes: 2_000_000_000,
                admitted: true,
            },
            &[],
        )
        .unwrap();
    let loading = spine
        .record(
            model,
            &Transition::Loading {
                pid: 777,
                endpoint: "127.0.0.1:9090".into(),
            },
            std::slice::from_ref(&declared),
        )
        .unwrap();
    spine
        .record(
            model,
            &Transition::Resident {
                endpoint: "127.0.0.1:9090".into(),
                warmup_ms: 11,
            },
            std::slice::from_ref(&loading),
        )
        .unwrap();
}

#[test]
fn criterion_3_corrupted_mirror_is_detected_rebuilt_and_served_hashes_match_spine() {
    let (dir, path) = temp_mirror("criterion-3");
    let spine = Spine::in_memory();
    record_pull_chain(&spine, "muse-a");
    record_simple_chain(&spine, "muse-b");

    let expected = collect_spine_events(&spine);
    assert_eq!(
        expected.len(),
        8,
        "five pull-chain events + three simple-chain events: {expected:?}"
    );
    let spine_hash_by_seq: BTreeMap<u64, String> = expected
        .iter()
        .map(|ev| (ev.seq, ev.hash.clone().unwrap_or_default()))
        .collect();
    assert!(
        spine_hash_by_seq.values().all(|h| !h.is_empty()),
        "every spine event carries the hash the mirror must record"
    );

    // Poll the spine into a mirror on a real file.
    let mut store = MirrorStore::open(&path).unwrap();
    let appended = poll_once(&mut store, &spine);
    assert_eq!(appended, expected, "one poll mirrors the whole history");
    assert!(
        store.verify_against(&spine).is_empty(),
        "a fresh mirror verifies clean: {:?}",
        store.verify_against(&spine)
    );
    for entry in store.entries() {
        assert_eq!(
            entry.hash.as_deref(),
            spine_hash_by_seq.get(&entry.seq).map(String::as_str),
            "mirror entry at seq {} carries the spine's hash",
            entry.seq
        );
    }
    drop(store);

    // The cross-module half: serve that same mirror file through
    // EventsServer as the hearth EventLog, replay it over a real
    // socket, and check the hashes on the wire — the `row.hash`
    // of each block — against spine truth.
    let server = EventsServer::bind(
        "127.0.0.1:0",
        Arc::new(MirrorStore::open(&path).unwrap()),
        Arc::new(EmptyLog),
        EventHub::new(16),
        EventsConfig::default(),
    )
    .unwrap();
    let out = http_get_to_eof(&server.local_addr(), "/gaia/v1/events?source=hearth");
    assert!(out.starts_with("HTTP/1.1 200 OK"), "{out}");
    let mut wire_hash_by_seq: BTreeMap<u64, String> = BTreeMap::new();
    for block in out.split("\n\n") {
        let Some(data_line) = block.lines().find(|l| l.starts_with("data: ")) else {
            continue;
        };
        let data: Value = serde_json::from_str(&data_line["data: ".len()..])
            .unwrap_or_else(|e| panic!("wire data is not JSON ({e}): {block}"));
        let seq = data["seq"].as_u64().expect("wire event carries its seq");
        let hash = data["row"]["hash"]
            .as_str()
            .expect("hearth wire rows carry the spine hash")
            .to_string();
        wire_hash_by_seq.insert(seq, hash);
    }
    assert_eq!(
        wire_hash_by_seq, spine_hash_by_seq,
        "every hash on the wire is the spine's hash at that seq"
    );
    drop(server);

    // Fault injection: rewrite one entry's hash in the file on
    // disk, exactly the corruption a mirror must never hide.
    let tampered_seq = expected[2].seq;
    let text = std::fs::read_to_string(&path).unwrap();
    let mut lines: Vec<String> = text.lines().map(str::to_string).collect();
    assert_eq!(lines.len(), expected.len());
    let mut tampered: Value = serde_json::from_str(&lines[2]).unwrap();
    assert_eq!(
        tampered["seq"].as_u64(),
        Some(tampered_seq),
        "the line being tampered is the entry it claims to be"
    );
    tampered["hash"] = json!("tampered-hash");
    lines[2] = tampered.to_string();
    std::fs::write(&path, lines.join("\n") + "\n").unwrap();

    let mut store = MirrorStore::open(&path).unwrap();
    let failures = store.verify_against(&spine);
    assert_eq!(
        failures.len(),
        1,
        "verify names exactly the tampered entry: {failures:?}"
    );
    assert!(
        failures[0].contains(&format!("seq {tampered_seq}:")),
        "the one failure names the tampered seq: {failures:?}"
    );

    // The spine wins: rebuild, and the mirror is faithful again.
    let count = store.rebuild(&spine).unwrap();
    assert_eq!(count, expected.len());
    assert!(
        store.verify_against(&spine).is_empty(),
        "a rebuilt mirror verifies clean"
    );
    assert_eq!(
        store.entries(),
        expected.as_slice(),
        "rebuilt entries equal the spine's collected events"
    );
    drop(store);
    let reopened = MirrorStore::open(&path).unwrap();
    assert_eq!(reopened.entries(), expected.as_slice());
    assert!(reopened.verify_against(&spine).is_empty());

    let _ = std::fs::remove_dir_all(&dir);
}

// -- criterion (2): blocked consumer ------------------------------------------

#[test]
fn criterion_2_blocked_consumer_gets_resync_and_close_and_writers_are_never_delayed() {
    // A ring of 4 and a 300 ms write timeout: a consumer that
    // stops reading must cost the server its resync frame and a
    // closed connection, never a stalled writer.
    let hub = EventHub::new(4);
    let buffer = SiEventBuffer::new();
    buffer.attach_hub(hub.clone());
    let config = EventsConfig {
        max_connections: 8,
        heartbeat: Duration::from_secs(60),
        write_timeout: Duration::from_millis(300),
    };
    let server = EventsServer::bind(
        "127.0.0.1:0",
        Arc::new(EmptyLog),
        Arc::new(buffer.clone()),
        hub.clone(),
        config,
    )
    .unwrap();
    let addr = server.local_addr();

    let (mut stream, banner) = follow_connection(&addr, "si");
    assert!(banner.contains("200 OK"), "{banner}");
    // From here the consumer stops reading entirely. Whatever
    // the server sends sits in kernel buffers, unread.

    // ~8 KiB of facts per event, so the unread bytes fill kernel
    // buffers quickly; 60 events is far more than ring (4) plus
    // buffers can absorb for a reader that never reads.
    let payload = "x".repeat(8 * 1024);
    let push_started = Instant::now();
    for seq in 1..=60u64 {
        buffer
            .push(StreamEvent::from_si(
                seq,
                "task_open",
                "experiences",
                json!({"kind": "task", "seq": seq, "payload": payload}),
            ))
            .unwrap();
    }
    let push_elapsed = push_started.elapsed();
    assert!(
        push_elapsed < Duration::from_secs(5),
        "60 pushes took {push_elapsed:?} — a writer waited on the blocked consumer"
    );
    assert!(
        hub.stats().overflowed_total >= 1,
        "the blocked consumer's ring must have overflowed: {:?}",
        hub.stats()
    );

    // Now read: the stream must end — the server closes a
    // consumer it has told to resync — and the resync frame must
    // be in what was sent. A read timeout here is a failure, not
    // an ending: a still-open stream is exactly what this
    // criterion forbids.
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut received: Vec<u8> = Vec::new();
    let mut buf = [0u8; 16384];
    let mut ended_cleanly = false;
    loop {
        match stream.read(&mut buf) {
            Ok(0) => {
                ended_cleanly = true;
                break;
            }
            Ok(n) => received.extend_from_slice(&buf[..n]),
            Err(e)
                if e.kind() == std::io::ErrorKind::ConnectionReset
                    || e.kind() == std::io::ErrorKind::BrokenPipe =>
            {
                // A reset on a socket the peer closed with unread
                // data in flight is still the peer closing it.
                break;
            }
            Err(e) => panic!("blocked consumer's stream did not end: read failed ({e})"),
        }
    }
    assert!(
        ended_cleanly || !received.is_empty(),
        "the stream must end by the server's close, with its final frames delivered"
    );
    let received_text = String::from_utf8_lossy(&received);
    assert!(
        received_text.contains("event: resync"),
        "the blocked consumer must be told to resync before the close; \
         got {} bytes without a resync frame",
        received.len()
    );
}

// -- criterion (4): 40 subscribers, gaia budget only ---------------------------

#[test]
fn criterion_4_forty_subscribers_served_and_the_gaia_budget_caps_at_48_with_503() {
    // max_connections = 48 is this server's whole budget. 40
    // subscribers must fit with room to spare, 8 more must
    // exactly fill it, and a 49th concurrent connection must be
    // refused — all counted on this server's own counter, which
    // is the structural claim: nothing here touches any other
    // (inference) pool, because this listener owns no other.
    let hub = EventHub::new(64);
    let buffer = SiEventBuffer::new();
    buffer.attach_hub(hub.clone());
    let config = EventsConfig {
        max_connections: 48,
        heartbeat: Duration::from_secs(60),
        write_timeout: Duration::from_secs(5),
    };
    let server = EventsServer::bind(
        "127.0.0.1:0",
        Arc::new(EmptyLog),
        Arc::new(buffer.clone()),
        hub,
        config,
    )
    .unwrap();
    let addr = server.local_addr();

    let mut subscribers: Vec<TcpStream> = Vec::new();
    for _ in 0..40 {
        let (stream, banner) = follow_connection(&addr, "si");
        assert!(banner.contains("200 OK"), "{banner}");
        subscribers.push(stream);
    }
    wait_for_active(&server, 40);
    assert_eq!(server.active_connections(), 40);

    // One event, and every one of the 40 receives its block.
    buffer.push(si_task_event(1)).unwrap();
    for (idx, stream) in subscribers.iter_mut().enumerate() {
        let got = read_until_contains(stream, b"id: 1\n", Duration::from_secs(10));
        assert!(
            String::from_utf8_lossy(&got).contains("event: si:task_open"),
            "subscriber {idx} got the id but not the event block"
        );
    }
    assert_eq!(
        server.active_connections(),
        40,
        "all 40 subscribers are still live after the fan-out"
    );

    // The budget has exactly 8 slots left: fill them, prove they
    // are served, then prove the cap.
    let mut extra: Vec<TcpStream> = Vec::new();
    for _ in 0..8 {
        let (stream, banner) = follow_connection(&addr, "si");
        assert!(banner.contains("200 OK"), "{banner}");
        extra.push(stream);
    }
    wait_for_active(&server, 48);
    assert_eq!(server.active_connections(), 48);

    let refused = http_get_to_eof(&addr, "/gaia/v1/events?source=si");
    assert!(
        refused.starts_with("HTTP/1.1 503"),
        "the 49th concurrent connection must be refused: {refused}"
    );
    assert!(
        refused.contains("gaia_event_budget_exhausted"),
        "the refusal speaks the gaia budget vocabulary: {refused}"
    );
    assert_eq!(
        server.active_connections(),
        48,
        "the refused connection was never counted against the budget"
    );

    drop(subscribers);
    drop(extra);
}

// -- criterion (1): the SI-49 follower -----------------------------------------

#[test]
fn criterion_1_si49_follower_survives_a_drop_and_reconnect_with_no_missed_or_duplicated_events() {
    // Prerequisites, checked plainly: python3 on PATH, and an SI
    // checkout holding si/gaia/events_client.py ($GAIA_SI_REPO,
    // else the SI worktree on this VM). Absent either, this test
    // says so and passes — the follower is SI's code, and a
    // machine without SI's checkout cannot run it. On the review
    // VM both exist, and there this test must actually run.
    let python_ok = std::process::Command::new("python3")
        .arg("--version")
        .output()
        .map(|out| out.status.success())
        .unwrap_or(false);
    let si_repo: PathBuf = std::env::var_os("GAIA_SI_REPO")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let home = std::env::var_os("HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("~"));
            home.join("workspace").join("si51-work").join("repo")
        });
    let follower_present = si_repo.join("si/gaia/events_client.py").is_file();
    if !python_ok || !follower_present {
        println!(
            "criterion 1 SKIPPED: python3 available = {python_ok}, \
             SI follower at {} present = {follower_present}",
            si_repo.display()
        );
        return;
    }

    // Choosing the interpreter, plainly: importing
    // si.gaia.events_client executes si/__init__.py, which pulls
    // in SI's whole runtime (safetensors among it). The system
    // python3 on this VM does not carry those packages; the venvs
    // SI's own work uses do. So interpreters are probed, in
    // order — $GAIA_SI_PYTHON, python3 on PATH, then the SI venvs
    // known on this VM — with the real import the harness will
    // perform, and the first one that can actually import the
    // follower is the one that runs it. If none can, that is a
    // missing prerequisite and is reported as a skip, the same
    // as the checks above.
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("~"));
    let mut candidates: Vec<String> = Vec::new();
    if let Some(python) = std::env::var_os("GAIA_SI_PYTHON") {
        candidates.push(python.to_string_lossy().into_owned());
    }
    candidates.push("python3".to_string());
    for venv in ["si40-venv", "si2-venv"] {
        candidates.push(
            home.join("workspace")
                .join(venv)
                .join("bin")
                .join("python3")
                .to_string_lossy()
                .into_owned(),
        );
    }
    candidates.push(
        home.join("hfvenv")
            .join("bin")
            .join("python3")
            .to_string_lossy()
            .into_owned(),
    );
    for venv in [".venv", "venv"] {
        candidates.push(
            si_repo
                .join(venv)
                .join("bin")
                .join("python3")
                .to_string_lossy()
                .into_owned(),
        );
    }
    let mut chosen_python: Option<String> = None;
    for candidate in &candidates {
        let probe = std::process::Command::new(candidate)
            .arg("-c")
            .arg("from si.gaia.events_client import collect_followed, FollowConfig")
            .env("PYTHONPATH", &si_repo)
            .output();
        if matches!(probe, Ok(out) if out.status.success()) {
            chosen_python = Some(candidate.clone());
            break;
        }
    }
    let python = match chosen_python {
        Some(python) => {
            println!("criterion 1: running SI's follower under {python}");
            python
        }
        None => {
            println!(
                "criterion 1 SKIPPED: no interpreter among {candidates:?} \
                 could import si.gaia.events_client from {}",
                si_repo.display()
            );
            return;
        }
    };
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("follow_si49.py");
    assert!(
        script.is_file(),
        "harness script missing: {}",
        script.display()
    );

    let hub = EventHub::new(64);
    let buffer = SiEventBuffer::new();
    buffer.attach_hub(hub.clone());
    let config = EventsConfig {
        max_connections: 8,
        heartbeat: Duration::from_secs(1),
        write_timeout: Duration::from_secs(5),
    };
    let server = EventsServer::bind(
        "127.0.0.1:0",
        Arc::new(EmptyLog),
        Arc::new(buffer.clone()),
        hub,
        config,
    )
    .unwrap();

    // Events 1..=3 exist before the follower starts: its first
    // connection is a replay, and the proxy severs it after the
    // 3rd event block — so the resume it then performs is the
    // only way 4..=6 can arrive.
    for seq in 1..=3u64 {
        buffer.push(si_task_event(seq)).unwrap();
    }

    // Events 4..=6 arrive shortly after, from this thread: if
    // the follower reconnects quickly they are live tail; if it
    // is still starting they are replay on the reconnect. Either
    // way they can only be reached *through* the resume.
    let pusher_buffer = buffer.clone();
    let pusher = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(1500));
        for seq in 4..=6u64 {
            pusher_buffer.push(si_task_event(seq)).unwrap();
        }
    });

    // Run the Python harness on its own thread so this test can
    // bound the whole follow, not just hope it ends. (It carries
    // its own bounds too: max_duration 60 s in the follower, a
    // 10 s socket timeout — this deadline is the outer one.)
    let (tx, rx) = mpsc::channel();
    let real_addr = server.local_addr();
    let si_repo_arg = si_repo.to_string_lossy().into_owned();
    let runner = std::thread::spawn(move || {
        let output = std::process::Command::new(&python)
            .arg(&script)
            .arg(&real_addr)
            .arg(&si_repo_arg)
            .output();
        let _ = tx.send(output);
    });
    let output = rx
        .recv_timeout(Duration::from_secs(90))
        .unwrap_or_else(|e| panic!("the SI-49 follower harness did not finish in 90 s ({e})"))
        .unwrap_or_else(|e| panic!("could not run the follower harness: {e}"));
    let _ = runner.join();
    pusher.join().unwrap();

    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        output.status.success(),
        "follower harness failed ({:?})\nstdout: {stdout}\nstderr: {stderr}",
        output.status.code()
    );
    let ids_line = stdout
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .unwrap_or_else(|| panic!("harness printed no ids\nstdout: {stdout}\nstderr: {stderr}"));
    let ids: Vec<u64> = serde_json::from_str(ids_line)
        .unwrap_or_else(|e| panic!("harness ids are not a JSON list ({e}): {ids_line}"));
    assert_eq!(
        ids,
        vec![1, 2, 3, 4, 5, 6],
        "zero missed, zero duplicated, across a severed first connection \
         (harness stderr: {stderr})"
    );
}
