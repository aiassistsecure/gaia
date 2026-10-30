//! Acceptance tests for S-6a — SI events ingest, fork half
//! (§15 S-6 acceptance criteria, fork side).
//!
//! Until this slice the fork's `si` source was fed only by
//! in-process `SiEventBuffer::push` — a deployment with SI in
//! another process had no way in. S-6a adds the crossing:
//! `POST /gaia/v1/events/ingest` on the agency listener, one
//! SI journal event per request
//! (`{"id", "event", "collection", "row"}`), answered `200`
//! with `{"accepted": true, "seq", "latest_seq"}` when the
//! event lands in order, `409 gaia_ingest_refused` for a
//! duplicate or out-of-order sequence, and
//! `400 gaia_ingest_malformed` for a body that is not one
//! well-formed SI event. Accepted events land in the same
//! [`SiEventBuffer`] the events listener serves, so replay
//! and live tail read the ingested history unchanged.
//!
//! These tests map one-to-one onto the fork-half criteria:
//!
//!   (1a) SI's SI-49 reference follower consumes events that
//!        arrived **via the ingest endpoint** — not in-process
//!        push; that reversal is the point of this slice —
//!        through a real drop-and-reconnect, with zero missed
//!        and zero duplicated events, driven through the
//!        unmodified `follow_si49.py` harness from
//!        `sse_acceptance.rs` criterion 1 (same follower, same
//!        proxy: it adds `source=si` and collects exactly
//!        [1..6], severing after the 3rd block);
//!   (1b) a direct `GET /gaia/v1/events?source=si&follow=1`
//!        read asserts exact seq fidelity for the same
//!        ingested events — ids 1..6 in order, event names
//!        `si:<event>`, collections and rows intact;
//!   (2) a duplicate sequence, and then a lower out-of-order
//!       sequence, are each refused `409 gaia_ingest_refused`,
//!       and the buffer afterwards holds exactly the accepted
//!       sequences;
//!   (3) events ingested with **no** `EventsServer` bound yet
//!       are replayed whole once one is bound — ingest does
//!       not depend on the stream listener existing;
//!   (4) malformed bodies (invalid JSON, missing `id`, `id`
//!       as a string, missing `event`, `row` as an array) are
//!       each `400 gaia_ingest_malformed`, and nothing lands:
//!       the buffer's `latest_seq` is still 0 and a replay is
//!       empty;
//!   (5) light: `source=hearth` still serves from the hearth
//!       log handed to the same `EventsServer` — the heavy
//!       lifting for the prior behaviour is the unmodified
//!       prior suites, which the coordinator runs.
//!
//! Everything here runs over real sockets: ingest goes
//! through a real agency-shaped listener (a `hearth_api::Server`
//! whose per-connection handler is `read_request` →
//! `gaia_api::ingest::handle` → `render_response`, the exact
//! shape `gaia-serve` uses for its agency routes), and the
//! stream is read from a real `EventsServer`. Timeouts are
//! generous but bounded; no test touches the external
//! network, and no test leaves temp state behind (this file
//! creates no files at all).

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use gaia_api::events::{EventFilter, EventLog, Source, StreamEvent};
use gaia_api::sse::{EventHub, EventsConfig, EventsServer, SiEventBuffer};
use serde_json::{json, Value};

// -- shared helpers ----------------------------------------------------------

/// An `EventLog` with nothing in it, for the source a test
/// does not exercise. Copied in shape from `sse_acceptance.rs`:
/// the two sources keep separate logs because their sequence
/// spaces are separate; a test that follows one still has to
/// hand the server something honest for the other.
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

/// A simple Vec-backed `EventLog` (criterion 5's hearth log):
/// events after the cursor, in stored order, through the
/// filter, for the source they belong to and no other.
struct VecLog {
    events: Vec<StreamEvent>,
}

impl EventLog for VecLog {
    fn events_since(
        &self,
        source: Source,
        after_seq: u64,
        filter: &EventFilter,
    ) -> Vec<StreamEvent> {
        self.events
            .iter()
            .filter(|ev| ev.source == source && ev.seq > after_seq && filter.matches(ev))
            .cloned()
            .collect()
    }

    fn latest_seq(&self, source: Source) -> u64 {
        self.events
            .iter()
            .filter(|ev| ev.source == source)
            .map(|ev| ev.seq)
            .max()
            .unwrap_or(0)
    }
}

/// The SI shape of journal seq `seq`: (event name, collection,
/// row kind). Collections alternate between SI's `self_edits`
/// and `experiences`, as its journal rows do; every row carries
/// `"kind"` and `"seq"`, the fields SI's own rows carry and its
/// filters read.
fn si_spec(seq: u64) -> (&'static str, &'static str, &'static str) {
    match seq {
        1 => ("self_edit.tool", "self_edits", "tool"),
        2 => ("task_open", "experiences", "task"),
        3 => ("self_edit.memory", "self_edits", "memory"),
        4 => ("task_update", "experiences", "task"),
        5 => ("self_edit.tool", "self_edits", "tool"),
        6 => ("task_close", "experiences", "task"),
        _ => ("task_open", "experiences", "task"),
    }
}

/// One ingest request body for journal seq `seq`, in the fixed
/// S-6a contract shape.
fn si_body(seq: u64) -> String {
    let (event, collection, kind) = si_spec(seq);
    json!({
        "id": seq,
        "event": event,
        "collection": collection,
        "row": {"kind": kind, "seq": seq},
    })
    .to_string()
}

// -- the agency (ingest) listener ---------------------------------------------

/// Serve one agency connection in the agency handler's exact
/// shape: read the request with `hearth_api::http::read_request`,
/// dispatch to `gaia_api::ingest::handle`, fall through to a
/// 404 gaia response when the handler declines (`None`), and
/// write the answer with `hearth_api::http::render_response`.
fn handle_agency_connection(mut stream: TcpStream, buffer: SiEventBuffer) {
    let req = match hearth_api::http::read_request(&stream) {
        Ok(r) => r,
        Err(e) => {
            let body = json!({ "error": { "message": e } }).to_string();
            let _ = stream
                .write_all(hearth_api::http::render_response(400, &body, None).as_bytes());
            return;
        }
    };
    let resp = gaia_api::ingest::handle(&req, &buffer).unwrap_or_else(|| {
        hearth_api::Response::openai_error(
            404,
            format!("no gaia route for {} {}", req.method, req.path),
            "gaia_route_not_found",
            json!({}),
        )
    });
    let _ = stream.write_all(
        hearth_api::http::render_response(resp.status, &resp.body, resp.retry_after).as_bytes(),
    );
}

/// Bind the agency listener for ingest on `127.0.0.1:0` in a
/// thread and return its address. The listener thread is
/// detached — it serves for the life of the test process,
/// which is the life of these tests; every test binds its own
/// port, so nothing is shared between tests.
fn start_ingest_server(buffer: SiEventBuffer) -> String {
    let server = hearth_api::Server::bind("127.0.0.1:0").expect("bind agency listener");
    let addr = server.local_addr();
    std::thread::spawn(move || {
        for conn in server.incoming() {
            let stream = match conn {
                Ok(s) => s,
                Err(_) => continue,
            };
            let buffer = buffer.clone();
            std::thread::spawn(move || handle_agency_connection(stream, buffer));
        }
    });
    addr
}

/// POST one raw body to the ingest route over a raw `TcpStream`
/// and return `(status, body_json)`. Reading to EOF is the
/// completion check: the agency handler renders
/// `connection: close`, so a response that never closes is a
/// hang, and the read timeout turns it into a failure rather
/// than a stuck test.
fn post_ingest(addr: &str, body: &str) -> (u16, Value) {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .write_all(
            format!(
                "POST /gaia/v1/events/ingest HTTP/1.1\r\n\
                 host: 127.0.0.1\r\n\
                 content-type: application/json\r\n\
                 content-length: {}\r\n\
                 connection: close\r\n\
                 \r\n\
                 {body}",
                body.len()
            )
            .as_bytes(),
        )
        .unwrap();
    stream.flush().unwrap();
    let mut out: Vec<u8> = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => out.extend_from_slice(&buf[..n]),
            Err(e) => panic!("ingest response read failed before EOF ({e})"),
        }
    }
    let text = String::from_utf8_lossy(&out).into_owned();
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("ingest response has no status line: {text}"));
    let resp_body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b)
        .unwrap_or_else(|| panic!("ingest response has no body: {text}"));
    let parsed: Value = serde_json::from_str(resp_body)
        .unwrap_or_else(|e| panic!("ingest response body is not JSON ({e}): {text}"));
    (status, parsed)
}

/// POST journal seq `seq` and assert the success contract:
/// `200 {"accepted": true, "seq": id, "latest_seq": n}` with
/// `latest_seq` standing at the just-accepted seq (these tests
/// ingest strictly in order from an empty buffer).
fn post_si_ok(addr: &str, seq: u64) -> Value {
    let (status, body) = post_ingest(addr, &si_body(seq));
    assert_eq!(status, 200, "ingest of seq {seq} refused: {body}");
    assert_eq!(body["accepted"], json!(true), "seq {seq}: {body}");
    assert_eq!(body["seq"], json!(seq), "seq {seq}: {body}");
    assert_eq!(
        body["latest_seq"],
        json!(seq),
        "after accepting seq {seq} the buffer's latest is that seq: {body}"
    );
    body
}

// -- the events (stream) side --------------------------------------------------

/// Bind a real `EventsServer` with `buffer` as the `si` log
/// and an empty hearth log, the harness shape the task fixes:
/// `Arc::new(buffer.clone())` so the test, the ingest
/// listener, and the stream server all read one history.
fn bind_events_server(buffer: &SiEventBuffer) -> EventsServer {
    let hub = EventHub::new(64);
    buffer.attach_hub(hub.clone());
    let config = EventsConfig {
        max_connections: 8,
        heartbeat: Duration::from_secs(1),
        write_timeout: Duration::from_secs(5),
    };
    EventsServer::bind(
        "127.0.0.1:0",
        Arc::new(EmptyLog),
        Arc::new(buffer.clone()),
        hub,
        config,
    )
    .unwrap()
}

/// A plain replay request, read to EOF. Returning at all means
/// the server closed the connection after the replay; a read
/// timeout panics instead, so "closed" is never assumed.
/// (Same helper shape as `sse_acceptance.rs`.)
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

/// Parse the event blocks out of an SSE response text: one
/// `(id, event_name, data_json)` per block carrying an `id:`
/// line, in wire order. Comment blocks (the banner, heartbeats)
/// carry no id and are skipped, exactly as a consumer skips
/// them.
fn parse_sse_events(text: &str) -> Vec<(u64, String, Value)> {
    let mut events = Vec::new();
    for block in text.split("\n\n") {
        let mut id: Option<u64> = None;
        let mut name: Option<String> = None;
        let mut data: Option<Value> = None;
        for line in block.lines() {
            let line = line.trim_end_matches('\r');
            if let Some(v) = line.strip_prefix("id: ") {
                id = v.trim().parse().ok();
            } else if let Some(v) = line.strip_prefix("event: ") {
                name = Some(v.trim().to_string());
            } else if let Some(v) = line.strip_prefix("data: ") {
                data = serde_json::from_str(v).ok();
            }
        }
        if let (Some(id), Some(name), Some(data)) = (id, name, data) {
            events.push((id, name, data));
        }
    }
    events
}

/// Open a following connection and read until `want` event
/// blocks have arrived, then drop it. The deadline is a
/// failure bound: a follow that never delivers its replay is
/// a failed test, not a long one.
fn follow_read_events(addr: &str, target: &str, want: usize) -> String {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .write_all(format!("GET {target} HTTP/1.1\r\nhost: 127.0.0.1\r\n\r\n").as_bytes())
        .unwrap();
    stream.flush().unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut acc: Vec<u8> = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        let text = String::from_utf8_lossy(&acc).into_owned();
        if parse_sse_events(&text).len() >= want {
            return text;
        }
        assert!(
            Instant::now() < deadline,
            "follow delivered {} of {want} events before the deadline: {text}",
            parse_sse_events(&text).len()
        );
        match stream.read(&mut buf) {
            Ok(0) => panic!(
                "follow closed after {} of {want} events: {text}",
                parse_sse_events(&text).len()
            ),
            Ok(n) => acc.extend_from_slice(&buf[..n]),
            Err(e) => panic!("follow read failed ({e})"),
        }
    }
}

/// Assert the parsed blocks are exactly journal seqs `seqs`,
/// in order, with the wire name, collection, and row each
/// ingested body carried.
fn assert_exact_si_events(events: &[(u64, String, Value)], seqs: &[u64]) {
    let ids: Vec<u64> = events.iter().map(|(id, _, _)| *id).collect();
    assert_eq!(ids, seqs, "exact seqs, in order, none missed or duplicated");
    for (id, name, data) in events {
        let (event, collection, kind) = si_spec(*id);
        assert_eq!(
            name,
            &format!("si:{event}"),
            "seq {id}: the wire event name is si:<event>"
        );
        assert_eq!(data["source"], json!("si"), "seq {id}: {data}");
        assert_eq!(data["seq"], json!(*id), "seq {id}: {data}");
        assert_eq!(
            data["collection"],
            json!(collection),
            "seq {id}: the ingested collection rides the stream: {data}"
        );
        assert_eq!(
            data["row"]["kind"],
            json!(kind),
            "seq {id}: the ingested row rides unmodified: {data}"
        );
        assert_eq!(
            data["row"]["seq"],
            json!(*id),
            "seq {id}: the ingested row rides unmodified: {data}"
        );
    }
}

// -- criterion (1a): the SI-49 follower, fed by ingest -------------------------

#[test]
fn criterion_1a_si49_follower_survives_a_drop_and_reconnect_on_ingested_events() {
    // Prerequisites, checked plainly — copied honestly from
    // `sse_acceptance.rs` criterion 1: python3 on PATH, and an
    // SI checkout holding si/gaia/events_client.py
    // ($GAIA_SI_REPO, else the SI worktree on this VM). Absent
    // either, this test says so and passes — the follower is
    // SI's code, and a machine without SI's checkout cannot run
    // it. On the review VM both exist, and there this test must
    // actually run.
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
            "criterion 1a SKIPPED: python3 available = {python_ok}, \
             SI follower at {} present = {follower_present}",
            si_repo.display()
        );
        return;
    }

    // Choosing the interpreter, plainly (same probing as
    // sse_acceptance criterion 1): importing
    // si.gaia.events_client executes si/__init__.py, which pulls
    // in SI's whole runtime. Interpreters are probed, in order —
    // $GAIA_SI_PYTHON, python3 on PATH, then the SI venvs known
    // on this VM — with the real import the harness will
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
            println!("criterion 1a: running SI's follower under {python}");
            python
        }
        None => {
            println!(
                "criterion 1a SKIPPED: no interpreter among {candidates:?} \
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

    let buffer = SiEventBuffer::new();
    let ingest_addr = start_ingest_server(buffer.clone());
    let server = bind_events_server(&buffer);

    // Events 1..=3 are ingested — through the HTTP ingest
    // endpoint, NOT in-process push; that reversal is the point
    // of this slice — before the follower starts: its first
    // connection is a replay, and the proxy severs it after the
    // 3rd event block, so the resume it then performs is the
    // only way 4..=6 can arrive.
    for seq in 1..=3u64 {
        post_si_ok(&ingest_addr, seq);
    }

    // Events 4..=6 are ingested shortly after, from this
    // thread, through the same endpoint: if the follower
    // reconnects quickly they are live tail; if it is still
    // starting they are replay on the reconnect. Either way
    // they can only be reached *through* the resume.
    let pusher_addr = ingest_addr.clone();
    let pusher = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(1500));
        for seq in 4..=6u64 {
            post_si_ok(&pusher_addr, seq);
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
        "zero missed, zero duplicated, across a severed first connection, \
         for events that arrived via the ingest endpoint \
         (harness stderr: {stderr})"
    );
}

// -- criterion (1b): direct follow, exact seq fidelity -------------------------

#[test]
fn criterion_1b_direct_follow_reads_ingested_events_with_exact_seq_fidelity() {
    let buffer = SiEventBuffer::new();
    let ingest_addr = start_ingest_server(buffer.clone());
    let server = bind_events_server(&buffer);
    let addr = server.local_addr();

    for seq in 1..=6u64 {
        post_si_ok(&ingest_addr, seq);
    }

    let text = follow_read_events(&addr, "/gaia/v1/events?source=si&follow=1", 6);
    assert!(text.contains("200 OK"), "{text}");
    assert!(text.contains(": gaia events"), "{text}");
    let events = parse_sse_events(&text);
    assert_eq!(
        events.len(),
        6,
        "the follow delivers exactly the six ingested events: {text}"
    );
    assert_exact_si_events(&events, &[1, 2, 3, 4, 5, 6]);
}

// -- criterion (2): duplicate / out-of-order refused ---------------------------

#[test]
fn criterion_2_duplicate_and_out_of_order_ingest_are_409_and_the_buffer_is_unchanged() {
    let buffer = SiEventBuffer::new();
    let ingest_addr = start_ingest_server(buffer.clone());

    post_si_ok(&ingest_addr, 1);
    post_si_ok(&ingest_addr, 2);

    // The same seq again: a duplicate delivery is refused, in
    // the gaia ingest vocabulary — not silently deduplicated.
    let (status, body) = post_ingest(&ingest_addr, &si_body(2));
    assert_eq!(status, 409, "duplicate seq 2 must be refused: {body}");
    assert_eq!(
        body["error"]["type"],
        json!("gaia_ingest_refused"),
        "the refusal speaks the gaia ingest vocabulary: {body}"
    );

    // A lower seq than the buffer's latest: out of order, the
    // same refusal.
    let (status, body) = post_ingest(&ingest_addr, &si_body(1));
    assert_eq!(status, 409, "out-of-order seq 1 must be refused: {body}");
    assert_eq!(
        body["error"]["type"],
        json!("gaia_ingest_refused"),
        "the refusal speaks the gaia ingest vocabulary: {body}"
    );

    // The buffer afterwards holds exactly seqs [1, 2] — checked
    // on the buffer itself, and again on the wire through a
    // replay GET, so the refusal is proven in both views.
    let held = buffer.events_since(Source::Si, 0, &EventFilter::default());
    let held_seqs: Vec<u64> = held.iter().map(|ev| ev.seq).collect();
    assert_eq!(held_seqs, vec![1, 2], "exactly the accepted seqs are held");
    assert_eq!(buffer.latest_seq(Source::Si), 2);

    let server = bind_events_server(&buffer);
    let out = http_get_to_eof(&server.local_addr(), "/gaia/v1/events?source=si");
    assert!(out.starts_with("HTTP/1.1 200 OK"), "{out}");
    assert_exact_si_events(&parse_sse_events(&out), &[1, 2]);
}

// -- criterion (3): ingest before the stream exists ----------------------------

#[test]
fn criterion_3_events_ingested_before_the_events_server_binds_replay_whole() {
    let buffer = SiEventBuffer::new();
    let ingest_addr = start_ingest_server(buffer.clone());

    // No EventsServer is bound yet — ingest stands alone, and
    // the events land in the buffer with no stream audience.
    for seq in 1..=3u64 {
        post_si_ok(&ingest_addr, seq);
    }
    assert_eq!(buffer.latest_seq(Source::Si), 3);

    // Only now does the stream listener bind, over that same
    // buffer: a plain (non-follow) GET replays 1..3 whole.
    let server = bind_events_server(&buffer);
    let out = http_get_to_eof(&server.local_addr(), "/gaia/v1/events?source=si");
    assert!(out.starts_with("HTTP/1.1 200 OK"), "{out}");
    assert_exact_si_events(&parse_sse_events(&out), &[1, 2, 3]);
}

// -- criterion (4): malformed bodies -------------------------------------------

#[test]
fn criterion_4_malformed_bodies_are_400_and_nothing_lands() {
    let buffer = SiEventBuffer::new();
    let ingest_addr = start_ingest_server(buffer.clone());

    let cases: Vec<(&str, String)> = vec![
        ("invalid JSON", "not json".to_string()),
        (
            "missing id",
            json!({
                "event": "task_open",
                "collection": "experiences",
                "row": {"kind": "task", "seq": 1},
            })
            .to_string(),
        ),
        (
            "id as string",
            json!({
                "id": "1",
                "event": "task_open",
                "collection": "experiences",
                "row": {"kind": "task", "seq": 1},
            })
            .to_string(),
        ),
        (
            "missing event",
            json!({
                "id": 1,
                "collection": "experiences",
                "row": {"kind": "task", "seq": 1},
            })
            .to_string(),
        ),
        (
            "row as array",
            json!({
                "id": 1,
                "event": "task_open",
                "collection": "experiences",
                "row": [],
            })
            .to_string(),
        ),
    ];
    for (case, body) in &cases {
        let (status, resp) = post_ingest(&ingest_addr, body);
        assert_eq!(status, 400, "{case}: a malformed body is a 400: {resp}");
        assert_eq!(
            resp["error"]["type"],
            json!("gaia_ingest_malformed"),
            "{case}: malformed bodies speak the gaia ingest vocabulary: {resp}"
        );
    }

    // Nothing landed: the buffer is still at seq 0, holds no
    // events, and a replay served from it is empty.
    assert_eq!(
        buffer.latest_seq(Source::Si),
        0,
        "no malformed body may advance the buffer"
    );
    assert!(
        buffer
            .events_since(Source::Si, 0, &EventFilter::default())
            .is_empty(),
        "no malformed body may land an event"
    );
    let server = bind_events_server(&buffer);
    let out = http_get_to_eof(&server.local_addr(), "/gaia/v1/events?source=si");
    assert!(out.starts_with("HTTP/1.1 200 OK"), "{out}");
    assert!(
        parse_sse_events(&out).is_empty(),
        "the replay after malformed posts is empty: {out}"
    );
}

// -- criterion (5): the hearth source still serves (light) ---------------------

#[test]
fn criterion_5_hearth_source_still_serves_from_the_hearth_log_on_the_same_server() {
    // One hearth event in a Vec-backed log, handed to the same
    // EventsServer shape every other criterion uses. The heavy
    // lifting for hearth behaviour is the prior suites; this is
    // the light check that adding ingest did not move the
    // other source.
    let hearth_event = StreamEvent {
        source: Source::Hearth,
        seq: 3,
        kind: "resident".to_string(),
        model: Some("muse-a".to_string()),
        task: None,
        facts: json!({"endpoint": "127.0.0.1:8080", "warmup_ms": 11}),
        collection: "spine".to_string(),
        row: json!({
            "model": "muse-a",
            "kind": "resident",
            "facts": {"endpoint": "127.0.0.1:8080", "warmup_ms": 11},
            "seq": 3,
            "hash": "spine-hash-3",
            "ts": 1.5,
        }),
        row_kind: Some("resident".to_string()),
        hash: Some("spine-hash-3".to_string()),
        ts: Some(1.5),
    };
    let buffer = SiEventBuffer::new();
    let hub = EventHub::new(16);
    buffer.attach_hub(hub.clone());
    let server = EventsServer::bind(
        "127.0.0.1:0",
        Arc::new(VecLog {
            events: vec![hearth_event],
        }),
        Arc::new(buffer.clone()),
        hub,
        EventsConfig::default(),
    )
    .unwrap();
    let addr = server.local_addr();

    let out = http_get_to_eof(&addr, "/gaia/v1/events?source=hearth");
    assert!(out.starts_with("HTTP/1.1 200 OK"), "{out}");
    let events = parse_sse_events(&out);
    assert_eq!(events.len(), 1, "exactly the one hearth event: {out}");
    let (id, name, data) = &events[0];
    assert_eq!(*id, 3, "the hearth seq is the spine seq, unmerged");
    assert_eq!(name, "hearth:resident");
    assert_eq!(data["source"], json!("hearth"));
    assert_eq!(data["collection"], json!("spine"));
    assert_eq!(data["row"]["hash"], json!("spine-hash-3"));

    // And the two sequence spaces stay separate on this same
    // server: the si source, fed nothing here, replays empty.
    let out = http_get_to_eof(&addr, "/gaia/v1/events?source=si");
    assert!(out.starts_with("HTTP/1.1 200 OK"), "{out}");
    assert!(
        parse_sse_events(&out).is_empty(),
        "no hearth event leaks into the si space: {out}"
    );
}
