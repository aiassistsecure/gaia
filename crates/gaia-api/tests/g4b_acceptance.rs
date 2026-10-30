//! Acceptance tests for G-4b in `gaia-api` — persistent work
//! and combined-source SSE (the G-4b entry).
//!
//! These map one-to-one onto the G-4b criteria assigned to
//! this crate, in criterion order in the test names:
//!
//!   (1) persistence round-trip: a `FileWorkStore` record
//!       that was submitted, actuated, and completed — with
//!       `spine_event_refs` in its completion result —
//!       survives a drop/reopen whole; a duplicate submit of
//!       the same SI `journal_seq` returns the same work id
//!       and actuates nothing twice; a new `journal_seq`
//!       mints the next id;
//!   (2) torn tail: a truncated JSON line appended to the
//!       work file is counted in `load_failures()` on reopen,
//!       prior records stay intact, and the store still
//!       accepts new work;
//!   (3) combined subscription: one following connection to
//!       `?source=hearth&source=si&follow=1` receives live
//!       events from both sources in publish order, every
//!       wire `id:` source-qualified (`hearth:<seq>` /
//!       `si:<seq>`), while a separate single-source `si`
//!       connection receives only `si` events with bare ids;
//!   (4) combined resume: the combined resume header
//!       `Last-Event-ID: hearth:<seq>,si:<seq>` and the
//!       query-pairing form
//!       `source=hearth&since=N&source=si&since=M` each yield
//!       exactly the events after both per-source cursors —
//!       nothing repeated, nothing missed, ids qualified.
//!
//! Criteria (1)–(2) run against the file store in a temp
//! directory; (3)–(4) run over real sockets against a real
//! `EventsServer`, following the `sse_acceptance.rs` harness
//! style. Timeouts are generous but bounded: each one is a
//! failure bound, not an expectation.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use gaia_api::events::{EventFilter, EventLog, Source, StreamEvent};
use gaia_api::sse::{EventHub, EventsConfig, EventsServer};
use gaia_api::{
    run_actuation, Actuator, FileWorkStore, GaiaError, Intent, Verdict, WorkRecord, WorkState,
    WorkStore,
};
use serde_json::{json, Value};

// -- shared helpers ----------------------------------------------------------

/// A pid- and test-suffixed temp directory. (No `tempfile`
/// dependency in this crate; each test removes its own
/// directory afterwards, as in `sse_acceptance.rs`.)
fn temp_dir(test: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("gaia-g4b-api-{}-{test}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn admitted_intent(journal_seq: u64) -> Intent {
    Intent {
        tool: "hearth.restart".to_string(),
        args: json!({ "model": "glimmer" }),
        reason: "g4b persistence test".to_string(),
        task_id: "task-g4b".to_string(),
        journal_seq,
        class: "execute".to_string(),
        verdict: Some(Verdict::Admit),
    }
}

/// A stub actuator that counts its invocations and returns a
/// fixed result — the result carries `spine_event_refs`, so
/// the completed record's provenance has something real to
/// carry across the reopen.
struct CountingActuator {
    calls: usize,
    result: Value,
}

impl Actuator for CountingActuator {
    fn actuate(&mut self, work: &WorkRecord) -> Result<Value, GaiaError> {
        self.calls += 1;
        assert_eq!(work.state, WorkState::Admitted);
        Ok(self.result.clone())
    }
}

fn stub_result() -> Value {
    json!({
        "outcome": "executed",
        "detail": "counting stub actuated",
        "spine_event_refs": [{ "hash": "spine-hash-7", "seq": 7 }],
    })
}

// -- SSE helpers (the sse_acceptance.rs harness style) ------------------------

/// An `EventLog` over a shared, appendable event list — one
/// instance per source, so a test can add history between a
/// disconnect and a reconnect and the server's next replay
/// reads the history as it then stands.
#[derive(Clone, Default)]
struct StubLog {
    events: Arc<Mutex<Vec<StreamEvent>>>,
}

impl StubLog {
    fn with(events: Vec<StreamEvent>) -> StubLog {
        StubLog {
            events: Arc::new(Mutex::new(events)),
        }
    }

    fn add(&self, ev: StreamEvent) {
        self.events
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(ev);
    }
}

impl EventLog for StubLog {
    fn events_since(
        &self,
        source: Source,
        after_seq: u64,
        filter: &EventFilter,
    ) -> Vec<StreamEvent> {
        self.events
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter(|ev| ev.source == source && ev.seq > after_seq && filter.matches(ev))
            .cloned()
            .collect()
    }

    fn latest_seq(&self, source: Source) -> u64 {
        self.events
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter(|ev| ev.source == source)
            .map(|ev| ev.seq)
            .max()
            .unwrap_or(0)
    }
}

/// One stream event, constructed directly. `ts` is set (and
/// set increasing across a scenario's events) so a replay
/// that orders by timestamp and a replay that reads log
/// order agree on the same sequence.
fn stream_event(source: Source, seq: u64, ts: f64) -> StreamEvent {
    let (kind, collection, hash) = match source {
        Source::Hearth => ("resident", "spine", Some(format!("spine-hash-{seq}"))),
        Source::Si => ("task_open", "experiences", None),
    };
    StreamEvent {
        source,
        seq,
        kind: kind.to_string(),
        model: None,
        task: None,
        facts: json!({}),
        collection: collection.to_string(),
        row: json!({ "kind": "tool", "seq": seq }),
        row_kind: Some("tool".to_string()),
        hash,
        ts: Some(ts),
    }
}

fn hearth_event(seq: u64, ts: f64) -> StreamEvent {
    stream_event(Source::Hearth, seq, ts)
}

fn si_event(seq: u64, ts: f64) -> StreamEvent {
    stream_event(Source::Si, seq, ts)
}

fn server_with(hearth: StubLog, si: StubLog, hub: EventHub, config: EventsConfig) -> EventsServer {
    EventsServer::bind("127.0.0.1:0", Arc::new(hearth), Arc::new(si), hub, config).unwrap()
}

fn quiet_config() -> EventsConfig {
    EventsConfig {
        max_connections: 8,
        // No heartbeat frames inside an assertion window: a
        // heartbeat is a comment, but a quiet stream keeps
        // the received bytes exactly the events under test.
        heartbeat: Duration::from_secs(60),
        write_timeout: Duration::from_secs(5),
    }
}

/// Read a stream one byte at a time until the SSE banner has
/// fully arrived, and return everything read (headers +
/// banner). Byte-at-a-time is deliberate, for the reason
/// `sse_acceptance.rs` gives: a chunked read could pull event
/// bytes into a userspace buffer the rest of the test cannot
/// see.
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

/// Open one connection to `target` and read it through its
/// banner.
fn open_through_banner(addr: &str, target: &str) -> (TcpStream, String) {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .write_all(format!("GET {target} HTTP/1.1\r\nhost: 127.0.0.1\r\n\r\n").as_bytes())
        .unwrap();
    stream.flush().unwrap();
    let banner = read_through_banner(&mut stream);
    (stream, banner)
}

/// Send one raw request and read the response to EOF.
/// Returning at all means the server closed the connection;
/// a read timeout panics instead, so "closed" is never
/// assumed.
fn http_exchange(addr: &str, request: &str) -> String {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream.write_all(request.as_bytes()).unwrap();
    stream.flush().unwrap();
    let mut out: Vec<u8> = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => out.extend_from_slice(&buf[..n]),
            Err(e) => panic!("read failed before EOF ({e})"),
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Read from `stream` until `needle` appears in the
/// accumulated bytes, the stream ends, or the deadline
/// passes.
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

/// The `id:` lines of a received stream, in arrival order,
/// with the `id: ` prefix stripped.
fn id_lines(text: &str) -> Vec<String> {
    text.lines()
        .filter_map(|line| line.strip_prefix("id: "))
        .map(|id| id.trim().to_string())
        .collect()
}

/// The `event:` lines of a received stream, in arrival order.
fn event_lines(text: &str) -> Vec<String> {
    text.lines()
        .filter_map(|line| line.strip_prefix("event: "))
        .map(|name| name.trim().to_string())
        .collect()
}

// -- criterion (1): persistence round-trip ------------------------------------

#[test]
fn criterion_1_file_work_store_round_trip_survives_reopen_and_dedups() {
    let dir = temp_dir("criterion-1");
    let path = dir.join("work.jsonl");
    let mut actuator = CountingActuator {
        calls: 0,
        result: stub_result(),
    };

    let submitted = {
        let mut store = FileWorkStore::open(&path).unwrap();
        assert!(store.records().is_empty());
        assert!(store.load_failures().is_empty());

        let submitted = store.submit(admitted_intent(4201)).unwrap();
        assert_eq!(submitted.id, "work-000001");
        assert_eq!(submitted.state, WorkState::Admitted);

        let done = run_actuation(&mut store, &mut actuator, &submitted.id).unwrap();
        assert_eq!(actuator.calls, 1);
        assert_eq!(done.state, WorkState::Executed);
        assert_eq!(done.result, Some(stub_result()));

        // Provenance before the drop: the status body reports
        // the completion result's spine refs.
        let body = done.status_body();
        assert_eq!(body["provenance"]["si_journal_seqs"], json!([4201]));
        assert_eq!(
            body["provenance"]["spine_event_refs"],
            json!([{ "hash": "spine-hash-7", "seq": 7 }]),
            "status body carries the result's spine refs: {body}"
        );
        assert_eq!(body["result"], stub_result());
        done
    };
    // The store is dropped here; everything below is read back
    // from the file alone.

    let mut store = FileWorkStore::open(&path).unwrap();
    assert!(
        store.load_failures().is_empty(),
        "a clean file loads with no failures: {:?}",
        store.load_failures()
    );
    assert_eq!(store.records().len(), 1);
    let reopened = store
        .get(&submitted.id)
        .expect("the record survived the reopen");
    assert_eq!(
        reopened, submitted,
        "record, terminal state, result, and intent all intact after reopen"
    );
    assert_eq!(reopened.state, WorkState::Executed);
    assert_eq!(reopened.result, Some(stub_result()));
    let body = reopened.status_body();
    assert_eq!(
        body["provenance"]["spine_event_refs"],
        json!([{ "hash": "spine-hash-7", "seq": 7 }]),
        "provenance refs intact after reopen: {body}"
    );

    // Dedup survives the reopen: the same journal_seq is the
    // same work, in the state it now stands in.
    let duplicate = store.submit(admitted_intent(4201)).unwrap();
    assert_eq!(duplicate.id, submitted.id);
    assert_eq!(duplicate.state, WorkState::Executed);
    assert_eq!(
        store.records().len(),
        1,
        "the duplicate recorded nothing new"
    );

    // And nothing re-actuates: claiming the executed record
    // is a conflict, and a full run_actuation attempt leaves
    // the counting actuator exactly where it was.
    let err = store.claim_for_actuation(&submitted.id).unwrap_err();
    assert_eq!(err.error_type(), "gaia_verdict_conflict");
    let err = run_actuation(&mut store, &mut actuator, &submitted.id).unwrap_err();
    assert_eq!(err.error_type(), "gaia_verdict_conflict");
    assert_eq!(actuator.calls, 1, "nothing re-actuated after reopen");

    // A new journal_seq is new work, and it mints the next id
    // — the duplicate above spent none.
    let next = store.submit(admitted_intent(4202)).unwrap();
    assert_eq!(next.id, "work-000002");
    assert_eq!(next.state, WorkState::Admitted);
    assert_eq!(store.records().len(), 2);

    drop(store);
    let _ = std::fs::remove_dir_all(&dir);
}

// -- criterion (2): torn tail ---------------------------------------------------

#[test]
fn criterion_2_torn_tail_counts_one_load_failure_and_prior_records_stay_intact() {
    let dir = temp_dir("criterion-2");
    let path = dir.join("work.jsonl");
    let mut actuator = CountingActuator {
        calls: 0,
        result: stub_result(),
    };

    let done = {
        let mut store = FileWorkStore::open(&path).unwrap();
        let submitted = store.submit(admitted_intent(4301)).unwrap();
        let done = run_actuation(&mut store, &mut actuator, &submitted.id).unwrap();
        assert_eq!(done.state, WorkState::Executed);
        done
    };
    assert_eq!(actuator.calls, 1);

    // Torn tail: a truncated JSON line appended to the file,
    // exactly what a crash mid-append leaves behind.
    {
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        writeln!(file, "{{\"work_id\":\"work-000099\",\"intent\":{{\"tool\":").unwrap();
        file.flush().unwrap();
    }

    let mut store = FileWorkStore::open(&path).unwrap();
    assert_eq!(
        store.load_failures().len(),
        1,
        "exactly the torn line fails to load: {:?}",
        store.load_failures()
    );
    assert_eq!(store.records().len(), 1, "prior records intact");
    let reopened = store
        .get(&done.id)
        .expect("the prior record survived the torn tail");
    assert_eq!(reopened, done);
    assert_eq!(reopened.state, WorkState::Executed);
    assert_eq!(reopened.result, Some(stub_result()));

    // The store accepts new work after the torn tail, minting
    // the next id after the records it actually holds.
    let next = store.submit(admitted_intent(4302)).unwrap();
    assert_eq!(next.id, "work-000002");
    assert_eq!(next.state, WorkState::Admitted);
    assert_eq!(store.records().len(), 2);

    drop(store);
    let _ = std::fs::remove_dir_all(&dir);
}

// -- criterion (3): combined subscription ---------------------------------------

#[test]
fn criterion_3_combined_subscription_interleaves_live_events_with_qualified_ids() {
    let hub = EventHub::new(64);
    let server = server_with(
        StubLog::default(),
        StubLog::default(),
        hub.clone(),
        quiet_config(),
    );
    let addr = server.local_addr();

    // Both connections are open before anything is published:
    // what they receive is the live segment, and its order is
    // publish order.
    let (mut combined, banner) =
        open_through_banner(&addr, "/gaia/v1/events?source=hearth&source=si&follow=1");
    assert!(banner.contains("200 OK"), "{banner}");
    let (mut single, banner) = open_through_banner(&addr, "/gaia/v1/events?source=si&follow=1");
    assert!(banner.contains("200 OK"), "{banner}");

    // Interleaved live events, published in exactly the order
    // the combined connection must receive them.
    hub.publish(&hearth_event(1, 1.0));
    hub.publish(&si_event(1, 2.0));
    hub.publish(&hearth_event(2, 3.0));
    hub.publish(&si_event(2, 4.0));

    let received = read_until_contains(&mut combined, b"id: si:2\n", Duration::from_secs(10));
    let received = String::from_utf8_lossy(&received).into_owned();
    assert_eq!(
        id_lines(&received),
        vec!["hearth:1", "si:1", "hearth:2", "si:2"],
        "combined ids arrive in publish order, every one source-qualified: {received}"
    );
    assert_eq!(
        event_lines(&received),
        vec![
            "hearth:resident",
            "si:task_open",
            "hearth:resident",
            "si:task_open"
        ],
        "the event names interleave with the ids: {received}"
    );

    // The single-source connection, open through the same
    // publishes, receives only the si events — with bare ids,
    // the single-source wire shape unchanged.
    let received = read_until_contains(&mut single, b"id: 2\n", Duration::from_secs(10));
    let received = String::from_utf8_lossy(&received).into_owned();
    assert_eq!(
        id_lines(&received),
        vec!["1", "2"],
        "single-source ids stay bare: {received}"
    );
    assert_eq!(
        event_lines(&received),
        vec!["si:task_open", "si:task_open"],
        "only si events reached the si-only connection: {received}"
    );
    assert!(
        !received.contains("hearth:"),
        "no source-qualified (combined) frame on a single-source connection: {received}"
    );
}

// -- criterion (4): combined resume ----------------------------------------------

#[test]
fn criterion_4_combined_resume_by_header_and_by_query_pairing() {
    let hearth_log = StubLog::with(vec![hearth_event(1, 1.0)]);
    let si_log = StubLog::with(vec![si_event(1, 2.0)]);
    let server = server_with(
        hearth_log.clone(),
        si_log.clone(),
        EventHub::new(64),
        quiet_config(),
    );
    let addr = server.local_addr();

    // The first combined connection receives hearth 1 + si 1
    // by replay, then ends (a replay-only connection closes
    // after its history — that close is the disconnect).
    let first = http_exchange(
        &addr,
        "GET /gaia/v1/events?source=hearth&source=si HTTP/1.1\r\nhost: 127.0.0.1\r\n\r\n",
    );
    assert!(first.starts_with("HTTP/1.1 200 OK"), "{first}");
    assert_eq!(
        id_lines(&first),
        vec!["hearth:1", "si:1"],
        "the first connection's combined replay: {first}"
    );

    // While disconnected, each log gains its seq 2.
    hearth_log.add(hearth_event(2, 3.0));
    si_log.add(si_event(2, 4.0));

    // Resume by header: the combined Last-Event-ID carries
    // both sources' cursors, and exactly the two missed
    // events arrive — nothing repeated, nothing missed.
    let resumed = http_exchange(
        &addr,
        "GET /gaia/v1/events?source=hearth&source=si HTTP/1.1\r\nhost: 127.0.0.1\r\nLast-Event-ID: hearth:1,si:1\r\n\r\n",
    );
    assert!(resumed.starts_with("HTTP/1.1 200 OK"), "{resumed}");
    assert_eq!(
        id_lines(&resumed),
        vec!["hearth:2", "si:2"],
        "header resume yields exactly the missed events, ids qualified: {resumed}"
    );

    // The query-pairing form names each source's cursor with
    // its own `since`, beside its own `source`, and yields
    // the same two events.
    let paired = http_exchange(
        &addr,
        "GET /gaia/v1/events?source=hearth&since=1&source=si&since=1 HTTP/1.1\r\nhost: 127.0.0.1\r\n\r\n",
    );
    assert!(paired.starts_with("HTTP/1.1 200 OK"), "{paired}");
    assert_eq!(
        id_lines(&paired),
        vec!["hearth:2", "si:2"],
        "query-paired resume yields the same two events: {paired}"
    );
}
