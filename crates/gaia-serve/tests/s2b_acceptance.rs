//! Fork-half acceptance tests for G-9 §15 S-2 (proposal
//! egress, slice S-2b).
//!
//! Honest scope, stated plainly: **the stub in this file is a
//! test fixture, NOT SI.** It implements the SI-side contract
//! of G-9 §8.3 — `POST /gaia/proposals`, validation, dedup by
//! `caused_by`, journal-before-ACK, the ACK shape — so the
//! fork half of S-2 can be proven before the real SI endpoint
//! exists. §15 S-2's joint acceptance closes only when the
//! real SI endpoint (slice S-2a, in the SentinelIntelligence
//! repo) lands and this same scenario runs against it. These
//! tests claim nothing about the real SI service: not its
//! gate, not its journal, not its performance.
//!
//! The four tests map to §15 S-2's acceptance criteria, fork
//! half:
//!
//! 1. [`killed_child_proposal_is_posted_and_its_ack_is_recorded`]
//!    — criterion 1: a watcher proposal for a killed
//!    miniature-fleet child is POSTed verbatim (§8.2: the body
//!    is the proposal record, `args.spine_event` agreeing with
//!    `caused_by`), and the ACK's disposition — including the
//!    `journal_seq` the ACK names — is recorded in the fork's
//!    egress log (§8.4). A second pass sends nothing.
//! 2. [`a_lost_ack_is_retried_and_the_identical_ack_accepted`]
//!    — criterion 2: the stub journals the proposal and then
//!    drops the response (the ACK lost in transit, §8.3's
//!    retry case). The retry yields exactly one stub journal
//!    entry and the identical ACK, deduped by `caused_by`;
//!    the fork records nothing before an answer arrives.
//! 3. [`reject_and_escalate_verdicts_are_terminal_and_submit_no_work`]
//!    — criterion 3, plus the ESCALATE half of §9.4: REJECT
//!    and ESCALATE dispositions are terminal in the egress
//!    log, are never re-sent, and submit no work. (Criterion
//!    3 names REJECT; §9.4 gives ESCALATE the same terminal,
//!    no-submission disposition for v1.)
//! 4. [`a_restart_resends_unacked_proposals_and_never_acked_ones`]
//!    — criterion 5: a fork restart (both stores reopened from
//!    their files) re-sends exactly the un-ACKed proposals and
//!    never an ACKed one; the reopened egress store already
//!    reports the ACKed disposition before any new drive.
//!
//! Criterion 4 (malformed proposal → 400, zero journal
//! growth) is SI-side behaviour; the stub fixture implements
//! the §8.3 validation half of it (400, nothing journaled),
//! but proving it against the real endpoint belongs to S-2a.
//!
//! The fleet scenario is a miniature: a python stand-in child
//! serving `{}` over HTTP — no machine learning runtime, no
//! real weights, no GPU. What is real is everything the
//! criteria are about: a real supervisor starting and losing
//! a real child process, the real spine, watcher, proposal
//! store and egress store, and real HTTP over loopback TCP
//! between the fork's egress client and the stub.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use gaia_api::{EventHub, MemoryWorkStore, MirrorStore};
use gaia_serve::{
    drive_egress, Composition, Disposition, EgressStore, HttpProposalSink, Proposal, ProposalStore,
    Watcher,
};
use hearth_core::{Budget, Declared, GIB};
use hearth_serve::server::{free_port, ServerSpec};
use hearth_serve::Supervisor;
use hearth_store::{EventRef, Spine};
use serde_json::{json, Value};

// ----------------------------------------------------------------
// The stub SI fixture
// ----------------------------------------------------------------

/// One stub journal key: the proposal's `caused_by` pair
/// (§8.2 — the pair is the key, neither half alone).
type StubKey = (String, u64);

#[derive(Default)]
struct StubState {
    /// Every valid proposal body received, in arrival order —
    /// logged before any response is written (§8.3: a retry
    /// is a second request but not a second journal entry).
    requests: Vec<Value>,
    /// The stub's journal: `(caused_by, ACK)` in journaling
    /// order. An entry's `journal_seq` is `5000 + position`,
    /// position 1-based, fixed when the entry is written.
    journal: Vec<(StubKey, Value)>,
    /// Per-key verdict overrides; absent keys get `ADMIT`.
    verdicts: HashMap<StubKey, String>,
    /// Responses still to drop: decremented when a newly
    /// journaled ACK is withheld (journaled, never sent).
    drop_next: usize,
}

impl StubState {
    fn ack_for(&self, key: &StubKey) -> Option<Value> {
        self.journal
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, ack)| ack.clone())
    }
}

/// A stub implementing SI's §8.3 proposal ingest, for these
/// tests only. See the file doc: this is a fixture, not SI.
struct StubSi {
    addr: String,
    state: Arc<Mutex<StubState>>,
    shutdown: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl StubSi {
    fn start() -> StubSi {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let state = Arc::new(Mutex::new(StubState::default()));
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_state = Arc::clone(&state);
        let thread_shutdown = Arc::clone(&shutdown);
        let handle = std::thread::spawn(move || {
            while !thread_shutdown.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _)) => handle_stub_connection(stream, &thread_state),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => std::thread::sleep(Duration::from_millis(5)),
                }
            }
        });
        StubSi {
            addr,
            state,
            shutdown,
            handle: Some(handle),
        }
    }

    fn addr(&self) -> String {
        self.addr.clone()
    }

    fn set_verdict(&self, hash: &str, seq: u64, verdict: &str) {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .verdicts
            .insert((hash.to_string(), seq), verdict.to_string());
    }

    fn drop_next_responses(&self, n: usize) {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .drop_next = n;
    }

    fn requests(&self) -> Vec<Value> {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .requests
            .clone()
    }

    fn journal_len(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .journal
            .len()
    }

    fn ack_for(&self, hash: &str, seq: u64) -> Option<Value> {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .ack_for(&(hash.to_string(), seq))
    }
}

impl Drop for StubSi {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// Find `needle` in `haystack`, for locating the header/body
/// boundary of a request read off the wire.
fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Write one HTTP/1.1 response: JSON body, `Content-Length`,
/// `Connection: close` — the client reads exactly this body
/// and the close ends the exchange.
fn write_stub_response(stream: &mut TcpStream, status: u16, reason: &str, body: &Value) {
    let payload = serde_json::to_string(body).unwrap();
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{payload}",
        payload.len()
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

/// Serve one connection to the stub: read the request head
/// and its `Content-Length` body, route on the path, and act
/// per §8.3 — validate, log, dedup, journal, then ACK (or,
/// when drops are armed, journal and withhold the ACK).
fn handle_stub_connection(mut stream: TcpStream, state: &Arc<Mutex<StubState>>) {
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));

    // Read until the head is complete and the whole body the
    // head promises has arrived (or the peer gives up).
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 4096];
    let mut header_end: Option<usize> = None;
    let mut content_length = 0usize;
    loop {
        if header_end.is_none() {
            if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&buf[..pos]).into_owned();
                content_length = head
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        if name.trim().eq_ignore_ascii_case("content-length") {
                            value.trim().parse::<usize>().ok()
                        } else {
                            None
                        }
                    })
                    .unwrap_or(0);
                header_end = Some(pos);
            }
        }
        if let Some(pos) = header_end {
            if buf.len() >= pos + 4 + content_length {
                break;
            }
        }
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(_) => break,
        }
    }
    let Some(pos) = header_end else { return };
    if buf.len() < pos + 4 + content_length {
        return;
    }
    let head = String::from_utf8_lossy(&buf[..pos]).into_owned();
    let path = head.split_whitespace().nth(1).unwrap_or("").to_string();
    let body_bytes = buf[pos + 4..pos + 4 + content_length].to_vec();

    if path != "/gaia/proposals" {
        write_stub_response(
            &mut stream,
            404,
            "Not Found",
            &json!({"error": "not found"}),
        );
        return;
    }

    // §8.3 step 1: validate shape; refuse malformed with 400,
    // journaling nothing.
    let parsed: Option<Value> = serde_json::from_slice(&body_bytes).ok();
    let key: Option<StubKey> = parsed.as_ref().and_then(|v| {
        let caused = v.get("caused_by")?;
        Some((
            caused.get("hash")?.as_str()?.to_string(),
            caused.get("seq")?.as_u64()?,
        ))
    });
    let (Some(body), Some(key)) = (parsed, key) else {
        write_stub_response(
            &mut stream,
            400,
            "Bad Request",
            &json!({"error": "malformed proposal"}),
        );
        return;
    };

    // Log every request before responding; then dedup (§8.3
    // step 2), then journal-before-ACK (steps 4–5).
    let (ack, drop_response) = {
        let mut st = state.lock().unwrap_or_else(|p| p.into_inner());
        st.requests.push(body);
        if let Some(existing) = st.ack_for(&key) {
            // Dedup: the original ACK, unchanged.
            (existing, false)
        } else {
            let verdict = st
                .verdicts
                .get(&key)
                .cloned()
                .unwrap_or_else(|| "ADMIT".to_string());
            let journal_seq = 5000 + st.journal.len() as u64 + 1;
            let ack = json!({
                "task_seq": 9,
                "journal_seq": journal_seq,
                "verdict": verdict,
            });
            st.journal.push((key, ack.clone()));
            if st.drop_next > 0 {
                st.drop_next -= 1;
                (ack, true)
            } else {
                (ack, false)
            }
        }
    };
    if drop_response {
        // The ACK was journaled but is lost in transit: close
        // without responding. The fork's retry is answered by
        // the dedup branch above, with this same ACK.
        drop(stream);
        return;
    }
    write_stub_response(&mut stream, 200, "OK", &ack);
}

// ----------------------------------------------------------------
// Shared helpers
// ----------------------------------------------------------------

/// A standalone proposal with a distinct causal ref per call
/// site, shaped exactly as the watcher builds them — `args`
/// carries `spine_event` duplicating `caused_by` (§8.2).
fn make_proposal(model: &str, hash: &str, seq: u64) -> Proposal {
    Proposal {
        model: model.to_string(),
        tool: "hearth.restart".to_string(),
        args: json!({
            "model": model,
            "spine_event": { "hash": hash, "seq": seq },
        }),
        reason: format!("observed loss at seq {seq}"),
        caused_by: EventRef {
            hash: hash.to_string(),
            seq,
        },
    }
}

/// Lines in a JSONL store file; a missing file is zero lines
/// (the store has recorded nothing — the file may not exist
/// until the first record lands).
fn line_count(path: &Path) -> usize {
    match std::fs::read_to_string(path) {
        Ok(text) => text.lines().count(),
        Err(_) => 0,
    }
}

/// An address nothing listens on: bind, take the address,
/// drop the listener. Connecting is refused — a transport
/// failure, which §8.5/§13 make retryable, never terminal.
fn dead_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    drop(listener);
    addr
}

/// The `(journal_seq, task_seq)` of an `Acked` disposition;
/// any other disposition is a failure of the test calling
/// this, said so in the panic.
fn acked_pair(disposition: Disposition) -> (u64, Option<u64>) {
    match disposition {
        Disposition::Acked {
            journal_seq,
            task_seq,
        } => (journal_seq, task_seq),
        _ => panic!("expected an Acked disposition"),
    }
}

// ----------------------------------------------------------------
// Criterion 1 — the fleet scenario, end to end (fork half)
// ----------------------------------------------------------------

/// The fake serving child, copied from
/// `composition_acceptance.rs`: a tiny HTTP server that
/// answers 200 with `{}` on any GET, including the
/// supervisor's `/health` probe. It parses `--port` out of
/// its argv and ignores the rest.
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

#[test]
fn killed_child_proposal_is_posted_and_its_ack_is_recorded() {
    let dir = tempfile::tempdir().unwrap();
    let stub = StubSi::start();

    // The fake server script, executable, and a dummy weights
    // file — the supervisor only checks the path exists before
    // spawning, and the script never reads it.
    let script = dir.path().join("fake_server.py");
    std::fs::write(&script, FAKE_SERVER).unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let gguf = dir.path().join("fake.gguf");
    std::fs::write(&gguf, b"not a real gguf").unwrap();

    // The composition, exactly as the binary builds it.
    let spine = Spine::open(&dir.path().join("spine")).unwrap();
    let mirror = Arc::new(Mutex::new(
        MirrorStore::open(&dir.path().join("mirror.jsonl")).unwrap(),
    ));
    let proposals = ProposalStore::open(&dir.path().join("proposals.jsonl")).unwrap();
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
    let mut comp = Composition::new(supervisor, mirror, watcher, proposals, hub);

    // Start the model through the composition's supervisor.
    let mut spec = ServerSpec::new("fake", &gguf, free_port().unwrap());
    spec.binary = script.clone();
    spec.log_dir = dir.path().to_path_buf();
    comp.supervisor().start(spec).unwrap();

    // Step the composition until the spine calls the model
    // resident. Bounded: a model that never comes up is a
    // failure of this test, not a hang.
    let mut resident = false;
    for _ in 0..300 {
        comp.tick_once().unwrap();
        if comp
            .spine()
            .latest("fake")
            .is_some_and(|e| e.kind == "resident")
        {
            resident = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(resident, "fake never became resident");

    // The child's pid is a spine fact (the `loading` event's
    // facts), not a process-table guess.
    let pid = comp
        .spine()
        .why("fake")
        .into_iter()
        .find(|e| e.kind == "loading")
        .expect("a resident model has a loading event")
        .facts["pid"]
        .as_u64()
        .expect("the loading event records a pid");

    // Kill the child out from under the supervisor.
    let status = std::process::Command::new("kill")
        .args(["-9", &pid.to_string()])
        .status()
        .unwrap();
    assert!(status.success(), "kill -9 {pid} failed: {status}");

    // Wait until the child is actually dead before ticking
    // again — the same synchronisation as
    // `composition_acceptance.rs`: a probe landing between
    // the signal and the exit is a different scenario with a
    // different, upstream-correct answer.
    for _ in 0..200 {
        match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            Err(_) => break, // Gone entirely: dead by any reading.
            Ok(stat) => {
                let state = stat
                    .rsplit_once(')')
                    .and_then(|(_, rest)| rest.split_whitespace().next());
                if state == Some("Z") {
                    break;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    // Step until the watcher has recorded exactly one proposal.
    for _ in 0..300 {
        comp.tick_once().unwrap();
        if comp.proposals().len() == 1 {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        comp.proposals().len(),
        1,
        "exactly one proposal for the loss"
    );
    let proposal = comp.proposals()[0].clone();

    // Install the file-backed egress store and the HTTP sink
    // to the stub, exactly as the binary wires `--si-addr`.
    comp.set_egress_store(EgressStore::open(&dir.path().join("egress.jsonl")).unwrap());
    let si_addr = stub.addr();
    comp.set_egress_sink(Box::new(HttpProposalSink::new(&si_addr)));

    // One egress pass: the proposal is posted and ACKed.
    let pass = comp.egress_pass().unwrap();
    assert_eq!(pass.attempted, 1, "one proposal offered");
    assert_eq!(pass.acked, 1, "one proposal ACKed ADMIT");

    // The stub received exactly one request, and its body is
    // the proposal record verbatim (§8.2).
    let requests = stub.requests();
    assert_eq!(requests.len(), 1, "the stub received exactly one request");
    assert_eq!(
        requests[0],
        proposal.to_json(),
        "the body is the proposal verbatim"
    );
    // §8.2 agreement: `args.spine_event` duplicates
    // `caused_by`, field for field.
    assert_eq!(
        requests[0]["args"]["spine_event"]["hash"].as_str(),
        Some(proposal.caused_by.hash.as_str())
    );
    assert_eq!(
        requests[0]["args"]["spine_event"]["seq"].as_u64(),
        Some(proposal.caused_by.seq)
    );
    assert_eq!(requests[0]["args"]["spine_event"], requests[0]["caused_by"]);

    // The fork recorded the ACK (§8.4): the disposition is
    // Acked, carrying the stub ACK's journal_seq and the
    // seam task_seq.
    let stub_ack = stub
        .ack_for(&proposal.caused_by.hash, proposal.caused_by.seq)
        .expect("the stub journaled the proposal");
    let (journal_seq, task_seq) = acked_pair(comp.egress().disposition(&proposal.caused_by));
    assert_eq!(
        journal_seq,
        stub_ack["journal_seq"].as_u64().unwrap(),
        "the recorded journal_seq is the ACK's"
    );
    assert_eq!(task_seq, Some(9), "the recorded task_seq is the ACK's");

    // A second pass attempts nothing: an ACKed proposal is
    // never re-sent, and the stub sees no second request.
    let second = comp.egress_pass().unwrap();
    assert_eq!(second.attempted, 0, "an ACKed proposal is not re-sent");
    assert_eq!(
        stub.requests().len(),
        1,
        "the stub still has exactly one request"
    );
}

// ----------------------------------------------------------------
// Criterion 2 — a lost ACK is retried; the identical ACK lands
// ----------------------------------------------------------------

#[test]
fn a_lost_ack_is_retried_and_the_identical_ack_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let egress_path = dir.path().join("egress.jsonl");
    let stub = StubSi::start();
    stub.drop_next_responses(1);

    let proposal = make_proposal("fake", "hash-lost-ack", 41);
    let mut proposals = ProposalStore::open(&dir.path().join("proposals.jsonl")).unwrap();
    proposals.record(proposal.clone()).unwrap();
    let mut egress = EgressStore::open(&egress_path).unwrap();
    let si_addr = stub.addr();
    let sink = HttpProposalSink::new(&si_addr);

    // First drive: the stub journals the proposal, then the
    // ACK is lost in transit. Nothing is recorded before an
    // answer arrives — the proposal stays Pending, and the
    // egress file holds no line for it.
    let first = drive_egress(proposals.proposals(), &mut egress, &sink).unwrap();
    assert_eq!(first.attempted, 1);
    assert_eq!(
        first.still_pending, 1,
        "a lost ACK leaves the proposal pending"
    );
    assert_eq!(first.acked, 0);
    assert_eq!(
        line_count(&egress_path),
        0,
        "nothing is recorded before an answer"
    );
    assert!(
        matches!(
            egress.disposition(&proposal.caused_by),
            Disposition::Pending
        ),
        "the disposition is Pending after the lost ACK"
    );

    // Second drive: the retry is deduped by `caused_by` at
    // the stub (§8.3 step 2) and the identical ACK accepted.
    let second = drive_egress(proposals.proposals(), &mut egress, &sink).unwrap();
    assert_eq!(second.attempted, 1);
    assert_eq!(second.acked, 1, "the retry is ACKed");

    assert_eq!(
        stub.journal_len(),
        1,
        "one stub journal entry despite two POSTs — the dedup held"
    );
    assert_eq!(stub.requests().len(), 2, "both POSTs reached the stub");
    assert_eq!(egress.records().len(), 1, "exactly one egress record");
    assert_eq!(
        line_count(&egress_path),
        1,
        "the egress file has exactly one line"
    );

    let stub_ack = stub
        .ack_for(&proposal.caused_by.hash, proposal.caused_by.seq)
        .expect("the stub journaled the proposal");
    let (journal_seq, task_seq) = acked_pair(egress.disposition(&proposal.caused_by));
    assert_eq!(
        journal_seq,
        stub_ack["journal_seq"].as_u64().unwrap(),
        "the accepted ACK is the original ACK, journal_seq unchanged"
    );
    assert_eq!(task_seq, Some(9));
}

// ----------------------------------------------------------------
// Criterion 3 — REJECT / ESCALATE are terminal, submit no work
// ----------------------------------------------------------------

/// Neither verdict actuates anything, and this test does not
/// pretend to assert that with a counter: there is no
/// actuator in the egress path at all. Egress ends at
/// recording the disposition — submission is a later slice
/// (S-3), and actuation remains reachable only through
/// `Composition::actuate_work` on an ADMIT-ted work record.
/// What this test *can* pin is the structural half: no work
/// record is created anywhere egress can see, and neither
/// proposal is ever offered again.
#[test]
fn reject_and_escalate_verdicts_are_terminal_and_submit_no_work() {
    let dir = tempfile::tempdir().unwrap();
    let stub = StubSi::start();

    let rejected = make_proposal("fake", "hash-rejected", 51);
    let escalated = make_proposal("fake", "hash-escalated", 52);
    stub.set_verdict(&rejected.caused_by.hash, rejected.caused_by.seq, "REJECT");
    stub.set_verdict(
        &escalated.caused_by.hash,
        escalated.caused_by.seq,
        "ESCALATE",
    );

    let mut proposals = ProposalStore::open(&dir.path().join("proposals.jsonl")).unwrap();
    proposals.record(rejected.clone()).unwrap();
    proposals.record(escalated.clone()).unwrap();
    let mut egress = EgressStore::open(&dir.path().join("egress.jsonl")).unwrap();

    // A work store the test holds, standing in for the fork's:
    // egress has no handle to it and no path to it, so any
    // submission would have to be visible here. It must stay
    // empty — REJECT and ESCALATE submit no work (§9.4).
    let work = MemoryWorkStore::new();

    let si_addr = stub.addr();
    let sink = HttpProposalSink::new(&si_addr);
    let pass = drive_egress(proposals.proposals(), &mut egress, &sink).unwrap();
    assert_eq!(pass.attempted, 2);
    assert_eq!(pass.rejected, 1, "one REJECT verdict");
    assert_eq!(pass.escalated, 1, "one ESCALATE verdict");
    assert_eq!(pass.acked, 0, "neither proposal was admitted");

    // Both dispositions are terminal and carry the stub
    // ACKs' journal_seqs.
    let rejected_ack = stub
        .ack_for(&rejected.caused_by.hash, rejected.caused_by.seq)
        .expect("the stub journaled the rejected proposal");
    match egress.disposition(&rejected.caused_by) {
        Disposition::Rejected { journal_seq, .. } => assert_eq!(
            journal_seq,
            rejected_ack["journal_seq"].as_u64().unwrap(),
            "the rejected disposition carries the ACK's journal_seq"
        ),
        _ => panic!("expected a Rejected disposition for the REJECT verdict"),
    }
    let escalated_ack = stub
        .ack_for(&escalated.caused_by.hash, escalated.caused_by.seq)
        .expect("the stub journaled the escalated proposal");
    match egress.disposition(&escalated.caused_by) {
        Disposition::Escalated { journal_seq, .. } => assert_eq!(
            journal_seq,
            escalated_ack["journal_seq"].as_u64().unwrap(),
            "the escalated disposition carries the ACK's journal_seq"
        ),
        _ => panic!("expected an Escalated disposition for the ESCALATE verdict"),
    }

    assert!(
        work.records().is_empty(),
        "no work was submitted for either verdict"
    );

    // Terminal means terminal: a second drive offers neither
    // proposal again.
    let second = drive_egress(proposals.proposals(), &mut egress, &sink).unwrap();
    assert_eq!(second.attempted, 0, "terminal dispositions are not re-sent");
    assert_eq!(second.skipped, 2, "both proposals are skipped as answered");
    assert_eq!(stub.requests().len(), 2, "the stub saw no further requests");
    assert!(work.records().is_empty(), "still no work submitted");
}

// ----------------------------------------------------------------
// Criterion 5 — a restart re-sends un-ACKed, never ACKed
// ----------------------------------------------------------------

#[test]
fn a_restart_resends_unacked_proposals_and_never_acked_ones() {
    let dir = tempfile::tempdir().unwrap();
    let proposals_path = dir.path().join("proposals.jsonl");
    let egress_path = dir.path().join("egress.jsonl");
    let stub = StubSi::start();

    let p1 = make_proposal("fake", "hash-restart-one", 61);
    let p2 = make_proposal("fake", "hash-restart-two", 62);

    // Phase 1: P1 is recorded, offered to a dead address
    // (transport failure — still pending), then offered to
    // the live stub and ACKed.
    {
        let mut store = ProposalStore::open(&proposals_path).unwrap();
        store.record(p1.clone()).unwrap();
    }
    let mut egress = EgressStore::open(&egress_path).unwrap();
    let dead = dead_addr();
    let dead_sink = HttpProposalSink::new(&dead);
    let refused = drive_egress(std::slice::from_ref(&p1), &mut egress, &dead_sink).unwrap();
    assert_eq!(
        refused.attempted, 1,
        "P1 was attempted against the dead address"
    );
    assert_eq!(
        refused.still_pending, 1,
        "a transport failure leaves P1 pending"
    );
    assert!(
        matches!(egress.disposition(&p1.caused_by), Disposition::Pending),
        "P1 is Pending after the refused drive"
    );

    let si_addr = stub.addr();
    let sink = HttpProposalSink::new(&si_addr);
    let acked = drive_egress(std::slice::from_ref(&p1), &mut egress, &sink).unwrap();
    assert_eq!(acked.acked, 1, "P1 is ACKed by the live stub");
    let p1_pair = acked_pair(egress.disposition(&p1.caused_by));
    let stub_ack = stub
        .ack_for(&p1.caused_by.hash, p1.caused_by.seq)
        .expect("the stub journaled P1");
    assert_eq!(p1_pair.0, stub_ack["journal_seq"].as_u64().unwrap());
    drop(egress);

    // P2 is recorded into the proposals file (store reopened,
    // as a later watcher pass in a longer-lived process — or
    // a restarted one — would do).
    {
        let mut store = ProposalStore::open(&proposals_path).unwrap();
        assert_eq!(store.proposals().len(), 1, "the file carried P1");
        store.record(p2.clone()).unwrap();
    }

    // Reopen both stores and drive against the dead address:
    // P1 is skipped (ACKed), P2 is attempted and stays
    // pending.
    {
        let proposals = ProposalStore::open(&proposals_path).unwrap();
        assert_eq!(proposals.proposals().len(), 2, "both proposals are on file");
        let mut egress = EgressStore::open(&egress_path).unwrap();
        let pass = drive_egress(proposals.proposals(), &mut egress, &dead_sink).unwrap();
        assert_eq!(pass.attempted, 1, "only P2 is attempted");
        assert_eq!(pass.still_pending, 1, "P2 stays pending");
        assert_eq!(pass.skipped, 1, "P1 is skipped as already ACKed");
    }

    // Phase 2 — the restart: both stores reopened from their
    // files. Before any drive, the reopened egress store
    // already reports P1's disposition as Acked — the file
    // carried it across the restart.
    let proposals = ProposalStore::open(&proposals_path).unwrap();
    let mut egress = EgressStore::open(&egress_path).unwrap();
    assert_eq!(
        acked_pair(egress.disposition(&p1.caused_by)),
        p1_pair,
        "after restart, P1's Acked disposition is read from the file"
    );
    assert!(
        matches!(egress.disposition(&p2.caused_by), Disposition::Pending),
        "after restart, P2 is still Pending"
    );

    // Drive against a fresh stub: exactly one attempt, for
    // P2 alone.
    let stub2 = StubSi::start();
    let si_addr2 = stub2.addr();
    let sink2 = HttpProposalSink::new(&si_addr2);
    let pass = drive_egress(proposals.proposals(), &mut egress, &sink2).unwrap();
    assert_eq!(
        pass.attempted, 1,
        "exactly one proposal is re-sent after restart"
    );
    assert_eq!(pass.acked, 1, "the re-sent proposal is ACKed");
    assert_eq!(pass.skipped, 1, "P1 is skipped, not re-sent");

    let requests2 = stub2.requests();
    assert_eq!(
        requests2.len(),
        1,
        "the fresh stub received exactly one request"
    );
    assert_eq!(
        requests2[0]["caused_by"]["hash"].as_str(),
        Some(p2.caused_by.hash.as_str()),
        "the re-sent proposal is P2"
    );
    assert_eq!(
        requests2[0]["caused_by"]["seq"].as_u64(),
        Some(p2.caused_by.seq),
        "the re-sent proposal is P2"
    );
    assert_eq!(requests2[0], p2.to_json(), "P2 was re-sent verbatim");

    // P2 is now Acked with the fresh stub's journal_seq; P1's
    // record is unchanged.
    let stub2_ack = stub2
        .ack_for(&p2.caused_by.hash, p2.caused_by.seq)
        .expect("the fresh stub journaled P2");
    assert_eq!(
        acked_pair(egress.disposition(&p2.caused_by)),
        (stub2_ack["journal_seq"].as_u64().unwrap(), Some(9),),
        "P2 is Acked with the fresh stub's journal_seq"
    );
    assert_eq!(
        acked_pair(egress.disposition(&p1.caused_by)),
        p1_pair,
        "P1's record is unchanged by the restart drive"
    );
    assert_eq!(egress.records().len(), 2, "one egress record per proposal");
}
