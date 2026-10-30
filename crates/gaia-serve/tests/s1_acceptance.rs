//! Acceptance tests for S-1, end to end — submission dedup
//! and provenance population through the composition
//! (G-9 §9.3, §11.2, §15).
//!
//! Honest labels: these are miniature-fleet scenarios, not
//! production fleet evidence. The "model" is a python stand-in
//! child process serving `{}` over HTTP — no machine learning
//! runtime, no real weights, no GPU. What is real is
//! everything the criteria are about: a real `Supervisor`
//! starting and losing real child processes, the real spine,
//! the real proposal store and work store, and actuation that
//! happens only through [`Composition::actuate_work`] — the
//! gate-checked drain step the binary's ticker runs.
//!
//! The criteria:
//!
//! 1. Exactly-once notify: the same `hearth.notify` intent,
//!    submitted twice under one SI journal sequence, records
//!    one work order and — drained twice, exactly as the
//!    ticker drains — produces exactly one notify outcome.
//!    Notify has no stale-state check, so before S-1 this
//!    scenario produced two records and two outcomes; that
//!    hole is why S-1 exists (§9.3).
//! 2. Provenance, end to end: a watcher's restart proposal,
//!    submitted twice under one journal sequence, actuates
//!    once through the composition, and the completed record's
//!    status body reports the outcome's spine refs in
//!    `provenance.spine_event_refs` — where before actuation
//!    it honestly reported the empty list (§11.2).
//!
//! The dedup half is implemented by a parallel slice in
//! `gaia-api`'s `work.rs` — until that lands, both criteria
//! fail at their duplicate-submission assertions, which is
//! expected and is not worked around here.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use gaia_api::{EventHub, Intent, MemoryWorkStore, MirrorStore, Verdict, WorkState, WorkStore};
use gaia_serve::{Composition, Proposal, ProposalStore, Watcher};
use hearth_core::{Budget, Declared, GIB};
use hearth_serve::server::{free_port, ServerSpec};
use hearth_serve::Supervisor;
use hearth_store::Spine;
use serde_json::{json, Value};

/// The fake serving child: a tiny HTTP server that answers 200
/// with `{}` on any GET, including the supervisor's `/health`
/// probe. It parses `--port` out of its argv and ignores the
/// rest — the supervisor passes the llama-server argv shape
/// (`-m`, `--host`, `--n-gpu-layers`, …) and a stand-in runtime
/// is under no obligation to honour knobs it does not have.
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

// -- Scenario construction ------------------------------------

/// The composition, exactly as the binary builds it, with the
/// watcher scoped to `fake` (§5.5: scope is opt-in).
fn build_composition(dir: &Path) -> Composition {
    let spine = Spine::open(&dir.join("spine")).unwrap();
    let mirror = Arc::new(Mutex::new(
        MirrorStore::open(&dir.join("mirror.jsonl")).unwrap(),
    ));
    let proposals = ProposalStore::open(&dir.join("proposals.jsonl")).unwrap();
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

/// The fake server script, a dummy weights file, and the spec
/// that starts the script as `fake`'s serving child.
fn fake_spec(dir: &Path) -> ServerSpec {
    let script = dir.join("fake_server.py");
    std::fs::write(&script, FAKE_SERVER).unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let gguf = dir.join("fake.gguf");
    std::fs::write(&gguf, b"not a real gguf").unwrap();

    let mut spec = ServerSpec::new("fake", &gguf, free_port().unwrap());
    spec.binary = script;
    spec.log_dir = dir.to_path_buf();
    spec
}

// -- Bounded ticks and process control -------------------------

/// Step the composition until the spine's latest event for
/// `fake` has kind `kind`. Bounded: a model that never gets
/// there is a failure of the test, not a hang.
fn tick_until_latest_kind(comp: &mut Composition, kind: &str) {
    for _ in 0..300 {
        comp.tick_once().unwrap();
        if comp.spine().latest("fake").is_some_and(|e| e.kind == kind) {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!(
        "fake never reached latest kind {kind}; latest is {:?}",
        comp.spine().latest("fake")
    );
}

/// Step the composition until the watcher has recorded
/// exactly one proposal. Bounded, on the same terms.
fn tick_until_one_proposal(comp: &mut Composition) {
    for _ in 0..300 {
        comp.tick_once().unwrap();
        if comp.proposals().len() == 1 {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!(
        "expected exactly one proposal, got {}",
        comp.proposals().len()
    );
}

/// Every `loading` event in the spine's causal chain for
/// `fake`, oldest first — one per child ever started.
fn loading_events(comp: &Composition) -> Vec<hearth_store::Event> {
    let mut events: Vec<hearth_store::Event> = comp
        .spine()
        .why("fake")
        .into_iter()
        .filter(|e| e.kind == "loading")
        .collect();
    events.sort_by_key(|e| e.seq);
    events
}

/// The pid a `loading` event recorded — a spine fact, not a
/// process-table guess.
fn event_pid(event: &hearth_store::Event) -> u64 {
    event.facts["pid"]
        .as_u64()
        .expect("the loading event records a pid")
}

/// Wait until the child is actually dead (a zombie awaiting
/// the supervisor's reap, or gone entirely) before ticking
/// again. This is synchronisation, not patience: the scenario
/// under test is a *dead* child, and a probe that lands in the
/// window between the signal and the exit is a different
/// scenario with a different, upstream-correct answer — §1.2
/// maps a resident's failed probe to `Evicted`. (Copied from
/// the G-5 composition test, which documents the race in
/// full.)
fn wait_until_dead(pid: u64) {
    for _ in 0..200 {
        match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            Err(_) => break, // Gone entirely: dead by any reading.
            Ok(stat) => {
                // The state is the field after the comm in
                // parentheses — comm itself may contain parens,
                // so split on the LAST closing paren.
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
}

fn kill_child(pid: u64) {
    let status = std::process::Command::new("kill")
        .args(["-9", &pid.to_string()])
        .status()
        .unwrap();
    assert!(status.success(), "kill -9 {pid} failed: {status}");
}

// -- The loss scenario -----------------------------------------

/// A fleet of one resident fake. Field order matters: the
/// composition (and its children) drops before the tempdir.
struct ResidentFleet {
    comp: Composition,
    first_pid: u64,
    _dir: tempfile::TempDir,
}

/// A fleet whose resident fake was killed and whose loss the
/// watcher has answered with exactly one proposal.
struct LossFleet {
    comp: Composition,
    proposal: Proposal,
    _dir: tempfile::TempDir,
}

/// Note the spec with the composition (the §5.3 stored spec an
/// admitted restart re-runs), start the fake through the
/// composition's supervisor, tick to resident, and return the
/// first child's pid as recorded in the spine.
fn start_resident(comp: &mut Composition, spec: &ServerSpec) -> u64 {
    comp.note_spec(spec.clone());
    comp.supervisor().start(spec.clone()).unwrap();
    tick_until_latest_kind(comp, "resident");
    let loadings = loading_events(comp);
    assert_eq!(loadings.len(), 1, "one start, one loading event");
    event_pid(&loadings[0])
}

fn resident_fleet() -> ResidentFleet {
    let dir = tempfile::tempdir().unwrap();
    let mut comp = build_composition(dir.path());
    let spec = fake_spec(dir.path());
    let first_pid = start_resident(&mut comp, &spec);
    ResidentFleet {
        comp,
        first_pid,
        _dir: dir,
    }
}

/// Resident fleet → kill the child out from under the
/// supervisor → wait for real death (the zombie-wait above) →
/// tick until the watcher records exactly one proposal for
/// the loss.
fn loss_fleet() -> LossFleet {
    let ResidentFleet {
        mut comp,
        first_pid,
        _dir,
        ..
    } = resident_fleet();

    kill_child(first_pid);
    wait_until_dead(first_pid);
    tick_until_one_proposal(&mut comp);

    let lost = comp.spine().latest("fake").expect("a latest event");
    assert_eq!(lost.kind, "lost", "the latest spine event is the loss");
    let proposal = comp.proposals()[0].clone();
    assert_eq!(proposal.caused_by.seq, lost.seq);
    assert_eq!(proposal.caused_by.hash, lost.hash);

    LossFleet {
        comp,
        proposal,
        _dir,
    }
}

// -- The ticker's drain, copied ---------------------------------

/// Drain admitted work exactly as the binary's ticker does
/// (`main.rs`): snapshot the store's `Admitted` ids first,
/// then hand each one to `actuate_work`. A completed record
/// leaves `Admitted`, so a second drain finds nothing to do.
fn drain(comp: &mut Composition, store: &mut MemoryWorkStore) {
    let admitted: Vec<String> = store
        .records()
        .into_iter()
        .filter(|r| r.state == WorkState::Admitted)
        .map(|r| r.id)
        .collect();
    for id in admitted {
        comp.actuate_work(store, &id)
            .unwrap_or_else(|e| panic!("draining work {id} failed: {e}"));
    }
}

/// The proposal, submitted as an SI tool intent: the
/// proposal's tool, args, and reason verbatim (§5.3 —
/// actuation executes the admitted args as proposed).
fn proposal_intent(proposal: &Proposal, journal_seq: u64, task_id: &str) -> Intent {
    Intent {
        tool: proposal.tool.clone(),
        args: proposal.args.clone(),
        reason: proposal.reason.clone(),
        task_id: task_id.to_string(),
        journal_seq,
        class: "execute".into(),
        verdict: Some(Verdict::Admit),
    }
}

// -- Criterion 1: exactly-once notify ----------------------------

#[test]
fn a_doubly_submitted_notify_actuates_exactly_once() {
    let dir = tempfile::tempdir().unwrap();
    let mut comp = build_composition(dir.path());
    let mut store = MemoryWorkStore::new();

    // One intent, journaled in SI at seq 7001 — submitted
    // twice, as a transport retry would submit it.
    let intent = Intent {
        tool: "hearth.notify".to_string(),
        args: json!({"model": "fake", "spine_event": {"hash": "deadbeef", "seq": 1}}),
        reason: "s1 dedup test".to_string(),
        task_id: "task-s1-notify".to_string(),
        journal_seq: 7001,
        class: "execute".into(),
        verdict: Some(Verdict::Admit),
    };
    let first = store.submit(intent.clone()).unwrap();
    let second = store.submit(intent).unwrap();

    assert_eq!(
        first.id, second.id,
        "the retry returns the original work_id (§9.3)"
    );
    assert_eq!(
        store.records().len(),
        1,
        "one journal_seq, one record — the retry recorded nothing new"
    );

    // Drain twice, exactly as the ticker drains: the first
    // drain actuates the one record; the second finds no
    // admitted work left to act on.
    drain(&mut comp, &mut store);
    drain(&mut comp, &mut store);

    assert_eq!(
        comp.outcomes().len(),
        1,
        "exactly one notify outcome was recorded"
    );
    assert_eq!(comp.outcomes()[0].tool, "hearth.notify");
    assert_eq!(comp.outcomes()[0].work_id, first.id);
    let record = store.get(&first.id).expect("the record is in the store");
    assert_eq!(record.state, WorkState::Executed);
}

// -- Criterion 2: provenance, end to end --------------------------

#[test]
fn restart_provenance_reports_the_outcomes_spine_refs_after_actuation() {
    // `_dir` is bound (never `..`-dropped) in every scenario
    // destructuring: dropping the TempDir would delete the
    // spine, the script, and the gguf out from under the test.
    let LossFleet {
        mut comp,
        proposal,
        _dir,
        ..
    } = loss_fleet();
    let mut store = MemoryWorkStore::new();

    // The proposal's restart intent, submitted twice under
    // one journal sequence — a retry must not mint a second
    // work order for the same loss.
    let intent = proposal_intent(&proposal, 7002, "task-s1-restart");
    let first = store.submit(intent.clone()).unwrap();
    let second = store.submit(intent).unwrap();
    assert_eq!(
        first.id, second.id,
        "the retry returns the original work_id (§9.3)"
    );
    assert_eq!(store.records().len(), 1, "one journal_seq, one record");

    // Starting state: before actuation there is no outcome,
    // and the status body honestly reports no spine refs.
    let before = store
        .get(&first.id)
        .expect("the record is in the store")
        .status_body();
    assert_eq!(
        before["provenance"]["spine_event_refs"],
        json!([]),
        "provenance is empty before actuation: {before}"
    );

    let done = comp.actuate_work(&mut store, &first.id).unwrap();
    assert_eq!(done.state, WorkState::Executed);

    // The outcome filed for this work carries the restart's
    // spine evidence — at least the new loading event.
    let outcome = comp
        .outcomes()
        .iter()
        .find(|o| o.work_id == first.id)
        .expect("an outcome was recorded for this work");
    assert!(
        !outcome.spine_event_refs.is_empty(),
        "the restart outcome records spine refs"
    );
    let expected: Value = json!(outcome
        .spine_event_refs
        .iter()
        .map(|r| json!({ "hash": r.hash, "seq": r.seq }))
        .collect::<Vec<_>>());

    // And the status body reports exactly those refs as the
    // record's provenance (§11.2).
    let after = store
        .get(&first.id)
        .expect("the record is in the store")
        .status_body();
    assert_eq!(
        after["provenance"]["spine_event_refs"], expected,
        "provenance reports the outcome's refs serialised as hash/seq pairs: {after}"
    );
}
