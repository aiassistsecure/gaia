//! Acceptance test for G-4b in `gaia-serve` — restart
//! recovery across a process restart, at the composition
//! level (the G-4b entry, criterion (5)).
//!
//!   (5) restart-recovery: an admitted `hearth.restart`
//!       work order, drained from a `FileWorkStore` exactly
//!       as the binary's ticker drains its work store
//!       (snapshot the `Admitted` ids, hand each to
//!       [`Composition::actuate_work`]), actuates against a
//!       miniature fleet; the store and the composition are
//!       then dropped and both stores are reopened from the
//!       same home paths — `FileWorkStore::open` on
//!       `work.jsonl`, `OutcomeStore::open` on
//!       `outcomes.jsonl` — and the work record is still
//!       `Executed` with its outcome result, the outcome
//!       record is still present, and re-submitting the same
//!       SI `journal_seq` returns the same work id while a
//!       second actuation attempt errors at the claim and
//!       actuates nothing (no new spine event; the restarted
//!       child's pid, read back out of the spine, unchanged).
//!
//! Honest labels, as in `s1_acceptance.rs`: this is a
//! miniature-fleet scenario, not production fleet evidence.
//! The "model" is a python stand-in child process serving
//! `{}` over HTTP — no machine learning runtime, no real
//! weights, no GPU. What is real is everything the criterion
//! is about: a real `Supervisor` starting and losing real
//! child processes, the real spine, the real proposal and
//! outcome stores, the persistent work store, and actuation
//! that happens only through `Composition::actuate_work`.
//!
//! The fleet helpers are adapted from `s1_acceptance.rs`
//! (same fake server, same bounded ticks, same zombie-wait
//! before ticking a killed child), with two changes: the
//! composition's outcome store is the file-backed one the
//! binary installs, and the work store is a `FileWorkStore`
//! on `work.jsonl` in the same home directory.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use gaia_api::{EventHub, FileWorkStore, Intent, MirrorStore, Verdict, WorkState, WorkStore};
use gaia_serve::{Composition, Outcome, OutcomeStore, Proposal, ProposalStore, Watcher};
use hearth_core::{Budget, Declared, GIB};
use hearth_serve::server::{free_port, ServerSpec};
use hearth_serve::Supervisor;
use hearth_store::Spine;
use serde_json::{json, Value};

/// The fake serving child, as in `s1_acceptance.rs`: a tiny
/// HTTP server that answers 200 with `{}` on any GET,
/// including the supervisor's `/health` probe.
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

/// The composition, as the binary builds it under one home
/// directory — spine, mirror, and proposals on their files,
/// the file-backed outcome store on `outcomes.jsonl`
/// installed via `set_outcome_store` — with the watcher
/// scoped to `fake`.
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
    let mut comp = Composition::new(supervisor, mirror, watcher, proposals, hub);
    comp.set_outcome_store(OutcomeStore::open(&dir.join("outcomes.jsonl")).unwrap());
    comp
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

/// Wait until the child is actually dead before ticking
/// again — the synchronisation `s1_acceptance.rs` documents
/// in full: a probe landing between the signal and the exit
/// is a different scenario (`Evicted`, upstream-correct),
/// not the `ProcessExited` loss under test.
fn wait_until_dead(pid: u64) {
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
    dir: tempfile::TempDir,
}

/// A fleet whose resident fake was killed and whose loss the
/// watcher has answered with exactly one proposal.
struct LossFleet {
    comp: Composition,
    proposal: Proposal,
    dir: tempfile::TempDir,
}

/// Note the spec with the composition (the stored spec an
/// admitted restart re-runs), start the fake through the
/// composition's supervisor, tick to resident, and return
/// the first child's pid as recorded in the spine.
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
        dir,
    }
}

/// Resident fleet → kill the child → wait for real death →
/// tick until the watcher records exactly one proposal for
/// the loss.
fn loss_fleet() -> LossFleet {
    let ResidentFleet {
        mut comp,
        first_pid,
        dir,
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
        dir,
    }
}

// -- The ticker's drain, over the file store -------------------

/// Drain admitted work exactly as the binary's ticker does
/// (`main.rs`): snapshot the store's `Admitted` ids first,
/// then hand each one to `actuate_work`.
fn drain(comp: &mut Composition, store: &mut FileWorkStore) {
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
/// proposal's tool, args, and reason verbatim — for a loss
/// proposal that is a `hearth.restart`-shaped intent.
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

// -- Criterion 5: restart-recovery --------------------------------

#[test]
fn criterion_5_restart_recovery_across_store_and_composition_reopen() {
    let LossFleet {
        mut comp,
        proposal,
        dir,
    } = loss_fleet();
    assert_eq!(proposal.tool, "hearth.restart");
    let home = dir.path().to_path_buf();
    let work_path = home.join("work.jsonl");
    let outcome_path = home.join("outcomes.jsonl");

    // The admitted restart intent, in the persistent store,
    // drained the way the binary's ticker drains.
    let intent = proposal_intent(&proposal, 7101, "task-g4b-restart");
    let (work_id, expected_result, expected_outcome) = {
        let mut store = FileWorkStore::open(&work_path).unwrap();
        let submitted = store.submit(intent.clone()).unwrap();
        assert_eq!(submitted.id, "work-000001");
        assert_eq!(submitted.state, WorkState::Admitted);

        drain(&mut comp, &mut store);

        let record = store
            .get(&submitted.id)
            .expect("the record is in the store");
        assert_eq!(record.state, WorkState::Executed);
        let result = record
            .result
            .clone()
            .expect("the completed record carries its outcome result");
        assert_eq!(result["outcome"], json!("executed"), "result: {result}");

        let outcome = comp
            .outcomes()
            .iter()
            .find(|o| o.work_id == submitted.id)
            .expect("an outcome was recorded for this work")
            .clone();
        assert_eq!(outcome.outcome, Outcome::Executed);
        assert_eq!(outcome.result, result);
        assert!(
            !outcome.spine_event_refs.is_empty(),
            "the restart outcome records spine refs"
        );
        (submitted.id, result, outcome)
    };

    // The restart actuated exactly once: a second child, with
    // a second pid, is loading. Capture both spine facts
    // before the drop — after the reopen, the loading event
    // at this seq is how the pid is read back.
    let loadings = loading_events(&comp);
    assert_eq!(loadings.len(), 2, "the original start + the restart");
    let restarted_loading_seq = loadings[1].seq;
    let restarted_pid = event_pid(&loadings[1]);
    assert_eq!(
        expected_outcome.spine_event_refs[0].seq, restarted_loading_seq,
        "the outcome's first spine ref is the restart's loading event"
    );

    // Drop the composition (and with it the restarted child
    // and the spine handle). The work store was already
    // dropped at the end of the block above. Everything
    // below is read back from the home paths alone.
    drop(comp);

    // Reopen both stores from the same home paths.
    let mut store = FileWorkStore::open(&work_path).unwrap();
    assert!(
        store.load_failures().is_empty(),
        "the work file loads clean: {:?}",
        store.load_failures()
    );
    assert_eq!(store.records().len(), 1);
    let record = store
        .get(&work_id)
        .expect("the work record survived the restart");
    assert_eq!(record.state, WorkState::Executed);
    assert_eq!(record.result, Some(expected_result.clone()));
    let body = record.status_body();
    let expected_refs: Value = json!(expected_outcome
        .spine_event_refs
        .iter()
        .map(|r| json!({ "hash": r.hash, "seq": r.seq }))
        .collect::<Vec<_>>());
    assert_eq!(
        body["provenance"]["spine_event_refs"], expected_refs,
        "the reopened record's provenance still reports the outcome's refs: {body}"
    );

    let outcomes = OutcomeStore::open(&outcome_path).unwrap();
    assert_eq!(
        outcomes.outcomes(),
        [expected_outcome.clone()].as_slice(),
        "the outcome record is present after reopen, exactly as filed"
    );
    assert_eq!(outcomes.for_work(&work_id).len(), 1);
    drop(outcomes);

    // A fresh composition over the same home — its outcome
    // store, installed by `build_composition` from the same
    // `outcomes.jsonl`, holds the same single outcome.
    let mut comp = build_composition(&home);
    assert_eq!(comp.outcomes().len(), 1);
    assert_eq!(comp.outcomes()[0], expected_outcome);

    // The restarted child's pid, read back out of the
    // reopened spine at the restart's loading seq.
    let pid_before = {
        let at = comp
            .spine()
            .state_as_of("fake", restarted_loading_seq)
            .expect("the spine still answers for the restart's loading seq");
        assert_eq!(at.kind, "loading");
        event_pid(&at)
    };
    assert_eq!(pid_before, restarted_pid);
    let spine_seq_before = comp.spine().seq();

    // Re-submitting the same journal_seq is the same work:
    // same id, terminal state, no new record.
    let duplicate = store.submit(intent).unwrap();
    assert_eq!(duplicate.id, work_id);
    assert_eq!(duplicate.state, WorkState::Executed);
    assert_eq!(store.records().len(), 1);

    // And it cannot actuate a second time: the claim inside
    // `actuate_work` refuses the executed record before any
    // actuator is built.
    let err = comp
        .actuate_work(&mut store, &work_id)
        .expect_err("an executed record must not actuate twice");
    assert_eq!(err.error_type(), "gaia_verdict_conflict");

    // Nothing actuated: no new spine event (a restart would
    // have recorded a fresh `loading`), no new outcome, the
    // record untouched, and the pid at the restart's loading
    // seq still the restarted child's pid.
    assert_eq!(
        comp.spine().seq(),
        spine_seq_before,
        "no spine event was recorded by the refused second actuation"
    );
    assert_eq!(comp.outcomes().len(), 1, "no second outcome was filed");
    let after = store.get(&work_id).expect("the record is still there");
    assert_eq!(after.state, WorkState::Executed);
    assert_eq!(after.result, Some(expected_result));
    let at = comp
        .spine()
        .state_as_of("fake", restarted_loading_seq)
        .expect("the spine still answers for the restart's loading seq");
    assert_eq!(event_pid(&at), restarted_pid, "fleet pid unchanged");
}
