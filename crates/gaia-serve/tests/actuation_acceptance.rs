//! Composition acceptance tests for G-6 — gated actuation
//! (G-2 §7, G-6 criteria (1)–(5), plus the `hearth.stop`
//! actuation named in the G-6 slice title alongside restart
//! and re-pull).
//!
//! Honest labels: these are miniature-fleet scenarios, not
//! production fleet evidence. The "model" is a python stand-in
//! child process serving `{}` over HTTP — no machine learning
//! runtime, no real weights, no GPU. What is real is
//! everything the criteria are about: a real `Supervisor`
//! starting and losing real child processes, the real spine,
//! the real proposal store and work store, and actuation that
//! happens only through [`Composition::actuate_work`] — the
//! gate-checked drain step — never through a composition tick.
//! Assertions read the spine, the fleet state, the outcome
//! store, and the process table (`/proc`) the way an operator
//! would.
//!
//! The criteria, one test each:
//!
//! 1. An ADMIT-ted restart intent restarts the model and the
//!    spine shows the outcome causally linked to the observed
//!    loss (and the same intent, submitted verdict-less first,
//!    actuates nothing).
//! 2. A REJECTED or never-verdicted intent actuates nothing
//!    (process table + spine both checked).
//! 3. An intent whose target recovered on its own is refused,
//!    recorded, and reported — not executed.
//! 4. With a journaled leash stop in force, an
//!    otherwise-admitted restart refuses; after a journaled
//!    resume, a new admitted intent executes (the leash is a
//!    state check, not a ban, and refused work is answered
//!    rather than queued).
//! 5. Re-pull actuation records `pull_started` /
//!    `pull_completed` through the existing spine path.
//! 6. An ADMIT-ted `hearth.stop` intent unloads the model.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use gaia_api::{EventHub, Intent, MemoryWorkStore, MirrorStore, Verdict, WorkState, WorkStore};
use gaia_serve::{Composition, Outcome, Proposal, ProposalStore, Watcher};
use hearth_core::{Budget, Declared, LostReason, Residency, GIB};
use hearth_pull::PullConfig;
use hearth_serve::server::{free_port, ServerSpec};
use hearth_serve::Supervisor;
use hearth_store::{Event, Spine};
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

// -- Bounded ticks and spine reads ----------------------------

/// Step the composition until the spine's latest event for
/// `fake` has kind `kind`. Bounded, like G-5: a model that
/// never gets there is a failure of the test, not a hang.
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
fn loading_events(comp: &Composition) -> Vec<Event> {
    let mut events: Vec<Event> = comp
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
fn event_pid(event: &Event) -> u64 {
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

fn assert_fleet_lost_process_exited(comp: &mut Composition) {
    let state = &comp
        .supervisor()
        .fleet()
        .slot("fake")
        .expect("fake is declared")
        .state;
    assert!(
        matches!(
            state,
            Residency::Lost {
                reason: LostReason::ProcessExited,
                ..
            }
        ),
        "fleet state is Lost{{ProcessExited}}, got {state:?}"
    );
}

// -- The full G-6 scenario ------------------------------------

/// A fleet of one resident fake. Field order matters: the
/// composition (and its children) drops before the tempdir.
struct ResidentFleet {
    comp: Composition,
    spec: ServerSpec,
    first_pid: u64,
    _dir: tempfile::TempDir,
}

/// A fleet whose resident fake was killed and whose loss the
/// watcher has answered with exactly one proposal.
struct LossFleet {
    comp: Composition,
    spec: ServerSpec,
    first_pid: u64,
    proposal: Proposal,
    lost_seq: u64,
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
        spec,
        first_pid,
        _dir: dir,
    }
}

/// The full G-6 scenario helper: resident fleet → kill the
/// child out from under the supervisor → wait for real death
/// (the zombie-wait above) → tick until the watcher records
/// exactly one proposal for the loss.
fn loss_fleet() -> LossFleet {
    let ResidentFleet {
        mut comp,
        spec,
        first_pid,
        _dir,
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
        spec,
        first_pid,
        proposal,
        lost_seq: lost.seq,
        _dir,
    }
}

// -- Intent construction --------------------------------------

/// The proposal, submitted as an SI tool intent: the
/// proposal's tool, args, and reason verbatim (§5.3 —
/// actuation executes the admitted args as proposed), with the
/// verdict the scenario chooses.
fn proposal_intent(
    proposal: &Proposal,
    verdict: Option<Verdict>,
    journal_seq: u64,
    task_id: &str,
) -> Intent {
    Intent {
        tool: proposal.tool.clone(),
        args: proposal.args.clone(),
        reason: proposal.reason.clone(),
        task_id: task_id.to_string(),
        journal_seq,
        class: "execute".into(),
        verdict,
    }
}

/// An intent not derived from a proposal (re-pull, stop).
fn plain_intent(
    tool: &str,
    args: Value,
    reason: &str,
    verdict: Option<Verdict>,
    journal_seq: u64,
    task_id: &str,
) -> Intent {
    Intent {
        tool: tool.to_string(),
        args,
        reason: reason.to_string(),
        task_id: task_id.to_string(),
        journal_seq,
        class: "execute".into(),
        verdict,
    }
}

// -- Criterion (1) --------------------------------------------

#[test]
fn an_admitted_restart_restarts_the_model_and_the_spine_links_the_outcome_to_the_loss() {
    // `_dir` is bound (never `..`-dropped) in every scenario
    // destructuring: dropping the TempDir would delete the
    // spine, the script, and the gguf out from under the test.
    let LossFleet {
        mut comp,
        first_pid,
        proposal,
        lost_seq,
        _dir,
        ..
    } = loss_fleet();
    let mut store = MemoryWorkStore::new();

    // First: the proposal's intent submitted verdict-less.
    // Actuation must refuse at the claim — the gate has not
    // spoken — and nothing may change: no outcome recorded,
    // the spine still ends at the loss, still one loading
    // event, the fleet still lost.
    let submitted = store
        .submit(proposal_intent(&proposal, None, 1001, "task-g6-restart"))
        .unwrap();
    let err = comp
        .actuate_work(&mut store, &submitted.id)
        .expect_err("a verdict-less intent cannot actuate");
    assert_eq!(err.error_type(), "gaia_verdict_required");
    assert_eq!(
        store.get(&submitted.id).unwrap().state,
        WorkState::AwaitingVerdict,
        "the refused attempt leaves the record awaiting its verdict"
    );
    assert!(comp.outcomes().is_empty(), "no outcome was recorded");
    assert_eq!(comp.spine().latest("fake").unwrap().seq, lost_seq);
    assert_eq!(loading_events(&comp).len(), 1, "nothing restarted");
    assert_fleet_lost_process_exited(&mut comp);

    // The gate speaks: ADMIT, recorded against the same work.
    store
        .record_verdict(&submitted.id, Verdict::Admit)
        .unwrap();

    // Now the same work actuates, and the record completes
    // with the outcome as its result.
    let done = comp.actuate_work(&mut store, &submitted.id).unwrap();
    assert_eq!(done.state, WorkState::Executed);
    let result = done.result.clone().expect("a completed record has a result");
    assert_eq!(result["outcome"], json!("executed"));

    // The outcome store holds exactly that execution, against
    // this work, this tool, this model.
    assert_eq!(comp.outcomes().len(), 1, "exactly one outcome recorded");
    let outcome = &comp.outcomes()[0];
    assert_eq!(outcome.work_id, done.id);
    assert_eq!(outcome.tool, "hearth.restart");
    assert_eq!(outcome.model, "fake");
    assert!(
        matches!(outcome.outcome, Outcome::Executed),
        "the recorded outcome is Executed, got {:?}",
        outcome.outcome
    );
    assert!(outcome.refusal.is_none(), "an execution has no refusal");

    // Tick until the restarted child is resident again.
    tick_until_latest_kind(&mut comp, "resident");

    // The spine links the outcome to the loss: the causal
    // chain behind the new resident state still contains the
    // lost event, with a resident event above it (newer, and
    // reached through the chain the loss feeds).
    let why = comp.spine().why("fake");
    let lost_pos = why
        .iter()
        .position(|e| e.kind == "lost" && e.seq == lost_seq)
        .expect("the why() chain contains the lost event");
    let resident_above = why[..lost_pos]
        .iter()
        .find(|e| e.kind == "resident" && e.seq > lost_seq);
    assert!(
        resident_above.is_some(),
        "a resident event sits above the loss in the chain: {why:?}"
    );
    assert_eq!(why[0].kind, "resident", "the chain's head is the recovery");

    // Exactly two loading events — the original start and the
    // gated restart — with different pids, and the second
    // child is a live process.
    let loadings = loading_events(&comp);
    assert_eq!(loadings.len(), 2, "exactly two loading events");
    assert_eq!(event_pid(&loadings[0]), first_pid);
    let second_pid = event_pid(&loadings[1]);
    assert_ne!(second_pid, first_pid, "the restart is a new child");
    assert!(
        Path::new(&format!("/proc/{second_pid}")).exists(),
        "the restarted child exists in the process table"
    );
}

// -- Criterion (2) --------------------------------------------

#[test]
fn rejected_and_unverdicted_intents_actuate_nothing() {
    let LossFleet {
        mut comp,
        lost_seq,
        proposal,
        _dir,
        ..
    } = loss_fleet();
    let mut store = MemoryWorkStore::new();

    // An escalated intent, never verdicted.
    let escalated = store
        .submit(proposal_intent(&proposal, None, 1002, "task-g6-escalated"))
        .unwrap();
    let err = comp
        .actuate_work(&mut store, &escalated.id)
        .expect_err("an un-verdicted intent cannot actuate");
    assert_eq!(err.error_type(), "gaia_verdict_required");

    // An intent the gate verdicted REJECT.
    let to_reject = store
        .submit(proposal_intent(&proposal, None, 1003, "task-g6-rejected"))
        .unwrap();
    store
        .record_verdict(&to_reject.id, Verdict::Reject)
        .unwrap();
    let err = comp
        .actuate_work(&mut store, &to_reject.id)
        .expect_err("a rejected intent can never actuate");
    assert_eq!(err.error_type(), "gaia_verdict_conflict");

    // Nothing actuated, by every surface an operator can read:
    // the spine still ends at the same lost event, exactly one
    // loading event exists, the fleet state is still the loss,
    // and no outcome was recorded for either attempt.
    let latest = comp.spine().latest("fake").unwrap();
    assert_eq!(latest.kind, "lost");
    assert_eq!(latest.seq, lost_seq, "the spine never moved past the loss");
    assert_eq!(loading_events(&comp).len(), 1, "exactly one loading event");
    assert_fleet_lost_process_exited(&mut comp);
    assert!(
        comp.outcomes().is_empty(),
        "gate-refused attempts record no actuation outcome"
    );
}

// -- Criterion (3) --------------------------------------------

#[test]
fn an_intent_whose_target_recovered_on_its_own_is_refused_recorded_and_reported() {
    let LossFleet {
        mut comp,
        spec,
        proposal,
        _dir,
        ..
    } = loss_fleet();

    // The target recovers on its own: an outside actor — an
    // operator, or a self-recovery path that is not the gate —
    // restarts the model directly through the supervisor.
    // This start is deliberately NOT actuation: no intent, no
    // verdict, no outcome.
    comp.supervisor().start(spec.clone()).unwrap();
    tick_until_latest_kind(&mut comp, "resident");
    let loadings = loading_events(&comp);
    assert_eq!(loadings.len(), 2, "initial start + the manual restart");
    let manual_pid = event_pid(&loadings[1]);

    // Only now is the proposal's intent submitted, pre-admitted
    // — the gate said yes to a restart the world no longer
    // needs. Actuation must refuse on stale state, and the
    // refusal is the work's answer: the record completes with
    // it rather than erroring or queueing.
    let mut store = MemoryWorkStore::new();
    let submitted = store
        .submit(proposal_intent(
            &proposal,
            Some(Verdict::Admit),
            1004,
            "task-g6-stale",
        ))
        .unwrap();
    let done = comp.actuate_work(&mut store, &submitted.id).unwrap();
    assert_eq!(done.state, WorkState::Executed, "a refusal completes the work");
    let result = done.result.clone().expect("the refusal is the result");
    assert_eq!(result["outcome"], json!("refused"));
    assert_eq!(result["refusal"], json!("stale_state"));

    // The refusal is recorded in the outcome store — exactly
    // that one, against this work.
    assert_eq!(comp.outcomes().len(), 1, "exactly one outcome recorded");
    let outcome = &comp.outcomes()[0];
    assert_eq!(outcome.work_id, done.id);
    assert!(
        matches!(outcome.outcome, Outcome::Refused),
        "the recorded outcome is Refused, got {:?}",
        outcome.outcome
    );
    assert_eq!(outcome.refusal.as_deref(), Some("stale_state"));

    // And nothing executed: the gated actuation added no
    // loading event, and the running child is still the manual
    // restart's child.
    let loadings = loading_events(&comp);
    assert_eq!(
        loadings.len(),
        2,
        "still two loading events — the refusal started nothing"
    );
    assert_eq!(event_pid(&loadings[1]), manual_pid);
    assert!(
        Path::new(&format!("/proc/{manual_pid}")).exists(),
        "the manual restart's child is the one still running"
    );
    assert_eq!(comp.spine().latest("fake").unwrap().kind, "resident");
}

// -- Criterion (4) --------------------------------------------

#[test]
fn a_journaled_leash_stop_refuses_an_otherwise_admitted_restart() {
    let LossFleet {
        mut comp,
        proposal,
        lost_seq,
        _dir,
        ..
    } = loss_fleet();
    let mut store = MemoryWorkStore::new();

    // A leash stop for the model, journaled in SI (its journal
    // seq and reason ride along as provenance).
    let _ = comp
        .leash()
        .record_stop("fake", 777, "operator stop, journaled in SI");

    // An otherwise-admitted restart now refuses on the leash.
    let first = store
        .submit(proposal_intent(
            &proposal,
            Some(Verdict::Admit),
            1005,
            "task-g6-leashed",
        ))
        .unwrap();
    let done = comp.actuate_work(&mut store, &first.id).unwrap();
    assert_eq!(done.state, WorkState::Executed, "a refusal completes the work");
    let result = done.result.clone().expect("the refusal is the result");
    assert_eq!(result["outcome"], json!("refused"));
    assert_eq!(result["refusal"], json!("leash"));

    // The refusal is recorded, and nothing moved: the spine
    // still ends at the loss, still one loading event.
    assert_eq!(comp.outcomes().len(), 1, "exactly one outcome recorded");
    assert_eq!(comp.outcomes()[0].refusal.as_deref(), Some("leash"));
    let latest = comp.spine().latest("fake").unwrap();
    assert_eq!(latest.kind, "lost");
    assert_eq!(latest.seq, lost_seq);
    assert_eq!(loading_events(&comp).len(), 1, "nothing restarted");

    // The stop is resumed — also journaled. A NEW pre-admitted
    // intent with the same args (the refused work was answered,
    // not queued, so re-asking means a new intent) now
    // executes: the leash is a state check, not a ban.
    let _ = comp
        .leash()
        .record_resume("fake", 778, "operator resume, journaled in SI");
    let second = store
        .submit(proposal_intent(
            &proposal,
            Some(Verdict::Admit),
            1006,
            "task-g6-after-resume",
        ))
        .unwrap();
    let done = comp.actuate_work(&mut store, &second.id).unwrap();
    let result = done.result.clone().expect("a completed record has a result");
    assert_eq!(result["outcome"], json!("executed"));

    tick_until_latest_kind(&mut comp, "resident");
    assert_eq!(
        loading_events(&comp).len(),
        2,
        "the post-resume intent restarted the model"
    );
}

// -- Criterion (5) --------------------------------------------

#[test]
fn repull_actuation_records_pull_started_and_pull_completed_through_the_spine() {
    // A fresh composition: the model is declared and never
    // started — a re-pull is about bytes, not residency.
    let dir = tempfile::tempdir().unwrap();
    let mut comp = build_composition(dir.path());

    // A small fake.gguf, served over loopback HTTP by a python
    // child this test owns and kills at the end.
    let serve_dir = dir.path().join("serve");
    std::fs::create_dir_all(&serve_dir).unwrap();
    let payload: &[u8] = b"small fake gguf bytes for the repull scenario";
    std::fs::write(serve_dir.join("fake.gguf"), payload).unwrap();
    let file_len = payload.len() as u64;

    let port = free_port().unwrap();
    let mut server = std::process::Command::new("python3")
        .args([
            "-m",
            "http.server",
            &port.to_string(),
            "--bind",
            "127.0.0.1",
            "--directory",
            serve_dir.to_str().unwrap(),
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();

    // Bounded readiness: poll the port until the server
    // accepts a connection — synchronisation, not a sleep.
    let mut reachable = false;
    for _ in 0..300 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            reachable = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    // Re-pulls land their blobs inside the tempdir, quietly.
    comp.set_pull_config(PullConfig {
        blobs_dir: dir.path().join("blobs"),
        progress: false,
        verify_existing: false,
    });

    let mut store = MemoryWorkStore::new();
    let reference = format!("http://127.0.0.1:{port}/fake.gguf");
    let submitted = store
        .submit(plain_intent(
            "hearth.repull",
            json!({ "model": "fake", "reference": reference }),
            "operator suspects stale weights",
            Some(Verdict::Admit),
            1007,
            "task-g6-repull",
        ))
        .unwrap();
    let actuation = reachable.then(|| comp.actuate_work(&mut store, &submitted.id));

    // Read the spine evidence before killing the server, so
    // the assertions below run after cleanup either way.
    let latest_pull = comp.spine().latest_pull("fake");
    let provenance_kinds: Vec<String> = comp
        .spine()
        .provenance("fake")
        .into_iter()
        .map(|e| e.kind)
        .collect();
    let outcomes_len = comp.outcomes().len();

    let _ = server.kill();
    let _ = server.wait();

    assert!(reachable, "the python http.server never came up on {port}");
    let done = actuation
        .expect("the actuation ran")
        .expect("an admitted re-pull actuates");
    assert_eq!(done.state, WorkState::Executed);
    let result = done.result.clone().expect("a completed record has a result");
    assert_eq!(result["outcome"], json!("executed"));
    assert_eq!(outcomes_len, 1, "exactly one outcome recorded");

    // The pull went through the existing spine path: the
    // latest pull event is the completion, carrying the served
    // file's exact size, and the pull's provenance chain holds
    // both halves of the pull.
    let completed = latest_pull.expect("a pull was recorded in the spine");
    assert_eq!(completed.kind, "pull_completed");
    assert_eq!(
        completed.facts["size_bytes"],
        json!(file_len),
        "the recorded size is the served file's length"
    );
    assert!(
        provenance_kinds.iter().any(|k| k == "pull_started"),
        "provenance contains pull_started: {provenance_kinds:?}"
    );
    assert!(
        provenance_kinds.iter().any(|k| k == "pull_completed"),
        "provenance contains pull_completed: {provenance_kinds:?}"
    );
}

// -- Criterion (6): hearth.stop -------------------------------

#[test]
fn an_admitted_stop_unloads_the_model() {
    let ResidentFleet {
        mut comp,
        first_pid,
        _dir,
        ..
    } = resident_fleet();

    // The intent answers the current resident event — the
    // spine ref it names is the model's latest, so the state
    // it was admitted against is the state that still holds.
    let resident = comp.spine().latest("fake").expect("a latest event");
    assert_eq!(resident.kind, "resident");
    let mut store = MemoryWorkStore::new();
    let submitted = store
        .submit(plain_intent(
            "hearth.stop",
            json!({
                "model": "fake",
                "spine_event": { "hash": resident.hash, "seq": resident.seq },
            }),
            "operator asked for the model to be unloaded",
            Some(Verdict::Admit),
            1008,
            "task-g6-stop",
        ))
        .unwrap();

    let done = comp.actuate_work(&mut store, &submitted.id).unwrap();
    assert_eq!(done.state, WorkState::Executed);
    let result = done.result.clone().expect("a completed record has a result");
    assert_eq!(result["outcome"], json!("executed"));
    assert_eq!(comp.outcomes().len(), 1, "exactly one outcome recorded");
    assert!(matches!(comp.outcomes()[0].outcome, Outcome::Executed));

    // The spine's latest is the deliberate unload, the fleet
    // state is Stopped (nobody's problem — not a loss), and
    // the child is gone from the process table.
    let latest = comp.spine().latest("fake").unwrap();
    assert_eq!(latest.kind, "unloaded", "the latest spine event is the unload");
    let state = &comp
        .supervisor()
        .fleet()
        .slot("fake")
        .expect("fake is declared")
        .state;
    assert!(
        matches!(state, Residency::Stopped { .. }),
        "fleet state is Stopped, got {state:?}"
    );
    assert!(
        !Path::new(&format!("/proc/{first_pid}")).exists(),
        "the stopped child no longer exists in the process table"
    );
}
