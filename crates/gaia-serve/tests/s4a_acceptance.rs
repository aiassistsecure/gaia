//! Composition acceptance tests for S-4a — the leash
//! crossing, fork half (G-9 §10.3, §12.4).
//!
//! Honest labels: these are miniature-fleet scenarios in the
//! style of `actuation_acceptance.rs` — same fixtures, same
//! real supervisor / spine / stores, no real model. What is
//! new is the leash feed: SI-journaled stop/resume records
//! reach the composition's leash through the feed's
//! watermark discipline (§10.3.2), heartbeats keep the
//! replica fresh (§10.3.4), and a stale replica refuses
//! runtime-touching actuation with the §12.4 shape while
//! `hearth.notify` — which actuates nothing — stays exempt
//! (§10.3.5).
//!
//! The `LeashFeed` API used here is the as-built one in
//! `leash_feed.rs` (verified against that file):
//! `apply_record(&mut self, leash: &mut Leash, record:
//! LeashRecord) -> Result<ApplyOutcome, String>`,
//! `note_heartbeat(model, leash_tip, journal_tip)`,
//! `set_ttl(Duration)`, `ephemeral()`, and
//! `open(state_path, &Leash)` — activity and watermark
//! survive a reopen, heartbeats do not (receipt stamps are
//! runtime-only, §10.3.4). The default TTL is 30 s, far
//! longer than any test run below.
//!
//! Composition wiring, verified against `lib.rs` as the
//! coordinator landed it: `Composition::set_leash_feed`
//! installs the feed, `actuate_work` passes
//! `Some(&self.leash_feed)` to the actuator,
//! `leash_feed_mut()` returns `&mut LeashFeed`, and
//! `gaia_serve` re-exports `LeashFeed` and `ApplyOutcome`.
//!
//! One adaptation was needed at integration, and it is
//! already landed: `apply_record_comp` below applies via
//! `Composition::apply_leash_record`, because the feed
//! and the leash are both the composition's stores and
//! two mutable accessors cannot be borrowed at once —
//! the composition's apply method borrows its own fields
//! disjointly, which also makes it the one writer path
//! outside the route sink. Nothing else in this file
//! touches that seam.
//!
//! The cases, one test each:
//!
//! 1. A stop applied via the feed refuses an
//!    otherwise-admitted restart (refusal `"leash"`, detail
//!    naming the stop's journal_seq); an explicit resume
//!    record restores actuation.
//! 2. Out-of-order safety at the actuator level: stop@10,
//!    resume@12, stop@15 applied; a delayed duplicate
//!    resume@12 arrives via `apply_record` and is a
//!    `Duplicate` — the admitted restart still refuses.
//! 3. Staleness fails closed: heartbeat beyond TTL refuses
//!    an admitted restart with the §12.4 detail; the
//!    refusal is in the outcome store; notify still
//!    completes under the same stale feed; a fresh
//!    heartbeat (TTL restored first) re-enables restart.
//! 4. Restart-of-fork at feed level: a feed reopened from
//!    persisted state (active for the model, watermark
//!    carried) but with no heartbeat received in this
//!    process refuses an admitted restart with
//!    `NeverReceived` — "no heartbeat received".
//! 5. The route itself: a stop POSTed through
//!    `gaia_api::leash::handle` over the composition's
//!    sink (the agency handler's exact shape) ACKs
//!    `applied: true` with the watermark, a heartbeat
//!    POST keeps the replica fresh, and the stop then
//!    refuses an admitted restart — the endpoint-to-
//!    actuation chain in one test.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use gaia_api::{EventHub, Intent, MemoryWorkStore, MirrorStore, Verdict, WorkState, WorkStore};
use gaia_serve::{
    ApplyOutcome, Composition, Leash, LeashFeed, LeashKind, LeashRecord, Outcome, Proposal,
    ProposalStore, Watcher,
};
use hearth_core::{Budget, Declared, GIB};
use hearth_serve::server::{free_port, ServerSpec};
use hearth_serve::Supervisor;
use hearth_store::{Event, Spine};
use serde_json::{json, Value};

// -- Fleet fixtures (copied from actuation_acceptance.rs) -----

/// The fake serving child: a tiny HTTP server that answers 200
/// with `{}` on any GET, including the supervisor's `/health`
/// probe. (Verbatim from `actuation_acceptance.rs`.)
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

fn event_pid(event: &Event) -> u64 {
    event.facts["pid"]
        .as_u64()
        .expect("the loading event records a pid")
}

/// Wait until the child is actually dead before ticking
/// again — synchronisation, not patience; see the G-5 /
/// actuation_acceptance documentation of the probe race.
fn wait_until_dead(pid: u64) {
    for _ in 0..200 {
        match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            Err(_) => break,
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

struct LossFleet {
    comp: Composition,
    proposal: Proposal,
    lost_seq: u64,
    _dir: tempfile::TempDir,
}

fn loss_fleet() -> LossFleet {
    let dir = tempfile::tempdir().unwrap();
    let mut comp = build_composition(dir.path());
    let spec = fake_spec(dir.path());

    comp.note_spec(spec.clone());
    comp.supervisor().start(spec.clone()).unwrap();
    tick_until_latest_kind(&mut comp, "resident");
    let loadings = loading_events(&comp);
    assert_eq!(loadings.len(), 1, "one start, one loading event");
    let first_pid = event_pid(&loadings[0]);

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
        lost_seq: lost.seq,
        _dir: dir,
    }
}

// -- Intent construction (from actuation_acceptance.rs) -------

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

// -- Feed helpers (the assumption-isolation layer) ------------
// Every call into the assumed LeashFeed / Composition feed
// API lives in this section, so a shape difference at
// integration is a small, localised adaptation.

/// The composition's feed, mutably (always installed:
/// the composition constructs an ephemeral one and
/// `set_leash_feed` replaces it).
fn feed_mut(comp: &mut Composition) -> &mut LeashFeed {
    comp.leash_feed_mut()
}

/// Apply one record through a feed into a leash — the
/// feed's one writer path: the watermark gate decides, and
/// an `Applied` record lands in the leash through the
/// leash's own record functions (no fork-minted records).
fn apply_to(feed: &mut LeashFeed, leash: &mut Leash, record: LeashRecord) -> ApplyOutcome {
    feed.apply_record(leash, record).unwrap()
}

/// `apply_to` for a composition's own feed and leash, via
/// the composition's apply method (the two stores are both
/// the composition's, so it borrows them disjointly in one
/// call — two mutable accessors at once would not compile,
/// and should not: this is the one writer path).
fn apply_record_comp(comp: &mut Composition, record: LeashRecord) -> ApplyOutcome {
    comp.apply_leash_record(record).unwrap()
}

fn record(model: &str, kind: LeashKind, journal_seq: u64, reason: &str) -> LeashRecord {
    LeashRecord {
        model: model.to_string(),
        kind,
        journal_seq,
        reason: reason.to_string(),
    }
}

fn note_heartbeat(comp: &mut Composition, model: &str, leash_tip: u64) {
    // `journal_tip` is provenance only in this slice (no
    // staleness rule reads it); the scenarios pass the
    // leash tip for both, as a heartbeat from a journal
    // whose latest leash-relevant entry is that tip.
    feed_mut(comp).note_heartbeat(model, leash_tip, leash_tip);
}

fn set_ttl(comp: &mut Composition, ttl: Duration) {
    feed_mut(comp).set_ttl(ttl);
}

/// Submit a pre-admitted intent from `proposal` and actuate
/// it, returning the completed work's result JSON.
fn actuate_proposal(
    comp: &mut Composition,
    store: &mut MemoryWorkStore,
    proposal: &Proposal,
    journal_seq: u64,
    task_id: &str,
) -> Value {
    let submitted = store
        .submit(proposal_intent(
            proposal,
            Some(Verdict::Admit),
            journal_seq,
            task_id,
        ))
        .unwrap();
    let done = comp.actuate_work(store, &submitted.id).unwrap();
    assert_eq!(
        done.state,
        WorkState::Executed,
        "an answer completes the work"
    );
    done.result
        .clone()
        .expect("a completed record has a result")
}

// -- Case (1): stop via feed refuses; resume restores ---------

#[test]
fn a_stop_applied_via_the_feed_refuses_restart_and_a_resume_restores_it() {
    let LossFleet {
        mut comp,
        proposal,
        lost_seq,
        _dir,
    } = loss_fleet();
    let mut store = MemoryWorkStore::new();
    comp.set_leash_feed(LeashFeed::ephemeral());

    // A stop journaled at SI at journal seq 501 crosses via
    // the feed into the composition's leash, and a heartbeat
    // carrying that tip keeps the replica fresh — so what
    // speaks below is the stop, not staleness.
    let applied = apply_record_comp(
        &mut comp,
        record(
            "fake",
            LeashKind::Stop,
            501,
            "operator stop, journaled in SI",
        ),
    );
    assert!(matches!(applied, ApplyOutcome::Applied));
    note_heartbeat(&mut comp, "fake", 501);
    assert!(comp.leash().is_stopped("fake"));

    // An otherwise-admitted restart refuses on the leash,
    // and the detail names the stop's journal_seq.
    let result = actuate_proposal(&mut comp, &mut store, &proposal, 2101, "task-s4a-leashed");
    assert_eq!(result["outcome"], json!("refused"));
    assert_eq!(result["refusal"], json!("leash"));
    let detail = result["detail"].as_str().unwrap();
    assert!(
        detail.contains("501"),
        "detail names the stop's journal_seq: {detail}"
    );
    assert!(
        !detail.contains("leash state stale"),
        "a fresh replica refuses on the stop itself, not staleness: {detail}"
    );
    assert_eq!(comp.outcomes().len(), 1, "the refusal is recorded");
    assert_eq!(comp.outcomes()[0].refusal.as_deref(), Some("leash"));
    assert_eq!(comp.spine().latest("fake").unwrap().seq, lost_seq);
    assert_eq!(loading_events(&comp).len(), 1, "nothing restarted");

    // An explicit resume record (journal seq 502) restores
    // actuation: a NEW admitted intent with the same args
    // now executes — the refused work was answered, not
    // queued, so re-asking is a new intent.
    let applied = apply_record_comp(
        &mut comp,
        record(
            "fake",
            LeashKind::Resume,
            502,
            "operator resume, journaled in SI",
        ),
    );
    assert!(matches!(applied, ApplyOutcome::Applied));
    note_heartbeat(&mut comp, "fake", 502);
    assert!(!comp.leash().is_stopped("fake"));

    let result = actuate_proposal(&mut comp, &mut store, &proposal, 2102, "task-s4a-resumed");
    assert_eq!(result["outcome"], json!("executed"));

    tick_until_latest_kind(&mut comp, "resident");
    assert_eq!(
        loading_events(&comp).len(),
        2,
        "the post-resume intent restarted the model"
    );
}

// -- Case (2): out-of-order duplicate cannot lift a stop ------

#[test]
fn a_delayed_duplicate_resume_does_not_lift_a_newer_stop() {
    let LossFleet {
        mut comp,
        proposal,
        _dir,
        ..
    } = loss_fleet();
    let mut store = MemoryWorkStore::new();
    comp.set_leash_feed(LeashFeed::ephemeral());

    // The feed applies, in journal order: stop@10,
    // resume@12, stop@15. The watermark is now 15 and the
    // stop@15 is in force.
    for (kind, seq) in [
        (LeashKind::Stop, 10),
        (LeashKind::Resume, 12),
        (LeashKind::Stop, 15),
    ] {
        let applied = apply_record_comp(&mut comp, record("fake", kind, seq, "seam leash record"));
        assert!(
            matches!(applied, ApplyOutcome::Applied),
            "record @{seq} applies in order"
        );
    }
    note_heartbeat(&mut comp, "fake", 15);
    assert!(comp.leash().is_stopped("fake"));

    // A delayed duplicate of resume@12 arrives after the
    // newer stop. Under latest-arrival-wins it would
    // silently lift the stop; under the watermark it is a
    // Duplicate — ACKed, not applied (§10.3.2).
    let outcome = apply_record_comp(
        &mut comp,
        record("fake", LeashKind::Resume, 12, "delayed duplicate resume"),
    );
    assert!(
        matches!(outcome, ApplyOutcome::Duplicate),
        "the delayed resume@12 is a duplicate under the watermark, got {outcome:?}"
    );
    assert!(
        comp.leash().is_stopped("fake"),
        "the stop@15 is still in force after the duplicate"
    );
    assert_eq!(
        comp.leash().stop_in_force("fake").unwrap().journal_seq,
        15,
        "the stop in force is still the @15 stop"
    );

    // And the actuator agrees: an admitted restart still
    // refuses on the leash, naming the @15 stop.
    let result = actuate_proposal(
        &mut comp,
        &mut store,
        &proposal,
        2201,
        "task-s4a-out-of-order",
    );
    assert_eq!(result["outcome"], json!("refused"));
    assert_eq!(result["refusal"], json!("leash"));
    assert!(
        result["detail"].as_str().unwrap().contains("15"),
        "detail names the stop@15 journal_seq: {result}"
    );
    assert_eq!(loading_events(&comp).len(), 1, "nothing restarted");
}

// -- Case (3): staleness fails closed; notify exempt ----------

#[test]
fn stale_leash_state_refuses_restart_but_not_notify_and_a_fresh_heartbeat_restores() {
    let LossFleet {
        mut comp,
        proposal,
        _dir,
        ..
    } = loss_fleet();
    let mut store = MemoryWorkStore::new();
    comp.set_leash_feed(LeashFeed::ephemeral());

    // The feed is active for the model: a resume@1 applied
    // (no stop in force) and a heartbeat noted.
    let applied = apply_record_comp(
        &mut comp,
        record("fake", LeashKind::Resume, 1, "seam activation resume"),
    );
    assert!(matches!(applied, ApplyOutcome::Applied));
    note_heartbeat(&mut comp, "fake", 1);

    // TTL 0: the heartbeat is immediately beyond TTL. The
    // staleness rule is elapsed > ttl, so with TTL 0 any
    // real elapsed qualifies — the 1 ms sleep below is the
    // guarantee that some real time has elapsed since the
    // heartbeat was noted, not a timing assumption about
    // the system under test. (The only ad-hoc sleep in this
    // file; the fixture tick loops above are bounded
    // synchronisation, as in actuation_acceptance.rs.)
    set_ttl(&mut comp, Duration::ZERO);
    std::thread::sleep(Duration::from_millis(1));

    // An admitted restart refuses, fail-closed, with the
    // §12.4 shape: refusal "leash", detail naming the
    // staleness and the watermark, no spine refs.
    let result = actuate_proposal(&mut comp, &mut store, &proposal, 2301, "task-s4a-stale");
    assert_eq!(result["outcome"], json!("refused"));
    assert_eq!(result["refusal"], json!("leash"));
    let detail = result["detail"].as_str().unwrap();
    assert!(
        detail.contains("leash state stale"),
        "detail names the staleness: {detail}"
    );
    assert!(
        detail.contains("watermark 1"),
        "detail carries the watermark: {detail}"
    );
    assert_eq!(result["spine_event_refs"], json!([]));

    // The refusal is present in the outcome store.
    assert_eq!(comp.outcomes().len(), 1, "the refusal is recorded");
    let refusal = &comp.outcomes()[0];
    assert!(matches!(refusal.outcome, Outcome::Refused));
    assert_eq!(refusal.refusal.as_deref(), Some("leash"));
    assert!(refusal.detail.contains("leash state stale"));
    assert_eq!(loading_events(&comp).len(), 1, "nothing restarted");

    // hearth.notify for the same model still completes
    // under the same stale feed (§10.3.5): it actuates
    // nothing, so neither the leash nor staleness applies.
    let lost = comp.spine().latest("fake").expect("a latest event");
    let notify = store
        .submit(plain_intent(
            "hearth.notify",
            json!({
                "model": "fake",
                "spine_event": { "hash": lost.hash, "seq": lost.seq },
            }),
            "operator notification about the loss",
            Some(Verdict::Admit),
            2302,
            "task-s4a-stale-notify",
        ))
        .unwrap();
    let done = comp.actuate_work(&mut store, &notify.id).unwrap();
    let result = done
        .result
        .clone()
        .expect("a completed record has a result");
    assert_eq!(
        result["outcome"],
        json!("executed"),
        "notify completes under stale leash state: {result}"
    );
    assert_eq!(
        comp.outcomes().len(),
        2,
        "the notify outcome is recorded too"
    );

    // Recovery: restore a large TTL BEFORE re-noting the
    // heartbeat (order matters — a heartbeat noted while
    // TTL is still 0 is stale again by the next actuation),
    // then note a fresh heartbeat. An admitted restart now
    // executes: the model is still lost (notify actuated
    // nothing), so the stale-state check still answers
    // this same loss.
    set_ttl(&mut comp, Duration::from_secs(3600));
    note_heartbeat(&mut comp, "fake", 1);
    let result = actuate_proposal(
        &mut comp,
        &mut store,
        &proposal,
        2303,
        "task-s4a-fresh-again",
    );
    assert_eq!(result["outcome"], json!("executed"));

    tick_until_latest_kind(&mut comp, "resident");
    assert_eq!(
        loading_events(&comp).len(),
        2,
        "the post-heartbeat intent restarted the model"
    );
}

// -- Case (4): restart-of-fork — active, but no heartbeat -----

#[test]
fn a_reopened_feed_with_no_heartbeat_in_this_process_refuses_never_received() {
    // A previous process's leash + feed state, on disk.
    let restart_dir = tempfile::tempdir().unwrap();
    let leash_path = restart_dir.path().join("leash.jsonl");
    let state_path = restart_dir.path().join("leash-feed.json");

    // The previous feed: built over its leash, a resume@1
    // applied (the model is active, watermark 1, no stop in
    // force) and a heartbeat received — in THAT process.
    {
        let mut prev_leash = Leash::open(&leash_path).unwrap();
        let mut prev_feed = LeashFeed::open(&state_path, &prev_leash).unwrap();
        let applied = apply_to(
            &mut prev_feed,
            &mut prev_leash,
            record("fake", LeashKind::Resume, 1, "seam activation resume"),
        );
        assert!(matches!(applied, ApplyOutcome::Applied));
        prev_feed.note_heartbeat("fake", 1, 1);
    }

    let LossFleet {
        mut comp,
        proposal,
        _dir,
        ..
    } = loss_fleet();
    let mut store = MemoryWorkStore::new();

    // The fork restarts: the leash reopens from its file and
    // the feed reopens from its persisted state — active for
    // the model, watermark carried — but no heartbeat has
    // been received in THIS process, and a heartbeat receipt
    // time (a local monotonic instant) is exactly what
    // cannot survive a restart.
    let reopened_leash = Leash::open(&leash_path).unwrap();
    assert!(
        !reopened_leash.is_stopped("fake"),
        "no stop was ever in force"
    );
    let feed = LeashFeed::open(&state_path, &reopened_leash).unwrap();
    comp.set_leash(reopened_leash);
    comp.set_leash_feed(feed);

    // An admitted restart refuses with NeverReceived: the
    // detail is the §12.4 staleness shape and says no
    // heartbeat was received (§13.2: a restarted fork
    // actuates nothing until SI has spoken).
    let result = actuate_proposal(&mut comp, &mut store, &proposal, 2401, "task-s4a-restarted");
    assert_eq!(result["outcome"], json!("refused"));
    assert_eq!(result["refusal"], json!("leash"));
    let detail = result["detail"].as_str().unwrap();
    assert!(
        detail.contains("leash state stale"),
        "detail names the staleness: {detail}"
    );
    assert!(
        detail.contains("no heartbeat received"),
        "detail names the NeverReceived cause: {detail}"
    );
    assert_eq!(comp.outcomes().len(), 1, "the refusal is recorded");
    assert_eq!(comp.outcomes()[0].refusal.as_deref(), Some("leash"));
    assert_eq!(loading_events(&comp).len(), 1, "nothing restarted");
}

// -- Case (5): the route itself, through the sink -----------

#[test]
fn the_leash_route_through_the_composition_sink_gates_actuation() {
    let LossFleet {
        mut comp,
        proposal,
        _dir,
        ..
    } = loss_fleet();
    let mut store = MemoryWorkStore::new();
    comp.set_leash_feed(LeashFeed::ephemeral());

    // A stop crosses as an actual route call — the agency
    // handler's shape, `gaia_api::leash::handle` over the
    // composition's sink — not a direct store call.
    let req = hearth_api::Request {
        method: "POST".to_string(),
        path: "/gaia/v1/leash".to_string(),
        headers: std::collections::HashMap::new(),
        body: json!({
            "model": "fake",
            "kind": "stop",
            "journal_seq": 777,
            "reason": "operator stop, journaled in SI",
        })
        .to_string(),
    };
    let resp = {
        let mut sink = comp.leash_sink();
        gaia_api::leash::handle(&req, &mut sink)
    }
    .expect("the leash route answers");
    assert_eq!(resp.status, 200);
    let body: Value = serde_json::from_str(&resp.body).unwrap();
    assert_eq!(body["applied"], json!(true));
    assert_eq!(body["last_leash_seq"], json!(777));

    // A heartbeat over the same route keeps the replica
    // fresh, so the refusal below is the stop speaking.
    let hb = hearth_api::Request {
        method: "POST".to_string(),
        path: "/gaia/v1/leash/heartbeat".to_string(),
        headers: std::collections::HashMap::new(),
        body: json!({ "model": "fake", "leash_tip": 777, "journal_tip": 780 }).to_string(),
    };
    let resp = {
        let mut sink = comp.leash_sink();
        gaia_api::leash::handle(&hb, &mut sink)
    }
    .expect("the heartbeat route answers");
    assert_eq!(resp.status, 200);

    let result = actuate_proposal(&mut comp, &mut store, &proposal, 2501, "task-s4a-route");
    assert_eq!(result["outcome"], json!("refused"));
    assert_eq!(result["refusal"], json!("leash"));
    let detail = result["detail"].as_str().unwrap();
    assert!(
        detail.contains("777"),
        "the detail names the stop's journal_seq: {detail}"
    );
    assert!(
        !detail.contains("leash state stale"),
        "the stop speaks, not staleness: {detail}"
    );
}
