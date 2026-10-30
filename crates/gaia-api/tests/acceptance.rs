//! Acceptance tests for the G-3 `gaia-api` skeleton.
//!
//! These map one-to-one onto the G-3 acceptance criteria in
//! `docs/G-2-agency-design.md` §7 (and the route table / error
//! vocabulary rules in §3.2):
//!
//!   * a submitted pre-admitted intent returns 202 + a stable
//!     `work_id`, and its status round-trips;
//!   * an escalated intent cannot reach the actuation trait
//!     without a recorded verdict;
//!   * verdict conflicts (double verdict, verdict after
//!     execution) return 409;
//!   * no `/gaia/*` route produces a `model_*` error type and no
//!     inference route produces a `gaia_*` type — the two error
//!     vocabularies stay disjoint.
//!
//! Everything is exercised through the crate's pure decision
//! surface (`classify` / `decide` / `run_actuation`) against an
//! in-memory work store: no sockets, no server.

use std::collections::HashMap;

use gaia_api::work::run_actuation;
use gaia_api::{
    classify, decide, Actuator, GaiaError, MemoryWorkStore, Route, WorkRecord,
};
use hearth_api::{Decision, Endpoint, Request, Response};
use hearth_core::budget::{Budget, GIB};
use hearth_core::fleet::Fleet;
use serde_json::{json, Value};

// -- helpers ---------------------------------------------------------------

fn req(method: &str, path: &str, body: &str) -> Request {
    Request {
        method: method.to_string(),
        path: path.to_string(),
        headers: HashMap::new(),
        body: body.to_string(),
    }
}

fn body_json(resp: &Response) -> Value {
    serde_json::from_str(&resp.body)
        .unwrap_or_else(|e| panic!("response body is not JSON ({e}): {}", resp.body))
}

fn error_type(body: &Value) -> &str {
    body["error"]["type"]
        .as_str()
        .unwrap_or_else(|| panic!("error body has no string error.type: {body}"))
}

fn work_path(work_id: &str) -> String {
    format!("/gaia/v1/work/{work_id}")
}

fn verdict_path(work_id: &str) -> String {
    format!("/gaia/v1/work/{work_id}/verdict")
}

/// A tool-intent submission body in the §3.2 shape. `verdict` and
/// `class` are optional on the wire: omitting `verdict` is the
/// escalated configuration, omitting `class` exercises the
/// defaulting path.
fn submission_body(verdict: Option<&str>, class: Option<&str>) -> String {
    let mut body = json!({
        "type": "tool",
        "tool": "fs.read",
        "args": { "path": "/tmp/acceptance-fixture" },
        "reason": "G-3 acceptance test",
        "task_id": "task-1",
        "journal_seq": 42,
        "class": "read",
    });
    if let Some(class) = class {
        body["class"] = json!(class);
    }
    if let Some(verdict) = verdict {
        body["verdict"] = json!(verdict);
    }
    body.to_string()
}

/// Submit and return the recorded `work_id`, asserting the 202
/// acknowledgement every submission configuration shares.
fn submit(store: &mut MemoryWorkStore, verdict: Option<&str>) -> String {
    let resp = decide(
        &req("POST", "/gaia/v1/work", &submission_body(verdict, None)),
        store,
    );
    assert_eq!(resp.status, 202, "submission body: {}", resp.body);
    let body = body_json(&resp);
    assert_eq!(body["state"], json!("accepted"));
    let work_id = body["work_id"]
        .as_str()
        .unwrap_or_else(|| panic!("202 body has no string work_id: {body}"));
    assert!(!work_id.is_empty(), "work_id must not be empty");
    work_id.to_string()
}

fn status_of(store: &mut MemoryWorkStore, work_id: &str) -> Value {
    let resp = decide(&req("GET", &work_path(work_id), ""), store);
    assert_eq!(resp.status, 200, "status body: {}", resp.body);
    body_json(&resp)
}

/// An actuator that only counts: if an escalated intent could
/// reach actuation without a verdict, this counter moves and the
/// test that owns it fails.
struct CountingActuator {
    calls: usize,
}

impl CountingActuator {
    fn new() -> CountingActuator {
        CountingActuator { calls: 0 }
    }
}

impl Actuator for CountingActuator {
    fn actuate(&mut self, _work: &WorkRecord) -> Result<Value, GaiaError> {
        self.calls += 1;
        Ok(json!({ "done": true }))
    }
}

// -- (2) pre-admitted submission: 202 + status round-trip -------------------

#[test]
fn pre_admitted_submission_returns_202_and_status_round_trips() {
    let mut store = MemoryWorkStore::new();

    let resp = decide(
        &req(
            "POST",
            "/gaia/v1/work",
            &submission_body(Some("admit"), None),
        ),
        &mut store,
    );
    assert_eq!(resp.status, 202, "submission body: {}", resp.body);
    let ack = body_json(&resp);
    assert_eq!(ack["state"], json!("accepted"));
    assert_eq!(ack["journal_seq"], json!(42));
    let work_id = ack["work_id"]
        .as_str()
        .unwrap_or_else(|| panic!("202 body has no string work_id: {ack}"));
    assert!(!work_id.is_empty(), "work_id must not be empty");
    let work_id = work_id.to_string();

    let first = status_of(&mut store, &work_id);
    assert_eq!(first["work_id"], json!(work_id));
    assert_eq!(
        first["state"],
        json!("admitted"),
        "a pre-admitted intent is admitted, not awaiting a verdict: {first}"
    );
    assert_eq!(
        first["provenance"]["si_journal_seqs"],
        json!([42]),
        "provenance must carry the SI journal seq the intent was journaled at"
    );
    assert!(
        first["provenance"]["spine_event_refs"].is_array(),
        "provenance must carry spine event refs (possibly empty): {first}"
    );

    // The status answer is stable: a second read of the same work
    // reports the identical body — same id, same state, same
    // provenance.
    let second = status_of(&mut store, &work_id);
    assert_eq!(first, second, "status must round-trip unchanged");
}

// -- (3) escalated intent: no actuation without a recorded verdict ----------

#[test]
fn escalated_intent_cannot_actuate_without_a_recorded_verdict() {
    let mut store = MemoryWorkStore::new();
    let mut actuator = CountingActuator::new();

    // Escalated: submitted with no verdict attached.
    let work_id = submit(&mut store, None);

    let awaiting = status_of(&mut store, &work_id);
    assert_eq!(awaiting["state"], json!("awaiting_verdict"));

    // The actuation attempt itself is the proof: it must fail and
    // the actuator must never be invoked. The fork holds the work
    // and waits; it never manufactures a verdict.
    let attempted = run_actuation(&mut store, &mut actuator, &work_id);
    assert!(
        attempted.is_err(),
        "actuation without a recorded verdict must be refused"
    );
    assert_eq!(
        actuator.calls, 0,
        "the actuator must not run for an unverdicted intent"
    );
    assert_eq!(
        status_of(&mut store, &work_id)["state"],
        json!("awaiting_verdict"),
        "a refused actuation must not move the work's state"
    );

    // Deliver the gate's verdict on the return path.
    let resp = decide(
        &req(
            "POST",
            &verdict_path(&work_id),
            &json!({ "verdict": "admit" }).to_string(),
        ),
        &mut store,
    );
    assert_eq!(resp.status, 200, "verdict body: {}", resp.body);
    assert_eq!(body_json(&resp)["state"], json!("admitted"));

    // Now — and only now — actuation runs, exactly once.
    match run_actuation(&mut store, &mut actuator, &work_id) {
        Ok(_) => {}
        Err(_) => panic!("actuation after an admit verdict must succeed"),
    }
    assert_eq!(actuator.calls, 1);

    let executed = status_of(&mut store, &work_id);
    assert_eq!(executed["state"], json!("executed"));
    assert_eq!(
        executed["result"]["done"],
        json!(true),
        "the status answer carries the actuator's result: {executed}"
    );
}

// -- (4) verdict conflicts return 409 ----------------------------------------

#[test]
fn verdict_conflicts_return_409() {
    // (a) Double verdict: once a verdict is recorded, a second one
    // — even a contradictory one — conflicts.
    let mut store = MemoryWorkStore::new();
    let work_id = submit(&mut store, None);

    let resp = decide(
        &req(
            "POST",
            &verdict_path(&work_id),
            &json!({ "verdict": "admit" }).to_string(),
        ),
        &mut store,
    );
    assert_eq!(resp.status, 200, "first verdict body: {}", resp.body);

    let resp = decide(
        &req(
            "POST",
            &verdict_path(&work_id),
            &json!({ "verdict": "reject" }).to_string(),
        ),
        &mut store,
    );
    assert_eq!(resp.status, 409, "double verdict body: {}", resp.body);
    assert_eq!(
        error_type(&body_json(&resp)),
        "gaia_verdict_conflict"
    );

    // (b) Verdict after execution: the work has both a verdict and
    // an outcome; any further verdict conflicts.
    let mut store = MemoryWorkStore::new();
    let mut actuator = CountingActuator::new();
    let work_id = submit(&mut store, None);

    let resp = decide(
        &req(
            "POST",
            &verdict_path(&work_id),
            &json!({ "verdict": "admit" }).to_string(),
        ),
        &mut store,
    );
    assert_eq!(resp.status, 200, "verdict body: {}", resp.body);
    match run_actuation(&mut store, &mut actuator, &work_id) {
        Ok(_) => {}
        Err(_) => panic!("actuation after an admit verdict must succeed"),
    }
    assert_eq!(actuator.calls, 1);
    assert_eq!(
        status_of(&mut store, &work_id)["state"],
        json!("executed")
    );

    let resp = decide(
        &req(
            "POST",
            &verdict_path(&work_id),
            &json!({ "verdict": "reject" }).to_string(),
        ),
        &mut store,
    );
    assert_eq!(
        resp.status, 409,
        "verdict-after-execution body: {}",
        resp.body
    );
    assert_eq!(
        error_type(&body_json(&resp)),
        "gaia_verdict_conflict"
    );
}

// -- (5) the two error vocabularies stay disjoint ----------------------------

#[test]
fn route_table_error_vocabularies_stay_disjoint() {
    // Gaia side: a table of agency requests that must fail, each
    // with its own `gaia_*` error type — and never a `model_*`
    // type, which belongs to the inference vocabulary alone.
    let mut store = MemoryWorkStore::new();
    let escalated_id = submit(&mut store, None);

    let gaia_errors: Vec<(&str, Response)> = vec![
        (
            "unknown route",
            decide(&req("GET", "/gaia/v1/nope", ""), &mut store),
        ),
        (
            "unknown work id",
            decide(&req("GET", "/gaia/v1/work/work-999999", ""), &mut store),
        ),
        (
            "malformed submission",
            decide(&req("POST", "/gaia/v1/work", "not json"), &mut store),
        ),
        (
            "malformed verdict value",
            decide(
                &req(
                    "POST",
                    &verdict_path(&escalated_id),
                    &json!({ "verdict": "maybe" }).to_string(),
                ),
                &mut store,
            ),
        ),
        (
            "class not carried",
            decide(
                &req(
                    "POST",
                    "/gaia/v1/work",
                    &submission_body(Some("admit"), Some("external")),
                ),
                &mut store,
            ),
        ),
    ];
    for (case, resp) in &gaia_errors {
        assert!(
            resp.status >= 400,
            "{case}: expected an error status, got {} ({})",
            resp.status,
            resp.body
        );
        let body = body_json(resp);
        let kind = error_type(&body);
        assert!(
            kind.starts_with("gaia_"),
            "{case}: agency errors use gaia_* types, got {kind}"
        );
        assert!(
            !kind.contains("model_"),
            "{case}: no /gaia/* route may produce a model_* type, got {kind}"
        );
    }

    // A success on the agency surface carries no error at all.
    let health = decide(&req("GET", "/gaia/v1/health", ""), &mut store);
    assert_eq!(health.status, 200);
    assert!(
        body_json(&health).get("error").is_none(),
        "health success must not carry an error envelope"
    );

    // Hearth side: the inference family's routes over an empty
    // fleet must never speak `gaia_*` — the vocabularies are
    // disjoint in both directions.
    let fleet = Fleet::declare(Budget::with_reserve_pct(48 * GIB, 8), vec![]);
    let hearth_cases: [(&str, &str); 4] = [
        ("/health", ""),
        ("/v1/models", ""),
        ("/v1/chat/completions", r#"{"model":"ghost"}"#),
        ("/nope", ""),
    ];
    for (path, request_body) in hearth_cases {
        match hearth_api::gateway::decide(path, request_body, &fleet, 0) {
            Decision::Answer(resp) => {
                if let Some(kind) = body_json(&resp)["error"]["type"].as_str() {
                    assert!(
                        !kind.starts_with("gaia_"),
                        "{path}: inference routes never produce gaia_* types, got {kind}"
                    );
                }
            }
            // An empty fleet should answer everything itself, but a
            // proxy decision carries no error body to check.
            Decision::Proxy { .. } => {}
        }
    }

    // Classify level: each surface's classifier is blind to the
    // other's routes.
    assert!(
        matches!(classify("GET", "/v1/models"), Route::Unknown),
        "the gaia classifier must not claim an inference route"
    );
    assert_eq!(
        hearth_api::gateway::classify("/gaia/v1/work"),
        Endpoint::Unknown,
        "the hearth classifier must not claim an agency route"
    );
}

// -- §3.2: a class the fork is not configured to carry is refused ------------

#[test]
fn external_class_submission_is_refused_409() {
    let mut store = MemoryWorkStore::new();

    // Even fully pre-admitted, an `external`-class intent is one
    // the fork is configured never to carry while unconfigured.
    let resp = decide(
        &req(
            "POST",
            "/gaia/v1/work",
            &submission_body(Some("admit"), Some("external")),
        ),
        &mut store,
    );
    assert_eq!(resp.status, 409, "external class body: {}", resp.body);
    assert_eq!(
        error_type(&body_json(&resp)),
        "gaia_class_not_carried"
    );
}

// -- §3.2 route table: the agency health route --------------------------------

#[test]
fn health_route_answers_as_gaia() {
    let mut store = MemoryWorkStore::new();

    let resp = decide(&req("GET", "/gaia/v1/health", ""), &mut store);
    assert_eq!(resp.status, 200, "health body: {}", resp.body);
    let body = body_json(&resp);
    assert_eq!(body["status"], json!("ok"));
    assert_eq!(
        body["service"],
        json!("gaia"),
        "the agency health route answers as gaia, not hearth"
    );
}
